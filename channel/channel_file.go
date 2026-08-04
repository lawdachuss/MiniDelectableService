package channel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

var pendingDirMu sync.Map // map[string]*sync.Mutex, keyed by channel username

// pendingMu returns the per-channel mutex for the given username.
// Each channel has its own lock so one channel's ffmpeg encode does not
// block another channel's pending directory operations.
func pendingMu(username string) *sync.Mutex {
	v, _ := pendingDirMu.LoadOrStore(username, &sync.Mutex{})
	return v.(*sync.Mutex)
}

type Pattern struct {
	Username string
	Sequence int
	Year     string
	Month    string
	Day      string
	Hour     string
	Minute   string
	Second   string
}

// CloseMode controls whether Cleanup processes pending files immediately
// or defers processing for later (batched session stop).
type CloseMode int

const (
	CloseProcess CloseMode = iota // close files + process pending (rotation, pause, stream error)
	CloseQueue                    // close files only, defer processing to ProcessPending (session stop)
)

// NextFile prepares the next file to be created, by cleaning up the last file
// and generating a new one. ext is the file extension to use (e.g. ".ts" or ".mp4").
func (ch *Channel) NextFile(ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	if err := ch.cleanupLocked(); err != nil {
		return err
	}
	filename, err := ch.generateFilenameLocked()
	if err != nil {
		return err
	}
	if err := ch.createNewFileLocked(filename, ext); err != nil {
		return err
	}

	// Increment the sequence number for the next file.
	ch.Sequence++
	return nil
}

// Cleanup closes any open recording files and queues them for post-processing.
// CloseProcess also starts processing the queue immediately (rotation, pause,
// stream error); CloseQueue defers processing to ProcessPending (session stop).
func (ch *Channel) Cleanup(mode CloseMode) error {
	ch.fileMu.Lock()
	err := ch.cleanupLocked()
	ch.fileMu.Unlock()
	if err != nil {
		return err
	}

	if mode == CloseProcess {
		ch.flushPending()
	}
	return nil
}

// flushPending starts async post-processing of all currently queued pending files.
func (ch *Channel) flushPending() {
	ch.cleanupMu.Lock()
	files := ch.pendingFiles
	ch.pendingFiles = nil
	ch.cleanupMu.Unlock()

	if len(files) == 0 {
		return
	}
	ch.Info("cleanup: processing %d pending file(s)", len(files))
	ch.pendingWg.Add(1)
	go func() {
		defer ch.pendingWg.Done()
		for _, pf := range files {
			ch.processPendingFile(pf)
		}
	}()
}

// processPendingQueue processes all pending files synchronously.
// Caller must hold cleanupMu (see ProcessPending in channel.go).
func (ch *Channel) processPendingQueue() {
	if len(ch.pendingFiles) == 0 {
		return
	}
	ch.Info("cleanup: processing %d pending file(s)", len(ch.pendingFiles))
	files := ch.pendingFiles
	ch.pendingFiles = nil
	for _, pf := range files {
		ch.processPendingFile(pf)
	}
}

// processPendingFile finalizes a closed recording (seek index or ffmpeg),
// optionally relocates it to the completed dir, and routes it through the
// output pipeline (preview → upload → metadata → cleanup).
func (ch *Channel) processPendingFile(pf pendingFile) {
	videoPath := pf.videoPath
	if _, err := os.Stat(videoPath); err != nil {
		return
	}

	// goondvr-style finalization: BuildSeekIndex or ffmpeg remux/transcode.
	finalPath, err := ch.finalizeRecordingFile(videoPath)
	if err != nil {
		ch.Error("finalize %s: %s — keeping original recording", filepath.Base(videoPath), err.Error())
		finalPath = videoPath
	} else if finalPath != videoPath {
		// The finalizer produced a new file (e.g. .ts -> .mp4); drop the original.
		if rmErr := os.Remove(videoPath); rmErr != nil && !os.IsNotExist(rmErr) {
			ch.Error("remove original after finalization `%s`: %s", filepath.Base(videoPath), rmErr.Error())
		}
	}

	if _, err := os.Stat(finalPath); err != nil {
		return
	}

	// If no output dir is configured, recordings can be relocated to the
	// completed dir (goondvr semantics) before the pipeline runs in place.
	if server.Config.OutputDir == "" && server.Config.CompletedDir != "" {
		if dst, err := moveRecordingToDir(finalPath, recordingDirFromPattern(ch.Config.Pattern), server.Config.CompletedDir); err != nil {
			ch.Error("move completed recording `%s`: %s", finalPath, err.Error())
		} else {
			finalPath = dst
		}
	}

	if ch.Config.Compress {
		if !pf.skipMinDuration && ch.handleMinDurationAndMerge(finalPath) {
			return
		}
		ch.CompressFile(finalPath)
	} else if !pf.skipMinDuration && ch.handleMinDurationAndMerge(finalPath) {
		return
	} else {
		ch.MoveToOutputDir(finalPath)
	}

	// Refresh the disk-usage counter after finalization changes file sizes.
	go ch.ScanTotalDiskUsage()
}

// finalizeRecordingFile post-processes a closed recording according to the
// configured FinalizeMode:
//   - "none":      only build the in-place seek index for fMP4 files
//   - "remux":     ffmpeg stream-copy remux (+faststart)
//   - "transcode": ffmpeg re-encode with the configured encoder/quality
func (ch *Channel) finalizeRecordingFile(filename string) (string, error) {
	if server.Config.FinalizeMode == "none" {
		if strings.HasSuffix(filename, ".mp4") {
			if err := chaturbate.BuildSeekIndex(filename); err != nil {
				ch.Error("seek index %s: %v", filename, err)
			}
		}
		return filename, nil
	}
	return ch.runFFmpegFinalizer(filename)
}

// cleanupLocked closes the active recording file, removes empty files, and
// queues non-empty files for post-processing. Callers must hold fileMu.
func (ch *Channel) cleanupLocked() error {
	if ch.File == nil {
		return nil
	}
	filename := ch.File.Name()

	defer func() {
		ch.Filesize = 0
		ch.Duration = 0
	}()

	// Sync the file to ensure data is written to disk.
	if err := ch.File.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("sync file: %w", err)
	}
	if err := ch.File.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("close file: %w", err)
	}
	ch.File = nil
	ch.CurrentFilename = ""

	// Delete the empty file.
	fileInfo, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat file delete zero file: %w", err)
	}
	if fileInfo != nil && fileInfo.Size() == 0 {
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("remove zero file: %w", err)
		}
		go ch.ScanTotalDiskUsage()
	} else if fileInfo != nil {
		ch.cleanupMu.Lock()
		ch.pendingFiles = append(ch.pendingFiles, pendingFile{
			videoPath:       filename,
			skipMinDuration: ch.Config.IsPaused.Load(),
		})
		ch.cleanupMu.Unlock()
		ch.Info("cleanup: queued %s for post-processing", filepath.Base(filename))
	}

	return nil
}

// GenerateFilename creates a filename based on the configured pattern and the current timestamp.
func (ch *Channel) GenerateFilename() (string, error) {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.generateFilenameLocked()
}

func (ch *Channel) generateFilenameLocked() (string, error) {
	var buf bytes.Buffer

	// Parse the filename pattern defined in the channel's config.
	tpl, err := template.New("filename").Parse(ch.Config.Pattern)
	if err != nil {
		return "", fmt.Errorf("filename pattern error: %w", err)
	}

	// Get the current time based on the Unix timestamp when the stream was started.
	t := time.Unix(ch.StreamedAt, 0)
	pattern := &Pattern{
		Username: ch.Config.Username,
		Sequence: ch.Sequence,
		Year:     t.Format("2006"),
		Month:    t.Format("01"),
		Day:      t.Format("02"),
		Hour:     t.Format("15"),
		Minute:   t.Format("04"),
		Second:   t.Format("05"),
	}

	if err := tpl.Execute(&buf, pattern); err != nil {
		return "", fmt.Errorf("template execution error: %w", err)
	}
	return buf.String(), nil
}

// CreateNewFile creates a new file for the channel using the given filename and extension.
func (ch *Channel) CreateNewFile(filename, ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	return ch.createNewFileLocked(filename, ext)
}

func (ch *Channel) createNewFileLocked(filename, ext string) error {
	// Ensure the directory exists before creating the file.
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return fmt.Errorf("mkdir all: %w", err)
	}

	// Open the file in append mode, create it if it doesn't exist.
	file, err := os.OpenFile(filename+ext, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return fmt.Errorf("cannot open file: %s: %w", filename, err)
	}

	ch.File = file
	ch.CurrentFilename = filename
	return nil
}

// recordingDirFromPattern extracts the base directory from a filename pattern
// like "videos/{{.Username}}_..._..." → "videos".
func recordingDirFromPattern(pattern string) string {
	idx := strings.Index(pattern, "{{")
	if idx == -1 {
		return "."
	}
	dir := filepath.Dir(pattern[:idx])
	if dir == "" || dir == "." {
		return "."
	}
	return dir
}

func finalOutputExt(filename string) string {
	if server.Config.FFmpegContainer == "mkv" {
		return ".mkv"
	}
	if server.Config.FinalizeMode == "none" {
		return filepath.Ext(filename)
	}
	return ".mp4"
}

func finalOutputPath(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	return base + finalOutputExt(filename)
}

// ScanTotalDiskUsage calculates the total bytes of all recordings for this
// channel by walking the recording directory for files whose name starts with
// the username. The result is stored in TotalDiskUsageBytes.
func (ch *Channel) ScanTotalDiskUsage() {
	recordingDir := filepath.Clean(recordingDirFromPattern(ch.Config.Pattern))
	prefix := ch.Config.Username
	var total int64
	_ = filepath.WalkDir(recordingDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(filepath.Base(path), prefix) {
			if info, err2 := d.Info(); err2 == nil {
				total += info.Size()
			}
		}
		return nil
	})
	ch.fileMu.Lock()
	ch.TotalDiskUsageBytes = total
	ch.fileMu.Unlock()
}

// ShouldSwitchFile determines whether a new file should be created.
func (ch *Channel) ShouldSwitchFile() bool {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.shouldSwitchFileLocked()
}

func (ch *Channel) shouldSwitchFileLocked() bool {
	maxFilesizeBytes := ch.Config.MaxFilesize * 1024 * 1024
	maxDurationSeconds := ch.Config.MaxDuration * 60

	return (ch.Duration >= float64(maxDurationSeconds) && ch.Config.MaxDuration > 0) ||
		(ch.Filesize >= maxFilesizeBytes && ch.Config.MaxFilesize > 0)
}

// isMP4InitSegment reports whether b looks like an fMP4 init segment containing
// top-level ftyp/moov boxes and no media fragments yet.
func isMP4InitSegment(b []byte) bool {
	var hasFtyp bool
	var hasMoov bool

	for pos := 0; pos+8 <= len(b); {
		size := int(uint32(b[pos])<<24 | uint32(b[pos+1])<<16 | uint32(b[pos+2])<<8 | uint32(b[pos+3]))
		if size < 8 || pos+size > len(b) {
			return false
		}

		switch string(b[pos+4 : pos+8]) {
		case "ftyp":
			hasFtyp = true
		case "moov":
			hasMoov = true
		case "moof", "mdat", "mfra":
			return false
		}
		pos += size
	}

	return hasFtyp && hasMoov
}

func moveRecordingToDir(src, recordingRoot, completedDir string) (string, error) {
	dstDir := completedDir

	srcDir := filepath.Dir(src)
	cleanRoot := filepath.Clean(recordingRoot)
	cleanSrcDir := filepath.Clean(srcDir)
	if relDir, err := filepath.Rel(cleanRoot, cleanSrcDir); err == nil && relDir != ".." && !strings.HasPrefix(relDir, ".."+string(os.PathSeparator)) {
		if relDir != "." {
			dstDir = filepath.Join(completedDir, relDir)
		}
	}

	if err := os.MkdirAll(dstDir, 0777); err != nil {
		return "", fmt.Errorf("mkdir completed dir: %w", err)
	}

	dst := filepath.Join(dstDir, filepath.Base(src))
	if src == dst {
		return dst, nil
	}

	if err := os.Rename(src, dst); err == nil {
		return dst, nil
	} else if !isCrossDeviceRename(err) {
		return "", fmt.Errorf("rename completed file: %w", err)
	}

	if err := copyFile(src, dst); err != nil {
		return "", err
	}
	if err := os.Remove(src); err != nil {
		return "", fmt.Errorf("remove source after copy: %w", err)
	}
	return dst, nil
}

func isCrossDeviceRename(err error) bool {
	linkErr := &os.LinkError{}
	return errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy file: %w", err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync destination file: %w", err)
	}
	return nil
}

func (ch *Channel) runFFmpegFinalizer(filename string) (string, error) {
	outExt := finalOutputExt(filename)
	finalPath := finalOutputPath(filename)
	tempOutput := strings.TrimSuffix(filename, filepath.Ext(filename)) + ".finalizing" + outExt
	_ = os.Remove(tempOutput)

	args := []string{"-nostdin", "-y", "-i", filename}
	switch server.Config.FinalizeMode {
	case "remux":
		args = append(args, "-c", "copy")
		if outExt == ".mp4" {
			args = append(args, "-movflags", "+faststart")
		}
	case "transcode":
		encoder := strings.TrimSpace(server.Config.FFmpegEncoder)
		if encoder == "" {
			encoder = "libx264"
		}
		args = append(args, "-c:v", encoder)
		args = append(args, qualityArgsForEncoder(encoder, server.Config.FFmpegQuality)...)
		if preset := strings.TrimSpace(server.Config.FFmpegPreset); preset != "" {
			args = append(args, "-preset", preset)
		}
		args = append(args, "-c:a", "copy")
		if outExt == ".mp4" {
			args = append(args, "-movflags", "+faststart")
		}
	default:
		return "", fmt.Errorf("unsupported finalization mode %q", server.Config.FinalizeMode)
	}
	args = append(args, tempOutput)

	ch.Info("running ffmpeg %s for `%s`", server.Config.FinalizeMode, filepath.Base(filename))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	config.AcquireFFmpegHeavy()
	defer config.ReleaseFFmpegHeavy()

	outputBytes, err := config.FFmpegCommandContext(ctx, args...).CombinedOutput()
	if err != nil {
		_ = os.Remove(tempOutput)
		msg := strings.TrimSpace(string(outputBytes))
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("%s", msg)
	}
	if finalPath == filename {
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			_ = os.Remove(tempOutput)
			return "", fmt.Errorf("remove original before replace: %w", err)
		}
	}
	if err := os.Rename(tempOutput, finalPath); err != nil {
		_ = os.Remove(tempOutput)
		return "", fmt.Errorf("rename finalized output: %w", err)
	}
	return finalPath, nil
}

func qualityArgsForEncoder(encoder string, quality int) []string {
	if quality <= 0 {
		quality = 23
	}
	lower := strings.ToLower(strings.TrimSpace(encoder))
	switch {
	case strings.Contains(lower, "nvenc"):
		return []string{"-cq", fmt.Sprintf("%d", quality)}
	case strings.Contains(lower, "qsv"), strings.Contains(lower, "vaapi"), strings.Contains(lower, "amf"):
		return []string{"-global_quality", fmt.Sprintf("%d", quality)}
	default:
		return []string{"-crf", fmt.Sprintf("%d", quality)}
	}
}

// ─── Output pipeline (MoveToOutputDir → thumbnail → upload → metadata → cleanup) ───

// MoveToOutputDir relocates a finalized recording into server.Config.OutputDir.
// Errors are non-fatal: the recording is already safely written at srcPath.
func (ch *Channel) MoveToOutputDir(srcPath string) string {
	// Enqueue the file into the pipeline for thumbnail → upload → metadata → cleanup.
	// The pipeline handles all lifecycle (semaphore, waitgroup, state persistence).
	//
	// EnqueueFileClaimed is used because every path here marks the file
	// in-flight before enqueueing (to keep the OutputDir watcher from
	// double-claiming it).  The plain EnqueueFile would see that marker and
	// drop the enqueue as a "duplicate" — leaving the freshly moved recording
	// permanently stuck, never uploaded until the next restart.
	enqueue := func(filePath string) {
		ch.PipelineQueue.EnqueueFileClaimed(filePath)
	}

	if server.Config == nil || server.Config.OutputDir == "" {
		enqueue(srcPath)
		return srcPath
	}

	destDir := server.Config.OutputDir
	if server.Config.PerModelFolder {
		destDir = filepath.Join(destDir, ch.Config.Username)
	}
	if err := os.MkdirAll(destDir, 0777); err != nil {
		ch.Error("output-dir: mkdir %s: %s", destDir, err.Error())
		return srcPath
	}

	destPath := uniqueDestPath(filepath.Join(destDir, filepath.Base(srcPath)))
	ch.Info("output-dir: moving %s (%s) -> %s", filepath.Base(srcPath), resolvePathForLog(srcPath), destPath)
	// Mark in-flight before moveFile so the watcher's fsnotify handler
	// sees the file as already claimed by the pipeline.
	MarkUploadInFlight(destPath)
	if err := moveFile(srcPath, destPath); err != nil {
		ch.Error("output-dir: move %s to %s: %s — uploading from original location (%s)", filepath.Base(srcPath), destDir, err.Error(), resolvePathForLog(srcPath))
		MarkUploadDone(destPath) // release the failed-dest marker before marking src
		MarkUploadInFlight(srcPath)
		enqueue(srcPath)
		return srcPath
	}
	// Verify the destination actually exists after the move — on some
	// Windows configurations os.Rename can return nil without moving
	// the file (e.g. when src and dest resolve to the same path via
	// symlinks or junctions).  If the dest is missing, fall back to
	// the original location.
	if _, statErr := os.Stat(destPath); statErr != nil {
		ch.Error("output-dir: post-move stat of dest %s failed: %v — uploading from original location (%s)", destPath, statErr, resolvePathForLog(srcPath))
		MarkUploadDone(destPath) // release the failed-dest marker before marking src
		MarkUploadInFlight(srcPath)
		enqueue(srcPath)
		return srcPath
	}
	ch.Info("output-dir: moved %s -> %s", filepath.Base(srcPath), destPath)
	enqueue(destPath)
	return destPath
}

// resolvePathForLog resolves a path to its absolute form for logging.
// If resolution fails, returns the original path unchanged.
func resolvePathForLog(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return path
}

// uniqueDestPath returns path if it does not exist, otherwise appends
// " (n)" before the extension until an unused path is found.
func uniqueDestPath(path string) string {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return path
	}
	ext := filepath.Ext(path)
	base := path[:len(path)-len(ext)]
	for i := 1; i < 100000; i++ {
		candidate := fmt.Sprintf("%s (%d)%s", base, i, ext)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
	return fmt.Sprintf("%s (99999)%s", base, ext)
}

func moveFile(src, dest string) error {
	// Retry rename with backoff for transient Windows locks (AV, Search Indexer, etc.).
	for i := 0; i < 3; i++ {
		if err := os.Rename(src, dest); err == nil {
			return nil
		}
		time.Sleep(time.Duration(50*(1<<i)) * time.Millisecond)
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		in.Close()
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		in.Close()
		out.Close()
		os.Remove(dest)
		return err
	}
	// Sync before close so a crash between close and os.Remove(src) can't
	// leave a truncated destination alongside a deleted source.
	if err := out.Sync(); err != nil {
		in.Close()
		out.Close()
		os.Remove(dest)
		return err
	}
	if err := out.Close(); err != nil {
		in.Close()
		os.Remove(dest)
		return err
	}
	// Close the source handle BEFORE removing the file.  On Windows,
	// DeleteFileW fails with ERROR_ACCESS_DENIED when any handle is
	// still open, so defer in.Close() would keep the file busy.
	in.Close()

	// Retry remove with aggressive backoff (up to ~50s total) so transient
	// Windows locks (AV scanner, Search Indexer) have time to release.
	// If still locked, try rename + delete as a fallback.
	for i := 0; i < 20; i++ {
		if err := os.Remove(src); err == nil {
			return nil
		}
		if i >= 10 {
			tmpPath := fmt.Sprintf("%s.deleting.%d", src, i)
			if renameErr := os.Rename(src, tmpPath); renameErr == nil {
				if removeErr := os.Remove(tmpPath); removeErr == nil {
					return nil
				}
				os.Rename(tmpPath, src)
			}
		}
		backoff := time.Duration(100*(1<<uint(min(i, 8)))) * time.Millisecond // 100ms, 200ms, 400ms, … 25.6s
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
		time.Sleep(backoff)
	}
	// Source could not be removed — return the error so the caller can handle
	// the duplicate without silently leaving orphaned files.
	return fmt.Errorf("could not remove source after copy: %w", os.Remove(src))
}

// ─── Min-duration pending segments ─────────────────────────────────────────

// MaybeDeferToPending checks whether min-duration is enabled and, if so,
// whether filePath is short enough to be deferred.  When the file should be
// deferred (or on probe failure — we'd rather be safe) it is moved into
// .pending/<user>/ and the function returns true so callers skip upload.
func MaybeDeferToPending(filePath string) bool {
	minDur := 0
	if server.Config != nil {
		minDur = server.Config.MinDurationBeforeUpload
	}
	if minDur <= 0 {
		return false // feature disabled — upload directly
	}

	username := extractUsernameFromFilename(filepath.Base(filePath))
	if username == "" {
		// Can't determine the user; fall back to "unknown"
		username = "unknown"
	}

	dur, err := VideoDurationSeconds(filePath)
	if err != nil {
		log.Printf("[cleanup] min-duration: could not probe %s (%v) — deferring to pending", filepath.Base(filePath), err)
		_ = moveToPendingDir(filePath, username)
		return true
	}

	if dur < float64(minDur) {
		log.Printf("[cleanup] min-duration: %s = %.1fs (< %ds) — deferring to pending",
			filepath.Base(filePath), dur, minDur)
		_ = moveToPendingDir(filePath, username)
		return true
	}

	return false // meets threshold — upload normally
}

// moveToPendingDir moves a file into the .pending/<username>/ directory.
// Acquires pendingDirMu so it cannot race with handleMinDurationAndMerge or
// processAllPendingSegments, which may call deletePendingSegments concurrently.
func moveToPendingDir(filePath, username string) error {
	mu := pendingMu(username)
	mu.Lock()
	defer mu.Unlock()

	pendingDir := pendingSegmentsDir(username)
	if err := os.MkdirAll(pendingDir, 0777); err != nil {
		return fmt.Errorf("create pending dir: %w", err)
	}
	dest := filepath.Join(pendingDir, filepath.Base(filePath))
	return os.Rename(filePath, dest)
}

// CleanupOrphanedFiles processes orphaned sidecar files left behind by
// cancelled or crashed post-processing runs. Instead of deleting them,
// it runs them through the full pipeline: mux (if split A/V), generate
// thumbnails, upload to hosts, save metadata to Supabase, then delete.
func CleanupOrphanedFiles() {
	if server.Config == nil {
		return
	}

	dirs := []string{"videos"}
	if server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		// Classify files by type
		type fileInfo struct {
			path string
			name string
		}
		mainVideos := map[string]fileInfo{} // stem -> info
		muxedFiles := map[string]fileInfo{} // stem -> info
		videoParts := map[string]fileInfo{} // stem -> info (.video.mp4)
		audioParts := map[string]fileInfo{} // stem -> info (.audio.mp4)

		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			path := filepath.Join(dir, name)
			ext := strings.ToLower(filepath.Ext(name))

			switch {
			case strings.HasSuffix(name, ".video.muxed.mp4"):
				stem := strings.TrimSuffix(name, ".video.muxed.mp4")
				muxedFiles[stem] = fileInfo{path, name}
			case strings.HasSuffix(name, ".video.mp4"):
				stem := strings.TrimSuffix(name, ".video.mp4")
				videoParts[stem] = fileInfo{path, name}
			case strings.HasSuffix(name, ".audio.mp4"):
				stem := strings.TrimSuffix(name, ".audio.mp4")
				audioParts[stem] = fileInfo{path, name}
			case (ext == ".mp4" || ext == ".mkv" || ext == ".ts") &&
				!strings.Contains(name, ".video.") &&
				!strings.Contains(name, ".audio.") &&
				!strings.Contains(name, ".muxed.") &&
				!strings.Contains(name, ".preview."):
				stem := strings.TrimSuffix(name, filepath.Ext(name))
				mainVideos[stem] = fileInfo{path, name}
			}
		}

		// Process orphaned muxed files (output from a mux that was never uploaded)
		sem := make(chan struct{}, 5)
		for stem, info := range muxedFiles {
			if _, hasMain := mainVideos[stem]; hasMain {
				continue
			}
			stem, info := stem, info
			sem <- struct{}{}
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[PANIC] processing orphaned muxed file %s: %v", info.name, r)
					}
					<-sem
				}()
				recoveryLogf(info.name, "processing orphaned muxed file %s", info.name)

				// Check journal to skip files that were already fully uploaded
				if IsAlreadyFullyUploaded(info.path) {
					recoveryLogf(info.name, "all hosts already have this file per journal — removing local copy")
					os.Remove(info.path)
					DeleteSidecarFiles(info.path)
					_ = stem
					return
				}

			if MaybeDeferToPending(info.path) {
				_ = stem
				return
			}
			// Never race another flow that owns this file right now (an active
			// channel pipeline or the OutputDir watcher).  Their in-flight
			// marker is cleared when they finish; the periodic scan (or the
			// manual rescan) picks the file back up afterwards.
			if IsUploadInFlight(info.path) {
				return
			}
			thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(info.path)
			UploadOrphanedFile(info.path, thumbURL, spriteURL, previewURL)
			DeleteSidecarFiles(info.path)
			_ = stem
		}()
		}

		// Process orphaned split A/V pairs (mux them first, then upload)
		for stem, vInfo := range videoParts {
			if _, hasMain := mainVideos[stem]; hasMain {
				continue
			}
			aInfo, hasAudio := audioParts[stem]
			if !hasAudio {
				// No matching audio sidecar — this video part is stale.
				// If a muxed result exists for this stem, delete the stale video part.
				if _, hasMuxed := muxedFiles[stem]; hasMuxed {
					recoveryLogf(vInfo.name, "recovery: deleting stale video sidecar %s (muxed version exists)", vInfo.name)
					os.Remove(vInfo.path)
					continue
				}
				// No muxed result either — upload the video part on its own.
				if !MaybeDeferToPending(vInfo.path) && !IsUploadInFlight(vInfo.path) {
					thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(vInfo.path)
					UploadOrphanedFile(vInfo.path, thumbURL, spriteURL, previewURL)
				}
				DeleteSidecarFiles(vInfo.path)
				continue
			}

			stem, vInfo, aInfo := stem, vInfo, aInfo
			sem <- struct{}{}
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[PANIC] muxing orphaned split A/V pair %s: %v", stem, r)
					}
					<-sem
				}()

				// Mux the pair
				muxedPath := filepath.Join(dir, stem+".video.muxed.mp4")
				recoveryLogf(vInfo.name, "recovery: muxing orphaned split A/V pair %s", stem)
				if err := muxVideoAudio(vInfo.path, aInfo.path, muxedPath); err != nil {
					recoveryLogf(vInfo.name, "recovery: mux failed for %s: %v — uploading video-only", stem, err)
					// Fall back to uploading just the video track
					if !MaybeDeferToPending(vInfo.path) && !IsUploadInFlight(vInfo.path) {
						thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(vInfo.path)
						UploadOrphanedFile(vInfo.path, thumbURL, spriteURL, previewURL)
					}
					DeleteSidecarFiles(vInfo.path)
					return
				}

				// Delete source sidecars
				os.Remove(vInfo.path)
				os.Remove(aInfo.path)

				// Generate thumbnails, upload, and clean up
				if MaybeDeferToPending(muxedPath) {
					DeleteSidecarFiles(muxedPath)
					os.Remove(muxedPath)
					return
				}
				// Another flow (watcher/pipeline) already owns the freshly muxed
				// file — let it finish; never upload or delete it out from
				// under that upload.
				if IsUploadInFlight(muxedPath) {
					return
				}
				thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(muxedPath)
				UploadOrphanedFile(muxedPath, thumbURL, spriteURL, previewURL)
				DeleteSidecarFiles(muxedPath)
				os.Remove(muxedPath)
			}()
		}

		// Wait for all orphan processing to complete
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}

		// Process any pending segments (short videos awaiting merge).
		// Pending segments are stored under .pending/{username}/.
		processAllPendingSegments()

		// Clean up orphaned sidecar files whose main video no longer exists
		sidecarExts := []string{".thumb.webp", ".thumb.jpg", ".sprite.webp", ".sprite.jpg", ".preview.webp", ".preview.mp4", ".thumb", ".sprite"}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			path := filepath.Join(dir, name)
			for _, suffix := range sidecarExts {
				if !strings.HasSuffix(name, suffix) {
					continue
				}
				base := strings.TrimSuffix(name, suffix)
				hasMain := false
				for ext := range map[string]bool{".mp4": true, ".mkv": true, ".ts": true} {
					if _, ok := mainVideos[base+ext]; ok {
						hasMain = true
						break
					}
				}
				if !hasMain {
					os.Remove(path)
				}
				break
			}
		}
	}
}

// DeleteSidecarFiles removes preview sidecar files associated with a video path.
func DeleteSidecarFiles(videoPath string) {
	for _, suffix := range []string{".thumb.webp", ".thumb.jpg", ".sprite.webp", ".sprite.jpg", ".preview.webp", ".preview.mp4", ".thumb", ".sprite"} {
		os.Remove(videoPath + suffix)
	}
}

// removeFileWithRetry attempts to remove a file, retrying up to 5 times
// with exponential backoff.  This handles transient Windows file locks
// from AV scanners, Search Indexer, upload handles still closing, etc.
//
// After 2 attempts, it tries a rename-then-delete strategy: renaming
// the file can succeed even when deletion is blocked by a reader (common with
// Windows Defender), which often releases the lock on the original path.
// Returns nil if the file was removed (or didn't exist).
func removeFileWithRetry(path string) error {
	for i := 0; i < 5; i++ {
		if err := os.Remove(path); err == nil || os.IsNotExist(err) {
			return nil
		}

		if i >= 2 {
			tmpPath := fmt.Sprintf("%s.deleting.%d", path, i)
			if renameErr := os.Rename(path, tmpPath); renameErr == nil {
				if removeErr := os.Remove(tmpPath); removeErr == nil {
					return nil
				}
				os.Rename(tmpPath, path)
			}
		}

		backoff := time.Duration(500*(1<<uint(min(i, 6)))) * time.Millisecond // 0.5s, 1s, 2s, 4s, 8s, 16s, 32s
		if backoff > 15*time.Second {
			backoff = 15 * time.Second
		}
		time.Sleep(backoff)
	}
	return os.Remove(path) // final attempt, return the error
}

// muxVideoAudio combines a separate video and audio file into a single MP4.
// Used only for recovering legacy split A/V sidecars from previous versions.
// Uses a 5-minute timeout so a hung ffmpeg cannot leak the caller's goroutine.
func muxVideoAudio(videoPath, audioPath, outputPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := config.FFmpegCommandContext(ctx, "-y",
		"-i", videoPath,
		"-i", audioPath,
		"-c", "copy",
		"-movflags", "+faststart",
		outputPath,
	)
	return cmd.Run()
}

// dateSeparatorRe matches the "_YYYY-MM-DD_" / "_YYYY-MM-DD-" timestamp separator
// the recorder writes between the username and the time portion.  Anchoring on
// the full date (not just "_20") is what keeps usernames that themselves contain
// "_20" (e.g. "alice_20_fan_2025-01-01_...") from being mis-split: the regex
// skips the "_20" inside the username and lands on the real date.
var dateSeparatorRe = regexp.MustCompile(`_(20\d{2}-\d{2}-\d{2})[_-]`)

// findDateSeparatorIndex returns the byte index in stem of the "_" that begins
// the "_YYYY-MM-DD_" timestamp separator, or -1 if no date separator is found.
// Both extractUsernameFromFilename and extractTimestampFromFilename use it so
// they always agree on where the username ends.
func findDateSeparatorIndex(stem string) int {
	loc := dateSeparatorRe.FindStringSubmatchIndex(stem)
	if loc == nil {
		return -1
	}
	return loc[0] // index of the leading "_"
}

// extractUsernameFromFilename parses "username_YYYY-MM-DD_HH-MM-SS.ext" to get the username.
// It locates the "_YYYY-MM-DD_" timestamp separator (not merely the substring
// "_20", which can legitimately appear inside a username such as
// "alice_20_fan_2025-01-01_...") so the username portion is split correctly.
func extractUsernameFromFilename(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))

	// Strip "merged-" prefix that the merge system prepends.
	stem := strings.TrimPrefix(base, "merged-")

	// Find the real timestamp separator (_YYYY-MM-DD_ or _YYYY-MM-DD-).
	idx := findDateSeparatorIndex(stem)
	if idx < 0 {
		return ""
	}

	candidate := stem[:idx]

	// Deduplicate: merged filenames become "<user>-<user>" via the merge
	// system.  Usernames may contain hyphens (e.g. "Awesome-sona"), so we
	// try every split point and check whether left == right.
	if hyphen := strings.Index(candidate, "-"); hyphen > 0 {
		rightSide := candidate[hyphen+1:]
		if candidate[:hyphen] == rightSide {
			return candidate[:hyphen]
		}
		// Username might contain a hyphen — try later split positions.
		for h := strings.Index(candidate[hyphen+1:], "-"); h >= 0; h = strings.Index(candidate[hyphen+1:], "-") {
			hyphen += 1 + h
			rightSide = candidate[hyphen+1:]
			if candidate[:hyphen] == rightSide {
				return candidate[:hyphen]
			}
		}
	}

	return candidate
}

// extractTimestampFromFilename parses the standard recording timestamp from a
// filename like "username_2025-01-01_12-00-00.mp4" and returns it in Supabase
// format ("2025-01-01T12:00:00Z").  Returns "" if the pattern is not found.
func extractTimestampFromFilename(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	idx := findDateSeparatorIndex(base)
	if idx < 0 {
		return ""
	}
	ts := base[idx+1:] // "YYYY-MM-DD_HH-MM-SS..."
	if len(ts) >= 19 && ts[4] == '-' && ts[7] == '-' && ts[10] == '_' && ts[13] == '-' && ts[16] == '-' {
		return ts[:10] + "T" + ts[11:13] + ":" + ts[14:16] + ":" + ts[17:19] + "Z"
	}
	return ""
}

// recoveryLogf logs to both stdout and the channel's SSE log stream if the
// file can be associated with an active channel via its Manager.
func recoveryLogf(filename, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	username := extractUsernameFromFilename(filename)
	log.Printf("recovery [%s]: %s", username, msg)
	if m := server.Manager; m != nil && username != "" {
		m.PublishLog(username, "[recovery] "+msg)
	}
}

// isAlreadyFullyUploaded checks the upload journal to determine if a file has
// been successfully uploaded to all configured hosts.
func IsAlreadyFullyUploaded(filePath string) bool {
	if !server.RecordingExists(filepath.Base(filePath)) {
		return false
	}
	fileHash, err := internal.FastFileHash(filePath)
	if err != nil || fileHash == "" {
		return false
	}
	completed, err := server.LoadCompletedHosts(fileHash)
	if err != nil {
		return false
	}
	// Build the set of all configured hosts
	hosts := configuredUploadHosts()
	if len(hosts) == 0 {
		return false
	}
	done := make(map[string]bool, len(completed))
	for _, h := range completed {
		done[h] = true
	}
	for _, h := range hosts {
		if !done[h] {
			return false
		}
	}
	return true
}

// configuredUploadHosts returns the list of upload hosts that have their
// API keys configured in the server config.
//
// This delegates to uploader.NewMultiHostUploader(...).AvailableHosts() so that
// the set of hosts checked by IsAlreadyFullyUploaded is always identical to the
// set the pipeline actually uploads to.  Previously this maintained a separate
// hand-written list that drifted out of sync, which caused the watcher to
// consider a file "fully uploaded" — and delete the local copy — before every
// configured host had received it.
func configuredUploadHosts() []string {
	cfg := server.Config
	if cfg == nil {
		return nil
	}
	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeKey,
		cfg.MixdropEmail,
		cfg.MixdropToken,
		cfg.VidaraKey,
		nil,
	)
	return upl.AvailableHosts()
}

// UploadOrphanedFile uploads a file to all configured hosts and saves metadata
// to Supabase. Unlike Channel.uploadFile, this doesn't require an active channel.
// Username is extracted from the filename; metadata fields are left empty.
//
// If every configured host fails on the first attempt, it retries up to 2 more
// times with a 60-second delay between attempts.  This handles transient network
// or API outages that can occur when the app restarts after a crash.
func UploadOrphanedFile(filePath, thumbURL, spriteURL, previewURL string) bool {
	MarkUploadInFlight(filePath)
	defer MarkUploadDone(filePath)
	cfg := server.Config
	if cfg == nil {
		return false
	}

	filename := filepath.Base(filePath)

	recoveryLogf(filename, "uploading %s", filename)

	// Compute file hash for upload journal
	fileHash, hashErr := internal.FastFileHash(filePath)
	if hashErr != nil {
		recoveryLogf(filename, "could not hash (journal skipped): %v", hashErr)
	}

	// Load completed hosts from journal
	var completedHosts []string
	if fileHash != "" {
		var loadErr error
		completedHosts, loadErr = server.LoadCompletedHosts(fileHash)
		if loadErr != nil {
			recoveryLogf(filename, "could not load journal: %v", loadErr)
		}
	}

	// Save preview links first
	if thumbURL != "" || spriteURL != "" || previewURL != "" {
		if err := server.SavePreviewLinks(filename, thumbURL, spriteURL, previewURL); err != nil {
			recoveryLogf(filename, "could not save preview links: %v", err)
		}
	}

	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeKey,
		cfg.MixdropEmail,
		cfg.MixdropToken,
		cfg.VidaraKey,
		nil, // no logger for orphan recovery
	)

	allHosts := upl.AvailableHosts()

	// Determine which hosts still need the file
	hostsToTry := allHosts
	if len(completedHosts) > 0 {
		hostsToTry = difference(allHosts, completedHosts)
		if len(hostsToTry) == 0 {
			if server.RecordingExists(filename) {
				recoveryLogf(filename, "all hosts already have this file per journal — skipping upload")
				return true
			}
			recoveryLogf(filename, "stale journal has no Supabase recording; clearing journal and re-uploading")
			if fileHash != "" {
				if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
					recoveryLogf(filename, "could not clear stale journal: %v", jErr)
				}
			}
			completedHosts = nil
			hostsToTry = allHosts
		}
		recoveryLogf(filename, "%d/%d hosts already have this file — uploading to %d remaining",
			len(completedHosts), len(allHosts), len(hostsToTry))
	}

	// Use RetryManager for upload retries
	var allResults []uploader.UploadResult
	err := DoWithRetry("orphan-"+filename, func() error {
		attemptResults := upl.UploadSelected(filePath, hostsToTry)
		allResults = append(allResults, attemptResults...)

		// Save journal entries for each result
		if fileHash != "" {
			stat, _ := os.Stat(filePath)
			var filesize int64
			if stat != nil {
				filesize = stat.Size()
			}
			for _, r := range attemptResults {
				status := "success"
				errMsg := ""
				if r.Error != nil {
					status = "failed"
					errMsg = r.Error.Error()
				}
				if jErr := server.SaveJournalEntry(fileHash, filename, r.Host, status, filesize, errMsg); jErr != nil {
					recoveryLogf(filename, "could not save journal for %s: %v", r.Host, jErr)
				}
			}
		}

		success := uploader.GetSuccessfulUploads(attemptResults)
		recoveryLogf(filename, "upload attempt — %d/%d successful", len(success), len(allHosts))
		if len(success) >= len(allHosts) {
			return nil
		}

		failedHosts := failedHostNames(attemptResults, completedHosts)
		hostsToTry = failedHosts
		if len(hostsToTry) == 0 {
			return nil
		}

		return fmt.Errorf("%d hosts still pending", len(hostsToTry))
	}, WithUploadSem(), WithMaxAttempts(3), WithBaseBackoff(60*time.Second))
	if err != nil {
		recoveryLogf(filename, "[WARN] all upload attempts exhausted — file will be retried on next restart")
		return false
	}

	// Build links map from all accumulated results
	links := map[string]string{}
	var embedURL string
	for _, r := range allResults {
		if r.Error == nil && r.DownloadLink != "" {
			links[r.Host] = r.DownloadLink
			if embedURL == "" {
				embedURL = embedURLFromLink(r.Host, r.DownloadLink)
			}
		}
	}

	stat, _ := os.Stat(filePath)
	var filesize int64
	if stat != nil {
		filesize = stat.Size()
	}

	timestamp := extractTimestampFromFilename(filename)
	if timestamp == "" {
		timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	}

	dur, probeErr := VideoDurationSeconds(filePath)
	if probeErr != nil {
		recoveryLogf(filename, "could not probe duration: %v", probeErr)
	}

	dbSaved := false
	if err := server.SaveRecordingWithLinks(
		extractUsernameFromFilename(filename), filename, timestamp,
		"", nil, 0, "", 0, filesize, dur, "", embedURL, thumbURL, spriteURL, previewURL, links,
	); err != nil {
		recoveryLogf(filename, "failed to save recording to Supabase: %v", err)
		if fileHash != "" {
			if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
				recoveryLogf(filename, "could not delete journal after DB failure: %v", jErr)
			}
		}
	} else {
		dbSaved = true
		recoveryLogf(filename, "saved recording metadata")
	}

	// Delete local file only once ALL hosts have the file safely and metadata
	// is persisted. Otherwise the file remains available for retry.
	if cfg.DeleteLocalAfterUpload && len(uploader.GetSuccessfulUploads(allResults)) > 0 && dbSaved {
		DeleteSidecarFiles(filePath)
		if err := removeFileWithRetry(filePath); err != nil {
			recoveryLogf(filename, "could not remove local file: %v — will retry on next restart", err)
			return true
		}
		if fileHash != "" {
			if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
				recoveryLogf(filename, "could not delete journal: %v", jErr)
			}
		}
		recoveryLogf(filename, "removed local file")
	}

	return true
}

// pendingSegmentsDir returns the directory where short video segments are stored
// awaiting merge with the next recording.  A subdirectory per channel keeps
// segments from different models separate.
func pendingSegmentsDir(username string) string {
	dir := "videos"
	if server.Config != nil && server.Config.OutputDir != "" {
		dir = server.Config.OutputDir
	}
	return filepath.Join(dir, ".pending", username)
}

// collectPendingSegments returns sorted absolute paths of all pending segments
// for a given channel.
func collectPendingSegments(username string) []string {
	dir := pendingSegmentsDir(username)
	return collectPendingSegmentsInDir(dir)
}

// collectPendingSegmentsInDir returns sorted absolute paths of actual video
// files in dir, filtering out sidecar files and zero-byte files.  Previously
// consolidated "merged-*.mp4" files ARE intentionally included: merging a
// consolidated segment with newer ones preserves all of the recording content.
func collectPendingSegmentsInDir(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Skip sidecar files — they are not video segments.
		if isSidecar(name) {
			continue
		}
		// Skip zero-byte files — they are corrupt/empty segments.
		info, err := e.Info()
		if err != nil || info.Size() == 0 {
			continue
		}
		paths = append(paths, filepath.Join(dir, name))
	}
	sort.Strings(paths)
	return paths
}

// deletePendingSegments removes all pending segments for a channel and cleans
// up the (now empty) directory.
func deletePendingSegments(username string) {
	dir := pendingSegmentsDir(username)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
	_ = os.Remove(dir)
}

// mergedPendingName returns a stable name for a consolidated pending segment:
// "merged-" plus the oldest segment's base name, stripping any accumulated
// "merged-" prefixes so repeated consolidations don't grow the filename
// indefinitely (which could eventually exceed the Windows path length limit).
func mergedPendingName(segments []string) string {
	base := filepath.Base(segments[0])
	base = strings.TrimPrefix(base, "merged-")
	return "merged-" + base
}

// renameOverwriting moves src to dst, removing any existing dst first.  On
// Windows os.Rename refuses to overwrite, and a stable merged-pending name can
// already exist from a previous consolidation.
func renameOverwriting(src, dst string) error {
	_ = os.Remove(dst)
	return os.Rename(src, dst)
}

// VideoDurationSeconds probes a video file and returns its duration in seconds.
// Falls back to parsing ffmpeg stderr output when ffprobe is unavailable or fails.
func VideoDurationSeconds(videoPath string) (float64, error) {
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer probeCancel()
	config.AcquireFFmpeg()
	defer config.ReleaseFFmpeg()
	out, err := config.FFprobeCommandContext(probeCtx,
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		videoPath,
	).Output()
	if err == nil {
		dur, parseErr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		if parseErr == nil {
			return dur, nil
		}
	}

	// Fallback: ask ffmpeg to decode null and parse "Duration:" from stderr.
	fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer fallbackCancel()
	cmd := config.FFmpegCommandContext(fallbackCtx, "-i", videoPath, "-f", "null", "-")
	stderr, fbErr := cmd.CombinedOutput()
	if fbErr == nil && len(stderr) > 0 {
		line := string(stderr)
		const durationPrefix = "Duration: "
		if idx := strings.Index(line, durationPrefix); idx >= 0 {
			rest := line[idx+len(durationPrefix):]
			if end := strings.IndexAny(rest, "., "); end > 0 {
				rest = rest[:end]
			}
			parts := strings.SplitN(rest, ":", 3)
			if len(parts) == 3 {
				hours, _ := strconv.ParseFloat(parts[0], 64)
				minutes, _ := strconv.ParseFloat(parts[1], 64)
				seconds, _ := strconv.ParseFloat(parts[2], 64)
				if hours >= 0 || minutes >= 0 || seconds >= 0 {
					return hours*3600 + minutes*60 + seconds, nil
				}
			}
		}
	}

	if err != nil {
		return 0, fmt.Errorf("probe %s: %w", filepath.Base(videoPath), err)
	}
	return 0, fmt.Errorf("probe %s: could not parse duration from ffprobe or ffmpeg", filepath.Base(videoPath))
}

// mergeVideos concatenates multiple video files into a single output.
//
// Strategy (two-phase):
//  1. Fast path — concat demuxer with stream copy.  Works when all segments
//     share the same codec parameters and each (except the first) starts with
//     a keyframe.  If it succeeds AND the output contains all expected streams
//     with reasonable duration, we are done.
//  2. Fallback — concat demuxer with re-encode (libx264 + AAC).  Fixes
//     codec-parameter mismatch, missing-keyframe, and broken-timestamp issues
//     that the stream-copy path cannot handle.
//
// In both phases the output is always normalized: pending segments carry
// absolute server timestamps (PTS=5044s from LL-HLS) that would otherwise
// make the merged file unseekable and break playback after the first segment
// boundary.
func mergeVideos(inputs []string, outputPath string) error {
	if len(inputs) < 2 {
		return fmt.Errorf("need at least 2 inputs, got %d", len(inputs))
	}

	// ── Pre-flight: validate every segment ──────────────────────────────
	for _, p := range inputs {
		dur, err := VideoDurationSeconds(p)
		if err != nil || dur <= 0 {
			if err == nil {
				err = fmt.Errorf("zero duration")
			}
			return fmt.Errorf("segment %s is invalid: %w", filepath.Base(p), err)
		}
	}

	// ── Build concat list for both phases ────────────────────────────────
	listFile, err := os.CreateTemp("", "concat-*.txt")
	if err != nil {
		return fmt.Errorf("create concat list: %w", err)
	}
	defer listFile.Close()
	defer os.Remove(listFile.Name())

	for _, p := range inputs {
		abs, aErr := filepath.Abs(p)
		if aErr != nil {
			abs = p
		}
		escaped := strings.ReplaceAll(abs, "'", "'\\''")
		if _, wErr := fmt.Fprintf(listFile, "file '%s'\n", escaped); wErr != nil {
			return fmt.Errorf("write concat list: %w", wErr)
		}
	}

	// Compute total input duration for validation later.
	var totalInputDur float64
	for _, p := range inputs {
		if d, e := VideoDurationSeconds(p); e == nil {
			totalInputDur += d
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	config.AcquireFFmpegHeavy()
	defer config.ReleaseFFmpegHeavy()

	// ── Phase 1: Fast stream-copy concat ────────────────────────────────
	streamCopyOK := config.FFmpegCommandContext(ctx,
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c", "copy",
		"-fflags", "+genpts+igndts",
		"-movflags", "+faststart",
		outputPath,
	).Run()

	if streamCopyOK == nil {
		// Probe the output — check it has reasonable duration and at least a video track.
		outputDur, probeErr := VideoDurationSeconds(outputPath)
		if probeErr == nil && outputDur > 0 && (totalInputDur <= 0 || outputDur >= totalInputDur*0.5) {
			tmpPath := outputPath + ".normalized.mp4"
			if err := config.FFmpegCommandContext(ctx,
				"-y",
				"-fflags", "+genpts+igndts",
				"-i", outputPath,
				"-c", "copy",
				"-fflags", "+genpts",
				"-movflags", "+faststart",
				tmpPath,
			).Run(); err != nil {
				os.Remove(tmpPath)
				return nil // best-effort — return the concat output as-is
			}
			os.Remove(outputPath)
			if rErr := os.Rename(tmpPath, outputPath); rErr != nil {
				os.Remove(tmpPath)
			}
			return nil
		}
	}

	// Stream-copy failed or produced a bad output — clean up and re-try.
	os.Remove(outputPath)

	// ── Phase 2: Re-encode concat ───────────────────────────────────────
	reEncodeArgs := []string{
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-crf", "23",
		"-c:a", "aac",
		"-b:a", "128k",
	}

	// Force timestamps to start at zero on every segment boundary.
	// The setpts/asetpts filters reset the timeline so there are no gaps.
	reEncodeArgs = append(reEncodeArgs,
		"-vf", "setpts=PTS-STARTPTS",
		"-af", "asetpts=PTS-STARTPTS",
		"-movflags", "+faststart",
		outputPath,
	)

	if err := config.FFmpegCommandContext(ctx, reEncodeArgs...).Run(); err != nil {
		os.Remove(outputPath)
		return fmt.Errorf("merge re-encode: %w", err)
	}

	return nil
}

// handleMinDurationAndMerge checks whether a finalized video file meets the
// minimum-duration threshold.  If the feature is disabled the check is skipped
// and the caller proceeds to upload normally.  Callers should skip this
// function entirely when skipMinDuration is set (channel pause).
//
// When a video is shorter than the threshold it is moved into a pending
// directory.  If pending segments already exist (including the one just moved),
// they are merged together and the merged result is uploaded via
// MoveToOutputDir.
//
// Returns true if the video was handled (deferred to pending or merged+uploaded)
// so the caller should stop processing it.  Returns false when the caller
// should proceed with its normal upload logic.
func (ch *Channel) handleMinDurationAndMerge(videoPath string) bool {
	mu := pendingMu(ch.Config.Username)
	mu.Lock()

	minDur := ch.Config.MinDurationBeforeUpload
	if minDur <= 0 {
		if server.Config != nil && server.Config.MinDurationBeforeUpload > 0 {
			minDur = server.Config.MinDurationBeforeUpload
		} else {
			mu.Unlock()
			return false // feature disabled — proceed normally
		}
	}

	dur, err := VideoDurationSeconds(videoPath)
	if err != nil {
		ch.Warn("min-duration: could not probe %s: %v — deferring to pending", filepath.Base(videoPath), err)
		// On probe failure, keep the video pending rather than uploading a corrupt/short file
		pendingDir := pendingSegmentsDir(ch.Config.Username)
		if mErr := os.MkdirAll(pendingDir, 0777); mErr == nil {
			destPath := filepath.Join(pendingDir, filepath.Base(videoPath))
			if rErr := os.Rename(videoPath, destPath); rErr == nil {
				mu.Unlock()
				return true
			}
		}
		mu.Unlock()
		ch.Error("min-duration: probe failed and could not move %s to pending: %v — keeping it in place, NOT uploading",
			filepath.Base(videoPath), err)
		return true
	}

	if dur >= float64(minDur) {
		// Video is long enough. Before uploading, check if there are
		// pending segments to merge with.
		segments := collectPendingSegments(ch.Config.Username)
		if len(segments) == 0 {
			mu.Unlock()
			return false // no pending — proceed normally
		}

		// Merge pending segments with the current video.
		// Release the lock during the potentially long ffmpeg encode.
		mergedPath := videoPath + ".merged.mp4"
		mergeInputs := make([]string, len(segments))
		copy(mergeInputs, segments)
		allInputs := append(mergeInputs, videoPath)
		ch.Info("min-duration: merging %d pending segment(s) with %s", len(mergeInputs), filepath.Base(videoPath))
		mu.Unlock()
		mErr := mergeVideos(allInputs, mergedPath)
		if mErr != nil {
			os.Remove(mergedPath) // clean up partial output
			ch.Error("min-duration: merge failed: %v — uploading current video separately, KEEPING pending segments for a future merge", mErr)
			return false
		}

		mergedDur, probeErr := VideoDurationSeconds(mergedPath)
		if probeErr != nil || mergedDur < float64(minDur) {
			ch.Warn("min-duration: merged output for %s is %.1fs (< %ds) — uploading current video separately, KEEPING pending segments",
				filepath.Base(mergedPath), mergedDur, minDur)
			os.Remove(mergedPath)
			return false
		}

		mu.Lock()
		for _, s := range mergeInputs {
			os.Remove(s)
		}
		_ = os.Remove(videoPath)
		mu.Unlock()

		if ch.Config.Compress {
			ch.Info("min-duration: merged -> %s (%.1fs), compressing before upload", filepath.Base(mergedPath), mergedDur)
			ch.CompressFile(mergedPath)
		} else {
			ch.Info("min-duration: merged -> %s (%.1fs), proceeding with upload", filepath.Base(mergedPath), mergedDur)
			ch.MoveToOutputDir(mergedPath)
		}
		return true
	}

	// Video is too short — move to pending
	pendingDir := pendingSegmentsDir(ch.Config.Username)
	if err := os.MkdirAll(pendingDir, 0777); err != nil {
		mu.Unlock()
		ch.Error("min-duration: cannot create pending dir %s: %v — keeping %s in place, NOT uploading short video",
			pendingDir, err, filepath.Base(videoPath))
		return true
	}

	destPath := filepath.Join(pendingDir, filepath.Base(videoPath))
	if err := os.Rename(videoPath, destPath); err != nil {
		mu.Unlock()
		ch.Error("min-duration: cannot move %s to pending: %v — keeping it in place, NOT uploading short video", filepath.Base(videoPath), err)
		return true
	}
	ch.Info("min-duration: %s is %.1fs (< %ds) — deferred to pending", filepath.Base(videoPath), dur, minDur)

	// If multiple segments have now accumulated, merge them and check the
	// combined duration. Only upload if the merged result meets the threshold.
	segments := collectPendingSegments(ch.Config.Username)
	if len(segments) > 1 {
		// Write to a unique scratch name first so the output can never collide
		// with an existing "merged-*" input segment, then finalize below.
		stableName := mergedPendingName(segments)
		mergedPath := filepath.Join(pendingDir, fmt.Sprintf(".merging-%d-%s", time.Now().UnixNano(), stableName))
		mergeInputs := make([]string, len(segments))
		copy(mergeInputs, segments)
		ch.Info("min-duration: merging %d pending segment(s)", len(mergeInputs))
		mu.Unlock()
		mErr := mergeVideos(mergeInputs, mergedPath)
		if mErr != nil {
			os.Remove(mergedPath) // clean up partial output
			ch.Error("min-duration: merge failed: %v — segments remain pending for next recording", mErr)
			return true
		}
		// Finalize to the stable name (best-effort; the scratch path is also valid).
		stablePath := filepath.Join(pendingDir, stableName)
		if rErr := renameOverwriting(mergedPath, stablePath); rErr == nil {
			mergedPath = stablePath
		}
		mu.Lock()

		mergedDur, mErr := VideoDurationSeconds(mergedPath)
		if mErr != nil {
			// Keep the merged result pending rather than risking an upload of
			// unconfirmed duration — the min-duration guarantee must never be
			// violated just because probing failed.
			ch.Warn("min-duration: could not probe merged result (%v) — keeping pending", mErr)
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			mu.Unlock()
			return true
		}

		if mergedDur >= float64(minDur) {
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			ch.Info("min-duration: merged %d segments = %.1fs (>= %ds) — uploading", len(mergeInputs), mergedDur, minDur)
			mu.Unlock()

			if ch.Config.Compress {
				ch.CompressFile(mergedPath)
			} else {
				ch.MoveToOutputDir(mergedPath)
			}
		} else {
			ch.Info("min-duration: merged %d segments = %.1fs (< %ds) — still pending", len(mergeInputs), mergedDur, minDur)
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			mu.Unlock()
		}
	} else {
		mu.Unlock()
	}

	return true // video was deferred to pending (or merged+uploaded)
}

// processAllPendingSegments scans all .pending/* subdirectories and processes any
// accumulated segments.  If segments exist they are merged together and uploaded.
// This is called during startup orphan cleanup so short segments from a previous
// run don't stay pending forever when no new recording arrives.
func processAllPendingSegments() {
	minDur := 0
	if server.Config != nil {
		minDur = server.Config.MinDurationBeforeUpload
	}

	dirs := []string{"videos"}
	if server.Config != nil && server.Config.OutputDir != "" && server.Config.OutputDir != "videos" {
		dirs = append(dirs, server.Config.OutputDir)
	}
	for _, dir := range dirs {
		pendingRoot := filepath.Join(dir, ".pending")
		userDirs, err := os.ReadDir(pendingRoot)
		if err != nil {
			continue
		}
		for _, ud := range userDirs {
			if !ud.IsDir() {
				continue
			}
			username := ud.Name()
			mu := pendingMu(username)

			mu.Lock()
			segments := collectPendingSegmentsInDir(filepath.Join(pendingRoot, username))
			if len(segments) < 1 {
				mu.Unlock()
				continue
			}

			// If min-duration is disabled, upload everything directly (legacy behavior).
			if minDur <= 0 {
				for _, s := range segments {
					recoveryLogf(s, "recovery: uploading pending segment %s", filepath.Base(s))
					mu.Unlock()
					thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(s)
					UploadOrphanedFile(s, thumbURL, spriteURL, previewURL)
					_ = os.Remove(s)
					mu.Lock()
				}
				_ = os.Remove(pendingSegmentsDir(username))
				mu.Unlock()
				continue
			}

			// Min-duration is enabled — merge segments and only upload if threshold met.
			segCopy := make([]string, len(segments))
			copy(segCopy, segments)
			mu.Unlock()

			pendingDir := pendingSegmentsDir(username)
			stableName := mergedPendingName(segments)
			// Write to a unique scratch name first so the output can never
			// collide with an existing "merged-*" input, then finalize.
			mergedPath := filepath.Join(pendingDir, fmt.Sprintf(".merging-%d-%s", time.Now().UnixNano(), stableName))
			recoveryLogf(segments[0], "recovery: merging %d pending segments for %s", len(segments), username)
			if err := mergeVideos(segCopy, mergedPath); err != nil {
				os.Remove(mergedPath)
				recoveryLogf(segments[0], "recovery: merge failed for %s: %v — leaving segments pending", username, err)
				continue
			}
			// Finalize to the stable name (best-effort; the scratch path is also valid).
			if rErr := renameOverwriting(mergedPath, filepath.Join(pendingDir, stableName)); rErr == nil {
				mergedPath = filepath.Join(pendingDir, stableName)
			}

			mergedDur, durErr := VideoDurationSeconds(mergedPath)
			if durErr != nil || mergedDur < float64(minDur) {
				if durErr != nil {
					recoveryLogf(mergedPath, "recovery: could not probe merged duration (%v) — keeping pending", durErr)
				} else {
					recoveryLogf(mergedPath, "recovery: merged = %.1fs (< %ds) — keeping pending", mergedDur, minDur)
				}
				mu.Lock()
				for _, s := range segCopy {
					os.Remove(s)
				}
				mu.Unlock()
				continue
			}

			var totalInputDur float64
			for _, s := range segCopy {
				if d, e := VideoDurationSeconds(s); e == nil {
					totalInputDur += d
				}
			}
			if totalInputDur > 0 && mergedDur < totalInputDur*0.5 {
				recoveryLogf(mergedPath, "recovery: merged output %.1fs is <50%% of total input %.1fs — streams may be incompatible, keeping pending",
					mergedDur, totalInputDur)
				mu.Lock()
				for _, s := range segCopy {
					os.Remove(s)
				}
				mu.Unlock()
				continue
			}

			mu.Lock()
			for _, s := range segCopy {
				os.Remove(s)
			}
			mu.Unlock()
			recoveryLogf(mergedPath, "recovery: merged = %.1fs (>= %ds) — uploading", mergedDur, minDur)
			thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(mergedPath)
			UploadOrphanedFile(mergedPath, thumbURL, spriteURL, previewURL)
			_ = os.Remove(mergedPath)
		}
	}
}

// videoExt returns true if the extension is a known video extension.
func videoExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".mp4" || ext == ".mkv"
}

// isSidecar returns true if the filename appears to be a sidecar/preview file.
// Note: .video.muxed.mp4 is the final muxed output (not a sidecar), while
// .video.mp4 and .audio.mp4 are raw A/V track files (sidecars).
func isSidecar(name string) bool {
	return strings.HasSuffix(name, ".thumb.webp") ||
		strings.HasSuffix(name, ".thumb.jpg") ||
		strings.HasSuffix(name, ".sprite.webp") ||
		strings.HasSuffix(name, ".sprite.jpg") ||
		strings.HasSuffix(name, ".preview.webp") ||
		strings.HasSuffix(name, ".preview.mp4") ||
		strings.HasSuffix(name, ".thumb") ||
		strings.HasSuffix(name, ".sprite") ||
		strings.HasSuffix(name, ".video.mp4") ||
		strings.HasSuffix(name, ".audio.mp4")
}
