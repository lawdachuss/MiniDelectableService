package channel

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/uploader"
)

const (
	thumbWidth      = 1280
	thumbHeight     = 720
	spriteFrames    = 16
	spriteCols      = 4
	spriteRows      = 4
	spriteFrameW    = 640
	spriteFrameH    = 360
	previewWidth    = 320
	previewDuration = 6.0 // seconds
	previewSegments = 12  // number of smooth clips to stitch (each ~0.5s)
)

// ThumbnailResult holds the generated thumbnail, sprite, and preview URLs
// along with their mirror URLs from all hosts.
type ThumbnailResult struct {
	ThumbURL       string
	SpriteURL      string
	PreviewURL     string
	ThumbMirrors   map[string]string // host -> URL
	SpriteMirrors  map[string]string // host -> URL
	PreviewMirrors map[string]string // host -> URL
}

// OnHostUploadFunc is called the instant a single host finishes uploading
// a thumbnail/sprite/preview asset.  The caller can persist the URL to the
// database immediately instead of waiting for all hosts to finish.
type OnHostUploadFunc func(assetType, host, url string)

// generateThumbnail is the channel-scoped wrapper — logs go to the channel log.
// If onHost is non-nil it is called the instant each host succeeds for each
// asset (thumb, sprite, preview) so the caller can save to DB immediately.
func (ch *Channel) generateThumbnail(videoPath string, onHost OnHostUploadFunc) ThumbnailResult {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { ch.Info(f, a...) },
		func(f string, a ...interface{}) { ch.Error(f, a...) },
		onHost,
	)
}

// GenerateThumbnailForFile is a standalone thumbnail generator that can be
// called outside of a channel context (e.g. for pre-existing video files).
func GenerateThumbnailForFile(videoPath string) ThumbnailResult {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { log.Printf("[thumb] "+f, a...) },
		func(f string, a ...interface{}) { log.Printf("[thumb:err] "+f, a...) },
		nil,
	)
}

// generateThumbnailForFile creates a static thumbnail (JPEG), a multi-frame sprite
// sheet (JPEG), and an animated WEBP hover preview (6 seconds of smooth clips
// from across the full video).  All three are uploaded to remote hosts and the
// URLs returned.  Local temp files are always cleaned up.
//
// JPEG is used for thumbnail and sprite because:
//   - All image hosts support it (Pixhost, ImgBB, Catbox)
//   - mjpeg encoder is fast (minimal encoding lag)
//   - Small filesize with good visual quality
//
// Animated WEBP is used for the preview because:
//   - ~90% smaller than GIF at same quality, full 24-bit color
//   - Smooth native-framerate playback (GIF was variable ~1-8fps)
//   - Hosted by Catbox (primary) and ImgBB (fallback) — both accept WEBP
//     (the IamAPTBA/ImgBB API no-op, image hosts all accept it) so the
//     preview never depends on MP4-specific hosts like PixelDrain.
//
// The preview uses filter_complex to extract 12 short clips (~0.5s each)
// from evenly-spaced points across the full video and stitch them together.
// Each clip has consecutive frames for fully smooth motion, unlike a
// frame-sampled timelapse where every frame is a jarring jump.
//
// Thumbnail, sprite, and preview run in parallel with independent timeouts:
//   - thumbnail: 5 min  (single-frame seek)
//   - sprite:    15 min (seeks through full video for long recordings)
//   - preview:   15 min (12× trim + stitch, H.264 encode)
//
// Using separate contexts prevents one task from being killed prematurely
// when a long video causes another to exceed a shared short timeout.
// fileExists returns true if the path exists and is a regular file.
func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir()
}

// runFFmpegParallel runs fn for each index in [0, n) with up to workers
// goroutines running at once.  Each fn is responsible for acquiring its own
// ffmpeg slot (AcquireFFmpeg/ReleaseFFmpeg) so the global ffmpegSem bounds
// total concurrency across all channels.  Returns the first error (others are
// still awaited; their results are discarded).
func runFFmpegParallel(workers, n int, fn func(i int) error) error {
	if workers < 1 {
		workers = 1
	}
	if n < 1 {
		return nil
	}
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	slots := make(chan struct{}, workers)
	for i := 0; i < n; i++ {
		wg.Add(1)
		slots <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := fn(idx); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

func generateThumbnailForFile(videoPath string, info, errFn func(string, ...interface{}), onHost OnHostUploadFunc) ThumbnailResult {
	var result ThumbnailResult
	ext := strings.ToLower(filepath.Ext(videoPath))
	if ext != ".mp4" && ext != ".mkv" && ext != ".ts" {
		return result
	}

	st, err := os.Stat(videoPath)
	if err != nil {
		errFn("thumb: file not found %s: %v", filepath.Base(videoPath), err)
		return result
	}
	// Skip files too small to contain video frames — ffmpeg returns
	// exit code -22 (EINVAL) on header-only fMP4 from failed streams.
	if st.Size() < 100*1024 {
		errFn("thumb: skipping %s: too small (%d bytes)", filepath.Base(videoPath), st.Size())
		return result
	}

	baseName := filepath.Base(videoPath)

	// Probe video duration — short dedicated timeout.
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()

	var dur float64
	config.AcquireFFmpeg()
	probeOut, probeErr := config.FFprobeCommandContext(probeCtx,
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		videoPath,
	).Output()
	config.ReleaseFFmpeg() // release immediately — the 3 goroutines below also need slots
	if probeErr == nil {
		var parseErr error
		dur, parseErr = strconv.ParseFloat(strings.TrimSpace(string(probeOut)), 64)
		if parseErr != nil {
			log.Printf("WARN: could not parse probe duration %q: %v", strings.TrimSpace(string(probeOut)), parseErr)
		}
	}

	// MPEG-TS has no seek index: every -ss before -i still demuxes from byte 0,
	// so extracting 16 sprite tiles + 12 preview clips from a long .ts
	// recording means up to ~28 full-file scans (minutes to tens of minutes
	// for 2-4 h videos on shared hosts).  Remux ONCE to a seekable temp .mp4
	// (stream copy — no re-encode, purely I/O-bound) and extract from that:
	// the single remux read/write replaces all 28 scans, and the subsequent
	// keyframe seeks are instant index-based jumps.
	workPath := videoPath
	if ext == ".ts" {
		remuxCtx, remuxCancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer remuxCancel()
		workDir, mkErr := os.MkdirTemp("", "thumb-seekable-")
		if mkErr != nil {
			errFn("thumb: mkdir temp for seekable remux: %v", mkErr)
		} else {
			defer os.RemoveAll(workDir)
			seekablePath := filepath.Join(workDir, "seekable.mp4")
			config.AcquireFFmpeg()
			remuxErr := config.FFmpegCommandContext(remuxCtx,
				"-y",
				"-i", videoPath,
				"-c", "copy",
				"-movflags", "+faststart",
				seekablePath,
			).Run()
			config.ReleaseFFmpeg()
			if remuxErr == nil && fileExists(seekablePath) {
				workPath = seekablePath
				info("thumb: remuxed %s to seekable temp for fast extraction", baseName)
			} else {
				errFn("thumb: seekable remux failed for %s: %v — extracting directly", baseName, remuxErr)
			}
		}
	}

	thumbDone := make(chan string, 1)
	spriteDone := make(chan string, 1)
	previewDone := make(chan string, 1)

	// Mirror URL maps — populated by goroutines, read after they complete.
	var thumbMirrors, spriteMirrors, previewMirrors map[string]string
	var mirrorsMu sync.Mutex

	// ── Single thumbnail (static frame near the 10% mark) ──────────────────
	// Independent 90-second context: seeking to a single frame is always fast.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [thumb] generating thumbnail for %s: %v", baseName, r)
				select {
				case thumbDone <- "":
				default:
				}
			}
		}()
		thumbCtx, thumbCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer thumbCancel()

		thumbJPG := videoPath + ".thumb.jpg"
		defer os.Remove(thumbJPG)

		seekPos := "00:00:03"
		if dur > 0 && dur < 3 {
			seekPos = fmt.Sprintf("%.2f", dur*0.5)
		} else if dur > 0 {
			seekPos = fmt.Sprintf("%.2f", dur*0.1)
		}

		// generateThumb extracts the single thumbnail frame (slow=true uses
		// slow seek for codecs where fast seek crashes ffmpeg).
		generateThumb := func(slow bool) error {
			args := []string{"-y"}
			if slow {
				args = append(args, "-i", workPath, "-ss", seekPos)
			} else {
				args = append(args, "-ss", seekPos, "-i", workPath)
			}
			args = append(args,
				"-vframes", "1",
				"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
					thumbWidth, thumbHeight, thumbWidth, thumbHeight),
				"-c:v", "mjpeg",
				"-q:v", "5",
				thumbJPG,
			)
			return config.FFmpegCommandContext(thumbCtx, args...).Run()
		}

		config.AcquireFFmpeg()
		defer config.ReleaseFFmpeg()
		err := generateThumb(false)

		if err != nil {
			// Fast seek failed; retry with slow seek (-ss after -i).
			// This handles certain codecs/formats where fast seek causes
			// ffmpeg to crash (exit 0xffffffea on Windows).
			errFn("thumb: fast seek failed for %s: %v, retrying with slow seek", baseName, err)
			err = generateThumb(true)
		}

		// The freshly-written .thumb.jpg can be briefly invisible to os.Stat
		// (Windows AV scanners) OR deleted by a concurrent flow processing the
		// same video (a second pipeline's DeleteSidecarFiles). Either way a
		// missing file at upload time showed up as "pixhost: stat file". Poll
		// briefly, then regenerate (a single-frame extract is fast) before
		// uploading.
		if err == nil && !fileExists(thumbJPG) {
			errFn("thumb: %s missing after generation — regenerating", filepath.Base(thumbJPG))
			err = generateThumb(true)
		}
		if err == nil && !fileExists(thumbJPG) {
			err = fmt.Errorf("thumbnail file %s never appeared", filepath.Base(thumbJPG))
		}

		if err != nil {
			errFn("thumb: failed for %s: %v", baseName, err)
			thumbDone <- ""
			return
		}

		imgUploader := uploader.NewMultiImageUploader()
		thumbURLs := imgUploader.UploadToAllURLs(thumbJPG, func(host, url string) {
			if onHost != nil {
				onHost("thumb", host, url)
			}
		})
		if len(thumbURLs) > 0 {
			mirrorsMu.Lock()
			thumbMirrors = thumbURLs
			mirrorsMu.Unlock()
			// Pick the first successful URL as primary (prefer Catbox > Pixhost > freeimage.host).
			for _, host := range []string{"Catbox", "Pixhost", "freeimage.host"} {
				if url, ok := thumbURLs[host]; ok {
					info("thumb: ✓ %s (uploaded to %d hosts: %s)", baseName, len(thumbURLs), host)
					thumbDone <- url
					return
				}
			}
			// Fallback: just pick any successful URL.
			for _, url := range thumbURLs {
				info("thumb: ✓ %s (uploaded to %d hosts)", baseName, len(thumbURLs))
				thumbDone <- url
				return
			}
		}
		errFn("thumb: upload failed for %s — all hosts rejected", baseName)
		thumbDone <- ""
		}()

	// ── Sprite sheet (4×4 grid covering the full video duration) ───────────
	// Each frame is spriteFrameW×spriteFrameH px; total image is
	// (spriteCols*spriteFrameW) × (spriteRows*spriteFrameH) = 2560×1440.
	// Using 640×360 frames so HiDPI/Retina displays get sharp previews.
	//
	// FAST PATH: instead of decoding the ENTIRE video with a sequential
	// fps=1/INTERVAL filter (which costs one full decode pass — minutes to
	// tens of minutes for 2-4 h recordings on shared hosts), each tile is
	// extracted with an input keyframe seek (-ss before -i).  A seek decodes
	// only ~1 GOP (~1-4 s of frames) instead of the whole file, so the sprite
	// takes ~1 s regardless of recording length.  Tiles land on the keyframe
	// nearest each target position, which is visually identical in a 640×360
	// contact-sheet tile.
	//
	// Independent 15-minute context: extraction runs N quick seeks; a short
	// shared context would cause SIGKILL ("signal: killed") and silently skip
	// sprite generation on slow hosts.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [sprite] generating sprite for %s: %v", baseName, r)
				select {
				case spriteDone <- "":
				default:
				}
			}
		}()
		spriteCtx, spriteCancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer spriteCancel()

		spriteJPG := videoPath + ".sprite.jpg"
		defer os.Remove(spriteJPG)

		tileDir, err := os.MkdirTemp("", "sprite-tiles-")
		if err != nil {
			errFn("sprite: mkdir temp for %s: %v", baseName, err)
			spriteDone <- ""
			return
		}
		defer os.RemoveAll(tileDir)

		// Positions for the 16 tiles, evenly spaced across the video.
		// Clamp so the last tile never seeks past the end.
		positions := make([]float64, spriteFrames)
		if dur > 0 {
			spacing := dur / float64(spriteFrames)
			for i := range positions {
				positions[i] = spacing * float64(i)
			}
		} else {
			// No duration available — fall back to fixed 10 s spacing like
			// the old fps=1/10 filter did.
			for i := range positions {
				positions[i] = 10.0 * float64(i)
			}
		}

		// generateSprite extracts all 16 tiles via keyframe seeks (in parallel,
		// bounded by the global ffmpeg semaphore), then assembles them into the
		// contact sheet with one tile=4x4 pass.
		generateSprite := func() error {
			// Extract one tile via a fast keyframe seek.  Each tile acquires its
			// own ffmpeg slot so tiles run concurrently across the pool; -threads 1
			// keeps a single seek from grabbing the whole CPU when N run at once.
			extractTile := func(i int) error {
				pos := positions[i]
				tilePath := filepath.Join(tileDir, fmt.Sprintf("t%d.jpg", i))
				vf := fmt.Sprintf(
					"scale=%d:%d:force_original_aspect_ratio=decrease:flags=lanczos,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
					spriteFrameW, spriteFrameH,
					spriteFrameW, spriteFrameH,
				)
				// Fast keyframe seek: -ss before -i.  Decodes only the GOP
				// containing the target position.
				seekArgs := []string{
					"-y",
					"-threads", "1",
					"-ss", fmt.Sprintf("%.3f", pos),
					"-i", workPath,
					"-frames:v", "1",
					"-vf", vf,
					"-c:v", "mjpeg",
					"-q:v", "5",
					tilePath,
				}
				config.AcquireFFmpeg()
				err := config.FFmpegCommandContext(spriteCtx, seekArgs...).Run()
				config.ReleaseFFmpeg()
				if err != nil || !fileExists(tilePath) {
					// Fast seek failed (some codecs crash with -ss before -i on
					// Windows, exit 0xffffffea).  Retry with slow seek (-ss after
					// -i); the decode cost is bounded by the GOP, not the file.
					errFn("sprite: tile %d fast seek failed for %s: %v — retrying with slow seek", i, baseName, err)
					slowArgs := []string{
						"-y",
						"-threads", "1",
						"-i", workPath,
						"-ss", fmt.Sprintf("%.3f", pos),
						"-frames:v", "1",
						"-vf", vf,
						"-c:v", "mjpeg",
						"-q:v", "5",
						tilePath,
					}
					config.AcquireFFmpeg()
					err = config.FFmpegCommandContext(spriteCtx, slowArgs...).Run()
					config.ReleaseFFmpeg()
					if err != nil {
						return fmt.Errorf("tile %d at %.0fs: %w", i, pos, err)
					}
				}
				if !fileExists(tilePath) {
					return fmt.Errorf("tile %d at %.0fs never appeared", i, pos)
				}
				return nil
			}

			// Spawn tiles concurrently — 16 sequential ffmpeg spawns (each with
			// process startup + GOP decode) is the sprite's dominant cost.
			// Cap workers at NumCPU so we don't thrash; ffmpegSem bounds the
			// true concurrent ffmpeg count across the whole fleet of channels.
			workers := runtime.NumCPU()
			if workers > spriteFrames {
				workers = spriteFrames
			}
			if err := runFFmpegParallel(workers, spriteFrames, extractTile); err != nil {
				return err
			}

			// Assemble the 16 tiles into the 4×4 contact sheet via the image2
			// demuxer + tile filter (one cheap pass over the tiny JPEGs).
			pattern := filepath.ToSlash(filepath.Join(tileDir, "t%d.jpg"))
			config.AcquireFFmpeg()
			err := config.FFmpegCommandContext(spriteCtx,
				"-y",
				"-framerate", "1",
				"-start_number", "0",
				"-i", pattern,
				"-vf", fmt.Sprintf("tile=%dx%d", spriteCols, spriteRows),
				"-frames:v", "1",
				"-c:v", "mjpeg",
				"-q:v", "5",
				spriteJPG,
			).Run()
			config.ReleaseFFmpeg()
			return err
		}

		err = generateSprite()

		// Same missing-file-at-upload-time race as the thumbnail: a concurrent
		// flow can DeleteSidecarFiles on this video while we're between ffmpeg
		// and the upload. Poll + regenerate before uploading.
		if err == nil && !fileExists(spriteJPG) {
			errFn("sprite: %s missing after generation — regenerating", filepath.Base(spriteJPG))
			err = generateSprite()
		}
		if err == nil && !fileExists(spriteJPG) {
			err = fmt.Errorf("sprite file %s never appeared", filepath.Base(spriteJPG))
		}

		if err != nil {
			errFn("sprite: failed for %s: %v", baseName, err)
			spriteDone <- ""
			return
		}

		imgUploader := uploader.NewMultiImageUploader()
		spriteURLs := imgUploader.UploadToAllURLs(spriteJPG, func(host, url string) {
			if onHost != nil {
				onHost("sprite", host, url)
			}
		})
		if len(spriteURLs) > 0 {
			mirrorsMu.Lock()
			spriteMirrors = spriteURLs
			mirrorsMu.Unlock()
			for _, host := range []string{"Catbox", "Pixhost", "freeimage.host"} {
				if url, ok := spriteURLs[host]; ok {
					info("sprite: ✓ %s (uploaded to %d hosts: %s)", baseName, len(spriteURLs), host)
					spriteDone <- url
					return
				}
			}
			for _, url := range spriteURLs {
				info("sprite: ✓ %s (uploaded to %d hosts)", baseName, len(spriteURLs))
				spriteDone <- url
				return
			}
		}
		errFn("sprite: upload failed for %s — all hosts rejected", baseName)
		spriteDone <- ""
		}()

	// ── Animated WEBP hover preview (smooth clips from across the video, 6s) ─
	// Animated WEBP is used instead of GIF because:
	//   - ~90% smaller file size for the same visual quality
	//   - Full 24-bit color (vs 256-color palette in GIF)
	//   - Smooth native-framerate playback (GIF was variable ~1-8fps)
	//   - Catbox (primary) and ImgBB (fallback) both accept WEBP files, so no
	//     MP4-only host (PixelDrain) is required.
	//
	// Instead of isolated frame sampling (which produces a jerky slideshow),
	// we extract 12 short continuous clips (~0.5s each) from evenly-spaced
	// points across the video and stitch them together.  Each clip has fully
	// smooth motion because frames within it are consecutive.
	//
	//   <6 sec:  no segmenting, plays whole video at normal speed
	//   1 min:   12 clips × 0.5s = 6s (5s between clips)
	//   60 min:  12 clips × 0.5s = 6s (5 min between clips)
	//
	// FAST PATH: the old implementation decoded the ENTIRE video once through
	// a filter_complex (every trim=start=… branch forced a full sequential
	// decode from frame 0), which took minutes to tens of minutes for 2-4 h
	// recordings and even hit the 15-minute timeout — producing truncated
	// ~1.7 s previews.  Each clip is now extracted with an input keyframe
	// seek (-ss before -i), which decodes only ~1 GOP (~1-4 s of frames)
	// before the clip start, so the whole preview takes ~1-2 s regardless of
	// recording length.  Clips land on the keyframe nearest each target
	// position — visually identical for 0.5 s hover clips.
	//
	// Uploaded to Catbox.moe (free, permanent, CDN-backed) with ImgBB
	// as fallback — both return direct file URLs suitable for embedding.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [preview] generating preview for %s: %v", baseName, r)
				select {
				case previewDone <- "":
				default:
				}
			}
		}()
		previewCtx, previewCancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer previewCancel()

		previewPath := videoPath + ".preview.webp"
		// Remove on the final return, but NOT if ffmpeg failed — leave the
		// file on disk so a later restart or manual retry can pick it up.
		var previewGenerated bool
		defer func() {
			if previewGenerated {
				os.Remove(previewPath)
			}
		}()

		// waitForPreviewFile polls with backoff until the preview file is
		// confirmed to exist.  On Windows, an AV scanner (Defender, etc.) can
		// briefly hold an exclusive lock on a newly-created file, causing
		// os.Stat to return ERROR_FILE_NOT_FOUND even though ffmpeg exited
		// successfully.  Retrying with a short delay resolves this.
		waitForPreviewFile := func() bool {
			for delay := 0; delay < 5; delay++ {
				if fileExists(previewPath) {
					return true
				}
				time.Sleep(time.Duration(50*(1<<delay)) * time.Millisecond) // 50, 100, 200, 400, 800 ms
			}
			return false
		}

		config.AcquireFFmpeg()
		defer config.ReleaseFFmpeg()

		var err error
		if dur <= previewDuration || dur <= 0 {
			// Short or unmeasurable video — no segmenting needed, just scale.
			// libwebp needs a constant frame rate, so -r 15 forces CFR.
			err = config.FFmpegCommandContext(previewCtx,
				"-y",
				"-i", workPath,
				"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
				"-c:v", "libwebp",
				"-lossless", "0",
				"-q:v", "60",
				"-r", "15",
				"-an",
				previewPath,
			).Run()
		} else {
			// Extract 12 short clips via keyframe seeks into a temp dir, then
			// concat them and run ONE final WEBP encode.  Each clip is tiny
			// (0.5 s at 320 px), so the intermediate re-encode cost is
			// negligible compared to the full-decode the old filter_complex
			// approach required.
			segDuration := previewDuration / float64(previewSegments)
			step := dur / float64(previewSegments)

			clipDir, mkErr := os.MkdirTemp("", "preview-clips-")
			if mkErr != nil {
				err = fmt.Errorf("mkdir temp clips: %w", mkErr)
			} else {
				defer os.RemoveAll(clipDir)

				// Extract all 12 clips concurrently (bounded by the global ffmpeg
				// semaphore).  The clips are independent keyframe seeks, so 12
				// sequential ffmpeg spawns become ~1 round-trip each.
				extractClip := func(i int) error {
					midpoint := step * (float64(i) + 0.5)
					start := midpoint - segDuration/2
					if start+segDuration > dur {
						start = dur - segDuration
					}
					if start < 0 {
						start = 0
					}

					clipPath := filepath.Join(clipDir, fmt.Sprintf("c%d.mp4", i))
					seekArgs := []string{
						"-y",
						"-threads", "1",
						"-ss", fmt.Sprintf("%.3f", start),
						"-i", workPath,
						"-t", fmt.Sprintf("%.3f", segDuration),
						"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos,setpts=PTS-STARTPTS", previewWidth),
						"-c:v", "libx264",
						"-preset", "ultrafast",
						"-crf", "23",
						"-r", "15",
						"-an",
						clipPath,
					}
					config.AcquireFFmpeg()
					seekErr := config.FFmpegCommandContext(previewCtx, seekArgs...).Run()
					config.ReleaseFFmpeg()
					if seekErr != nil || !fileExists(clipPath) {
						if seekErr != nil {
							errFn("preview: clip %d fast seek failed for %s: %v — retrying with slow seek", i, baseName, seekErr)
						}
						slowArgs := []string{
							"-y",
							"-threads", "1",
							"-i", workPath,
							"-ss", fmt.Sprintf("%.3f", start),
							"-t", fmt.Sprintf("%.3f", segDuration),
							"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos,setpts=PTS-STARTPTS", previewWidth),
							"-c:v", "libx264",
							"-preset", "ultrafast",
							"-crf", "23",
							"-r", "15",
							"-an",
							clipPath,
						}
						config.AcquireFFmpeg()
						seekErr = config.FFmpegCommandContext(previewCtx, slowArgs...).Run()
						config.ReleaseFFmpeg()
						if seekErr != nil {
							return fmt.Errorf("clip %d at %.2fs: %w", i, start, seekErr)
						}
					}
					if !fileExists(clipPath) {
						return fmt.Errorf("clip %d at %.2fs never appeared", i, start)
					}
					return nil
				}

				workers := runtime.NumCPU()
				if workers > previewSegments {
					workers = previewSegments
				}
				if cErr := runFFmpegParallel(workers, previewSegments, extractClip); cErr != nil {
					err = cErr
				}

				if err == nil {
					// Concat the clips with the concat demuxer, then run ONE
					// final WEBP encode over the stitched 6 s.
					listPath := filepath.Join(clipDir, "list.txt")
					var list strings.Builder
					for i := 0; i < previewSegments; i++ {
						clipPath := filepath.ToSlash(filepath.Join(clipDir, fmt.Sprintf("c%d.mp4", i)))
						list.WriteString(fmt.Sprintf("file '%s'\n", clipPath))
					}
					if werr := os.WriteFile(listPath, []byte(list.String()), 0o666); werr != nil {
						err = fmt.Errorf("write concat list: %w", werr)
					} else {
						err = config.FFmpegCommandContext(previewCtx,
							"-y",
							"-f", "concat",
							"-safe", "0",
							"-i", listPath,
							"-c:v", "libwebp",
							"-lossless", "0",
							"-q:v", "60",
							"-r", "15",
							"-an",
							previewPath,
						).Run()
					}
				}
			}

			// If extraction or the concat encode failed, fall back to a simple
			// single-clip preview from the middle of the video.  The old
			// filter_complex could also silently produce no output on some
			// videos (e.g. unusual stream timing), so keep the fallback.
			//
			// Use a fresh context so the fallback gets its own 5-minute
			// timeout instead of inheriting the nearly-expired previewCtx.
			if err != nil || !fileExists(previewPath) {
				if err != nil {
					errFn("preview: clip extraction failed for %s: %v, trying simple fallback", baseName, err)
				} else {
					errFn("preview: clip concat produced no output for %s, trying simple fallback", baseName)
				}
				fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer fallbackCancel()
				err = config.FFmpegCommandContext(fallbackCtx,
					"-y",
					"-ss", fmt.Sprintf("%.2f", dur*0.3),
					"-i", workPath,
					"-t", fmt.Sprintf("%.2f", previewDuration),
					"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
					"-c:v", "libwebp",
					"-lossless", "0",
					"-q:v", "60",
					"-r", "15",
					"-an",
					previewPath,
				).Run()
			}
		}

		if err != nil {
			errFn("preview: failed for %s: %v", baseName, err)
			previewDone <- ""
			return
		}

		if !waitForPreviewFile() {
			errFn("preview: ffmpeg exited successfully but produced no output file for %s", baseName)
			previewDone <- ""
			return
		}

		previewGenerated = true

		imgUploader := uploader.NewMultiImageUploader()
		previewURLs := imgUploader.UploadToAllURLs(previewPath, func(host, url string) {
			if onHost != nil {
				onHost("preview", host, url)
			}
		})
		if len(previewURLs) > 0 {
			mirrorsMu.Lock()
			previewMirrors = previewURLs
			mirrorsMu.Unlock()
			for _, host := range []string{"Catbox", "freeimage.host"} {
				if url, ok := previewURLs[host]; ok {
					info("preview: ✓ %s (uploaded to %d hosts: %s)", baseName, len(previewURLs), host)
					previewDone <- url
					return
				}
			}
			for _, url := range previewURLs {
				info("preview: ✓ %s (uploaded to %d hosts)", baseName, len(previewURLs))
				previewDone <- url
				return
			}
		}
		errFn("preview: all hosts failed for %s (cosmetic — thumbnail+sprite still saved)", baseName)
		previewDone <- ""
	}()

	result.ThumbURL = <-thumbDone
	result.SpriteURL = <-spriteDone
	result.PreviewURL = <-previewDone

	mirrorsMu.Lock()
	result.ThumbMirrors = thumbMirrors
	result.SpriteMirrors = spriteMirrors
	result.PreviewMirrors = previewMirrors
	mirrorsMu.Unlock()

	return result
}
