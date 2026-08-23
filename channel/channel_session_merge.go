package channel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/server"
)

// ─── Session continuity merge ───────────────────────────────────────────────
// Chaturbate's HLS token refreshes roughly every 20 minutes, which makes the
// recorder finalize the current file and start a new one (a "cycle").  Left
// alone this produces one ~20-minute fragment per live session.  To honour the
// "record continuously, split only at max duration" rule we merge consecutive
// cycles of the SAME live session into a single long recording.
//
// Each finalized cycle file is a valid, self-contained MP4/MKV.  We join them
// with ffmpeg's concat demuxer (-c copy), which re-bases the timelines so the
// merged file plays continuously.  We never merge across a max-duration cut
// (that is an intentional split), and we never destroy originals until the
// merge is verified — on any failure the original files are uploaded
// individually so nothing is ever lost.

type sessionMergeEntry struct {
	path               string
	endedReconnecting bool
	updatedAt          time.Time
}

var (
	sessionMergeMu     sync.Mutex
	sessionMergeByUser = map[string]*sessionMergeEntry{}
	// sessionMergeLockByUser serializes merges for a single channel so two
	// finalized cycles can never be merged concurrently (which would race on
	// the shared running-merge file and the group map).
	sessionMergeLockByUser sync.Map // map[string]*sync.Mutex
)

func sessionMergeLock(user string) *sync.Mutex {
	v, _ := sessionMergeLockByUser.LoadOrStore(user, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func isReconnectingReason(r string) bool {
	return strings.Contains(r, "reconnecting")
}

func isMaxDurationReason(r string) bool {
	return r == "max duration or filesize reached"
}

// trySessionMerge decides whether finalPath should be merged into the running
// session for this channel instead of being uploaded on its own.  It returns
// true when the file has been consumed into a merge (the caller must NOT
// upload it individually); false when the file should proceed through the
// normal upload path (either it is a lone recording, or a merge failure forced
// a safe fallback to individual upload).
func (ch *Channel) trySessionMerge(finalPath, endReason string) bool {
	if server.Config == nil || (server.Config.FinalizeMode != "remux" && server.Config.FinalizeMode != "transcode") {
		return false
	}
	if _, err := os.Stat(finalPath); err != nil {
		return false
	}
	user := ch.Config.Username

	// Serialize merges for this channel so concurrent finalizations of the
	// same session can't race on the running-merge file.
	lock := sessionMergeLock(user)
	lock.Lock()
	defer lock.Unlock()

	sessionMergeMu.Lock()
	prev := sessionMergeByUser[user]
	sessionMergeMu.Unlock()

	switch {
	case isMaxDurationReason(endReason):
		// Intentional cut.  Flush the pre-cut group as its own file, then start
		// a fresh group with this file so later cycles merge into a new chunk.
		if prev != nil {
			MarkUploadDone(prev.path)
			ch.flushSessionEntry(prev)
		}
		sessionMergeMu.Lock()
		sessionMergeByUser[user] = &sessionMergeEntry{path: finalPath, endedReconnecting: false, updatedAt: time.Now()}
		sessionMergeMu.Unlock()
		MarkUploadInFlight(finalPath)
		return true

	case isReconnectingReason(endReason):
		// Continuation of the same live session.
		if prev == nil {
			sessionMergeMu.Lock()
			sessionMergeByUser[user] = &sessionMergeEntry{path: finalPath, endedReconnecting: true, updatedAt: time.Now()}
			sessionMergeMu.Unlock()
			MarkUploadInFlight(finalPath)
			return true
		}
		merged, err := mergeTwoFiles(prev.path, finalPath)
		if err != nil {
			// Merge failed: keep both files and let the normal path upload them
			// individually so nothing is lost.
			ch.Error("session merge %s + %s failed: %s — uploading separately", filepath.Base(prev.path), filepath.Base(finalPath), err.Error())
			sessionMergeMu.Lock()
			delete(sessionMergeByUser, user)
			sessionMergeMu.Unlock()
			return false
		}
		MarkUploadInFlight(merged)
		sessionMergeMu.Lock()
		sessionMergeByUser[user] = &sessionMergeEntry{path: merged, endedReconnecting: true, updatedAt: time.Now()}
		sessionMergeMu.Unlock()
		return true

	default:
		// Session ended.  If we have a running group, merge it with this final
		// file and upload the result.  Otherwise this is a lone recording.
		if prev == nil {
			return false
		}
		if !prev.endedReconnecting {
			// prev was a max-duration flush start; upload it, then let this
			// final file upload on its own (the cut boundary is respected).
			MarkUploadDone(prev.path)
			ch.flushSessionEntry(prev)
			return false
		}
		merged, err := mergeTwoFiles(prev.path, finalPath)
		if err != nil {
			ch.Error("session merge (end) %s + %s failed: %s — uploading separately", filepath.Base(prev.path), filepath.Base(finalPath), err.Error())
			sessionMergeMu.Lock()
			delete(sessionMergeByUser, user)
			sessionMergeMu.Unlock()
			return false
		}
		sessionMergeMu.Lock()
		delete(sessionMergeByUser, user)
		sessionMergeMu.Unlock()
		MarkUploadDone(prev.path)
		// Upload the finished, merged session recording.
		ch.MoveToOutputDir(merged, endReason)
		return true
	}
}

// flushSessionEntry uploads a stored (already-merged or single) session file so
// it is no longer held in the running group.
func (ch *Channel) flushSessionEntry(e *sessionMergeEntry) {
	if e == nil || e.path == "" {
		return
	}
	if _, err := os.Stat(e.path); err != nil {
		return
	}
	ch.MoveToOutputDir(e.path, "max duration or filesize reached")
}

// mergeTwoFiles concatenates a then b into a single MP4/MKV via ffmpeg's concat
// demuxer (timeline-rebasing copy).  On success it removes both inputs and
// returns the merged file path.  On failure it leaves the inputs intact and
// returns an error.
func mergeTwoFiles(a, b string) (string, error) {
	outExt := ".mp4"
	if server.Config != nil && server.Config.FFmpegContainer == "mkv" {
		outExt = ".mkv"
	}
	stem := a
	for strings.HasSuffix(stem, ".merged"+outExt) {
		stem = strings.TrimSuffix(stem, ".merged"+outExt)
	}
	mergedPath := stem + ".merged" + outExt

	// Write to a unique temp file first so the output never collides with an
	// input (a re-merge reuses the stable name as its input).
	tmpPath := mergedPath + ".tmp" + outExt
	_ = os.Remove(tmpPath)

	listPath := mergedPath + ".concat.txt"
	list := fmt.Sprintf("file '%s'\nfile '%s'\n", escapeConcatPath(a), escapeConcatPath(b))
	if err := os.WriteFile(listPath, []byte(list), 0666); err != nil {
		return "", fmt.Errorf("write concat list: %w", err)
	}
	defer os.Remove(listPath)

	args := []string{"-nostdin", "-y", "-f", "concat", "-safe", "0", "-i", listPath, "-c", "copy"}
	if outExt == ".mp4" {
		args = append(args, "-movflags", "+faststart")
	}
	args = append(args, tmpPath)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	config.AcquireFFmpegHeavy()
	defer config.ReleaseFFmpegHeavy()
	out, err := config.FFmpegCommandContext(ctx, args...).CombinedOutput()
	if err != nil {
		_ = os.Remove(tmpPath)
		msg := strings.TrimSpace(string(out))
		if len(msg) > 600 {
			msg = msg[len(msg)-600:]
		}
		return "", fmt.Errorf("%s", msg)
	}

	// Validate the merged duration is (approximately) the sum of the inputs.
	inA, errA := VideoDurationSeconds(a)
	inB, errB := VideoDurationSeconds(b)
	if errA == nil && errB == nil {
		want := inA + inB
		got, errG := VideoDurationSeconds(tmpPath)
		if errG == nil && want > 0 && got < want*0.85 {
			_ = os.Remove(tmpPath)
			return "", fmt.Errorf("merged duration %.1fs < 85%% of inputs %.1fs", got, want)
		}
	}

	// Success: remove the originals, then publish the merged file under the
	// stable session-start name.
	if rmErr := os.Remove(a); rmErr != nil && !os.IsNotExist(rmErr) {
		_ = rmErr
	}
	if rmErr := os.Remove(b); rmErr != nil && !os.IsNotExist(rmErr) {
		_ = rmErr
	}
	if err := os.Rename(tmpPath, mergedPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("rename merged output: %w", err)
	}
	return mergedPath, nil
}

// escapeConcatPath quotes a path for ffmpeg's concat demuxer list format and
// normalizes Windows separators so absolute paths resolve correctly.
func escapeConcatPath(p string) string {
	p = filepath.ToSlash(p)
	return strings.ReplaceAll(p, "'", "'\\''")
}
