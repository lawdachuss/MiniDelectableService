package channel

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// generateThumbnail is the channel-scoped wrapper — logs go to the channel log.
func (ch *Channel) generateThumbnail(videoPath string) (thumbURL, spriteURL, previewURL string) {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { ch.Info(f, a...) },
		func(f string, a ...interface{}) { ch.Error(f, a...) },
	)
}

// GenerateThumbnailForFile is a standalone thumbnail generator that can be
// called outside of a channel context (e.g. for pre-existing video files).
func GenerateThumbnailForFile(videoPath string) (thumbURL, spriteURL, previewURL string) {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { log.Printf("[thumb] "+f, a...) },
		func(f string, a ...interface{}) { log.Printf("[thumb:err] "+f, a...) },
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

func generateThumbnailForFile(videoPath string, info, errFn func(string, ...interface{})) (thumbURL, spriteURL, previewURL string) {
	ext := strings.ToLower(filepath.Ext(videoPath))
	if ext != ".mp4" && ext != ".mkv" && ext != ".ts" {
		return "", "", ""
	}

	st, err := os.Stat(videoPath)
	if err != nil {
		errFn("thumb: file not found %s: %v", filepath.Base(videoPath), err)
		return "", "", ""
	}
	// Skip files too small to contain video frames — ffmpeg returns
	// exit code -22 (EINVAL) on header-only fMP4 from failed streams.
	if st.Size() < 100*1024 {
		errFn("thumb: skipping %s: too small (%d bytes)", filepath.Base(videoPath), st.Size())
		return "", "", ""
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

	// Compute the interval so we get exactly spriteFrames frames spread
	// evenly across the whole video.  Clamp to at least 0.1 s.
	interval := 10.0
	if dur > 0 {
		interval = dur / float64(spriteFrames)
		if interval < 0.1 {
			interval = 0.1
		}
	}

	thumbDone := make(chan string, 1)
	spriteDone := make(chan string, 1)
	previewDone := make(chan string, 1)

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

		config.AcquireFFmpeg()
		defer config.ReleaseFFmpeg()
		err := config.FFmpegCommandContext(thumbCtx,
			"-y",
			"-ss", seekPos,
			"-i", videoPath,
			"-vframes", "1",
			"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
				thumbWidth, thumbHeight, thumbWidth, thumbHeight),
			"-c:v", "mjpeg",
			"-q:v", "5",
			thumbJPG,
		).Run()

		if err != nil {
			// Fast seek failed; retry with slow seek (-ss after -i).
			// This handles certain codecs/formats where fast seek causes
			// ffmpeg to crash (exit 0xffffffea on Windows).
			errFn("thumb: fast seek failed for %s: %v, retrying with slow seek", baseName, err)
			err = config.FFmpegCommandContext(thumbCtx,
				"-y",
				"-i", videoPath,
				"-ss", seekPos,
				"-vframes", "1",
				"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
					thumbWidth, thumbHeight, thumbWidth, thumbHeight),
				"-c:v", "mjpeg",
				"-q:v", "5",
				thumbJPG,
			).Run()
		}

		if err != nil {
			errFn("thumb: failed for %s: %v", baseName, err)
			thumbDone <- ""
			return
		}

		imgUploader := uploader.NewMultiImageUploader()
		if remoteURL, _, uploadErr := imgUploader.Upload(thumbJPG); uploadErr == nil {
			info("thumb: ✓ %s", baseName)
			thumbDone <- remoteURL
		} else {
			errFn("thumb: upload failed for %s: %v", baseName, uploadErr)
			thumbDone <- ""
		}
	}()

	// ── Sprite sheet (4×4 grid covering the full video duration) ───────────
	// Each frame is spriteFrameW×spriteFrameH px; total image is
	// (spriteCols*spriteFrameW) × (spriteRows*spriteFrameH) = 2560×1440.
	// Using 640×360 frames so HiDPI/Retina displays get sharp previews.
	//
	// Independent 15-minute context: for long recordings (1 h+), ffmpeg must
	// seek to 16 evenly-spaced positions, which can take several minutes on a
	// slow or resource-constrained host.  A short shared context would cause
	// SIGKILL ("signal: killed") and silently skip sprite generation.
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

		// fps=1/INTERVAL extracts one frame per interval.
		// scale with lanczos gives sharper results than the default bilinear.
		// pad keeps each tile at exactly spriteFrameW×spriteFrameH.
		// tile=COLSxROWS assembles them into the contact sheet.
		vf := fmt.Sprintf(
			"fps=1/%.4f,scale=%d:%d:force_original_aspect_ratio=decrease:flags=lanczos,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,tile=%dx%d",
			interval,
			spriteFrameW, spriteFrameH,
			spriteFrameW, spriteFrameH,
			spriteCols, spriteRows,
		)

		config.AcquireFFmpeg()
		defer config.ReleaseFFmpeg()
		err := config.FFmpegCommandContext(spriteCtx,
			"-y",
			"-i", videoPath,
			"-vf", vf,
			"-frames:v", "1",
			"-c:v", "mjpeg",
			"-q:v", "5",
			spriteJPG,
		).Run()

		if err != nil {
			errFn("sprite: failed for %s: %v", baseName, err)
			spriteDone <- ""
			return
		}

		imgUploader := uploader.NewMultiImageUploader()
		if remoteURL, _, uploadErr := imgUploader.Upload(spriteJPG); uploadErr == nil {
			info("sprite: ✓ %s", baseName)
			spriteDone <- remoteURL
		} else {
			errFn("sprite: upload failed for %s: %v", baseName, uploadErr)
			spriteDone <- ""
		}
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
				"-i", videoPath,
				"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
				"-c:v", "libwebp",
				"-lossless", "0",
				"-q:v", "60",
				"-r", "15",
				"-an",
				previewPath,
			).Run()
		} else {
			// Build a filter_complex that extracts clips and stitches them.
			segDuration := previewDuration / float64(previewSegments)
			step := dur / float64(previewSegments)

			var filterParts []string
			var concatInputs []string

			for i := 0; i < previewSegments; i++ {
				midpoint := step * (float64(i) + 0.5)
				start := midpoint - segDuration/2

				if start+segDuration > dur {
					start = dur - segDuration
				}
				if start < 0 {
					start = 0
				}

				label := fmt.Sprintf("v%d", i)
				filterParts = append(filterParts, fmt.Sprintf(
					"[0:v]trim=start=%.3f:duration=%.3f,setpts=PTS-STARTPTS,scale=%d:-2:flags=lanczos[%s]",
					start, segDuration, previewWidth, label,
				))
				concatInputs = append(concatInputs, fmt.Sprintf("[%s]", label))
			}

			filterComplex := strings.Join(filterParts, ";") + ";" +
				strings.Join(concatInputs, "") +
				fmt.Sprintf("concat=n=%d:v=1:a=0[out]", previewSegments)

			err = config.FFmpegCommandContext(previewCtx,
				"-y",
				"-i", videoPath,
				"-filter_complex", filterComplex,
				"-map", "[out]",
				"-c:v", "libwebp",
				"-lossless", "0",
				"-q:v", "60",
				"-r", "15",
				"-an",
				previewPath,
			).Run()

			// If the complex filter_complex failed, fall back to a simple
			// single-clip preview from the middle of the video.  The
			// filter_complex can silently produce no output on some videos
			// (e.g. when the video stream has unusual timing), causing the
			// uploader to fail with "file not found" even though ffmpeg
			// exited 0.
			//
			// Use a fresh context so the fallback gets its own 5-minute
			// timeout instead of inheriting the nearly-expired previewCtx.
			if err != nil || !fileExists(previewPath) {
				if err != nil {
					errFn("preview: complex filter failed for %s: %v, trying simple fallback", baseName, err)
				} else {
					errFn("preview: complex filter produced no output for %s, trying simple fallback", baseName)
				}
				fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer fallbackCancel()
				err = config.FFmpegCommandContext(fallbackCtx,
					"-y",
					"-ss", fmt.Sprintf("%.2f", dur*0.3),
					"-i", videoPath,
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

		catboxUploader := uploader.NewCatboxUploader()
		imgbbUploader := uploader.NewImgBBUploader()

		remoteURL, uploadErr := catboxUploader.Upload(previewPath)
		if uploadErr != nil {
			errFn("preview: catbox failed for %s: %v, trying ImgBB fallback", baseName, uploadErr)
			remoteURL, uploadErr = imgbbUploader.Upload(previewPath)
		}

		if uploadErr == nil {
			info("preview: ✓ %s", baseName)
			previewDone <- remoteURL
		} else {
			errFn("preview: all hosts failed for %s: %v", baseName, uploadErr)
			previewDone <- ""
		}
	}()

	thumbURL = <-thumbDone
	spriteURL = <-spriteDone
	previewURL = <-previewDone

	return thumbURL, spriteURL, previewURL
}
