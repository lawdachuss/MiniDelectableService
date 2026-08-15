package channel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/notifier"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/site"
)

// Monitor starts monitoring the channel for live streams and records them.
// runID identifies this monitor run so late-arriving segments from a
// previous run can be rejected.
func (ch *Channel) Monitor(runID uint64) {
	defer ch.finishMonitor()

	s := resolveSite(ch)
	req := internal.NewReq()
	ch.Info("starting to record `%s` (%s)", ch.Config.Username, ch.Config.Site)

	// Seed total disk usage in the background so the UI shows it immediately.
	go ch.ScanTotalDiskUsage()

	// Seed StreamedAt from the site API if we haven't seen this channel stream yet.
	if ch.StreamedAt == 0 {
		if ts, err := s.FetchLastBroadcast(context.Background(), req, ch.Config.Username); err == nil && ts > 0 {
			ch.StreamedAt = ts
			ch.Config.StreamedAt = ts
			_ = server.Manager.SaveConfig()
			ch.Update()
		}
	}

	// On-demand full-profile scrape: piggybacks on the biocontext call above
	// (same endpoint family) and stores the complete channel details for the
	// archive site. Rate-limited so a fast pause/resume cycle doesn't re-scrape.
	ch.scrapeProfileOnDemand(s, req)

	// Create a new context with a cancel function; the CancelFunc is stored on
	// the channel and invoked by Pause/Stop.
	ctx, _ := ch.WithCancel(context.Background())

	var err error
	for {
		if err = ctx.Err(); err != nil {
			break
		}

		pipeline := func() error {
			return ch.RecordStream(ctx, runID, s, req)
		}
		// isExpectedOffline returns true for errors where the full interval
		// delay is appropriate. Transient errors (502, decode errors, network
		// hiccups) should retry quickly.
		isExpectedOffline := func(err error) bool {
			return errors.Is(err, internal.ErrChannelOffline) ||
				errors.Is(err, internal.ErrPrivateStream) ||
				errors.Is(err, internal.ErrHiddenStream) ||
				errors.Is(err, internal.ErrNotFound) ||
				errors.Is(err, internal.ErrAgeVerification) ||
				errors.Is(err, internal.ErrCloudflareBlocked) ||
				errors.Is(err, internal.ErrRoomPasswordRequired)
		}
		onRetry := func(_ uint, err error) {
			ch.UpdateOnlineStatus(false)

			// Reset the CF block count whenever a non-CF response is received.
			if !errors.Is(err, internal.ErrCloudflareBlocked) && ch.CFBlockCount > 0 {
				ch.CFBlockCount = 0
				server.Manager.ResetCFBlock(ch.Config.Username)
				notifier.Default.ResetCooldown(fmt.Sprintf(notifier.KeyCFChannel, ch.Config.Username))
			}

			if errors.Is(err, internal.ErrChannelOffline) {
				ch.Info("channel is offline, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrNotFound) {
				ch.Info("channel not found (deleted/renamed), try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrPrivateStream) {
				ch.Info("channel is in a private show, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrHiddenStream) {
				ch.Info("channel is hidden, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrCloudflareBlocked) {
				ch.CFBlockCount++
				cfThresh := server.Config.CFChannelThreshold
				if cfThresh <= 0 {
					cfThresh = 5
				}
				if ch.CFBlockCount >= cfThresh {
					notifier.Notify(
						fmt.Sprintf(notifier.KeyCFChannel, ch.Config.Username),
						"⚠️ Cloudflare Block",
						fmt.Sprintf("`%s` has been blocked by Cloudflare %d times consecutively", ch.Config.Username, ch.CFBlockCount),
					)
				}
				server.Manager.ReportCFBlock(ch.Config.Username)
				ch.Info("channel was blocked by Cloudflare; try with `-cookies` and `-user-agent`? try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrAgeVerification) {
				ch.Info("age verification required; pass cookies with `-cookies` to authenticate, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrRoomPasswordRequired) {
				ch.Info("room requires a password, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, context.Canceled) {
				// channel stopped/paused — silent
			} else if errors.Is(err, internal.ErrCircuitBreakerOpen) {
				ch.Info("%s: upstream circuit breaker open (cooldown ~%v), will retry once it cools down",
					ch.Config.Username, internal.CircuitBreakerCooldown())
			} else {
				ch.Error("on retry: %s: retrying in 10s", err.Error())
			}
		}
		delayFn := func(_ uint, err error, _ *retry.Config) time.Duration {
			// Cloudflare-blocked channels back off much longer than the normal
			// interval: with hundreds of blocked channels a 1-minute retry
			// floods Cloudflare and keeps the block alive, while the node-level
			// cookie refresh needs minutes to mint a fresh cf_clearance anyway.
			if errors.Is(err, internal.ErrCloudflareBlocked) {
				mins := server.Config.CFRetryMinutes
				if mins <= 0 {
					mins = 5
				}
				base := time.Duration(mins) * time.Minute
				jitter := time.Duration(rand.Int63n(int64(base/5))) - base/10 // ±10% of base
				return base + jitter
			}
			if isExpectedOffline(err) {
				base := time.Duration(server.Config.Interval) * time.Minute
				jitter := time.Duration(rand.Int63n(int64(base/5))) - base/10 // ±10% of interval
				return base + jitter
			}
			// Transient error (502, decode failure, network hiccup) — recover quickly.
			return 10 * time.Second
		}
		if err = retry.Do(
			pipeline,
			retry.Context(ctx),
			retry.Attempts(0),
			retry.DelayType(delayFn),
			retry.OnRetry(onRetry),
		); err != nil {
			break
		}
	}

	// Classify the final error so the cleanup log explains WHY the recording
	// ended. Skip the canceled case — pause/stop already set a precise reason
	// (manual / handoff) before canceling the context.
	if err != nil && !errors.Is(err, context.Canceled) {
		switch {
		case errors.Is(err, internal.ErrChannelOffline):
			ch.setCloseReason("channel went offline")
		case errors.Is(err, internal.ErrPrivateStream):
			ch.setCloseReason("channel entered a private show")
		case errors.Is(err, internal.ErrStreamStalled):
			ch.setCloseReason("stream session expired (no new segments)")
		case errors.Is(err, internal.ErrNotFound):
			ch.setCloseReason("channel not found (deleted/renamed)")
		case errors.Is(err, internal.ErrAgeVerification):
			ch.setCloseReason("age verification required")
		case errors.Is(err, internal.ErrCloudflareBlocked):
			ch.setCloseReason("cloudflare blocked")
		case errors.Is(err, internal.ErrRoomPasswordRequired):
			ch.setCloseReason("room password required")
		default:
			ch.setCloseReason(fmt.Sprintf("record stream error: %v", err))
		}
	}

	// Always cleanup when monitor exits, regardless of error.
	// On stop/pause the file is queued for processing by Stop(); on an
	// error-exit it is finalized and uploaded immediately.
	mode := CloseProcess
	if ctx.Err() != nil {
		mode = CloseQueue
	}
	if err := ch.Cleanup(mode); err != nil {
		ch.Error("cleanup on monitor exit: %s", err.Error())
	}

	// Log error if it's not a context cancellation.
	if err != nil && !errors.Is(err, context.Canceled) {
		ch.Error("record stream: %s", err.Error())
	}
}

// RecordStream records the stream of the channel using the provided site and
// HTTP client. It retrieves the stream information and starts watching the
// segments, writing them live-muxed into a single file.
func (ch *Channel) RecordStream(ctx context.Context, runID uint64, s site.Site, req *internal.Req) error {
	ch.fileMu.Lock()
	ch.mp4InitSegment = nil
	ch.fileMu.Unlock()

	streamInfo, err := s.FetchStream(ctx, req, ch.Config.Username)

	// Update static metadata whenever the site API returns it, even if the room
	// is currently offline/private/hidden. This keeps the archive index fresh.
	changed := false
	thumbChanged := false
	if streamInfo != nil {
		if streamInfo.RoomTitle != "" && streamInfo.RoomTitle != ch.RoomTitle {
			ch.RoomTitle = streamInfo.RoomTitle
			ch.Config.RoomTitle = streamInfo.RoomTitle
			changed = true
		}
		if streamInfo.Gender != "" && streamInfo.Gender != ch.Gender {
			ch.Gender = streamInfo.Gender
			ch.Config.Gender = streamInfo.Gender
			changed = true
		}
		if streamInfo.SummaryCardImage != "" && streamInfo.SummaryCardImage != ch.SummaryCardImage {
			ch.SummaryCardImage = streamInfo.SummaryCardImage
			ch.Config.SummaryCardImage = streamInfo.SummaryCardImage
			changed = true
			thumbChanged = true
		}
		if changed {
			_ = server.Manager.SaveConfig()
			ch.Update()
			_ = thumbChanged
		}
	}

	if err != nil {
		return fmt.Errorf("get stream: %w", err)
	}
	if streamInfo == nil {
		// Site returned nil, nil — channel is offline.
		return fmt.Errorf("get stream: %w", internal.ErrChannelOffline)
	}

	ch.StreamedAt = time.Now().Unix()
	ch.Config.StreamedAt = ch.StreamedAt
	_ = server.Manager.SaveConfig()
	ch.Sequence = 0
	ch.Viewers = streamInfo.NumUsers
	if ch.LiveThumbURL != streamInfo.LiveThumbURL {
		ch.LiveThumbURL = streamInfo.LiveThumbURL
		ch.Update()
	}

	playlist, err := chaturbate.FetchPlaylist(ctx, streamInfo.HLSSource, ch.Config.Resolution, ch.Config.Framerate, streamInfo.CDNReferer, streamInfo.MouflonPDKey)
	if err != nil {
		return fmt.Errorf("get playlist: %w", err)
	}

	ch.FileExt = playlist.FileExt
	if err := ch.NextFile(playlist.FileExt); err != nil {
		return fmt.Errorf("next file: %w", err)
	}

	// Ensure the file is cleaned up when this function exits in any case.
	defer func() {
		if err := ch.Cleanup(CloseProcess); err != nil {
			ch.Error("cleanup on record stream exit: %s", err.Error())
		}
	}()

	ch.stateMu.Lock()
	ch.RoomStatus = site.StatusPublic
	ch.stateMu.Unlock()
	ch.UpdateOnlineStatus(true) // Update online status after playlist is OK

	// Reset CF state on successful stream start.
	ch.CFBlockCount = 0
	notifier.Default.ResetCooldown(fmt.Sprintf(notifier.KeyCFChannel, ch.Config.Username))
	server.Manager.ResetCFBlock(ch.Config.Username)

	// Notify stream online if enabled.
	if server.Config.NotifyStreamOnline {
		title := fmt.Sprintf("📡 %s is live!", ch.Config.Username)
		body := ch.RoomTitle
		if body == "" {
			body = ch.Config.Username
		}
		notifier.Notify(fmt.Sprintf(notifier.KeyStreamOnline, ch.Config.Username), title, body)
	}

	streamType := "HLS"
	if playlist.FileExt == ".mp4" {
		if playlist.AudioPlaylistURL != "" {
			streamType = "LL-HLS (video+audio)"
		} else if playlist.MouflonPDKey != "" {
			streamType = "HLS (fMP4)"
		} else {
			streamType = "LL-HLS (video only)"
		}
	}
	ch.Info("stream type: %s, resolution %dp (target: %dp), framerate %dfps (target: %dfps)",
		streamType, playlist.Resolution, ch.Config.Resolution, playlist.Framerate, ch.Config.Framerate)
	if ch.Viewers > 0 {
		ch.Info("status: %d viewers", ch.Viewers)
	}
	if ch.RoomTitle != "" {
		title := ch.RoomTitle
		if len(title) > 80 {
			title = title[:80] + "…"
		}
		ch.Info("status: room title: %s", title)
	}

	watchErr := playlist.WatchSegments(ctx, func(b []byte, duration float64) error {
		return ch.handleSegmentForMonitor(runID, b, duration)
	})
	if watchErr != nil {
		if errors.Is(watchErr, internal.ErrStreamStalled) {
			ch.setCloseReason("stream session expired (no new segments)")
		} else if errors.Is(watchErr, context.Canceled) {
			// Paused or stopped — the pause/stop path already set a reason.
		} else if errors.Is(watchErr, internal.ErrMediaForbidden) || errors.Is(watchErr, internal.ErrNotFound) {
			// A CDN 403/404 mid-recording is ambiguous: the public stream may
			// have really ended (private show / offline) OR the HLS session
			// token may have expired while the model is still live. The CDN
			// cannot tell us, so probe the site API — it knows the room's
			// true room_status — instead of blindly labeling every CDN
			// rejection a private show (which ended live recordings and
			// benched the channel for the whole offline interval).
			if _, err := s.FetchStream(ctx, req, ch.Config.Username); err != nil {
				switch {
				case errors.Is(err, internal.ErrPrivateStream):
					ch.setCloseReason("channel entered a private show")
				case errors.Is(err, internal.ErrChannelOffline), errors.Is(err, internal.ErrNotFound):
					ch.setCloseReason("channel went offline")
				case errors.Is(err, context.Canceled):
					// paused/stopped — the pause/stop path already set a reason.
				default:
					ch.setCloseReason(fmt.Sprintf("HLS stream ended: %v", watchErr))
				}
			} else {
				// Model is STILL live — the CDN session/token expired or the
				// edge hiccuped. Treat it like a stalled session so the
				// Monitor reconnects with a fresh HLS URL in seconds instead
				// of ending the session and waiting out the offline interval.
				ch.setCloseReason("stream session expired (HLS session/token) — reconnecting")
				watchErr = internal.ErrStreamStalled
			}
		} else {
			ch.setCloseReason(fmt.Sprintf("HLS stream ended: %v", watchErr))
		}
	}
	return watchErr
}

// scrapeProfileOnDemand fetches the model's full public profile via the site
// API and persists it to the channels table for the archive site. On-demand
// only: it runs once per monitor start, at most once per profileScrapeMin
// minutes per channel, and never blocks recording (fire-and-forget).
func (ch *Channel) scrapeProfileOnDemand(s site.Site, req *internal.Req) {
	const profileScrapeMin = 30

	ch.profileMu.Lock()
	if time.Since(ch.lastProfileScrape) < profileScrapeMin*time.Minute {
		ch.profileMu.Unlock()
		return
	}
	ch.lastProfileScrape = time.Now()
	ch.profileMu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [%s] profile scrape: %v", ch.Config.Username, r)
			}
		}()
		p, err := s.FetchProfile(context.Background(), req, ch.Config.Username)
		if err != nil {
			ch.Verbose("profile scrape for %s: %v", ch.Config.Username, err)
			return
		}
		if p == nil {
			return // site has no profile API (e.g. Stripchat)
		}
		if err := server.SaveChannelProfile(p); err != nil {
			ch.Verbose("profile scrape save for %s: %v", ch.Config.Username, err)
			return
		}
		ch.Verbose("profile scraped for %s", ch.Config.Username)
	}()
}

// handleSegmentForMonitor processes and writes segment data for a specific
// monitor run, ignoring stale late-arriving segments from older runs.
func (ch *Channel) handleSegmentForMonitor(runID uint64, b []byte, duration float64) error {
	ch.fileMu.Lock()
	ch.monitorMu.Lock()
	isPaused := ch.Config.IsPaused.Load()
	isCurrentRun := ch.monitorRunID == runID
	ch.monitorMu.Unlock()

	if isPaused || !isCurrentRun {
		ch.fileMu.Unlock()
		return retry.Unrecoverable(internal.ErrPaused)
	}

	if ch.File == nil {
		ch.fileMu.Unlock()
		return fmt.Errorf("write file: no active file")
	}

	if isMP4InitSegment(b) {
		ch.mp4InitSegment = append(ch.mp4InitSegment[:0], b...)
	}
	if ch.FileExt == ".mp4" && ch.Filesize == 0 && !isMP4InitSegment(b) && len(ch.mp4InitSegment) > 0 {
		n, err := ch.File.Write(ch.mp4InitSegment)
		if err != nil {
			ch.fileMu.Unlock()
			return fmt.Errorf("write mp4 init segment: %w", err)
		}
		ch.Filesize += n
	}

	n, err := ch.File.Write(b)
	if err != nil {
		ch.fileMu.Unlock()
		return fmt.Errorf("write file: %w", err)
	}

	ch.Filesize += n
	ch.Duration += duration
	formattedDuration := internal.FormatDuration(ch.Duration)
	formattedFilesize := internal.FormatFilesize(ch.Filesize)
	shouldSwitch := ch.shouldSwitchFileLocked()

	var newFilename string
	if shouldSwitch {
		ch.closeReason = "max duration or filesize reached"
		if err := ch.cleanupLocked(); err != nil {
			ch.fileMu.Unlock()
			return fmt.Errorf("next file: %w", err)
		}
		filename, err := ch.generateFilenameLocked()
		if err != nil {
			ch.fileMu.Unlock()
			return err
		}
		if err := ch.createNewFileLocked(filename, ch.FileExt); err != nil {
			ch.fileMu.Unlock()
			return fmt.Errorf("next file: %w", err)
		}
		ch.Sequence++
		newFilename = ch.File.Name()
	}
	ch.fileMu.Unlock()

	// After a rotation, kick off post-processing (finalize + upload) of any
	// queued files so long recordings upload each part as it completes.
	if shouldSwitch {
		ch.flushPending()
	}

	ch.Verbose("duration: %s, filesize: %s", formattedDuration, formattedFilesize)

	// Send an SSE update to update the view.
	ch.Update()

	if newFilename != "" {
		ch.Info("max filesize or duration exceeded, new file created: %s", newFilename)
	}
	return nil
}
