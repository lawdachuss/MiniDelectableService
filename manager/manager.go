package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/r3labs/sse/v2"
	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/coordinator"
	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/notifier"
	"github.com/teacat/chaturbate-dvr/router/view"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/watcher"
)

// renderCacheEntry holds the last-rendered HTML and a fingerprint of the
// ChannelInfo fields that affect template output. When the fingerprint
// matches a previous publish, the SSE event is skipped entirely.
type renderCacheEntry struct {
	html        []byte
	fingerprint string
}

// channelInfoFingerprint produces a string that changes whenever any
// field displayed in the channel_info template changes. This is used
// to skip redundant template renders + SSE pushes.
func channelInfoFingerprint(info *entity.ChannelInfo) string {
	return fmt.Sprintf("%t|%t|%t|%t|%t|%s|%s|%s|%s|%s|%s|%.0f|%s",
		info.IsOnline,
		info.IsConnecting,
		info.IsPaused,
		info.IsCompressing,
		info.AutoResumedFromPause,
		info.RoomStatus,
		info.Duration,
		info.Filesize,
		info.Filename,
		info.StreamedAt,
		info.UploadStatus,
		info.UploadProgress,
		info.UploadFilename,
	)
}

// Manager is responsible for managing channels and their states.
type Manager struct {
	Channels sync.Map
	SSE      *sse.Server

	// Coordinator for distributed shards/nodes mode (nil in isolated mode).
	Coordinator *coordinator.Coordinator

	// watcherDone is closed during graceful shutdown so the fsnotify
	// watcher can release its file handles promptly.
	WatcherDone chan struct{}

	// saveDebounce coalesces rapid SaveConfig calls into a single
	// Supabase PATCH.  The first call starts a 1 s timer; subsequent
	// calls reset it.  When the timer fires, the actual save runs.
	// This prevents API hammering when many channels are paused,
	// resumed, or stopped in quick succession.
	saveDebounce   *time.Timer
	saveDebounceMu sync.Mutex

	// logRateLimit rate-limits SSE log events to at most 1 per second
	// per channel. Log lines still go to the in-memory buffer and
	// terminal output; only the SSE broadcast is throttled to prevent
	// browser lag when many channels are recording simultaneously.
	logRateLimit   map[string]time.Time
	logRateLimitMu sync.Mutex

	// thumbThrottle spaces out thumbnail regeneration so a mass backfill
	// (fresh node, wiped preview_images, host-outage recovery) can't exceed
	// the image hosts' rate limits, and skips files attempted recently so a
	// permanently-broken recording isn't re-hammered on every scan.
	// thumbScanning guards against the startup scan and the periodic ticker
	// overlapping (both would pass the cooldown check before either records).
	thumbThrottle   map[string]time.Time
	thumbThrottleMu sync.Mutex
	thumbScanning   bool

	// renderCache caches the last-rendered channel_info HTML per
	// channel.  Publish() skips the SSE push when the fingerprint
	// is unchanged, which eliminates redundant template execution
	// and browser DOM replacements for offline/paused channels.
	renderCache   map[string]*renderCacheEntry
	renderCacheMu sync.Mutex

	// watcherMu guards WatcherDone close + reset so the session loop
	// and the SIGTERM handler cannot panic on double-close.
	watcherMu     sync.Mutex
	watcherClosed bool

	// sessionDeadline tracks when the current recording session will end
	// (zero = no active session, recording or between cycles).
	sessionDeadline   time.Time
	sessionDeadlineMu sync.Mutex
	sessionDuration   time.Duration

	// sessionStopCh is created each sessionLoop iteration; TriggerSessionStop
	// sends on it to break out of the timer early and start processing.
	sessionStopCh chan struct{}
	sessionStopMu sync.Mutex

	// sessionMu prevents multiple concurrent sessionLoop goroutines when
	// StartSession is called more than once (e.g. from create-channel handler).
	sessionMu       sync.Mutex
	sessionStarted  bool
	sessionStopped  bool // set by StopSession to permanently stop the loop

	// Cloudflare block tracking: channels currently in a blocked state, plus
	// whether the global multi-channel alert has already fired.
	cfMu            sync.Mutex
	cfBlocked       map[string]struct{}
	cfGlobalNotified bool

	// cfRefreshFn is the registered cookie re-mint function (runs the cookie
	// scripts + reloads cookies from Supabase). Fired by RequestCookieRefresh
	// when the node is Cloudflare-starved, rate-limited by cfRefreshLast so a
	// persistent block can't trigger a refresh storm.
	cfRefreshFn   func()
	cfRefreshMu   sync.Mutex
	cfRefreshLast time.Time
}

// TriggerSessionStop signals the session loop to stop recording now and
// begin the mux/upload/processing phase early. No-op if no active session.
func (m *Manager) TriggerSessionStop() {
	m.sessionStopMu.Lock()
	ch := m.sessionStopCh
	m.sessionStopMu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// StopSession permanently stops the session loop so it won't restart
// after the current cycle finishes.  Call before StopAllChannels during
// graceful shutdown to prevent the loop from racing with teardown.
func (m *Manager) StopSession() {
	m.TriggerSessionStop()
	m.sessionMu.Lock()
	m.sessionStopped = true
	m.sessionMu.Unlock()
}

// SessionInfo returns the remaining recording time and whether a session
// is currently active (recording phase, not processing phase).
func (m *Manager) SessionInfo() (remaining time.Duration, active bool) {
	m.sessionDeadlineMu.Lock()
	defer m.sessionDeadlineMu.Unlock()
	if m.sessionDeadline.IsZero() {
		return 0, false
	}
	remaining = time.Until(m.sessionDeadline)
	if remaining <= 0 {
		return 0, false
	}
	return remaining, true
}

// New initializes a new Manager instance with an SSE server.
func New() (*Manager, error) {

	server := sse.New()
	server.SplitData = true

	updateStream := server.CreateStream("updates")
	updateStream.AutoReplay = false

	return &Manager{
		SSE:           server,
		logRateLimit:  make(map[string]time.Time),
		thumbThrottle: make(map[string]time.Time),
		renderCache:   make(map[string]*renderCacheEntry),
		WatcherDone:   make(chan struct{}),
		cfBlocked:     make(map[string]struct{}),
	}, nil
}

// debouncedSave is a non-blocking request to persist channel state.
// Multiple calls within 1 s are coalesced into a single Supabase write.
func (m *Manager) debouncedSave() {
	m.saveDebounceMu.Lock()
	defer m.saveDebounceMu.Unlock()
	if m.saveDebounce != nil {
		m.saveDebounce.Stop()
	}
	m.saveDebounce = time.AfterFunc(time.Second, func() {
		if err := m.SaveConfig(); err != nil {
			fmt.Printf("[WARN] debounced save: %v\n", err)
		}
	})
}

// SaveConfig saves the current channels to Supabase.
// In pooled mode, saves to the shared channel_pool instead of instance-scoped key.
func (m *Manager) SaveConfig() error {
	config := make([]*entity.ChannelConfig, 0)

	m.Channels.Range(func(key, value any) bool {
		config = append(config, value.(*channel.Channel).Config)
		return true
	})

	b, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if server.IsPooledMode() {
		return server.SavePoolToDB(b)
	}
	return server.SaveChannelsToDB(b)
}

// LoadConfig loads the channels from Supabase and starts them.
// All channels are automatically resumed on startup, regardless of their paused state.
func (m *Manager) LoadConfig() error {
	// Restore persisted cookies/user-agent before starting channels
	if err := server.LoadSettings(); err != nil {
		fmt.Printf("[WARN] could not load settings: %v\n", err)
	}

	// Load channels from Supabase
	b := server.LoadChannelsFromDB()
	if b == nil {
		return nil
	}

	var config []*entity.ChannelConfig
	if err := json.Unmarshal(b, &config); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	if len(config) == 0 {
		return nil
	}

	for _, conf := range config {
		conf.Sanitize()
		ch := channel.New(conf)
		m.Channels.Store(conf.Username, ch)
		ch.PipelineQueue.ResumePending()

		// Automatically resume all channels on startup
		if ch.Config.IsPaused.Load() {
			ch.Info("channel was paused, automatically resuming on startup")
			ch.Config.IsPaused.Store(false)
		}

		ch.Resume(0)
	}

	// Save the updated config to persist the resumed state.
	// This is best-effort — if Supabase is down, the web UI should still start
	// and channels will save their state on the next config change.
	if err := m.SaveConfig(); err != nil {
		fmt.Printf("[WARN] could not persist channel state to Supabase: %v\n", err)
		fmt.Println("[WARN] channels are running but state changes will be lost if the container restarts")
	}

	// Clean up orphaned sidecar files from previous interrupted runs
	go func() {
		channel.CleanupOrphanedFiles()
		m.ScanThumbnails()
	}()

	// Periodic orphan cleanup + thumbnail scan
	if server.Config.OrphanCleanupInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(server.Config.OrphanCleanupInterval) * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				channel.CleanupOrphanedFiles()
				m.ScanThumbnails()
			}
		}()
	}

	// File watcher for real-time orphan detection.
	// Only watch the output directory — files in the temp "videos/"
	// directory are either active recordings (which the watcher must
	// not touch) or are already handled by MoveToOutputDir's upload
	// goroutine after a failed move.
	go func() {
		dirs := []string{}
		if server.Config.OutputDir != "" {
			dirs = append(dirs, server.Config.OutputDir)
		}
		if len(dirs) == 0 {
			return
		}
		fw, err := watcher.New(dirs)
		if err != nil {
			log.Printf("[watcher] failed to start: %v", err)
			return
		}
		log.Printf("[watcher] watching %d directories for new video files", len(dirs))
		fw.Start(m.WatcherDone)
	}()

	return nil
}

// ============================================================================
// Pooled mode (distributed shards/nodes)
// ============================================================================

// LoadPooledConfig creates local channel objects from the channel_assignments
// rows assigned to this node.  Called instead of LoadConfig() when
// CHANNEL_POOL_MODE=pooled.  The channel_assignments table is the sole
// source of truth — the legacy channel_pool app_settings blob is ignored.
func (m *Manager) LoadPooledConfig() error {
	// Restore persisted cookies/user-agent
	if err := server.LoadSettings(); err != nil {
		fmt.Printf("[WARN] could not load settings: %v\n", err)
	}

	client := server.GetDBClient()
	if client == nil {
		return fmt.Errorf("supabase not configured")
	}

	// Fetch assignments that belong to this node (status != unassigned).
	// Retry with backoff across transient Supabase failures (e.g. Cloudflare
	// HTTP 530 / 1033 in front of the project). A brief flap at startup must
	// never abort the whole process (which previously caused an exit-1 restart
	// loop in keep-alive.ps1, wasting ~3 min on cookie refresh each cycle).
	// If it is still unreachable, degrade to an empty channel set: the
	// coordinator's 60s claim loop reconciles assignments and creates the
	// channels as soon as Supabase recovers.
	var myAssignments []database.ChannelAssignment
	var fetchErr error
	const maxAssignmentAttempts = 10
	for attempt := 0; attempt < maxAssignmentAttempts; attempt++ {
		myAssignments, fetchErr = client.GetNodeAssignments(server.NodeID())
		if fetchErr == nil {
			break
		}
		if attempt < maxAssignmentAttempts-1 {
			backoff := time.Duration(1<<uint(attempt)) * 2 * time.Second
			if backoff > 15*time.Second {
				backoff = 15 * time.Second
			}
			fmt.Printf("[WARN] LoadPooledConfig: get node assignments failed (attempt %d/%d), retrying in %v: %v\n",
				attempt+1, maxAssignmentAttempts, backoff, fetchErr)
			time.Sleep(backoff)
		}
	}
	if fetchErr != nil {
		fmt.Printf("[WARN] LoadPooledConfig: could not fetch node assignments after %d attempts: %v\n"+
			"   Starting with an empty channel set — the coordinator claim loop will pick up assignments when Supabase recovers.\n",
			maxAssignmentAttempts, fetchErr)
		myAssignments = nil
	}

	created := 0
	for _, a := range myAssignments {
		if a.Status == "unassigned" || a.AssignedNode == "" {
			continue
		}
		conf := coordinator.ConfigFromAssignment(&a)
		ch := channel.New(conf)
		m.Channels.Store(conf.Username, ch)
		ch.PipelineQueue.ResumePending()
		ch.Resume(0)
		created++
	}

	fmt.Printf("[manager] LoadPooledConfig: loaded %d channel(s) for node %q\n",
		created, server.NodeID())

	// Cleanup orphans + watcher (same as LoadConfig)
	go func() {
		channel.CleanupOrphanedFiles()
		m.ScanThumbnails()
	}()

	if server.Config.OrphanCleanupInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(server.Config.OrphanCleanupInterval) * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				channel.CleanupOrphanedFiles()
				m.ScanThumbnails()
			}
		}()
	}

	// File watcher
	if server.Config.OutputDir != "" {
		go func() {
			dirs := []string{server.Config.OutputDir}
			fw, err := watcher.New(dirs)
			if err != nil {
				log.Printf("[watcher] failed to start: %v", err)
				return
			}
			fw.Start(m.WatcherDone)
		}()
	}

	return nil
}

// CreateChannelFromAssignment implements coordinator.ChannelManager.
// Creates a channel from a channel_assignments row (claimed by coordinator).
func (m *Manager) CreateChannelFromAssignment(ca *database.ChannelAssignment) error {
	conf := coordinator.ConfigFromAssignment(ca)
	conf.Sanitize()

	// Check for duplicate. The DB assignment is the source of truth in pooled
	// mode: a channel assigned to this node must be RUNNING. If the channel
	// already exists locally it may be PAUSED (left over from a session
	// boundary, a UI pause, or an interrupted Stop during a handoff) — silently
	// returning would leave it paused forever with nothing ever reactivating
	// it. So resume a paused instance instead of skipping it — UNLESS the user
	// explicitly paused it: a manual pause reason is never overridden here.
	if existing, loaded := m.Channels.LoadOrStore(conf.Username, channel.New(conf)); loaded {
		if ch, ok := existing.(*channel.Channel); ok && ch.Config.IsPaused.Load() {
			if ch.PauseReason() == entity.PauseReasonManual {
				ch.Info("channel manually paused — leaving paused (not overriding user pause)")
				return nil
			}
			ch.PipelineQueue.ResumePending()
			ch.MarkAutoResumedFromPause()
			ch.Resume(0)
			// Browser-visible log (SSE) + stdout so the node web UI shows why
			// this channel came back from a stuck-paused state. Logged after
			// Resume so the browser log reads in order: "channel resumed",
			// then the recovery explanation.
			ch.Info("channel was paused but still assigned — automatically resumed (stuck-pause recovery)")
		}
		return nil
	}

	// Load the stored channel and start it
	thing, _ := m.Channels.Load(conf.Username)
	ch := thing.(*channel.Channel)
	ch.PipelineQueue.ResumePending()
	ch.Resume(0)

	// Restart the session loop if it exited (e.g. when channels were claimed after
	// an empty startup).  If the session is already active this is a no-op.
	// Newly claimed channels then participate in the next session boundary
	// (stop → process → upload → resume cycle).
	m.sessionDeadlineMu.Lock()
	dur := m.sessionDuration
	m.sessionDeadlineMu.Unlock()
	m.StartSession(dur)

	fmt.Printf("[manager] created channel from assignment: %s/%s\n", ca.Site, ca.Username)
	return nil
}

// GetLocalChannels implements coordinator.ChannelManager.
// Returns the list of usernames of channels active on this node.
func (m *Manager) GetLocalChannels() []string {
	var list []string
	m.Channels.Range(func(key, value interface{}) bool {
		if username, ok := key.(string); ok {
			list = append(list, username)
		}
		return true
	})
	return list
}

// HasPendingSegments implements coordinator.ChannelManager.
// Returns true if the channel has any pending recording segments on
// this node's disk (files waiting to be merged/uploaded).  The
// shuffle skips such channels so pending files are never orphaned
// by a reassignment.
func (m *Manager) HasPendingSegments(username string) bool {
	dir := "videos"
	if server.Config != nil && server.Config.OutputDir != "" {
		dir = server.Config.OutputDir
	}
	pendingDir := filepath.Join(dir, ".pending", username)
	entries, err := os.ReadDir(pendingDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			return true
		}
	}
	return false
}

// ChannelMinDurationBeforeUpload implements server.IManager.  It returns the
// live channel's per-channel min-duration-before-upload setting, so the
// orphan/pending flows gate with the same threshold the channel flow uses
// (a channel configured with 1200s in the pool stays gated even when the
// node's global MIN_DURATION_BEFORE_UPLOAD env var is unset).
func (m *Manager) ChannelMinDurationBeforeUpload(username string) int {
	if v, ok := m.Channels.Load(username); ok {
		if ch, ok := v.(*channel.Channel); ok {
			return ch.Config.MinDurationBeforeUpload
		}
	}
	return 0
}

// ManualPausedChannels implements coordinator.ChannelManager: returns the
// channels the user explicitly paused (pause reason = manual), so the
// coordinator can re-claim them for this node at session boundaries and keep
// automatic paths (live-check release, watchdog, resume) from ever fighting
// the user's pause.
func (m *Manager) ManualPausedChannels() []coordinator.ChannelPause {
	var out []coordinator.ChannelPause
	m.Channels.Range(func(key, value any) bool {
		ch := value.(*channel.Channel)
		if ch.PauseReason() == entity.PauseReasonManual {
			out = append(out, coordinator.ChannelPause{Username: ch.Config.Username, Site: ch.Config.Site})
		}
		return true
	})
	return out
}

// RemoveChannelForReassignment implements coordinator.ChannelManager.
// Removes a channel from this node when it's been reassigned to another node.
// A channel the user explicitly paused is NEVER discarded here — the
// session-boundary rebalance releases every assignment and this would otherwise
// destroy the paused object and let it be recreated recording (or claimed by
// another node), overriding the user's pause. Manual-paused channels stay
// parked+paused locally until the user resumes or removes them.
func (m *Manager) RemoveChannelForReassignment(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}

	ch := thing.(*channel.Channel)
	// Clear the channel's Cloudflare-block state once it is no longer actively
	// monitoring on this node. Without this, a CF-starved shed would leave its
	// (stopped) channels in the cfBlocked map forever — they never make another
	// request to trigger ResetCFBlock — so CFBlockedCount() stayed above the
	// starved threshold and the node never recovered (permanent starvation
	// until restart).
	m.ResetCFBlock(username)

	if ch.PauseReason() == entity.PauseReasonManual {
		ch.Info("channel manually paused — keeping it (not removing for reassignment)")
		return nil
	}

	m.Channels.Delete(username)
	go func() {
		ch.Stop()
	}()
	return nil
}

const (
	// thumbBackfillSpacing is the delay between consecutive thumbnail
	// regenerations inside one scan.  Image hosts (ImgBB ≈50 uploads/h/key,
	// Pixhost/Catbox with burst limits) rate-limit bulk uploads; spacing a
	// mass backfill turns a storm into a trickle.
	thumbBackfillSpacing = 2 * time.Second
	// thumbRetryCooldown skips files attempted within this window.  A
	// permanently-broken recording would otherwise be re-hammered (3 hosts ×
	// 3 attempts) on every startup and periodic scan.
	thumbRetryCooldown = 45 * time.Minute
)

// ScanThumbnails walks the videos directory and generates thumbnails for any
// video file that is missing preview URLs in Supabase.
func (m *Manager) ScanThumbnails() {
	// Re-entrancy guard: a slow backfill (2s/file) must never overlap with a
	// later startup or periodic scan — both would regenerate the same files.
	m.thumbThrottleMu.Lock()
	if m.thumbScanning {
		m.thumbThrottleMu.Unlock()
		return
	}
	m.thumbScanning = true
	m.thumbThrottleMu.Unlock()
	defer func() {
		m.thumbThrottleMu.Lock()
		m.thumbScanning = false
		m.thumbThrottleMu.Unlock()
	}()

	// .ts is included so legacy HLS recordings (finalize-mode=none) also get
	// thumbnail backfill; with remux enabled the finalized files are .mp4.
	videoExts := map[string]bool{".mp4": true, ".mkv": true, ".ts": true}
	dirs := []string{"videos"}
	if server.Config != nil && server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}

	for _, dir := range dirs {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() {
				return nil
			}
			ext := strings.ToLower(filepath.Ext(path))
			if !videoExts[ext] {
				return nil
			}
			// Skip A/V track sidecars (but keep .video.muxed.mp4 which is the final muxed output)
			if strings.HasSuffix(info.Name(), ".video.mp4") || strings.HasSuffix(info.Name(), ".audio.mp4") {
				return nil
			}
			// Skip ffmpeg finalizer scratch files — a crash mid-finalize leaves
			// a partial "<base>.finalizing.mp4" that cannot produce a valid
			// preview/sprite (the "complex filter failed" failures) and must
			// not be re-hammered on every scan.
			if channel.IsFinalizingTemp(info.Name()) || strings.Contains(info.Name(), ".deleting.") {
				return nil
			}
			// Skip files a pipeline is currently processing — it generates its
			// own thumbnails.  Racing it would collide on the shared sidecar
			// filenames and could cause spurious thumbnail failures.
			if channel.IsUploadInFlight(path) {
				return nil
			}
			// Only regenerate when the THUMBNAIL — the asset actually shown on the
			// video card — is missing.  Requiring all three pieces to be present
			// made a preview-only failure (Catbox down, ImgBB rate-limited — the
			// animated WEBP preview fails fleet-wide while thumb+sprite succeed)
			// regenerate AND re-upload the working thumbnail and sprite to the
			// image hosts on every 45-min cooldown window.  That self-inflicted
			// loop is what burns Pixhost/ImgBB quota and causes the fleet-wide
			// "rate limit reached" failures.  A missing sprite/preview is
			// cosmetic and can never succeed while the host is down; a missing
			// thumbnail is not.
			thumbURL, _, _ := server.LoadPreviewLinks(info.Name())
			if thumbURL != "" {
				return nil
			}
			// Skip files attempted within the cooldown window — a broken file
			// is not re-hammered on every scan, and overlapping directories
			// (e.g. videos/ == OutputDir) never double-generate.
			m.thumbThrottleMu.Lock()
			last, seen := m.thumbThrottle[path]
			m.thumbThrottleMu.Unlock()
			if seen && time.Since(last) < thumbRetryCooldown {
				return nil
			}
			newThumb, newSprite, newPreview := channel.GenerateThumbnailForFile(path)
			// Record the attempt regardless of outcome so failures cool down
			// too instead of burning host attempts on every single scan.
			m.thumbThrottleMu.Lock()
			m.thumbThrottle[path] = time.Now()
			m.thumbThrottleMu.Unlock()
			if newThumb != "" || newSprite != "" || newPreview != "" {
				if err := server.SavePreviewLinks(info.Name(), newThumb, newSprite, newPreview); err != nil {
					log.Printf("[thumb] failed to save preview links for %s: %v", info.Name(), err)
				}
			}
			// Space regenerations so a mass backfill cannot exceed the image
			// hosts' rate limits (the fleet-wide "imgbb: rate limit reached"
			// failures).
			time.Sleep(thumbBackfillSpacing)
			return nil
		})
	}
}

// CreateChannel starts monitoring an M3U8 stream
func (m *Manager) CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error {
	conf.Sanitize()

	// In pooled mode, create the assignment and try to claim for this node
	if server.IsPooledMode() && m.Coordinator != nil {
		if err := m.Coordinator.CreateChannelAssignment(conf); err != nil {
			return fmt.Errorf("create assignment: %w", err)
		}
		shouldSave = false // pool save is handled by coordinator
	}

	// prevent duplicate channels
	_, ok := m.Channels.Load(conf.Username)
	if ok {
		return fmt.Errorf("channel %s already exists", conf.Username)
	}

	ch := channel.New(conf)
	m.Channels.Store(conf.Username, ch)
	ch.PipelineQueue.ResumePending()
	ch.Resume(0)

	if shouldSave {
		m.debouncedSave()
	}
	return nil
}

// StopChannel deletes a channel permanently.
//
// Execution order:
//  1. Remove from the in-memory map immediately — the channel disappears from
//     the UI on the very next page load, and duplicate requests are no-ops.
//  2. Persist synchronously via SaveConfig (PATCH to app_settings blob) —
//     this is the only call that MUST complete before the HTTP redirect so the
//     deletion survives a subsequent app restart.
//  3. Stop the ffmpeg recording process in a goroutine — gracefully terminating
//     ffmpeg can take several seconds, and blocking the HTTP handler for that
//     long causes browser timeouts and duplicate click events. The OS will clean
//     up any still-running ffmpeg processes if the app exits before Stop returns.
//  4. Delete the secondary channels-table row in a goroutine — best-effort FK
//     cleanup that never needs to block the response.
func (m *Manager) StopChannel(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}

	ch := thing.(*channel.Channel)

	// Step 1: remove from memory so subsequent requests are immediate no-ops
	// and the UI reflects the deletion on the next GET /.
	m.Channels.Delete(username)
	m.ResetCFBlock(username) // a deleted channel is no longer this node's problem

	// Step 2: in pooled mode, release the assignment first
	if server.IsPooledMode() && m.Coordinator != nil {
		m.Coordinator.ReleaseChannel(username, ch.Config.Site)
	}

	// Step 3: synchronous PATCH to the authoritative app_settings blob.
	// In pooled mode, this saves to the shared pool.
	if err := m.SaveConfig(); err != nil {
		fmt.Printf("[ERROR] SaveConfig after delete of %q: %v\n", username, err)
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf(" INFO [manager] channel %q deleted and persisted to Supabase\n", username)

	// Step 4: non-blocking cleanup — stop the ffmpeg process.
	go func() {
		ch.Stop()
	}()

	return nil
}

// StopWatcher signals the fsnotify file watcher to shut down, releasing
// its file handles.  Call during graceful shutdown after all channels have
// been stopped so the watcher cannot race with deferred Cleanup goroutines.
// Safe to call multiple times — the close is guarded by watcherMu.
func (m *Manager) StopWatcher() {
	m.watcherMu.Lock()
	defer m.watcherMu.Unlock()
	if !m.watcherClosed {
		close(m.WatcherDone)
		m.watcherClosed = true
	}
}

// WaitForUploads processes queued recordings and blocks until their uploads
// and metadata saves have finished. Call this during graceful shutdown so
// recordings are not lost when the container receives SIGTERM.
func (m *Manager) WaitForUploads() {
	var chs []*channel.Channel
	m.Channels.Range(func(key, value any) bool {
		chs = append(chs, value.(*channel.Channel))
		return true
	})
	if len(chs) == 0 {
		return
	}

	sem := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for _, ch := range chs {
		sem <- struct{}{}
		wg.Add(1)
		go func(ch *channel.Channel) {
			defer wg.Done()
			defer func() { <-sem }()
			ch.ProcessPending()
		}(ch)
	}
	wg.Wait()
}

// StopAllChannels cancels all active channel Monitor goroutines without
// removing them from the map. Used during graceful shutdown so recordings
// can be finalized and uploaded before the process exits.
func (m *Manager) StopAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).Cancel()
		return true
	})
}

// WaitForAllChannels blocks until every channel's Monitor goroutine has
// fully exited. By the time this returns, Cleanup() has run for each
// channel, meaning all pending files have been queued into UploadWg.
// Always call this before WaitForUploads() during graceful shutdown.
func (m *Manager) WaitForAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).WaitMonitor()
		return true
	})
}

// CancelAllChannels cancels the recording context for every channel.
// Unlike StopAllChannels, this does NOT close ch.done, so channels
// can be resumed later via ResumeAllChannels.  The deferred Cleanup
// inside RecordStream still runs, queuing pending files into UploadWg.
func (m *Manager) CancelAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		ch := value.(*channel.Channel)
		ch.Cancel()
		return true
	})
}

// ResumeAllChannels resumes every channel in the map (skipping any whose
// ch.done is already closed), EXCEPT channels the user explicitly paused — the
// manual pause reason is sticky across session boundaries, so the automatic
// session restart never overrides a user's pause.
func (m *Manager) ResumeAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		ch := value.(*channel.Channel)
		if ch.PauseReason() == entity.PauseReasonManual {
			ch.Info("channel manually paused — leaving paused at session boundary")
			return true
		}
		ch.Resume(0)
		return true
	})
}

// StartWatcher creates a new fsnotify watcher on the output directory
// and runs it in a background goroutine.  Used by the session loop to
// re-enable orphan detection after the processing phase completes.
func (m *Manager) StartWatcher() {
	dirs := []string{}
	if server.Config != nil && server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}
	if len(dirs) == 0 {
		return
	}

	m.watcherMu.Lock()
	m.WatcherDone = make(chan struct{})
	m.watcherClosed = false
	m.watcherMu.Unlock()

	go func() {
		fw, err := watcher.New(dirs)
		if err != nil {
			log.Printf("[watcher] failed to start: %v", err)
			return
		}
		log.Printf("[watcher] watching %d directories for new video files", len(dirs))
		fw.Start(m.WatcherDone)
	}()
}

// StopWithProcessingQueue cancels all channels and processes their queued
// recordings in batches using a limited number of workers.  Each worker
// processes one channel at a time (mux all pending files, wait for all
// uploads) so CPU, disk, and network contention is minimised.
func (m *Manager) StopWithProcessingQueue(workers int) {
	var chs []*channel.Channel
	m.Channels.Range(func(key, value any) bool {
		chs = append(chs, value.(*channel.Channel))
		return true
	})

	m.CancelAllChannels()

	log.Printf("[session] waiting for %d channels to close recordings...", len(chs))
	m.WaitForAllChannels()

	if len(chs) == 0 {
		return
	}

	log.Printf("[session] processing %d channels with %d worker(s)...", len(chs), workers)

	// Publish a status log to each channel so the UI shows what's happening.
	for _, ch := range chs {
		ch.Info("session stopped — processing pending files (mux, compress, upload)")
	}

	// Broadcast upload state so the frontend activates the upload bar.
	m.PublishUploadState()

	// Progress ticker
	processingDone := make(chan struct{})
	defer close(processingDone)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		elapsed := 0
		for {
			select {
			case <-ticker.C:
				elapsed += 30
				log.Printf("[session] still processing... (%ds elapsed)", elapsed)
			case <-processingDone:
				return
			}
		}
	}()

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, ch := range chs {
		sem <- struct{}{}
		wg.Add(1)
		go func(ch *channel.Channel) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[session] PANIC processing channel %s: %v", ch.Config.Username, r)
				}
			}()
			ch.Info("processing pending files...")
			ch.ProcessPending()

			// Pause the channel so it moves to the "Paused" section in the UI
			// and logs reflect the completed state. Recorded as a session
			// boundary pause so the session restart and claim/reconcile loops
			// can auto-resume it — and so a channel the USER paused stays
			// paused (its manual reason is sticky).
			ch.PauseWithReason(entity.PauseReasonBoundary)
			ch.Info("channel paused — ready for next session")
		}(ch)
	}

	wg.Wait()

	// Final broadcast so upload bar hides when all processing is done.
	m.PublishUploadState()
}

// StartSession begins the automatic recording-session lifecycle.
// If duration is <= 0 this is a no-op (continuous recording).
// The session loop: record for duration → cancel all channels →
// wait for mux/upload/Supabase → resume all channels → repeat.
func (m *Manager) StartSession(d time.Duration) {
	if d <= 0 {
		return
	}
	m.sessionMu.Lock()
	m.sessionDuration = d // persist so CreateChannelFromAssignment can restart the session later
	if m.sessionStarted {
		m.sessionMu.Unlock()
		return
	}
	m.sessionStarted = true
	m.sessionStopped = false
	m.sessionMu.Unlock()
	go m.sessionLoop(d)
}

func (m *Manager) sessionLoop(d time.Duration) {
	log.Printf("[session] recording session started — next stop in %s with %d channel(s)", d, m.channelCount())

	deadline := time.Now().Add(d)
	m.sessionDeadlineMu.Lock()
	m.sessionDeadline = deadline
	m.sessionDuration = d
	m.sessionDeadlineMu.Unlock()

	stopCh := make(chan struct{}, 1)
	m.sessionStopMu.Lock()
	m.sessionStopCh = stopCh
	m.sessionStopMu.Unlock()

	timer := time.NewTimer(d)
	progress := time.NewTicker(30 * time.Minute)

sessionWait:
	for {
		select {
		case <-timer.C:
			progress.Stop()
			break sessionWait
		case <-stopCh:
			progress.Stop()
			if !timer.Stop() {
				<-timer.C
			}
			log.Println("[session] manual stop triggered")
			break sessionWait
		case <-progress.C:
			remaining := time.Until(deadline)
			if remaining > 0 {
				log.Printf("[session] %s remaining in recording session", remaining.Round(time.Second))
			}
		}
	}

	m.sessionStopMu.Lock()
	m.sessionStopCh = nil
	m.sessionStopMu.Unlock()

	log.Println("[session] duration reached — stopping all channels")

	m.sessionDeadlineMu.Lock()
	m.sessionDeadline = time.Time{}
	m.sessionDuration = 0
	m.sessionDeadlineMu.Unlock()

	m.StopWithProcessingQueue(10)

	log.Println("[session] all processing complete — session ended")

	// Session-boundary rebalance: release all DB assignments and run a fresh
	// claim cycle so channels are distributed evenly across all nodes for the
	// next session. Must happen AFTER uploads finish (no live data to lose)
	// and before the workflow reads upload-complete.flag.
	if m.Coordinator != nil {
		m.Coordinator.RebalanceAtSessionBoundary()
	}

	// Signal workflow that all uploads are done so it can safely exit.
	if err := os.WriteFile("upload-complete.flag", []byte("done"), 0644); err != nil {
		log.Printf("[session] WARNING: could not write upload-complete.flag: %v", err)
	} else {
		log.Println("[session] upload-complete.flag written")
	}

	// Restart the session: resume channels and begin the next recording cycle.
	// We keep sessionStarted = true so no other caller can start a duplicate
	// session loop.  If StopSession() was called (e.g. during graceful shutdown),
	// skip the restart.
	m.sessionMu.Lock()
	stopped := m.sessionStopped
	m.sessionMu.Unlock()
	if stopped {
		log.Println("[session] session permanently stopped — not restarting")
		m.sessionMu.Lock()
		m.sessionStarted = false
		m.sessionMu.Unlock()
		return
	}

	log.Println("[session] restarting recording session")
	m.ResumeAllChannels()
	m.sessionLoop(d)
}

// IsFileUploadInFlight returns true if the given file path is currently
// being uploaded by any channel's upload goroutine.
func (m *Manager) IsFileUploadInFlight(filePath string) bool {
	return channel.IsUploadInFlight(filePath)
}

// channelCount returns the number of channels currently in the map.
func (m *Manager) channelCount() int {
	count := 0
	m.Channels.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// PauseChannel pauses the channel and persists the state. The pause is
// recorded as MANUAL — automatic resume paths (session boundary restart,
// claim/reconcile) detect this reason and leave the channel paused, never
// fighting the user's explicit pause.
func (m *Manager) PauseChannel(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).PauseWithReason(entity.PauseReasonManual)
	m.debouncedSave()
	return nil
}

// ResumeChannel resumes the channel and persists the state.
func (m *Manager) ResumeChannel(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).Resume(0)
	m.debouncedSave()
	return nil
}

// ChannelInfo returns a list of channel information for the web UI.
func (m *Manager) ChannelInfo() []*entity.ChannelInfo {
	var channels []*entity.ChannelInfo

	// Iterate over the channels and append their information to the slice
	m.Channels.Range(func(key, value any) bool {
		channels = append(channels, value.(*channel.Channel).ExportInfo())
		return true
	})

	sort.Slice(channels, func(i, j int) bool {
		pi, pj := channelSortPriority(channels[i]), channelSortPriority(channels[j])
		if pi != pj {
			return pi < pj
		}
		// Same priority: sort by username alphabetically
		return strings.ToLower(channels[i].Username) < strings.ToLower(channels[j].Username)
	})

	return channels
}

func channelSortPriority(c *entity.ChannelInfo) int {
	switch {
	case !c.IsPaused && c.IsOnline:
		return 0 // Recording
	case c.IsPaused:
		return 1 // Paused, whether currently online or offline
	case c.IsConnecting:
		return 2 // Reconnecting / actively watching
	default:
		return 3 // Offline
	}
}

// Publish sends an SSE event to the specified channel.
func (m *Manager) Publish(evt entity.Event, info *entity.ChannelInfo) {
	switch evt {
	case entity.EventUpdate:
		fp := channelInfoFingerprint(info)

		m.renderCacheMu.Lock()
		cached := m.renderCache[info.Username]
		if cached != nil && cached.fingerprint == fp {
			m.renderCacheMu.Unlock()
			return // nothing changed since last publish
		}

		var b bytes.Buffer
		if err := view.InfoTpl.ExecuteTemplate(&b, "channel_info", info); err != nil {
			m.renderCacheMu.Unlock()
			fmt.Println("Error executing template:", err)
			return
		}
		html := b.Bytes()
		m.renderCache[info.Username] = &renderCacheEntry{html: html, fingerprint: fp}
		m.renderCacheMu.Unlock()

		m.SSE.Publish("updates", &sse.Event{
			Event: []byte(info.Username + "-info"),
			Data:  html,
		})
	case entity.EventLog:
		if len(info.Logs) > 0 {
			m.logRateLimitMu.Lock()
			last := m.logRateLimit[info.Username]
			now := time.Now()
			if now.Sub(last) < time.Second {
				m.logRateLimitMu.Unlock()
				return
			}
			m.logRateLimit[info.Username] = now
			m.logRateLimitMu.Unlock()
			m.SSE.Publish("updates", &sse.Event{
				Event: []byte(info.Username + "-log"),
				Data:  []byte(info.Logs[len(info.Logs)-1]),
			})
		}
	}
}

// PublishUploadState aggregates upload progress from all channels and
// broadcasts it as an SSE "upload" event for the session timer UI.
func (m *Manager) PublishUploadState() {
	state := entity.UploadState{Active: false}
	var entries []entity.UploadEntry
	m.Channels.Range(func(_, value any) bool {
		ch := value.(*channel.Channel)
		e := ch.UploadEntry()
		if e.Filename == "" && e.Status == "" {
			return true
		}
		state.Active = true
		entries = append(entries, e)
		return true
	})
	if len(entries) == 0 {
		entries = nil
	}
	state.Channels = entries

	payload, err := json.Marshal(state)
	if err != nil {
		return
	}
	m.SSE.Publish("updates", &sse.Event{
		Event: []byte("upload"),
		Data:  payload,
	})
}

// ReportCFBlock records that a channel is currently blocked by Cloudflare and
// fires the global multi-channel alert once CFGlobalThreshold channels are
// blocked at the same time.
func (m *Manager) ReportCFBlock(username string) {
	threshold := server.Config.CFGlobalThreshold
	if threshold <= 0 {
		threshold = 3
	}

	m.cfMu.Lock()
	m.cfBlocked[username] = struct{}{}
	count := len(m.cfBlocked)
	fire := count >= threshold && !m.cfGlobalNotified
	if fire {
		m.cfGlobalNotified = true
	}
	starved := count >= m.cfStarvedThreshold()
	m.cfMu.Unlock()

	if fire {
		notifier.Notify(notifier.KeyCFGlobal, "⚠️ Cloudflare Block Detected",
			fmt.Sprintf("%d channels are currently blocked by Cloudflare", count))
	}

	// When enough channels are blocked at once the node's IP is almost
	// certainly flagged — kick off a cookie re-mint (rate-limited) so the
	// fleet recovers without a restart.
	if starved {
		m.RequestCookieRefresh()
	}
}

// cfStarvedThreshold returns how many simultaneously-blocked channels mark
// this node as Cloudflare-starved (default 5, configurable).
func (m *Manager) cfStarvedThreshold() int {
	if server.Config != nil && server.Config.CFStarvedThreshold > 0 {
		return server.Config.CFStarvedThreshold
	}
	return 5
}

// ResetCFBlock marks a channel as no longer blocked by Cloudflare, re-arming
// the global alert once the blocked-channel count drops below the threshold.
func (m *Manager) ResetCFBlock(username string) {
	m.cfMu.Lock()
	delete(m.cfBlocked, username)
	if len(m.cfBlocked) < server.Config.CFGlobalThreshold {
		m.cfGlobalNotified = false
	}
	m.cfMu.Unlock()
}

// SetCookieRefreshFunc registers the function that re-mints this node's
// cookies (refresh scripts + Supabase reload). Called by main.go at startup.
func (m *Manager) SetCookieRefreshFunc(fn func()) {
	m.cfRefreshMu.Lock()
	m.cfRefreshFn = fn
	m.cfRefreshMu.Unlock()
}

// CFBlockedCount returns the number of channels currently in a
// Cloudflare-blocked state (their last response was a CF challenge).
func (m *Manager) CFBlockedCount() int {
	m.cfMu.Lock()
	defer m.cfMu.Unlock()
	return len(m.cfBlocked)
}

// RequestCookieRefresh triggers a cookie re-mint at most once every
// CFRefreshMin minutes. It is async (fire-and-forget) so the claim cycle and
// monitor loops never block on the browser-based grabber. After the refresh
// runs, the live config is reloaded from Supabase so running channels
// immediately use the fresh cookie set on their next probe.
func (m *Manager) RequestCookieRefresh() {
	m.cfRefreshMu.Lock()
	minBetween := 10
	if server.Config != nil && server.Config.CFRefreshMin > 0 {
		minBetween = server.Config.CFRefreshMin
	}
	if m.cfRefreshFn == nil || time.Since(m.cfRefreshLast) < time.Duration(minBetween)*time.Minute {
		m.cfRefreshMu.Unlock()
		return
	}
	m.cfRefreshLast = time.Now()
	fn := m.cfRefreshFn
	m.cfRefreshMu.Unlock()

	log.Printf("[manager] %d channel(s) Cloudflare-blocked — triggering cookie refresh (rate-limited to every %d min)",
		m.CFBlockedCount(), minBetween)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[manager] PANIC in cookie refresh: %v", r)
			}
		}()
		fn()
	}()
}

func (m *Manager) PublishLog(username, line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	m.logRateLimitMu.Lock()
	last := m.logRateLimit[username]
	now := time.Now()
	if now.Sub(last) < time.Second {
		m.logRateLimitMu.Unlock()
		return
	}
	m.logRateLimit[username] = now
	m.logRateLimitMu.Unlock()
	m.SSE.Publish("updates", &sse.Event{
		Event: []byte(username + "-log"),
		Data:  []byte(line),
	})
}

// UploadEntries returns the full uploads response (active + pending + history) for the API.
func (m *Manager) UploadEntries() *entity.UploadsResponse {
	resp := &entity.UploadsResponse{}
	m.Channels.Range(func(_, value any) bool {
		ch := value.(*channel.Channel)
		e := ch.UploadEntry()
		if e.Filename != "" || e.Status != "" {
			resp.Active = append(resp.Active, e)
		}
		queued := ch.PipelineQueue.QueuedEntries()
		resp.Pending = append(resp.Pending, queued...)
		hist := ch.PipelineQueue.HistoryEntries()
		resp.History = append(resp.History, hist...)
		return true
	})
	if resp.Active == nil {
		resp.Active = []entity.UploadEntry{}
	}
	if resp.Pending == nil {
		resp.Pending = []entity.PendingEntry{}
	}
	if resp.History == nil {
		resp.History = []entity.PendingEntry{}
	}
	return resp
}

// Subscriber handles SSE subscriptions for the specified channel.
func (m *Manager) Subscriber(w http.ResponseWriter, r *http.Request) {
	m.SSE.ServeHTTP(w, r)
}
