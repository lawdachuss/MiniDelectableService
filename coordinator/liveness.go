package coordinator

import (
	"context"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

// liveCheckReleaseStreak is how many consecutive live-check cycles (~120s each)
// a channel must report a DEFINITIVE offline before this node releases it back
// to the pool. A single flaky probe (affiliate API blip, rate-limited room
// status, transient network error) therefore can never pause a live channel —
// only ~4 minutes of uninterrupted offline evidence does. Genuinely offline
// channels still get released within the streak window; the brief pin in
// "recording" state is harmless (nothing is being recorded anyway).
const liveCheckReleaseStreak = 2

// StartLiveCheckLoop periodically checks which channels are live and updates
// the is_live flag in channel_assignments. Runs every 120 seconds.
// Requires LiveCheck to be set; if nil, this is a no-op.
func (c *Coordinator) StartLiveCheckLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil || c.LiveCheck == nil {
		return
	}

	const name = "live-check"
	const interval = 120 * time.Second

	c.runLoopWithRestart(ctx, name, interval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		// Random initial delay (0-30s) to prevent thundering herd
		randDelay := time.Duration(rand.Intn(30)) * time.Second
		select {
		case <-time.After(randDelay):
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-tickerC:
				c.cycleGuardLiveCheck.tryRun(name, c.runLiveCheck)
			}
		}
	})
}

// runLiveCheck checks all channels in the pool and updates their is_live status.
// Reads directly from channel_assignments (the source of truth in pooled mode).
//
// Uses a two-phase approach:
//   Phase 1: Bulk affiliate API check (single call covers ALL channels) —
//            models found in the affiliate online list are immediately live.
//            The endpoint is served on the cb.xxx domain this deployment uses
//            (the same platform as chaturbate.com), so no per-channel check is
//            needed for those channels.
//   Phase 2: Per-channel IsLive fallback for channels NOT in the affiliate list
//            (catches recently-online channels the affiliate API might have missed).
//
// In addition to toggling is_live, it keeps the DB authoritative for this
// node's recordings: live channels assigned here are marked status="recording"
// (MarkChannelRecording), and channels that were recording but are now
// definitively offline are released back to the pool so stale "recording" rows
// never pin a channel to a node forever.
//
// Releasing is DEBOUNCED: a channel is only released after
// liveCheckReleaseStreak consecutive cycles reporting a DEFINITIVE offline
// (CheckLive == LivenessOffline). A probe that errors or returns an ambiguous
// status (CheckLive == LivenessUnknown) is treated as "not confirmed offline"
// and never triggers a release — so a single flaky check cannot pause a live
// channel while the DVR is recording it.
//
// Uses a 2-minute timeout so a single stuck API call cannot hang the
// goroutine indefinitely. Skips entirely when draining.
func (c *Coordinator) runLiveCheck() {
	c.runLiveCheckWith(c.Client, c.LiveCheck)
}

// dbLiveCheck is the subset of *database.Client used by the live-check cycle,
// expressed as an interface so the cycle can be unit-tested with a mock.
type dbLiveCheck interface {
	GetAllAssignments() ([]database.ChannelAssignment, error)
	MarkChannelRecording(username, site string) error
	ReleaseChannel(username, site string) error
	SetChannelsLive(pairs [][2]string) error
	SetChannelsNotLive(pairs [][2]string) error
}

// bumpLiveCheckMiss records one definitive-offline observation for a channel
// and returns the consecutive count.
func (c *Coordinator) bumpLiveCheckMiss(site, username string) int {
	c.liveCheckMissMu.Lock()
	defer c.liveCheckMissMu.Unlock()
	key := site + "/" + username
	c.liveCheckMiss[key]++
	return c.liveCheckMiss[key]
}

// resetLiveCheckMiss clears the consecutive-offline streak for a channel
// (called on a confirmed-live or unknown result).
func (c *Coordinator) resetLiveCheckMiss(site, username string) {
	c.liveCheckMissMu.Lock()
	defer c.liveCheckMissMu.Unlock()
	delete(c.liveCheckMiss, site+"/"+username)
}

func (c *Coordinator) runLiveCheckWith(db dbLiveCheck, check LivenessChecker) {
	if check == nil {
		return
	}

	c.mu.Lock()
	if c.draining {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Read all channel assignments — this is the source of truth, not the
	// channel_pool app_settings blob (which is never written in pooled mode).
	assignments, err := db.GetAllAssignments()
	if err != nil || len(assignments) == 0 {
		return
	}

	// ── Phase 1: Bulk affiliate API check ──
	// Fetch ALL online models in one call. Channels in this list are
	// definitively live — no per-channel check needed.
	affiliateLive := make(map[string]bool, len(assignments))
	if server.Config != nil && server.Config.AffiliateWM != "" {
		models, err := internal.FetchAffiliateOnlineModels(ctx, server.Config.AffiliateWM, server.Config.Domain)
		if err == nil {
			for _, ca := range assignments {
				if _, found := models[strings.ToLower(ca.Username)]; found {
					affiliateLive[ca.Username] = true
				}
			}
			log.Printf("[coordinator] affiliate: %d/%d channels live", len(affiliateLive), len(assignments))
		}
	}

	// ── Phase 2: Per-channel fallback + DB bookkeeping ──
	// For channels NOT confirmed live by the affiliate API, do a full
	// per-channel CheckLive. Pairs are (username, site) because the
	// channel_assignments primary key is composite — a username alone can exist
	// on both sites and must not be toggled together.

	// Channels the user explicitly paused must never be released back to the
	// pool (another node could claim and record them, fighting the user's
	// pause). Build a site/username set once.
	manualSet := map[string]bool{}
	if c.Manager != nil {
		for _, mc := range c.Manager.ManualPausedChannels() {
			manualSet[mc.Site+"/"+mc.Username] = true
		}
	}

	var livePairs [][2]string
	for _, ca := range assignments {
		confirmedLive := affiliateLive[ca.Username]
		mine := ca.AssignedNode == c.NodeID
		release := false

		if !confirmedLive {
			switch check.CheckLive(ctx, ca.Site, ca.Username) {
			case LivenessLive:
				confirmedLive = true
				c.resetLiveCheckMiss(ca.Site, ca.Username)
			case LivenessOffline:
				if mine && ca.Status == "recording" {
					// Debounced: only release after liveCheckReleaseStreak
					// consecutive definitive-offline cycles.
					if c.bumpLiveCheckMiss(ca.Site, ca.Username) >= liveCheckReleaseStreak {
						c.resetLiveCheckMiss(ca.Site, ca.Username)
						release = true
					}
				}
			case LivenessUnknown:
				// Probe failed / ambiguous — never a reason to stop recording.
				// Reset the offline streak so a blip can't start it.
				c.resetLiveCheckMiss(ca.Site, ca.Username)
			}
		} else {
			c.resetLiveCheckMiss(ca.Site, ca.Username)
		}

		if confirmedLive {
			livePairs = append(livePairs, [2]string{ca.Username, ca.Site})
			if mine {
				if err := db.MarkChannelRecording(ca.Username, ca.Site); err != nil {
					log.Printf("[coordinator] live check: mark recording error for %s: %v", ca.Username, err)
				}
			}
			continue
		}

		// Not confirmed live on this cycle. Release only when the debounce
		// threshold was reached. We can't simply set status="offline" because
		// channel_assignments has a CHECK constraint preventing offline while
		// assigned_node is set — a node either owns a channel or doesn't.
		// Releasing also lets the channel be claimed by any node when it comes
		// back online; keeping it assigned in "recording" state (skipped by
		// ReleaseExcessOfflineChannels) would pin it to this node forever.
		if !release {
			continue
		}
		if manualSet[ca.Site+"/"+ca.Username] {
			log.Printf("[coordinator] live check: %s is user-paused — keeping it parked on this node", ca.Username)
			continue
		}
		if err := db.ReleaseChannel(ca.Username, ca.Site); err != nil {
			log.Printf("[coordinator] live check: release offline error for %s: %v", ca.Username, err)
		} else {
			log.Printf("[coordinator] live check: released %s (offline for %d cycles, was recording)", ca.Username, liveCheckReleaseStreak)
		}
	}

	// Bulk-update is_live flags
	if len(livePairs) > 0 {
		if err := db.SetChannelsLive(livePairs); err != nil {
			log.Printf("[coordinator] live check: set live error: %v", err)
		}
		if err := db.SetChannelsNotLive(livePairs); err != nil {
			log.Printf("[coordinator] live check: set not live error: %v", err)
		}
	} else {
		if err := db.SetChannelsNotLive(nil); err != nil {
			log.Printf("[coordinator] live check: set all not live error: %v", err)
		}
	}
}
