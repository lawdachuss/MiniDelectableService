package coordinator

import (
	"context"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

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
// (MarkChannelRecording), and channels that were recording but are now offline
// are released back to the pool so stale "recording" rows never pin a channel
// to a node forever.
//
// Uses a 2-minute timeout so a single stuck API call cannot hang the
// goroutine indefinitely. Skips entirely when draining.
func (c *Coordinator) runLiveCheck() {
	if c.LiveCheck == nil {
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
	assignments, err := c.Client.GetAllAssignments()
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
	// per-channel IsLive check. Pairs are (username, site) because the
	// channel_assignments primary key is composite — a username alone can exist
	// on both sites and must not be toggled together.
	var livePairs [][2]string
	for _, ca := range assignments {
		isLive := affiliateLive[ca.Username]

		if !isLive {
			// Not found in affiliate list — do a per-channel check. The
			// affiliate API is authoritative for offline, but we still want to
			// catch models that went live between affiliate API calls.
			isLive = c.LiveCheck.IsLive(ctx, ca.Site, ca.Username)
		}

		if isLive {
			livePairs = append(livePairs, [2]string{ca.Username, ca.Site})
			if ca.AssignedNode == c.NodeID {
				if err := c.Client.MarkChannelRecording(ca.Username, ca.Site); err != nil {
					log.Printf("[coordinator] live check: mark recording error for %s: %v", ca.Username, err)
				}
			}
		} else if ca.AssignedNode == c.NodeID && ca.Status == "recording" {
			// Release the channel back to the pool when it goes offline. We
			// can't simply set status="offline" because channel_assignments has
			// a CHECK constraint preventing offline while assigned_node is set —
			// a node either owns a channel or doesn't. Releasing also lets the
			// channel be claimed by any node when it comes back online; keeping
			// it assigned in "recording" state (skipped by
			// ReleaseExcessOfflineChannels) would pin it to this node forever.
			if err := c.Client.ReleaseChannel(ca.Username, ca.Site); err != nil {
				log.Printf("[coordinator] live check: release offline error for %s: %v", ca.Username, err)
			} else {
				log.Printf("[coordinator] live check: released %s (offline, was recording)", ca.Username)
			}
		}
	}

	// Bulk-update is_live flags
	if len(livePairs) > 0 {
		if err := c.Client.SetChannelsLive(livePairs); err != nil {
			log.Printf("[coordinator] live check: set live error: %v", err)
		}
		if err := c.Client.SetChannelsNotLive(livePairs); err != nil {
			log.Printf("[coordinator] live check: set not live error: %v", err)
		}
	} else {
		if err := c.Client.SetChannelsNotLive(nil); err != nil {
			log.Printf("[coordinator] live check: set all not live error: %v", err)
		}
	}
}
