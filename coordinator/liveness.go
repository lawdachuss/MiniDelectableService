package coordinator

import (
	"context"
	"log"
	"math/rand"
	"time"
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
func (c *Coordinator) runLiveCheck() {
	if c.LiveCheck == nil {
		return
	}

	ctx := context.Background()

	// Read all channel assignments — this is the source of truth, not the
	// channel_pool app_settings blob (which is never written in pooled mode).
	assignments, err := c.Client.GetAllAssignments()
	if err != nil || len(assignments) == 0 {
		return
	}

	// Check liveness for each channel.  Pairs are (username, site) because the
	// channel_assignments primary key is composite — a username alone can exist
	// on both sites and must not be toggled together.
	var livePairs [][2]string
	for _, ca := range assignments {
		if c.LiveCheck.IsLive(ctx, ca.Site, ca.Username) {
			livePairs = append(livePairs, [2]string{ca.Username, ca.Site})
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
