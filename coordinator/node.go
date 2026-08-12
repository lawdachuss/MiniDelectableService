package coordinator

import (
	"context"
	"log"
	"os"
	"time"
)

// maxHeartbeatFailures is how many consecutive failed heartbeats (at 30s each)
// before we assume a network partition / DB outage and FENCE this node. At 4
// failures that's ~2 minutes — comfortably before the 180s reaper timeout, so
// we stop recording BEFORE another node could reclaim our channels and cause a
// duplicate capture.
const maxHeartbeatFailures = 4

// heartbeatWatchdogStale is how long without a successful Supabase heartbeat
// before the heartbeat watchdog declares the node hung. The heartbeat tick
// goroutine can wedge (e.g. a pooled HTTP connection that never returns, a
// deadlocked DB call, or a frozen goroutine) while the rest of the process —
// recording, uploads, other Supabase saves — keeps working. In that state the
// node's channels are still being recorded locally while the reaper on another
// node marks it offline and reclaims them (duplicate capture), and
// keep-alive.ps1's $dvr.HasExited restart never fires because the process is
// still alive. 8 minutes is comfortably above the 4-failure fence (~2 min) and
// the DB client's retry windows, and short enough that a hung node recovers
// well before GitHub force-kills the run (~45+ min later, the observed
// fleet-wide "sessions die ~4h into a 5h25m session" pattern).
const heartbeatWatchdogStale = 8 * time.Minute

// heartbeatFatalExit is overridable in tests so the watchdog's CI force-exit
// can be asserted without terminating the test binary.
var heartbeatFatalExit = os.Exit

// heartbeatWatchdogCheck reports how long the node has gone without a
// successful heartbeat when that exceeds heartbeatWatchdogStale, or 0 when the
// node is healthy. A zero last-success time (never heartbeated yet, e.g. right
// after boot), a draining node (graceful shutdown), and a FENCED node are
// never flagged: a fence means heartbeats are failing with explicit errors
// (DB/network outage) and the fence already handles recovery — force-exiting
// every node on top of that would create a fleet-wide restart storm. The
// watchdog targets the OTHER failure mode: heartbeats silently stopping with
// no errors at all (a wedged tick, e.g. a blocked pooled connection), which
// never trips the fence and which keep-alive's $dvr.HasExited can never see.
func heartbeatWatchdogCheck(last time.Time, draining, fenced bool) time.Duration {
	if last.IsZero() || draining || fenced {
		return 0
	}
	stale := time.Since(last)
	if stale < heartbeatWatchdogStale {
		return 0
	}
	return stale
}

// heartbeatWatchdogAct reacts to a detected hang: on CI runners (GITHUB_RUN_ID
// set) it force-exits so keep-alive.ps1 restarts the DVR with the remaining
// session duration; permanent nodes only log (an operator monitors those).
func (c *Coordinator) heartbeatWatchdogAct(stale time.Duration) {
	if os.Getenv("GITHUB_RUN_ID") != "" {
		log.Printf("[coordinator] HEARTBEAT WATCHDOG: no successful heartbeat for %v (> %v) — heartbeat tick wedged; force-exiting so keep-alive restarts the DVR",
			stale.Round(time.Second), heartbeatWatchdogStale)
		heartbeatFatalExit(1)
	} else {
		log.Printf("[coordinator] HEARTBEAT WATCHDOG: no successful heartbeat for %v (> %v) — heartbeat tick likely wedged (permanent node, not exiting)",
			stale.Round(time.Second), heartbeatWatchdogStale)
	}
}

// StartHeartbeatLoop periodically updates the node's last_heartbeat timestamp.
// Runs every 30 seconds until the context is cancelled or Stop() is called.
// If the heartbeat fails repeatedly it fences the node (stops local recording
// and releases channels) to prevent duplicate recording during a partition.
// Each successful heartbeat also records lastHeartbeatOK, which the heartbeat
// watchdog (StartHeartbeatWatchdog) uses to detect a wedged tick.
func (c *Coordinator) StartHeartbeatLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		failures := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[coordinator] heartbeat cycle panicked (recovered): %v", r)
						}
					}()
					// Skip while gracefully shutting down: Stop() has already set
					// status=offline, and we must not let EnsureNodeOnline flip it
					// back to online. (This is the in-memory draining flag, NOT the
					// fence flag — a fenced node still heartbeats to detect recovery.)
					c.mu.Lock()
					draining := c.draining
					c.mu.Unlock()
					if draining {
						return
					}

					load := c.currentLoad()
					if err := c.Client.HeartbeatNode(c.NodeID, load); err != nil {
						failures++
						log.Printf("[coordinator] heartbeat failed (%d/%d): %v", failures, maxHeartbeatFailures, err)
						if failures >= maxHeartbeatFailures && c.isActive() {
							c.fence()
						}
						return
					}

					failures = 0
					c.lastHeartbeatMu.Lock()
					c.lastHeartbeatOK = time.Now()
					c.lastHeartbeatMu.Unlock()
					if c.isFenced() {
						c.unfence()
					} else {
						// Recover from a "stuck offline" state (e.g. reaper marked
						// us offline during a restart gap). Only patches when status
						// is not already online/draining, so it never fights draining.
						if err := c.Client.EnsureNodeOnline(c.NodeID); err != nil {
							log.Printf("[coordinator] ensure-online error: %v", err)
						}
					}
				}()
			}
		}
	}()
}

// StartHeartbeatWatchdog runs in its OWN goroutine, independent of the
// heartbeat tick, so a wedged tick (blocked HTTP call, deadlock) can never
// block the watchdog. Every 60s it checks how long ago the last heartbeat
// succeeded; if the node has been unable to heartbeat for > heartbeatWatchdogStale
// while not draining, it force-exits (CI) so keep-alive.ps1 restarts the DVR
// with the remaining session duration — turning the fleet-wide "hung DVR stays
// dead for 45+ minutes" pattern into a sub-10-minute recovery.
func (c *Coordinator) StartHeartbeatWatchdog(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.mu.Lock()
				draining := c.draining
				fenced := c.fenced
				c.mu.Unlock()
				c.lastHeartbeatMu.Lock()
				last := c.lastHeartbeatOK
				c.lastHeartbeatMu.Unlock()
				if stale := heartbeatWatchdogCheck(last, draining, fenced); stale > 0 {
					c.heartbeatWatchdogAct(stale)
				}
			}
		}
	}()
}

// fence stops all local recording and releases this node's channels so other
// (healthy) nodes take them over. This prevents a partitioned node from
// continuing to record channels that the cluster now considers orphaned.
func (c *Coordinator) fence() {
	c.mu.Lock()
	c.fenced = true
	c.mu.Unlock()

	log.Printf("[coordinator] PARTITION FENCE: DB unreachable %d times — stopping local recording and releasing channels to prevent duplicate capture", maxHeartbeatFailures)

	// Stop local recording FIRST, unconditionally — even if DB is unreachable
	// (which is the likely scenario when fencing due to heartbeat failure), we
	// must stop recording to prevent duplicate capture. The DB cleanup below is
	// best-effort.
	if c.Manager != nil {
		for _, username := range c.Manager.GetLocalChannels() {
			if err := c.Manager.RemoveChannelForReassignment(username); err != nil {
				log.Printf("[coordinator] fence: remove channel %s error: %v", username, err)
			}
		}
	}

	if c.Client != nil {
		// Mark draining so the reaper won't try to reclaim and so no node
		// assigns us new channels while we're fenced.
		if err := c.Client.UpdateNodeStatus(c.NodeID, "draining"); err != nil {
			log.Printf("[coordinator] fence: update status error: %v", err)
		}
		// Release DB assignments (best-effort — will fail if DB is unreachable,
		// but the reaper on another node will reclaim them after 180s).
		if err := c.Client.ReleaseNodeChannels(c.NodeID); err != nil {
			log.Printf("[coordinator] fence: release channels error: %v", err)
		}
		// Zero our load for dashboard consistency — same as the reaper and
		// graceful shutdown. Best-effort: if the DB is unreachable the row
		// simply keeps its last reported load until a healthy heartbeat
		// corrects it.
		if err := c.Client.ResetNodeLoad(c.NodeID); err != nil {
			log.Printf("[coordinator] fence: reset load error: %v", err)
		}
	}
}

// unfence resumes normal operation after a partition recovers: clear the fence
// flag and mark the node online so claim/migrate loops run again.
func (c *Coordinator) unfence() {
	c.mu.Lock()
	c.fenced = false
	c.mu.Unlock()

	log.Printf("[coordinator] PARTITION RECOVERED: resuming normal operation")

	if c.Client != nil {
		if err := c.Client.UpdateNodeStatus(c.NodeID, "online"); err != nil {
			log.Printf("[coordinator] unfence: update status error: %v", err)
		}
	}
}
