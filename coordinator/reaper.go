package coordinator

import (
	"context"
	"log"
	"time"
)

// StartReaperLoop periodically checks for dead nodes and reclaims their channels.
// Runs every 120 seconds. Uses a 180-second heartbeat timeout.
func (c *Coordinator) StartReaperLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const heartbeatTimeout = 180 * time.Second
	const reaperInterval = 120 * time.Second
	const name = "reaper"

	c.runLoopWithRestart(ctx, name, reaperInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-tickerC:
				c.cycleGuardReaper.tryRun(name, func() { c.runReapCycle(heartbeatTimeout) })
			}
		}
	})
}

// runReapCycle finds dead nodes and reclaims their channel assignments.
func (c *Coordinator) runReapCycle(timeout time.Duration) {
	// Self-heal first so stale orphaned rows (assigned_node set but
	// status=unassigned) don't deadlock the reclaim logic below.
	if repaired, err := c.Client.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] reaper: repair orphaned error: %v", err)
	} else if repaired > 0 {
		log.Printf("[coordinator] reaper: repaired %d orphaned assignment(s)", repaired)
	}

	// Find nodes with expired heartbeats
	deadNodeIDs, err := c.Client.GetDeadNodes(timeout)
	if err != nil {
		log.Printf("[coordinator] reaper: get dead nodes error: %v", err)
		return
	}

	if len(deadNodeIDs) == 0 {
		return
	}

	for _, deadNodeID := range deadNodeIDs {
		// Skip ourselves
		if deadNodeID == c.NodeID {
			continue
		}

		reclaimed, err := c.Client.ReclaimChannels(deadNodeID)
		if err != nil {
			log.Printf("[coordinator] reaper: reclaim from %s error: %v", deadNodeID, err)
			continue
		}

		if reclaimed > 0 {
			log.Printf("[coordinator] reaper: reclaimed %d channel(s) from dead node %s",
				reclaimed, deadNodeID)
		}

		// Mark the dead node as offline
		if err := c.Client.UpdateNodeStatus(deadNodeID, "offline"); err != nil {
			log.Printf("[coordinator] reaper: update status for %s error: %v", deadNodeID, err)
		}

		// Zero the dead node's frozen current_load so the dashboard's Total
		// Load stops counting channels that were just reclaimed. The dead
		// node's process is gone, so its heartbeat can never correct it.
		if err := c.Client.ResetNodeLoad(deadNodeID); err != nil {
			log.Printf("[coordinator] reaper: reset load for %s error: %v", deadNodeID, err)
		}
	}
}
