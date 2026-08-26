package coordinator

import (
	"context"
	"log"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
)

// assignmentSyncInterval is how often this node reconciles the channels it is
// locally running against the DB assignments written by the external
// Cloudflare Worker. The Worker is the sole authority on WHICH node owns a
// channel (it sets channel_assignments.assigned_node); this loop only obeys
// that decision — it starts channels assigned to this node and stops channels
// no longer assigned to it. It never moves, rebalances, or claims channels.
const assignmentSyncInterval = 30 * time.Second

// StartAssignmentSyncLoop keeps the local recorder in sync with the DB
// assignments owned by the external Cloudflare Worker. It starts channels
// assigned to this node that aren't running yet and stops local channels that
// are no longer assigned to this node.
func (c *Coordinator) StartAssignmentSyncLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "assignment-sync"

	c.runLoopWithRestart(ctx, name, assignmentSyncInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-tickerC:
				c.cycleGuardReconcile.tryRun(name, c.runAssignmentSyncCycle)
			}
		}
	})
}

// runAssignmentSyncCycle starts channels assigned to this node and stops local
// channels no longer assigned to this node. On any DB error it returns
// immediately and changes NOTHING — a transient DB hiccup must never cause us to
// drop live recordings. The external Worker owns assignment decisions; this
// loop merely applies them.
func (c *Coordinator) runAssignmentSyncCycle() {
	c.runAssignmentSyncCycleWith(c.Client)
}

func (c *Coordinator) runAssignmentSyncCycleWith(db *database.Client) {
	if !c.isActive() {
		return
	}
	if c.Manager == nil {
		return
	}

	dbAssignments, err := db.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] assignment-sync: get node assignments error: %v", err)
		return
	}

	dbMap := make(map[string]bool, len(dbAssignments))
	for _, a := range dbAssignments {
		dbMap[a.Username] = true
	}

	local := c.Manager.GetLocalChannels()
	localSet := make(map[string]bool, len(local))
	for _, lc := range local {
		localSet[lc] = true
	}

	// Stop local channels no longer assigned to this node by the Worker.
	for _, lc := range local {
		if dbMap[lc] {
			continue
		}
		// Never interrupt an in-progress live recording: stopping it mid-file
		// would fragment or strand the recording. Defer the stop until the
		// current recording cycle ends; the next sync cycle will catch it. The
		// Worker is the authority on assignment, so we do NOT re-pin the DB —
		// if the Worker reassigned this channel we let it go and the new owner
		// starts it.
		if c.Manager.IsRecording(lc) {
			log.Printf("[coordinator] assignment-sync: channel %s is actively recording — deferring stop until recording ends", lc)
			continue
		}
		log.Printf("[coordinator] assignment-sync: channel %s no longer assigned to this node — stopping", lc)
		if err := c.Manager.RemoveChannelForReassignment(lc); err != nil {
			log.Printf("[coordinator] assignment-sync: remove %s error: %v", lc, err)
		}
	}

	// Start channels assigned to this node that aren't running locally yet.
	for _, a := range dbAssignments {
		if !localSet[a.Username] {
			if err := c.Manager.CreateChannelFromAssignment(&a); err != nil {
				log.Printf("[coordinator] assignment-sync: start %s error: %v", a.Username, err)
			}
		}
	}

	// Re-affirm "recording" status for channels this node is actively recording.
	// This is the only place that sets status='recording' now that the
	// controller owns liveness/assignment: it tells the controller's rebalancer
	// (which refuses to move status='recording' rows) not to yank an
	// in-progress recording. The controller resets it to 'claimed' once the
	// stream is confirmed offline.
	for _, lc := range local {
		if !c.Manager.IsRecording(lc) {
			continue
		}
		if site, ok := c.Manager.LocalChannelSite(lc); ok {
			if err := db.MarkChannelRecording(lc, site); err != nil {
				log.Printf("[coordinator] assignment-sync: mark recording error for %s: %v", lc, err)
			}
		}
	}
}
