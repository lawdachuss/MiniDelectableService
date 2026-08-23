package coordinator

import (
	"context"
	"hash/fnv"
	"log"
	"math"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
)

// shuffleInterval is how often offline channels are re-evaluated and
// redistributed across healthy nodes ("keep shuffling until online").
const shuffleInterval = 5 * time.Minute

// deadlineMigrationInterval is how often we check for nodes whose session
// deadline is imminent and migrate their channels (incl. live+recording) to
// healthy nodes before the node is killed.
const deadlineMigrationInterval = 60 * time.Second

// deadlineMigrationWindow is how far ahead of a node's session_deadline we
// start migrating its channels away.
const deadlineMigrationWindow = 15 * time.Minute

// reconcileInterval is the fast watchdog that stops channels that are no longer
// assigned to this node (e.g. after a deadline migration or reaper reclaim),
// bounding any brief overlap to this interval.
const reconcileInterval = 15 * time.Second

// dbShuffler is the subset of *database.Client used by the shuffle and
// deadline-migration cycles, expressed as an interface so the cycles can be
// unit-tested with a mock.
type dbShuffler interface {
	RepairOrphanedAssignments() (int, error)
	GetAssignmentStats() (*database.AssignmentStats, error)
	GetAliveNodes() ([]database.Node, error)
	CountMyAssignments(nodeID string) (int, error)
	GetNodeAssignments(nodeID string) ([]database.ChannelAssignment, error)
	GetNodesWithImminentDeadline(window time.Duration) ([]database.Node, error)
	ReassignChannel(username, site, fromNode, toNode string) error
}

// StartOfflineShuffleLoop periodically rebalances OFFLINE channels across nodes.
// Runs every shuffleInterval (5 min). Offline channels keep migrating node to
// node; the moment a channel goes live it is protected and stays put. Live and
// recording channels are never released here (the fair-share claim loop already
// avoids them via ReleaseExcessOfflineChannels).
func (c *Coordinator) StartOfflineShuffleLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "offline-shuffle"

	c.runLoopWithRestart(ctx, name, shuffleInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		h := fnv.New32a()
		h.Write([]byte(c.NodeID))
		stagger := 5*time.Second + time.Duration(h.Sum32()%10)*time.Second

		select {
		case <-time.After(stagger):
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
				c.cycleGuardShuffle.tryRun(name, c.runOfflineShuffleCycle)
			}
		}
	})
}

// runOfflineShuffleCycle rebalances this node's OFFLINE channels to OTHER alive
// nodes. It reassigns (not just "releases to unassigned") so the channels
// actually migrate to a different node instead of being immediately re-claimed
// by the same node (which, after releasing, is under fair-share and would just
// absorb them again — the old behaviour that pinned every channel to its first
// claimer). If this node has any locally running channels (recording live
// broadcasts), the entire cycle is skipped — a node busy recording should not
// be moving channels around.
func (c *Coordinator) runOfflineShuffleCycle() {
	c.runOfflineShuffleCycleWith(c.Client)
}

func (c *Coordinator) runOfflineShuffleCycleWith(db dbShuffler) {
	if !c.isActive() {
		return
	}
	if c.Manager == nil {
		return
	}

	if repaired, err := db.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] offline shuffle: repair orphaned error: %v", err)
	} else if repaired > 0 {
		log.Printf("[coordinator] offline shuffle: repaired %d orphaned assignment(s)", repaired)
	}

	stats, err := db.GetAssignmentStats()
	if err != nil {
		log.Printf("[coordinator] offline shuffle: stats error: %v", err)
		return
	}

	aliveNodes, err := db.GetAliveNodes()
	if err != nil {
		log.Printf("[coordinator] offline shuffle: alive nodes error: %v", err)
		return
	}
	totalNodes := len(aliveNodes)
	if totalNodes == 0 {
		totalNodes = 1
	}

	// Candidate targets: alive nodes that are NOT this node.
	var candidates []database.Node
	for _, n := range aliveNodes {
		if n.NodeID == c.NodeID {
			continue
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		return
	}

	// Fair share is based on the total pool so offline channels (which we record
	// when they go live) are distributed evenly.
	fairShare := int(math.Ceil(float64(stats.TotalPoolChannels) / float64(totalNodes)))

	myLoad, err := db.CountMyAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] offline shuffle: count error: %v", err)
		return
	}

	moveCount := myLoad - fairShare
	if moveCount < 0 {
		moveCount = 0
	}

	// Gentle churn: even when balanced, move one offline channel to another node
	// so the pool keeps shuffling until channels come online.
	if moveCount == 0 && len(candidates) > 0 {
		moveCount = 1
	}

	// Pick our offline (not recording, not live) channels to move.
	myChannels, err := db.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] offline shuffle: get assignments error: %v", err)
		return
	}

	localSet := make(map[string]bool)
	for _, u := range c.Manager.GetLocalChannels() {
		localSet[u] = true
	}

	var offline []database.ChannelAssignment
	for _, ca := range myChannels {
		if ca.IsLive {
			continue
		}
		if ca.Status == "recording" {
			continue
		}
		if localSet[ca.Username] {
			continue
		}
		if c.Manager != nil && c.Manager.HasPendingSegments(ca.Username) {
			continue
		}
		// Never shuffle a channel that is actively recording live: reassignment
		// would let another node start a duplicate recording and this node's
		// reconcile would yank the in-progress file.
		if c.Manager != nil && c.Manager.IsRecording(ca.Username) {
			continue
		}
		offline = append(offline, ca)
	}
	if len(offline) == 0 {
		return
	}
	if moveCount > len(offline) {
		moveCount = len(offline)
	}

	// Local view of each candidate's load so we spread the moves evenly across
	// them rather than piling onto one node.
	load := make(map[string]int, len(candidates))
	for _, n := range candidates {
		load[n.NodeID] = n.CurrentLoad
	}

	moved := 0
	for i := 0; i < moveCount; i++ {
		ca := offline[i]
		target := leastLoaded(candidates, load)
		if target.NodeID == c.NodeID {
			continue
		}
		if err := db.ReassignChannel(ca.Username, ca.Site, c.NodeID, target.NodeID); err != nil {
			log.Printf("[coordinator] offline shuffle: reassign %s -> %s error: %v", ca.Username, target.NodeID, err)
			continue
		}
		load[target.NodeID]++
		moved++
		log.Printf("[coordinator] offline shuffle: moved %s/%s from %s -> %s", ca.Site, ca.Username, c.NodeID, target.NodeID)
		if err := c.Manager.RemoveChannelForReassignment(ca.Username); err != nil {
			log.Printf("[coordinator] offline shuffle: remove %s error: %v", ca.Username, err)
		}
	}

	if moved > 0 {
		log.Printf("[coordinator] offline shuffle: moved %d offline channel(s) to other node(s) (load: %d -> %d, fairShare: %d, totalPool: %d)",
			moved, myLoad, myLoad-moved, fairShare, stats.TotalPoolChannels)
	}
}

// leastLoaded returns the candidate node with the smallest current load from
// the supplied local load map.  Both the shuffle and deadline-migration cycles
// use it so channels are spread across candidates instead of piling onto a
// single node.
func leastLoaded(candidates []database.Node, load map[string]int) database.Node {
	best := candidates[0]
	bestLoad := load[best.NodeID]
	for _, n := range candidates[1:] {
		if l := load[n.NodeID]; l < bestLoad {
			best = n
			bestLoad = l
		}
	}
	return best
}

// StartDeadlineMigrationLoop migrates channels off nodes whose session_deadline
// is imminent (the GitHub 6-hour runner limit) to healthy nodes. Runs every
// deadlineMigrationInterval. Reassignment is atomic (SKIP LOCKED) so even if
// several nodes race to migrate the same channel, only one wins.
func (c *Coordinator) StartDeadlineMigrationLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "deadline-migration"

	c.runLoopWithRestart(ctx, name, deadlineMigrationInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		h := fnv.New32a()
		h.Write([]byte(c.NodeID))
		stagger := time.Duration(h.Sum32()%10) * time.Second

		select {
		case <-time.After(stagger):
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
				c.cycleGuardDeadline.tryRun(name, c.runDeadlineMigrationCycle)
			}
		}
	})
}

// runDeadlineMigrationCycle finds nodes about to hit their session deadline and
// reassigns all of their channels (including live+recording) to the least-loaded
// healthy node. A node migrating its OWN channels away is expected — that's the
// whole point of pre-deadline migration.
//
// The source node keeps recording its (now reassigned) channels until its own
// claim cycle runs — the claim cycle's DB↔local reconciliation stops channels
// no longer assigned to the node, bounding any overlap to ≤60s.
func (c *Coordinator) runDeadlineMigrationCycle() {
	c.runDeadlineMigrationCycleWith(c.Client)
}

func (c *Coordinator) runDeadlineMigrationCycleWith(db dbShuffler) {
	if !c.isActive() {
		return
	}

	if _, err := db.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] deadline migration: repair orphaned error: %v", err)
	}

	imminent, err := db.GetNodesWithImminentDeadline(deadlineMigrationWindow)
	if err != nil {
		log.Printf("[coordinator] deadline migration: imminent nodes error: %v", err)
		return
	}
	if len(imminent) == 0 {
		return
	}

	alive, err := db.GetAliveNodes()
	if err != nil {
		log.Printf("[coordinator] deadline migration: alive nodes error: %v", err)
		return
	}

	imminentSet := make(map[string]bool, len(imminent))
	for _, n := range imminent {
		imminentSet[n.NodeID] = true
	}

	// Candidate targets: alive, not draining, not themselves imminent.
	var candidates []database.Node
	for _, n := range alive {
		if imminentSet[n.NodeID] {
			continue
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		log.Printf("[coordinator] deadline migration: no healthy candidates to migrate %d imminent node(s) to", len(imminent))
		return
	}

	// Build a local load map so we spread channels across candidates instead of
	// piling all onto the single initially-least-loaded node.
	loadMap := make(map[string]int, len(candidates))
	for _, n := range candidates {
		loadMap[n.NodeID] = n.CurrentLoad
	}

	// Channels the user explicitly paused on THIS node are exempt from its own
	// deadline drain: handing a manual pause to another node would recreate it
	// there as a fresh recording channel, overriding the user's pause. (Manual
	// pauses on OTHER imminent nodes can't be detected here — the pause reason
	// is runtime-only, not persisted — so those still migrate; the session-
	// boundary re-claim is the cross-node protection.)
	manualSet := map[string]bool{}
	if c.Manager != nil {
		for _, mc := range c.Manager.ManualPausedChannels() {
			manualSet[mc.Site+"/"+mc.Username] = true
		}
	}

	for _, imm := range imminent {
		// Never drain a node whose deadline has already passed: it is still
		// alive and heartbeating (its session restart simply hasn't fired), so
		// its claim loop would immediately re-claim whatever we move — the
		// infinite claim→migrate→reclaim churn. Past deadlines are the
		// reaper's job: if the node truly dies, its channels are reclaimed
		// after the heartbeat timeout. (GetNodesWithImminentDeadline already
		// excludes past deadlines via gt.now(); this is defense in depth.)
		if imm.SessionDeadline == nil || !imm.SessionDeadline.After(time.Now()) {
			continue
		}
		if imm.NodeID == c.NodeID {
			log.Printf("[coordinator] deadline migration: this node's deadline is imminent — migrating channels away")
		}
		assignments, err := db.GetNodeAssignments(imm.NodeID)
		if err != nil {
			log.Printf("[coordinator] deadline migration: get assignments for %s error: %v", imm.NodeID, err)
			continue
		}
		for _, ca := range assignments {
			// Only this node's own migration skips manual pauses (see comment
			// above — the manual set is local knowledge).
			if imm.NodeID == c.NodeID && manualSet[ca.Site+"/"+ca.Username] {
				log.Printf("[coordinator] deadline migration: skipping manual-paused %s/%s (user pause preserved)", ca.Site, ca.Username)
				continue
			}
			// Never migrate a channel that is actively recording: pulling it
			// away mid-recording fragments or strands the in-progress file.
			// The DB status flag covers other nodes; the local check covers
			// this node's own imminent drain.
			if ca.Status == "recording" {
				log.Printf("[coordinator] deadline migration: skipping recording %s/%s (defer until session ends)", ca.Site, ca.Username)
				continue
			}
			if imm.NodeID == c.NodeID && c.Manager != nil && c.Manager.IsRecording(ca.Username) {
				log.Printf("[coordinator] deadline migration: skipping locally-recording %s/%s (defer until session ends)", ca.Site, ca.Username)
				continue
			}
			target := leastLoaded(candidates, loadMap)
			if target.NodeID == imm.NodeID {
				continue
			}
			if err := db.ReassignChannel(ca.Username, ca.Site, imm.NodeID, target.NodeID); err != nil {
				log.Printf("[coordinator] deadline migration: reassign %s from %s error: %v", ca.Username, imm.NodeID, err)
				continue
			}
			loadMap[target.NodeID]++
			log.Printf("[coordinator] deadline migration: moved %s/%s from %s -> %s", ca.Site, ca.Username, imm.NodeID, target.NodeID)
		}
	}
}

// StartReconcileLoop is a fast watchdog that keeps the local recorder in sync
// with DB assignments. It stops any local channel no longer assigned to this
// node (e.g. after a deadline migration or reaper reclaim) and starts channels
// assigned to this node that aren't running yet. This bounds the brief
// recording overlap after a migration/reclaim to reconcileInterval instead of
// waiting for the next claim cycle (up to 60s).
func (c *Coordinator) StartReconcileLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "reconcile"

	c.runLoopWithRestart(ctx, name, reconcileInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-tickerC:
				c.cycleGuardReconcile.tryRun(name, c.runReconcileCycle)
			}
		}
	})
}

// runReconcileCycle stops local channels that are no longer assigned to this
// node and starts assigned-but-not-running ones. On any DB error it returns
// immediately and removes NOTHING — a transient DB hiccup must never cause us
// to drop live recordings.
func (c *Coordinator) runReconcileCycle() {
	c.runReconcileCycleWith(c.Client)
}

func (c *Coordinator) runReconcileCycleWith(db dbShuffler) {
	if !c.isActive() {
		return
	}
	if c.Manager == nil {
		return
	}

	dbAssignments, err := db.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] reconcile: get node assignments error: %v", err)
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

	// Stop channels no longer assigned to us (e.g. migrated away / reaped).
	for _, lc := range local {
		if !dbMap[lc] {
			// Never interrupt an in-progress live recording: pausing/removing it
			// would fragment or strand the file.  Defer the removal until the
			// recording finishes; the next reconcile cycle will catch it.
			if c.Manager.IsRecording(lc) {
				log.Printf("[coordinator] reconcile: channel %s is actively recording — deferring removal until recording ends", lc)
				continue
			}
			log.Printf("[coordinator] reconcile: channel %s no longer assigned to this node — stopping", lc)
			if err := c.Manager.RemoveChannelForReassignment(lc); err != nil {
				log.Printf("[coordinator] reconcile: remove %s error: %v", lc, err)
			}
		}
	}

	// Start channels assigned to us that aren't running locally yet (e.g. a
	// channel migrated here by the deadline loop). CreateChannelFromAssignment
	// is idempotent, so this is safe to run every cycle.
	for _, a := range dbAssignments {
		if !localSet[a.Username] {
			if err := c.Manager.CreateChannelFromAssignment(&a); err != nil {
				log.Printf("[coordinator] reconcile: start %s error: %v", a.Username, err)
			}
		}
	}
}


