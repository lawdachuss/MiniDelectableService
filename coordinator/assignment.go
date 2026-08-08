package coordinator

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"math"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// StartClaimLoop periodically claims channels for this node based on fair-share.
// Runs every 60 seconds until the context is cancelled or Stop() is called.
func (c *Coordinator) StartClaimLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "claim"
	const interval = 60 * time.Second

	c.runLoopWithRestart(ctx, name, interval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		// Stagger initial delay by node-ID hash so nodes don't all
		// claim on the same cycle and race for the same channels.
		// Base delay 5s + up to 10s spread.
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
				// cycleGuard skips this tick if the previous claim cycle is
				// still running (slow DB), instead of stacking a second one.
				c.cycleGuardClaim.tryRun(name, c.runClaimCycle)
			}
		}
	})
}

// liveClaimBudget returns how many live channels this node may still claim to
// reach its live fair share: ceil(totalLive/aliveNodes) - myLiveCount, clamped
// at 0. A node already at or over its live share claims nothing new — live
// channels are sticky and must never be released, so an over-share node simply
// keeps what it has until the channels go offline naturally.
func liveClaimBudget(myLiveCount, totalLive, totalNodes int) int {
	if totalNodes <= 0 {
		totalNodes = 1
	}
	liveShare := int(math.Ceil(float64(totalLive) / float64(totalNodes)))
	if budget := liveShare - myLiveCount; budget > 0 {
		return budget
	}
	return 0
}

// ReleaseChannel releases a single channel back to the pool.
// Called when a channel is paused or deleted.
func (c *Coordinator) ReleaseChannel(username, site string) {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	if err := c.Client.ReleaseChannel(username, site); err != nil {
		log.Printf("[coordinator] error releasing channel %s/%s: %v", site, username, err)
	}
}

// dbClaimCycle is the subset of *database.Client used by the claim cycle,
// expressed as an interface so the cycle can be unit-tested with a mock.
type dbClaimCycle interface {
	RepairOrphanedAssignments() (int, error)
	GetAssignmentStats() (*database.AssignmentStats, error)
	GetNodeAssignments(nodeID string) ([]database.ChannelAssignment, error)
	ClaimOfflineChannels(nodeID string, limit int) ([]database.ChannelAssignment, error)
	ClaimLiveChannels(nodeID string, limit int) ([]database.ChannelAssignment, error)
	ReleaseExcessOfflineChannels(nodeID string, limit int) ([]database.ChannelAssignment, error)
	ClaimSpecificChannel(username, site, nodeID string) (bool, error)
}

// runClaimCycle executes one iteration of the fair-share claiming algorithm.
// Claims channels if this node has less than its fair share, releases channels
// if it has more than its fair share (only when multiple nodes are alive).
// Skips entirely when draining (graceful shutdown in progress).
func (c *Coordinator) runClaimCycle() {
	c.runClaimCycleWith(c.Client)
}

func (c *Coordinator) runClaimCycleWith(db dbClaimCycle) {
	// Don't claim new channels during draining — the node is shutting down
	// and new channels would just need to be released immediately.
	c.mu.Lock()
	draining := c.draining
	c.mu.Unlock()
	if draining {
		return
	}
	// Self-drain during the pre-deadline window: the deadline-migration cycle
	// is reassigning this node's channels away before it is killed, so stop
	// claiming new ones. Otherwise we would re-absorb the channels that were
	// just migrated away — an infinite claim→migrate→reclaim ping-pong that
	// pinned channels to no node while overloading the migration targets
	// (seen fleet-wide when stale session_deadlines kept 9 of 18 nodes drained
	// for hours). Claiming resumes once the deadline passes or the node is
	// restarted with a fresh deadline.
	if c.ownDeadlineImminent() {
		log.Printf("[coordinator] claim cycle: own session deadline imminent — pausing new claims until it passes")
		return
	}
	// Don't claim while fenced (DB unreachable / partitioned) — claiming would
	// fight the healthy nodes that took over our released channels.
	if c.isFenced() {
		return
	}
	// Reconcile needs the manager (to start/stop local channels).  In normal
	// pooled wiring the manager is always present, but guard anyway so a nil
	// manager can never panic the cycle.
	if c.Manager == nil {
		return
	}
	// Self-heal: repair rows stuck with assigned_node set but status=unassigned.
	// These rows are invisible to both claim and release, causing a deadlock.
	if repaired, err := db.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] claim cycle: repair orphaned error: %v", err)
	} else if repaired > 0 {
		log.Printf("[coordinator] repaired %d orphaned assignment(s) (assigned_node set but status=unassigned)", repaired)
	}

	// Reconcile database assignments with local manager channels.
	// This ensures we stop any channel that got reassigned away (e.g. by reaper)
	// and start any channel assigned to us that we missed or failed to start.
	//
	// Capture the user-paused channels up front — the excess-offline release
	// below is a raw DB operation that can sweep a manual pause back into the
	// pool; we re-claim those immediately so automatic load rebalancing never
	// hands a user's pause to another node (which would record over it).
	manualPaused := c.Manager.ManualPausedChannels()
	dbAssignments, err := db.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] claim cycle: get node assignments error: %v", err)
		return
	}

	localChannels := c.Manager.GetLocalChannels()

	// 1. Remove local channels that are no longer assigned to this node in DB
	dbMap := make(map[string]database.ChannelAssignment)
	for _, a := range dbAssignments {
		dbMap[a.Username] = a
	}

	for _, lc := range localChannels {
		if _, ok := dbMap[lc]; !ok {
			log.Printf("[coordinator] reconciliation: channel %s is running locally but not assigned to this node in DB. Removing.", lc)
			if err := c.Manager.RemoveChannelForReassignment(lc); err != nil {
				log.Printf("[coordinator] reconciliation error removing channel %s: %v", lc, err)
			}
		}
	}

	// 2. Start channels that are assigned to this node in DB but not running locally
	for _, a := range dbAssignments {
		found := false
		for _, lc := range localChannels {
			if lc == a.Username {
				found = true
				break
			}
		}
		if !found {
			log.Printf("[coordinator] reconciliation: channel %s is assigned in DB but not running locally. Starting.", a.Username)
			if err := c.Manager.CreateChannelFromAssignment(&a); err != nil {
				log.Printf("[coordinator] reconciliation error starting channel %s: %v", a.Username, err)
			}
		}
	}

	stats, err := db.GetAssignmentStats()
	if err != nil {
		log.Printf("[coordinator] claim cycle: get stats error: %v", err)
		return
	}

	totalPool := stats.TotalPoolChannels
	totalNodes := stats.TotalAliveNodes
	if totalNodes == 0 {
		totalNodes = 1
	}

	fairShare := int(math.Ceil(float64(totalPool) / float64(totalNodes)))

	// Count live vs offline assignments from the already-fetched dbAssignments.
	// This avoids a redundant CountMyAssignments call and lets us do live-aware
	// fair-share: live channels consume a node's capacity, so a node with many
	// live channels claims fewer offline ones.
	myLiveCount := 0
	myLoad := 0
	for _, a := range dbAssignments {
		if a.Status == "unassigned" {
			continue
		}
		myLoad++
		if a.IsLive || a.Status == "recording" {
			myLiveCount++
		}
	}

	// Live-aware capacity: a node's live channels count against its fair-share.
	// A node with 8 live channels and fairShare=10 gets at most 2 offline slots.
	effectiveCapacity := fairShare
	if myLiveCount > effectiveCapacity {
		effectiveCapacity = myLiveCount // never release live channels
	}
	maxOfflineAllowed := effectiveCapacity - myLiveCount
	if maxOfflineAllowed < 0 {
		maxOfflineAllowed = 0
	}
	myOfflineCount := myLoad - myLiveCount

	// Release excess OFFLINE channels if we have more offline than allowed.
	// Live+recording channels are NEVER released here. The release itself is a
	// raw DB sweep (ReleaseExcessOfflineChannels has no notion of local state),
	// so a user-paused channel in "claimed" state could be swept back into the
	// pool — re-claim those immediately (best-effort: another node's staggered
	// claim could win the narrow window, in which case the release log below
	// shows the channel leaving and the re-claim log shows the failure).
	if myOfflineCount > maxOfflineAllowed && totalNodes > 1 {
		excess := myOfflineCount - maxOfflineAllowed
		released, err := db.ReleaseExcessOfflineChannels(c.NodeID, excess)
		if err != nil {
			log.Printf("[coordinator] claim cycle: release excess error: %v", err)
			return
		}
		if len(released) > 0 {
			log.Printf("[coordinator] released %d excess offline channel(s) (offline: %d -> %d, live: %d, fairShare: %d, totalPool: %d)",
				len(released), myOfflineCount, myOfflineCount-len(released), myLiveCount, fairShare, totalPool)
			// Re-claim any user-paused channels caught in the sweep before they
			// can be claimed by another node and recorded over the user's pause.
			c.reclaimManualPausedChannelsWith(db, manualPaused)
			for _, ca := range released {
				if c.Manager != nil {
					c.Manager.RemoveChannelForReassignment(ca.Username)
				}
			}
		}
		return // let next cycle do the claiming to avoid races
	}

	didSomething := false

	// Claim OFFLINE channels up to our maxOfflineAllowed budget. Live channels
	// are deliberately NOT claimable here — an offline-budget claim must never
	// sweep the live channels that should be spread across nodes.
	if myOfflineCount < maxOfflineAllowed {
		budget := maxOfflineAllowed - myOfflineCount
		claimed, err := db.ClaimOfflineChannels(c.NodeID, budget)
		if err != nil {
			log.Printf("[coordinator] claim cycle: claim offline error (is database/migrate-combined.sql applied?): %v", err)
			return
		}
		if len(claimed) > 0 {
			didSomething = true
			log.Printf("[coordinator] claimed %d new offline channel(s) (offline: %d -> %d, live: %d, fairShare: %d, totalPool: %d)",
				len(claimed), myOfflineCount, myOfflineCount+len(claimed), myLiveCount, fairShare, totalPool)
			for _, ca := range claimed {
				if c.Manager != nil {
					if err := c.Manager.CreateChannelFromAssignment(&ca); err != nil {
						log.Printf("[coordinator] error creating channel from assignment %s: %v", ca.Username, err)
					}
				}
			}
		}
	}

	// Claim LIVE channels up to our live fair share. Live channels are the
	// ones that actually get recorded, so they're spread across nodes (each
	// node claims at most ceil(totalLive/aliveNodes)) instead of being swept
	// wholesale by whichever node had offline room after a reclaim. A node that
	// already holds more live than its share keeps them (live is sticky) but
	// claims nothing new. Claiming live can push a node slightly over its total
	// fair share; the excess-offline release above rebalances it next cycle.
	if liveBudget := liveClaimBudget(myLiveCount, stats.TotalLiveChannels, totalNodes); liveBudget > 0 {
		claimed, err := db.ClaimLiveChannels(c.NodeID, liveBudget)
		if err != nil {
			log.Printf("[coordinator] claim cycle: claim live error (is database/migrate-combined.sql applied?): %v", err)
			return
		}
		if len(claimed) > 0 {
			didSomething = true
			log.Printf("[coordinator] claimed %d new live channel(s) (live: %d -> %d, liveBudget: %d, totalLive: %d)",
				len(claimed), myLiveCount, myLiveCount+len(claimed), liveBudget, stats.TotalLiveChannels)
			for _, ca := range claimed {
				if c.Manager != nil {
					if err := c.Manager.CreateChannelFromAssignment(&ca); err != nil {
						log.Printf("[coordinator] error creating channel from assignment %s: %v", ca.Username, err)
					}
				}
			}
		}
	}

	// Nothing to claim or release this cycle — log for visibility
	if !didSomething {
		log.Printf("[coordinator] claim cycle: nothing to do (offline: %d, live: %d, fairShare: %d, maxOfflineAllowed: %d, totalPool: %d)",
			myOfflineCount, myLiveCount, fairShare, maxOfflineAllowed, totalPool)
	}
}

// RebalanceAtSessionBoundary is called at session boundaries (after uploads
// complete, before the next session's channels resume). It releases this node's
// DB assignments and triggers a fresh claim cycle so the pool is redistributed
// evenly across all nodes. All nodes hit the session boundary at roughly the
// same time (same SESSION_DURATION), so each releases its channels and then
// each node claims a random fair share.
//
// Channels the user explicitly paused (pause reason = manual) are EXEMPT: they
// are re-claimed for this node immediately after the release so the rebalance
// never hands them to another node or lets an automatic resume fight the user's
// pause. They stay paused + assigned until the user resumes or removes them.
func (c *Coordinator) RebalanceAtSessionBoundary() {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	log.Printf("[coordinator] session boundary — releasing %s assignments and rebalancing", c.NodeID)

	// Capture the user-paused channels BEFORE the release wipes every row.
	var manual []ChannelPause
	if c.Manager != nil {
		manual = c.Manager.ManualPausedChannels()
	}

	if err := c.Client.ReleaseNodeChannels(c.NodeID); err != nil {
		log.Printf("[coordinator] rebalance: release error: %v", err)
		return
	}

	// Re-claim the manual-paused channels for this node so they keep a DB
	// assignment (and stay parked+paused locally) through the rebalance.
	c.reclaimManualPausedChannelsWith(c.Client, manual)

	log.Printf("[coordinator] rebalance: assignments released, running fresh claim cycle")
	c.runClaimCycle()
}

// manualReclaimer is the subset of *database.Client used to re-claim
// user-paused channels after the boundary release.
type manualReclaimer interface {
	ClaimSpecificChannel(username, site, nodeID string) (bool, error)
}

// reclaimManualPausedChannelsWith re-claims the user-paused channels for this
// node after the boundary release, so they keep a DB assignment and stay
// parked+paused locally. Returns the number successfully re-claimed.
func (c *Coordinator) reclaimManualPausedChannelsWith(client manualReclaimer, manual []ChannelPause) int {
	reclaimed := 0
	for _, mc := range manual {
		claimed, err := client.ClaimSpecificChannel(mc.Username, mc.Site, c.NodeID)
		if err != nil {
			log.Printf("[coordinator] rebalance: re-claim manual pause %s/%s: %v", mc.Site, mc.Username, err)
			continue
		}
		if claimed {
			reclaimed++
		} else {
			log.Printf("[coordinator] rebalance: manual-paused channel %s/%s was claimed by another node first", mc.Site, mc.Username)
		}
	}
	return reclaimed
}

// CreateChannelAssignment creates a channel_assignments row for a new channel.
// The row is created with status='unassigned' so any node can claim it.
func (c *Coordinator) CreateChannelAssignment(conf *entity.ChannelConfig) error {
	if !c.IsPooled() || c.Client == nil {
		return nil
	}

	ca := database.ChannelAssignment{
		Username:                conf.Username,
		Site:                    conf.Site,
		Status:                  "unassigned",
		IsLive:                  false,
		Framerate:               conf.Framerate,
		Resolution:              conf.Resolution,
		Pattern:                 conf.Pattern,
		MaxDuration:             conf.MaxDuration,
		MaxFilesize:             conf.MaxFilesize,
		Compress:                conf.Compress,
		MinDurationBeforeUpload: conf.MinDurationBeforeUpload,
	}

	if err := c.Client.BulkInsertAssignments([]database.ChannelAssignment{ca}); err != nil {
		return err
	}

	// Try to claim it for ourselves right away
	claimed, err := c.Client.ClaimSpecificChannel(conf.Username, conf.Site, c.NodeID)
	if err != nil {
		return err
	}

	if claimed {
		log.Printf("[coordinator] claimed new channel %s for this node", conf.Username)
	} else {
		log.Printf("[coordinator] channel %s claimed by another node", conf.Username)
	}

	return nil
}

// DeleteChannelAssignment removes the channel_assignments row for a channel.
func (c *Coordinator) DeleteChannelAssignment(username, site string) error {
	if !c.IsPooled() || c.Client == nil {
		return nil
	}

	return c.Client.ReleaseChannel(username, site)
}

// ConfigFromAssignment converts a ChannelAssignment back to a ChannelConfig.
func ConfigFromAssignment(ca *database.ChannelAssignment) *entity.ChannelConfig {
	conf := &entity.ChannelConfig{
		Site:                    ca.Site,
		Username:                ca.Username,
		Framerate:               ca.Framerate,
		Resolution:              ca.Resolution,
		Pattern:                 ca.Pattern,
		MaxDuration:             ca.MaxDuration,
		MaxFilesize:             ca.MaxFilesize,
		Compress:                ca.Compress,
		MinDurationBeforeUpload: ca.MinDurationBeforeUpload,
		CreatedAt:               time.Now().Unix(),
	}
	// channel_assignments.pattern defaults to '' in the database, so a row
	// created without an explicit pattern would otherwise produce a channel
	// that can never generate a filename (GenerateFilename refuses an empty
	// name). Sanitize fills the default pattern — and the other defaults —
	// so every path that rebuilds a config from an assignment is safe.
	conf.Sanitize()
	return conf
}

// MarshalPool marshals a slice of ChannelConfig into JSON bytes.
func MarshalPool(pool []*entity.ChannelConfig) ([]byte, error) {
	if pool == nil {
		pool = []*entity.ChannelConfig{}
	}
	return json.MarshalIndent(pool, "", "  ")
}

// UnmarshalPool unmarshals JSON bytes into a slice of ChannelConfig.
func UnmarshalPool(data []byte) ([]*entity.ChannelConfig, error) {
	var pool []*entity.ChannelConfig
	if err := json.Unmarshal(data, &pool); err != nil {
		return nil, err
	}
	if pool == nil {
		pool = []*entity.ChannelConfig{}
	}
	return pool, nil
}
