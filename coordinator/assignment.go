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

// runClaimCycle executes one iteration of the fair-share claiming algorithm.
// Claims channels if this node has less than its fair share, releases channels
// if it has more than its fair share (only when multiple nodes are alive).
// Skips entirely when draining (graceful shutdown in progress).
func (c *Coordinator) runClaimCycle() {
	// Don't claim new channels during draining — the node is shutting down
	// and new channels would just need to be released immediately.
	c.mu.Lock()
	draining := c.draining
	c.mu.Unlock()
	if draining {
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
	if repaired, err := c.Client.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] claim cycle: repair orphaned error: %v", err)
	} else if repaired > 0 {
		log.Printf("[coordinator] repaired %d orphaned assignment(s) (assigned_node set but status=unassigned)", repaired)
	}

	// Reconcile database assignments with local manager channels.
	// This ensures we stop any channel that got reassigned away (e.g. by reaper)
	// and start any channel assigned to us that we missed or failed to start.
	dbAssignments, err := c.Client.GetNodeAssignments(c.NodeID)
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

	stats, err := c.Client.GetAssignmentStats()
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
	// Live+recording channels are NEVER released here.
	if myOfflineCount > maxOfflineAllowed && totalNodes > 1 {
		excess := myOfflineCount - maxOfflineAllowed
		released, err := c.Client.ReleaseExcessOfflineChannels(c.NodeID, excess)
		if err != nil {
			log.Printf("[coordinator] claim cycle: release excess error: %v", err)
			return
		}
		if len(released) > 0 {
			log.Printf("[coordinator] released %d excess offline channel(s) (offline: %d -> %d, live: %d, fairShare: %d, totalPool: %d)",
				len(released), myOfflineCount, myOfflineCount-len(released), myLiveCount, fairShare, totalPool)
			for _, ca := range released {
				if c.Manager != nil {
					c.Manager.RemoveChannelForReassignment(ca.Username)
				}
			}
		}
		return // let next cycle do the claiming to avoid races
	}

	// Claim offline channels up to our maxOfflineAllowed budget
	if myOfflineCount < maxOfflineAllowed {
		budget := maxOfflineAllowed - myOfflineCount
		claimed, err := c.Client.ClaimChannels(c.NodeID, budget)
		if err != nil {
			log.Printf("[coordinator] claim cycle: claim error: %v", err)
			return
		}
		if len(claimed) > 0 {
			log.Printf("[coordinator] claimed %d new channel(s) (offline: %d -> %d, live: %d, fairShare: %d, totalPool: %d)",
				len(claimed), myOfflineCount, myOfflineCount+len(claimed), myLiveCount, fairShare, totalPool)
			for _, ca := range claimed {
				if c.Manager != nil {
					if err := c.Manager.CreateChannelFromAssignment(&ca); err != nil {
						log.Printf("[coordinator] error creating channel from assignment %s: %v", ca.Username, err)
					}
				}
			}
		}
		return // claimed or error, nothing more to do this cycle
	}

	// Nothing to claim or release this cycle — log for visibility
	log.Printf("[coordinator] claim cycle: nothing to do (offline: %d, live: %d, fairShare: %d, maxOfflineAllowed: %d, totalPool: %d)",
		myOfflineCount, myLiveCount, fairShare, maxOfflineAllowed, totalPool)
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
