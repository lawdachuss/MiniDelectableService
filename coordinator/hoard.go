package coordinator

import (
	"context"
	"hash/fnv"
	"log"
	"math"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
)

// hoardRebalanceInterval is how often the pool-level hoard rebalance runs.
// Fair-share shedding normally happens inside each node's own claim cycle, but
// that path can wedge — the HTTP 414 bug (a release PATCH URL longer than the
// ~8KB proxy limit) silently wedged it fleet-wide and let one node hoard ~900
// channels while the other 15 sat idle. This cycle is the independent safety
// net: it releases a hoarder's OFFLINE channels straight to the pool with a
// tiny filter-based PATCH, bypassing the hoarder's own cycle entirely.
const hoardRebalanceInterval = 2 * time.Minute

// A node must hold more than hoardExcessFactor × fair share (with a floor of
// fairShare+hoardExcessFloor) before the net acts, so normal load jitter never
// triggers mass releases.
const (
	hoardExcessFactor = 2
	hoardExcessFloor  = 50
)

// dbHoardRebalance is the subset of *database.Client used by the hoard
// rebalance cycle.
type dbHoardRebalance interface {
	GetAliveNodes() ([]database.Node, error)
	GetAllAssignments() ([]database.ChannelAssignment, error)
	ReleaseNodeOfflineChannels(nodeID string, excludeUsernames []string) (int, error)
}

// StartHoardRebalanceLoop periodically runs the pool-level hoard rebalance.
func (c *Coordinator) StartHoardRebalanceLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "hoard-rebalance"

	c.runLoopWithRestart(ctx, name, hoardRebalanceInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
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
				c.cycleGuardHoard.tryRun(name, c.runHoardRebalanceCycle)
			}
		}
	})
}

// runHoardRebalanceCycle scans the whole pool for nodes holding egregiously
// more than their fair share of OFFLINE channels and releases that excess to
// the pool directly, without relying on the hoarder's own claim cycle.
//
// Only the lexicographically smallest online node acts per cycle (GetAliveNodes
// returns nodes ordered by node_id), so DB load stays minimal; the release is
// idempotent anyway, so a second actor would only match zero rows. Live and
// recording channels are never touched, and user-paused channels are excluded
// so they stay parked on their node.
func (c *Coordinator) runHoardRebalanceCycle() {
	c.runHoardRebalanceCycleWith(c.Client)
}

func (c *Coordinator) runHoardRebalanceCycleWith(db dbHoardRebalance) {
	if !c.isActive() {
		return
	}

	alive, err := db.GetAliveNodes()
	if err != nil {
		log.Printf("[coordinator] hoard rebalance: alive nodes error: %v", err)
		return
	}
	if len(alive) == 0 {
		return
	}

	// Single designated actor per cycle: the smallest online node_id.
	if c.NodeID != alive[0].NodeID {
		return
	}

	assignments, err := db.GetAllAssignments()
	if err != nil {
		log.Printf("[coordinator] hoard rebalance: assignments error: %v", err)
		return
	}
	if len(assignments) == 0 {
		return
	}

	fairShare := int(math.Ceil(float64(len(assignments)) / float64(len(alive))))
	threshold := fairShare * hoardExcessFactor
	if threshold < fairShare+hoardExcessFloor {
		threshold = fairShare + hoardExcessFloor
	}

	// User-paused channels must never be swept by the net — they are parked on
	// their node, and a re-claim by another node would record over the user's
	// pause.
	var paused []string
	if c.Manager != nil {
		for _, mc := range c.Manager.ManualPausedChannels() {
			paused = append(paused, mc.Username)
		}
	}

	// Count each node's offline (assigned, not live, not recording) channels.
	byNode := map[string]int{}
	var order []string
	for _, a := range assignments {
		if a.AssignedNode == "" || a.IsLive || a.Status == "recording" {
			continue
		}
		if _, ok := byNode[a.AssignedNode]; !ok {
			order = append(order, a.AssignedNode)
		}
		byNode[a.AssignedNode]++
	}

	for _, nodeID := range order {
		offline := byNode[nodeID]
		if offline <= threshold {
			continue
		}
		released, err := db.ReleaseNodeOfflineChannels(nodeID, paused)
		if err != nil {
			log.Printf("[coordinator] hoard rebalance: release %s error: %v", nodeID, err)
			continue
		}
		log.Printf("[coordinator] hoard rebalance: node %s held %d offline channel(s) (fair share %d, threshold %d) — released %d to the pool",
			nodeID, offline, fairShare, threshold, released)
	}
}
