package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/notifier"
)

// stuckPauseInterval is how often the fleet-wide stuck-pause check runs. The
// check is cheap (one HTTP call per online node with a web_url) and only the
// lowest-id ONLINE node executes it, so the fleet does one fan-out per cycle
// instead of N² probing.
const stuckPauseInterval = 5 * time.Minute

// stuckPauseConfirmCycles is how many consecutive observations (each
// stuckPauseInterval apart) a paused-but-assigned channel must survive before
// it is considered genuinely stuck and a notification fires. Session-boundary
// pauses are transient (seconds to minutes while uploads run) and are already
// excluded by the uploading/pending flags, so 2 cycles (~10 min) is far beyond
// any legitimate pause.
const stuckPauseConfirmCycles = 2

// StartStuckPauseMonitorLoop periodically scans the fleet for channels that are
// PAUSED on their owning node but still assigned in channel_assignments —
// the "stuck pause" failure mode where a channel never gets resumed and
// silently stops recording. Detection goes through each node's public web_url
// /api/pool (the same nodes/pool API the dashboard uses), which reports the
// node's LOCAL paused state via the paused/uploading/pending flags. Runs every
// stuckPauseInterval; skipped entirely when draining or fenced.
func (c *Coordinator) StartStuckPauseMonitorLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "stuck-pause"

	c.runLoopWithRestart(ctx, name, stuckPauseInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		// Random initial delay (0-60s) to avoid thundering-herd timing with
		// the other coordinator cycles.
		randDelay := time.Duration(rand.Intn(60)) * time.Second
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
				c.cycleGuardStuckPause.tryRun(name, c.runStuckPauseCheck)
			}
		}
	})
}

// runStuckPauseCheck fetches the fleet node list and delegates the scan.
func (c *Coordinator) runStuckPauseCheck() {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.mu.Lock()
	draining, fenced := c.draining, c.fenced
	c.mu.Unlock()
	if draining || fenced {
		return
	}

	nodes, err := c.Client.GetAllNodes()
	if err != nil {
		log.Printf("[coordinator] stuck-pause check: get nodes error: %v", err)
		return
	}

	c.runStuckPauseCheckWith(context.Background(), nodes)
}

// poolAPIEntry mirrors the subset of router.PoolEntry the monitor consumes
// from each node's /api/pool response.
type poolAPIEntry struct {
	Username     string `json:"username"`
	Site         string `json:"site"`
	AssignedNode string `json:"assigned_node"`
	Paused       bool   `json:"paused"`
	Uploading    bool   `json:"uploading"`
	Pending      bool   `json:"pending"`
}

type poolAPIResponse struct {
	Assignments []poolAPIEntry `json:"assignments"`
}

// runStuckPauseCheckWith executes one fleet scan against the given node list.
// Split out for testability — the tests feed it httptest node URLs directly.
func (c *Coordinator) runStuckPauseCheckWith(ctx context.Context, nodes []database.Node) {
	// Leader election: only the lowest-id ONLINE node scans, so all nodes
	// agree on who probes and the fleet does a single fan-out per cycle.
	leader := ""
	for _, n := range nodes {
		if n.Status != "online" {
			continue
		}
		if leader == "" || n.NodeID < leader {
			leader = n.NodeID
		}
	}
	if leader == "" || leader != c.NodeID {
		return
	}

	// observed tracks channels that are paused, still assigned to the queried
	// node, and NOT actively uploading/pending (a pause mid-processing is
	// legitimate and must never be flagged).
	observed := map[string]bool{}
	for _, n := range nodes {
		if n.Status != "online" || n.WebURL == "" {
			continue
		}
		entries, err := fetchNodePoolEntries(ctx, n.WebURL)
		if err != nil {
			log.Printf("[coordinator] stuck-pause check: %s api/pool: %v", n.NodeID, err)
			continue
		}
		for _, e := range entries {
			if e.AssignedNode != n.NodeID || !e.Paused {
				continue
			}
			if e.Uploading || e.Pending {
				continue // legitimately processing uploads, not stuck
			}
			observed[n.NodeID+"/"+e.Site+"/"+e.Username] = true
		}
	}

	// Update the consecutive-observation counters: increment seen channels,
	// prune recovered ones (resumed/released), and collect the confirmed set.
	c.stuckPauseMu.Lock()
	for k := range c.stuckPauseSeen {
		if !observed[k] {
			if c.stuckPauseSeen[k] >= stuckPauseConfirmCycles {
				log.Printf("[coordinator] stuck-pause check: channel %s recovered (resumed/released)", k)
			}
			delete(c.stuckPauseSeen, k)
		}
	}
	var confirmed []string
	for k := range observed {
		c.stuckPauseSeen[k]++
		if c.stuckPauseSeen[k] >= stuckPauseConfirmCycles {
			confirmed = append(confirmed, k)
		}
	}
	c.stuckPauseMu.Unlock()

	if len(observed) > 0 {
		log.Printf("[coordinator] stuck-pause check: %d paused-but-assigned channel(s) fleet-wide (%d confirmed stuck)",
			len(observed), len(confirmed))
	}
	if len(confirmed) == 0 {
		return
	}

	sort.Strings(confirmed)
	lines := make([]string, len(confirmed))
	for i, k := range confirmed {
		parts := strings.SplitN(k, "/", 3)
		lines[i] = fmt.Sprintf("%s: %s (%s)", parts[0], parts[2], parts[1])
	}
	msg := fmt.Sprintf("%d channel(s) paused but still assigned to their node for over %s (not recording, not uploading):\n%s",
		len(confirmed), (stuckPauseInterval * stuckPauseConfirmCycles).Round(time.Minute), strings.Join(lines, "\n"))
	notifier.Notify(notifier.KeyStuckPause, "⚠️ Stuck Paused Channels", msg)
	log.Printf("[coordinator] stuck-pause check: NOTIFIED about %d stuck channel(s): %s", len(confirmed), strings.Join(confirmed, ", "))
}

// fetchNodePoolEntries GETs a node's /api/pool and returns its assignments.
// Uses the shared httpcloak client so the probe survives Cloudflare in front
// of the trycloudflare tunnel URLs (plain HTTP is routed via the default
// transport, which also keeps tests against httptest servers working).
func fetchNodePoolEntries(ctx context.Context, webURL string) ([]poolAPIEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req := internal.NewReq()
	body, err := req.GetBytes(ctx, strings.TrimRight(webURL, "/")+"/api/pool")
	if err != nil {
		return nil, err
	}
	var resp poolAPIResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode api/pool: %w", err)
	}
	return resp.Assignments, nil
}
