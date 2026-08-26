package coordinator

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

// ============================================================================
// CONTROLLER — leader-elected channel assignment balancer
// ============================================================================
//
// This replaces the old per-node claim/shuffle/reaper system. A single node
// (the leader, elected via a DB lease) periodically:
//   1. Computes liveness for every pooled channel (Chaturbate via the bulk
//      affiliate API; Stripchat via per-model checks, throttled).
//   2. Writes is_live so the whole fleet shares one authoritative liveness view.
//   3. Force-releases channels owned by dead nodes and offline non-recording
//      channels on live nodes.
//   4. Resets stale "recording" rows whose stream is now confirmed offline.
//   5. Balances live channels PER SITE across active nodes so every node
//      records (roughly) the same number of live cb.xxx and stripchat channels.
//
// Cold-start guard: if more than cold_start_offline_threshold nodes are offline
// (e.g. a full fleet restart where some nodes boot faster), assignment is held
// for up to cold_start_wait_sec so the fast nodes don't grab every channel
// before the rest join. After the wait (or once enough nodes are up) it proceeds.
//
// The per-node assignment-sync loop (shuffle.go) still obeys these decisions:
// it starts channels assigned to its node and stops those no longer assigned.

const controllerUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

type assignerConfig struct {
	CycleIntervalSec    int `json:"cycle_interval_sec"`
	HeartbeatTimeoutSec int `json:"heartbeat_timeout_sec"`
	ColdStartOfflineThr int `json:"cold_start_offline_threshold"`
	ColdStartWaitSec    int `json:"cold_start_wait_sec"`
	SCConcurrency       int `json:"sc_concurrency"`
	SCCheckTimeoutSec   int `json:"sc_check_timeout_sec"`
	SCStaleMin          int `json:"sc_stale_min"`
	CBEnabled           bool `json:"cb_enabled"`
	LeaseTTLSec         int `json:"lease_ttl_sec"`
}

func defaultAssignerConfig() assignerConfig {
	return assignerConfig{
		CycleIntervalSec:    60,
		HeartbeatTimeoutSec: 180,
		ColdStartOfflineThr: 5,
		ColdStartWaitSec:    300,
		SCConcurrency:       4,
		SCCheckTimeoutSec:   10,
		SCStaleMin:          10,
		CBEnabled:           true,
		LeaseTTLSec:         120,
	}
}

// StartControllerAssignmentLoop runs the leader-elected assignment balancer.
// Every node runs this loop, but only the lease holder actually mutates
// assignments; the others return early after the (cheap) lease check.
func (c *Coordinator) StartControllerAssignmentLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	const name = "controller-assignment"
	interval := time.Duration(c.assignerConfig().CycleIntervalSec) * time.Second

	c.runLoopWithRestart(ctx, name, interval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-tickerC:
				c.cycleGuardController.tryRun(name, c.runControllerCycle)
			}
		}
	})
}

func (c *Coordinator) runControllerCycle() {
	if !c.isActive() {
		return
	}

	held, err := c.Client.TryAcquireControllerLease(c.NodeID, c.assignerConfig().LeaseTTLSec)
	if err != nil {
		log.Printf("[controller] lease acquire error: %v", err)
		return
	}
	if !held {
		return // not the leader; another node owns assignment
	}

	cfg := c.assignerConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	nodes, err := c.Client.GetNodes()
	if err != nil {
		log.Printf("[controller] get nodes error: %v", err)
		return
	}

	now := time.Now()
	activeSet := map[string]bool{}
	var active, dead []database.Node
	offlineCount := 0
	for _, n := range nodes {
		switch n.Status {
		case "draining":
			// Excluded from targets but not "offline" — its channels will be
			// rebalanced onto active nodes below.
			continue
		case "offline":
			dead = append(dead, n)
			offlineCount++
			continue
		}
		// status "online": require a fresh heartbeat or treat as dead.
		hb, perr := time.Parse(time.RFC3339, n.LastHeartbeat)
		fresh := perr == nil && now.Sub(hb) <= time.Duration(cfg.HeartbeatTimeoutSec)*time.Second
		if !fresh {
			dead = append(dead, n)
			offlineCount++
			continue
		}
		active = append(active, n)
		activeSet[n.NodeID] = true
	}

	// Cold-start guard: don't assign until the fleet has mostly joined.
	if offlineCount > cfg.ColdStartOfflineThr {
		c.assignerColdStartMu.Lock()
		if c.assignerColdStart == nil {
			t := now
			c.assignerColdStart = &t
			log.Printf("[controller] cold-start: %d nodes offline (>%d) — holding assignment up to %ds for fleet to join",
				offlineCount, cfg.ColdStartOfflineThr, cfg.ColdStartWaitSec)
		}
		elapsed := now.Sub(*c.assignerColdStart)
		if elapsed < time.Duration(cfg.ColdStartWaitSec)*time.Second {
			c.assignerColdStartMu.Unlock()
			return
		}
		c.assignerColdStartMu.Unlock()
		log.Printf("[controller] cold-start window elapsed — proceeding with assignment")
	} else {
		c.assignerColdStartMu.Lock()
		c.assignerColdStart = nil
		c.assignerColdStartMu.Unlock()
	}

	all, err := c.Client.GetAllAssignments()
	if err != nil {
		log.Printf("[controller] get assignments error: %v", err)
		return
	}

	// 1–2. Liveness + is_live write. Returns live[key] (true if live) and only
	// includes keys we definitively evaluated (unknown probes are skipped so we
	// never flip a flag we couldn't confirm).
	live := c.computeLiveness(ctx, cfg, all)

	// 3. Reset stale "recording" rows whose stream is now confirmed offline, so
	// the rebalancer (which refuses to move "recording" rows) can redistribute
	// them. Only touches definitively-evaluated offline channels.
	for _, ca := range all {
		if ca.Status != "recording" {
			continue
		}
		if v, ok := live[keyOf(ca)]; ok && !v {
			if err := c.Client.SetAssignmentStatus(ca.Username, ca.Site, "claimed"); err != nil {
				log.Printf("[controller] reset recording status error for %s/%s: %v", ca.Site, ca.Username, err)
			}
		}
	}

	// 4. Release channels owned by dead nodes (force, incl. recording) and
	// offline non-recording channels on live nodes.
	for _, d := range dead {
		n, err := c.Client.ReclaimChannels(d.NodeID)
		if err != nil {
			log.Printf("[controller] reclaim %s error: %v", d.NodeID, err)
		} else if n > 0 {
			log.Printf("[controller] reclaimed %d channels from dead node %s", n, d.NodeID)
		}
	}
	for _, a := range active {
		n, err := c.Client.ReleaseNodeOfflineChannels(a.NodeID, nil)
		if err != nil {
			log.Printf("[controller] release offline %s error: %v", a.NodeID, err)
		} else if n > 0 {
			log.Printf("[controller] released %d offline channels from node %s", n, a.NodeID)
		}
	}

	// 5. Per-site balanced distribution.
	for _, site := range []string{"chaturbate", "stripchat"} {
		c.balanceSite(site, all, active, activeSet, live)
	}
}

// computeLiveness evaluates liveness for every pooled channel and writes is_live.
// Returns a map keyed by "site/username" -> live (true if live). Only channels
// with a definitive result are present; unknown probes (errors, geo-bans) are
// skipped so their previous is_live is preserved.
func (c *Coordinator) computeLiveness(ctx context.Context, cfg assignerConfig, all []database.ChannelAssignment) map[string]bool {
	live := map[string]bool{}

	// ── Chaturbate: single bulk affiliate call (no per-channel cost) ──
	if cfg.CBEnabled && server.Config != nil && server.Config.AffiliateWM != "" {
		models, err := internal.FetchAffiliateOnlineModels(ctx, server.Config.AffiliateWM, server.Config.Domain)
		if err == nil {
			var cbLive, cbDead []string
			for _, ca := range all {
				if ca.Site != "chaturbate" {
					continue
				}
				k := keyOf(ca)
				if _, ok := models[strings.ToLower(ca.Username)]; ok {
					cbLive = append(cbLive, ca.Username)
					live[k] = true
				} else {
					cbDead = append(cbDead, ca.Username)
					live[k] = false
				}
			}
			cbDead = filterNonRecording(all, cbDead)
			if err := c.Client.SetSiteLiveness("chaturbate", cbLive, cbDead); err != nil {
				log.Printf("[controller] CB is_live update error: %v", err)
			}
		} else {
			log.Printf("[controller] CB affiliate check error: %v (keeping prior is_live)", err)
		}
	}

	// ── Stripchat: per-model checks, throttled ──
	var sc []database.ChannelAssignment
	for _, ca := range all {
		if ca.Site == "stripchat" {
			sc = append(sc, ca)
		}
	}
	if len(sc) > 0 {
		results := c.checkStripchatBatch(ctx, cfg, sc)
		var scLive, scDead []string
		for _, ca := range sc {
			entry, ok := results[ca.Username]
			if !ok || !entry.known {
				continue // geo-ban / error → keep prior flag
			}
			k := keyOf(ca)
			live[k] = entry.live
			if entry.live {
				scLive = append(scLive, ca.Username)
			} else {
				scDead = append(scDead, ca.Username)
			}
		}
		scDead = filterNonRecording(all, scDead)
		if err := c.Client.SetSiteLiveness("stripchat", scLive, scDead); err != nil {
			log.Printf("[controller] SC is_live update error: %v", err)
		}
	}

	return live
}

// scLiveResult is the outcome of a Stripchat liveness probe for one model.
type scLiveResult struct {
	live  bool
	known bool
}

// checkStripchatBatch checks many Stripchat models concurrently with a bounded
// worker pool. Returns username -> result. A parse/HTTP failure yields
// known=false so the caller preserves the prior flag.
func (c *Coordinator) checkStripchatBatch(ctx context.Context, cfg assignerConfig, chs []database.ChannelAssignment) map[string]scLiveResult {
	out := make(map[string]scLiveResult, len(chs))
	var mu sync.Mutex
	sem := make(chan struct{}, maxInt(1, cfg.SCConcurrency))
	var wg sync.WaitGroup

	client := &http.Client{Timeout: time.Duration(maxInt(1, cfg.SCCheckTimeoutSec)) * time.Second}

	for _, ca := range chs {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			live, known := fetchStripchatLive(client, u)
			if known {
				mu.Lock()
				out[u] = scLiveResult{live: live, known: true}
				mu.Unlock()
			}
		}(ca.Username)
	}
	wg.Wait()
	return out
}

// fetchStripchatLive queries the public Stripchat cam endpoint. A model is
// "live for recording" only when status=="public" AND isLive==true (private/
// group shows are not publicly recordable). Unknown/error → (false, false).
func fetchStripchatLive(client *http.Client, username string) (live, known bool) {
	url := "https://stripchat.com/api/front/v2/models/username/" + username + "/cam"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false, false
	}
	req.Header.Set("User-Agent", controllerUA)
	req.Header.Set("Referer", "https://stripchat.com/"+username)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := client.Do(req)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false, false
	}
	var data struct {
		User struct {
			User struct {
				Status string `json:"status"`
				IsLive bool   `json:"isLive"`
			} `json:"user"`
		} `json:"user"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return false, false
	}
	u := data.User.User
	if u.Status == "" && !u.IsLive {
		return false, true
	}
	return u.IsLive && u.Status == "public", true
}

// balanceSite distributes live channels of one site evenly across active nodes.
// Step A assigns currently-free live channels (unassigned, or assigned to a
// dead/draining node) to under-target nodes. Step B rebalances over-target
// nodes by moving only movable channels (status claimed/unassigned — never
// recording or paused) onto under-target nodes.
func (c *Coordinator) balanceSite(site string, all []database.ChannelAssignment, active []database.Node, activeSet map[string]bool, live map[string]bool) {
	counts := map[string]int{}
	var liveChans []database.ChannelAssignment
	for _, ca := range all {
		if ca.Site != site {
			continue
		}
		k := keyOf(ca)
		if v, ok := live[k]; !ok || !v {
			continue
		}
		liveChans = append(liveChans, ca)
		if activeSet[ca.AssignedNode] {
			counts[ca.AssignedNode]++
		}
	}
	if len(active) == 0 || len(liveChans) == 0 {
		return
	}

	// Target distribution: floor(N/M) each, first (N%M) nodes get +1.
	total := len(liveChans)
	base := total / len(active)
	rem := total % len(active)
	target := map[string]int{}
	sorted := append([]database.Node{}, active...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NodeID < sorted[j].NodeID })
	for i, n := range sorted {
		target[n.NodeID] = base
		if i < rem {
			target[n.NodeID]++
		}
	}

	// Step A: free live channels → under-target nodes.
	var free []database.ChannelAssignment
	for _, ca := range liveChans {
		if ca.AssignedNode == "" || !activeSet[ca.AssignedNode] {
			free = append(free, ca)
		}
	}
	sort.Slice(free, func(i, j int) bool { return free[i].Username < free[j].Username })
	for _, ca := range free {
		dst := pickUnderTarget(sorted, target, counts)
		if dst == "" {
			break
		}
		if ca.AssignedNode == "" {
			ok, err := c.Client.ClaimSpecificChannel(ca.Username, ca.Site, dst)
			if err != nil {
				log.Printf("[controller] claim %s/%s→%s error: %v", ca.Site, ca.Username, dst, err)
				continue
			}
			if !ok {
				continue
			}
		} else {
			if err := c.Client.ReassignChannel(ca.Username, ca.Site, ca.AssignedNode, dst); err != nil {
				log.Printf("[controller] reassign %s/%s %s→%s error: %v", ca.Site, ca.Username, ca.AssignedNode, dst, err)
				continue
			}
		}
		counts[dst]++
	}

	// Step B: rebalance over-target nodes (movable channels only).
	over := append([]database.Node{}, active...)
	sort.Slice(over, func(i, j int) bool { return counts[over[i].NodeID] > counts[over[j].NodeID] })
	for _, n := range over {
		excess := counts[n.NodeID] - target[n.NodeID]
		if excess <= 0 {
			continue
		}
		for _, ca := range liveChans {
			if excess <= 0 {
				break
			}
			if ca.AssignedNode != n.NodeID {
				continue
			}
			if ca.Status != "claimed" && ca.Status != "unassigned" {
				continue // never move recording/paused/error channels
			}
			dst := pickUnderTarget(sorted, target, counts)
			if dst == "" {
				break
			}
			if err := c.Client.ReassignChannel(ca.Username, ca.Site, n.NodeID, dst); err != nil {
				log.Printf("[controller] rebalance %s/%s %s→%s error: %v", ca.Site, ca.Username, n.NodeID, dst, err)
				continue
			}
			counts[n.NodeID]--
			counts[dst]++
			excess--
		}
	}
}

// pickUnderTarget returns an active node whose current count is below its
// target, preferring the least-loaded (tie-break by node_id). "" if none.
func pickUnderTarget(active []database.Node, target, counts map[string]int) string {
	best := ""
	bestCount := -1
	for _, n := range active {
		cnt := counts[n.NodeID]
		if cnt >= target[n.NodeID] {
			continue
		}
		if best == "" || cnt < bestCount || (cnt == bestCount && n.NodeID < best) {
			best = n.NodeID
			bestCount = cnt
		}
	}
	return best
}

func keyOf(ca database.ChannelAssignment) string {
	return ca.Site + "/" + ca.Username
}

// filterNonRecording removes usernames whose row is currently "recording",
// so a transient offline reading can never flip a live recording's is_live.
func filterNonRecording(all []database.ChannelAssignment, usernames []string) []string {
	rec := map[string]bool{}
	for _, ca := range all {
		if ca.Status == "recording" {
			rec[strings.ToLower(ca.Username)] = true
		}
	}
	out := usernames[:0]
	for _, u := range usernames {
		if !rec[strings.ToLower(u)] {
			out = append(out, u)
		}
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// assignerConfig returns the (cached) assignment tunables, loading any overrides
// from app_settings.key = 'assigner_config' on first use.
func (c *Coordinator) assignerConfig() assignerConfig {
	c.assignerCfgMu.Lock()
	defer c.assignerCfgMu.Unlock()
	if c.assignerCfg != nil {
		return *c.assignerCfg
	}
	cfg := defaultAssignerConfig()
	if c.Client != nil {
		var raw map[string]int
		if err := c.Client.GetSetting("assigner_config", &raw); err == nil {
			if v, ok := raw["cycle_interval_sec"]; ok && v > 0 {
				cfg.CycleIntervalSec = v
			}
			if v, ok := raw["heartbeat_timeout_sec"]; ok && v > 0 {
				cfg.HeartbeatTimeoutSec = v
			}
			if v, ok := raw["cold_start_offline_threshold"]; ok {
				cfg.ColdStartOfflineThr = v
			}
			if v, ok := raw["cold_start_wait_sec"]; ok && v > 0 {
				cfg.ColdStartWaitSec = v
			}
			if v, ok := raw["sc_concurrency"]; ok && v > 0 {
				cfg.SCConcurrency = v
			}
			if v, ok := raw["sc_check_timeout_sec"]; ok && v > 0 {
				cfg.SCCheckTimeoutSec = v
			}
			if v, ok := raw["sc_stale_min"]; ok && v > 0 {
				cfg.SCStaleMin = v
			}
			if v, ok := raw["cb_enabled"]; ok {
				cfg.CBEnabled = v != 0
			}
			if v, ok := raw["lease_ttl_sec"]; ok && v > 0 {
				cfg.LeaseTTLSec = v
			}
		}
	}
	c.assignerCfg = &cfg
	return cfg
}
