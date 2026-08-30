package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
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
// A single node (the leader, elected via a DB lease) is responsible for
// assigning live channels across the fleet. Its behaviour is deliberately NOT
// a continuous reshuffler:
//
//   - Every cycle it recomputes liveness for every pooled channel (Chaturbate
//     via the bulk affiliate API; Stripchat via per-model checks, throttled)
//     and writes is_live, so the whole fleet shares one authoritative,
//     continuously-updated view of live channels on BOTH cb.xxx and stripchat.
//
//   - Assignment (moving channels between nodes) happens ONLY:
//       1. ONCE at startup, after every node is live — an equal split of live
//          channels per site across all active nodes.
//       2. On a membership change — a node goes offline (its channels are
//          reclaimed and redistributed once to the remaining live nodes) or a
//          new node comes online (it receives an equal share).
//
//     A stable, healthy fleet is therefore never reshuffled; each node keeps
//     the channels it was given. This avoids the old "continuous shuffle"
//     problem where channels jumped between nodes every tick.
//
//   - When reassignment does occur, recording channels are NEVER moved
//     (balanceSite only relocates claimed/unassigned rows), so in-progress
//     recordings are never lost.
//
//   - Dead-node housekeeping (reclaim) and offline-non-recording release run
//     every cycle; ReleaseNodeOfflineChannels excludes status=recording, so a
//     live node's in-progress recordings are never disturbed.
//
// Cold-start guard: the one-time assignment waits until ALL nodes are live
// (offlineCount==0). A broken node can never block the fleet forever — after
// cold_start_wait_sec the leader proceeds with whoever is up.
//
// The per-node assignment-sync loop (shuffle.go) still obeys these decisions:
// it starts channels assigned to its node and stops those no longer assigned.

const controllerUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

type assignerConfig struct {
	CycleIntervalSec    int  `json:"cycle_interval_sec"`
	HeartbeatTimeoutSec int  `json:"heartbeat_timeout_sec"`
	ColdStartOfflineThr int  `json:"cold_start_offline_threshold"`
	ColdStartWaitSec    int  `json:"cold_start_wait_sec"`
	SCConcurrency       int  `json:"sc_concurrency"`
	SCCheckTimeoutSec   int  `json:"sc_check_timeout_sec"`
	SCStaleMin          int  `json:"sc_stale_min"`
	CBEnabled           bool `json:"cb_enabled"`
	LeaseTTLSec         int  `json:"lease_ttl_sec"`
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
		// Start promptly after a fleet restart.  The cold-start gate in the
		// controller still prevents a partial fleet from taking an early split.
		c.cycleGuardController.tryRun(name, c.runControllerCycle)
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

// nodeReclaimGrace is how long a node must be unreachable before the controller
// frees all of its channels and redistributes them evenly across the remaining
// active nodes. A brief outage (restart, network blip) stays well under this, so
// a node's channels are HELD during the window — no mass reassignment and no
// recordings lost. Only a sustained outage (>= grace) triggers reclamation.
const nodeReclaimGrace = 20 * time.Minute

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

	nodes, err := c.Client.GetNodes()
	if err != nil {
		log.Printf("[controller] get nodes error: %v", err)
		return
	}

	now := time.Now()
	activeSet := map[string]bool{}
	protectedOwnerSet := map[string]bool{}
	reclaimSet := map[string]bool{} // nodes whose channels are freed + redistributed
	heldSet := map[string]bool{}    // briefly-offline nodes: channels HELD during grace
	var active, heldNodes, reclaim []database.Node
	offlineCount := 0
	for _, n := range nodes {
		// A recording remains pinned while its owner is online or draining and
		// has checked in within the offline grace window. This is intentionally
		// broader than "active": a node being drained must finish its current
		// file before anything can take the channel away.
		if n.Status != "offline" {
			if hb, err := time.Parse(time.RFC3339Nano, n.LastHeartbeat); err == nil && now.Sub(hb) < nodeReclaimGrace {
				protectedOwnerSet[n.NodeID] = true
			}
		}
		switch n.Status {
		case "draining":
			// Intentionally removed: free its channels immediately (no grace) so
			// they're rebalanced onto the remaining active nodes.
			reclaim = append(reclaim, n)
			reclaimSet[n.NodeID] = true
			continue
		}
		// How long has this node been unreachable? Use its last heartbeat as the
		// proxy for when it went down (a node stops heartbeating when it leaves).
		// PostgREST returns timestamptz with fractional seconds
		// (e.g. "2026-08-28T05:28:53.484415+00:00"); time.RFC3339 rejects the
		// fractional part, which would make EVERY node look stale and disable
		// assignment entirely. RFC3339Nano parses both forms.
		hb, perr := time.Parse(time.RFC3339Nano, n.LastHeartbeat)
		off := nodeReclaimGrace + 1
		if perr == nil {
			off = now.Sub(hb)
		}
		// Fresh receiver only if explicitly online with a recent heartbeat.
		fresh := n.Status == "online" && perr == nil && off <= time.Duration(cfg.HeartbeatTimeoutSec)*time.Second
		if fresh {
			active = append(active, n)
			activeSet[n.NodeID] = true
			continue
		}
		// Not fresh → offline/stale. Hold its channels during the grace window so
		// a brief blip doesn't cause a mass reassignment; only reclaim (free +
		// redistribute) once it has been down for >= nodeReclaimGrace.
		offlineCount++
		if off >= nodeReclaimGrace {
			reclaim = append(reclaim, n)
			reclaimSet[n.NodeID] = true
		} else {
			heldNodes = append(heldNodes, n)
			heldSet[n.NodeID] = true
		}
	}

	all, err := c.Client.GetAllAssignments()
	if err != nil {
		log.Printf("[controller] get assignments error: %v", err)
		return
	}

	// Housekeeping every cycle: free channels owned by DEAD nodes only. A dead
	// node can't record, so its channels must be reassigned (Step A below claims
	// them onto live nodes). We deliberately do NOT release offline channels on a
	// live node: the operator requires that NO channel is ever left unassigned,
	// so an offline channel keeps its node and is simply not recorded until it
	// goes live. balanceSite then redistributes the whole pool equally; it never
	// moves "recording" rows, so in-progress recordings are never lost.
	// Free channels only from nodes that have been down past the grace window.
	// Held nodes (briefly offline) keep their channels so reassignments — and the
	// recordings they carry — are not disturbed by a transient blip.
	for _, d := range reclaim {
		n, err := c.Client.ReclaimChannels(d.NodeID)
		if err != nil {
			log.Printf("[controller] reclaim %s error: %v", d.NodeID, err)
		} else if n > 0 {
			log.Printf("[controller] reclaimed %d channels from node %s (down >= %s) and redistributing", n, d.NodeID, nodeReclaimGrace)
		}
	}

	// ── Assignment gating ──────────────────────────────────────────────────
	// The fleet gets ONE equal assignment once every node is live (the "startup"
	// assignment). After that, channels are NOT continuously reshuffled: a
	// stable, healthy fleet keeps exactly the channels it was given. Reassignment
	// happens ONLY on a membership change —
	//   • a node goes offline for >= the grace window (20m) → ALL its channels are
	//     reclaimed and redistributed equally ONCE to the remaining live nodes;
	//   • a node comes back / a new node joins → it receives an equal share (moved
	//     from others).
	// A node that is merely briefly offline (< grace) is HELD: it keeps its
	// channels so a blip never triggers a mass reassignment or recording loss.
	// Only the ELECTED leader (this goroutine, guarded by the controller lease)
	// assigns or unassigns channels — no other node ever reassigns. Recording
	// channels are never moved by the controller, so in-progress recordings on a
	// live node are never lost; balanceSite only relocates non-recording rows.
	needAssignment := false
	// inFleet = nodes that still own their channels (live or briefly offline and
	// within the grace window). A node only leaves inFleet once it has been down
	// past the grace, so a transient blip does NOT change the signature and does
	// NOT trigger a rebalance. Crossing the grace (or a node joining/leaving)
	// changes the signature → one rebalance.
	inFleet := append(append([]database.Node{}, active...), heldNodes...)
	fleetSig := fleetSignature(inFleet, reclaim)
	c.assignerAssignMu.Lock()
	if !c.assignerAssigned {
		// Cold-start: wait until ALL nodes are live before the one-time split,
		// so no node grabs every channel before the rest boot. A broken node can
		// never block the fleet forever — after ColdStartWaitSec we proceed with
		// whoever is up.
		if offlineCount == 0 {
			needAssignment = true
		} else {
			if c.assignerColdStart == nil {
				t := now
				c.assignerColdStart = &t
				log.Printf("[controller] cold-start: %d node(s) offline — holding one-time assignment until all nodes are live (max %ds)",
					offlineCount, cfg.ColdStartWaitSec)
			}
			if now.Sub(*c.assignerColdStart) >= time.Duration(cfg.ColdStartWaitSec)*time.Second {
				needAssignment = true
				log.Printf("[controller] cold-start window elapsed with %d offline — proceeding with one-time assignment", offlineCount)
			}
		}
	} else if fleetSig != c.assignerFleetSig {
		needAssignment = true
		log.Printf("[controller] fleet membership changed (was %q) — rebalancing once", c.assignerFleetSig)
	}
	c.assignerAssignMu.Unlock()

	// A cancelled runner leaves its final recording marker in the database.  A
	// fresh marker is protected, while an expired one is made movable so a full
	// restart can converge instead of preserving stale imbalance forever.
	c.clearStaleRecordingLeases(all, now, protectedOwnerSet)

	// Do not let the repair check bypass the cold-start gate.  In particular, an
	// early worker must not claim the entire unassigned pool merely because it
	// arrived before the rest of the fleet.  Once an initial allocation has been
	// made, later unassigned rows and genuine imbalance can be repaired safely.
	c.assignerAssignMu.Lock()
	assignedBefore := c.assignerAssigned
	c.assignerAssignMu.Unlock()
	canAssign := needAssignment || assignedBefore

	// Assign the complete pool as one deterministic sequence.  Per-site splits
	// could give the same early-sorting nodes an extra channel for each site.
	// Whole-pool splitting guarantees a max difference of one with no unassigned
	// rows, and only moves existing assignments while repairing an imbalance.
	if canAssign && len(active) > 0 {
		needAssignment = needAssignment || c.hasMovableImbalance(all, active, activeSet, heldSet)
		renewLease := func() bool {
			held, err := c.Client.TryAcquireControllerLease(c.NodeID, cfg.LeaseTTLSec)
			if err != nil {
				log.Printf("[controller] lease renewal error: %v", err)
				return false
			}
			return held
		}
		c.balanceSite("", all, active, activeSet, reclaimSet, heldSet, needAssignment, renewLease)
	}
	if needAssignment {
		c.assignerAssignMu.Lock()
		c.assignerAssigned = true
		c.assignerColdStart = nil
		c.assignerFleetSig = fleetSig
		c.assignerAssignMu.Unlock()
		log.Printf("[controller] assignment sweep: %d active node(s), equal split applied (Step B rebalance on membership change only)", len(active))
	}
}

const recordingLeaseTTL = 2 * time.Minute

func (c *Coordinator) clearStaleRecordingLeases(all []database.ChannelAssignment, now time.Time, protectedOwnerSet map[string]bool) {
	protected := make([]string, 0, len(protectedOwnerSet))
	for nodeID := range protectedOwnerSet {
		protected = append(protected, nodeID)
	}
	reset, err := c.Client.ResetStaleRecordingAssignments(now.Add(-recordingLeaseTTL), protected)
	if err != nil {
		log.Printf("[controller] clear stale recording leases: %v", err)
		return
	}
	if len(reset) == 0 {
		return
	}
	resetKeys := make(map[string]struct{}, len(reset))
	for _, ca := range reset {
		resetKeys[keyOf(ca)] = struct{}{}
	}
	for i := range all {
		if _, ok := resetKeys[keyOf(all[i])]; ok {
			all[i].Status = "claimed"
		}
	}
	log.Printf("[controller] reset %d stale recording lease(s)", len(reset))
}

func recordingLeaseFresh(lastHeartbeat string, now time.Time) bool {
	if lastHeartbeat == "" {
		return false
	}
	hb, err := time.Parse(time.RFC3339Nano, lastHeartbeat)
	return err == nil && now.Sub(hb) <= recordingLeaseTTL
}

// hasMovableImbalance measures ownership counts, never a channel's position in
// a sorted list.  Equal counts are already balanced even when a different
// perfectly-valid prior allocation gave a particular channel to another node.
// That distinction is what prevents a stable fleet from shuffling channels.
func (c *Coordinator) hasMovableImbalance(all []database.ChannelAssignment, active []database.Node, activeSet, heldSet map[string]bool) bool {
	if len(active) == 0 || len(all) == 0 {
		return false
	}
	pool := make([]database.ChannelAssignment, 0, len(all))
	for _, ca := range all {
		if !heldSet[ca.AssignedNode] {
			pool = append(pool, ca)
		}
	}
	nodes := append([]database.Node(nil), active...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	targets := equalSplitCounts(len(pool), nodes)
	counts := make(map[string]int, len(nodes))
	for _, ca := range pool {
		if ca.AssignedNode == "" || !activeSet[ca.AssignedNode] {
			return true
		}
		counts[ca.AssignedNode]++
	}
	for _, n := range nodes {
		if counts[n.NodeID] != targets[n.NodeID] {
			return true
		}
	}
	return false
}

// fleetSignature returns a stable string describing the current fleet
// membership (which nodes are active vs dead). It changes exactly when a node
// joins or leaves, and is used to trigger a one-time rebalance only on such a
// membership change rather than on every liveness tick.
func fleetSignature(active, dead []database.Node) string {
	act := make([]string, 0, len(active))
	for _, n := range active {
		act = append(act, "A:"+n.NodeID)
	}
	d := make([]string, 0, len(dead))
	for _, n := range dead {
		d = append(d, "D:"+n.NodeID)
	}
	sort.Strings(act)
	sort.Strings(d)
	return strings.Join(act, ",") + "|" + strings.Join(d, ",")
}

// computeLiveness evaluates liveness for every pooled channel and writes is_live.
// Returns a map keyed by "site/username" -> live (true if live). Only channels
// with a definitive result are present; unknown probes (errors, geo-bans) are
// skipped so their previous is_live is preserved.
func (c *Coordinator) computeLiveness(ctx context.Context, cfg assignerConfig, all []database.ChannelAssignment) map[string]bool {
	live := map[string]bool{}

	// ── Chaturbate: bulk affiliate call (fast path) + per-channel fallback ──
	// The onlinerooms affiliate API returns ONLY the models under that white-label
	// affiliate, so even when it works it structurally under-reports the full
	// channel set the DVRs actually record. We use it as a fast confirmation for
	// the models it lists, then probe every channel it did NOT confirm live
	// per-channel (the cookie-less chatvideocontext endpoint, cached + rate-limited)
	// so the controller always holds a truthful live set to distribute. When
	// AffiliateWM is empty or the call errors, we probe every channel.
	if cfg.CBEnabled && server.Config != nil {
		affiliateAvailable := false
		var affiliate map[string]internal.AffiliateModel
		if server.Config.AffiliateWM != "" {
			models, affiliateErr := internal.FetchAffiliateOnlineModels(ctx, server.Config.AffiliateWM, server.Config.Domain)
			if affiliateErr == nil {
				affiliate = models
				affiliateAvailable = true
			} else {
				log.Printf("[controller] CB affiliate check error: %v — falling back to per-channel liveness", affiliateErr)
			}
		} else {
			log.Printf("[controller] CB affiliate WM not configured — using per-channel liveness fallback")
		}

		var cbLive, cbDead []string
		sem := make(chan struct{}, maxInt(4, cfg.SCConcurrency*2))
		var wg sync.WaitGroup
		var liveMu sync.Mutex
		for _, ca := range all {
			if ca.Site != "chaturbate" {
				continue
			}
			k := keyOf(ca)
			if affiliateAvailable {
				if m, ok := affiliate[strings.ToLower(ca.Username)]; ok && m.CurrentShow != "away" && m.CurrentShow != "offline" {
					liveMu.Lock()
					live[k] = true
					cbLive = append(cbLive, ca.Username)
					liveMu.Unlock()
					continue
				}
			}
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			go func(username, key string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				res := c.checkChaturbateLive(ctx, username)
				liveMu.Lock()
				switch res {
				case LivenessLive:
					live[key] = true
					cbLive = append(cbLive, username)
				case LivenessOffline:
					live[key] = false
					cbDead = append(cbDead, username)
				}
				liveMu.Unlock()
			}(ca.Username, k)
		}
		wg.Wait()
		cbDead = filterNonRecording(all, cbDead)
		if err := c.Client.SetSiteLiveness("chaturbate", cbLive, cbDead); err != nil {
			log.Printf("[controller] CB is_live update error: %v", err)
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

// cbLiveEntry caches a single per-channel chaturbate liveness probe result.
type cbLiveEntry struct {
	res LivenessResult
	at  time.Time
}

// checkChaturbateLive probes one chaturbate channel via the configured
// LivenessChecker (the chatvideocontext room check, cookie-authenticated when
// server.Config.Cookies is set) and caches the result. This is the per-channel
// fallback used when the bulk affiliate onlinerooms API is unset or
// under-reports non-affiliate models, so the controller always has a truthful
// live set to distribute.
//
// Definitive results (live/offline) are cached for 2 minutes. Transient/unknown
// probes are cached only briefly (20s) so a channel whose probe was cut off by
// the cycle timeout (or hit a Cloudflare challenge) is retried on the very next
// cycle instead of being stranded as unassigned for minutes — otherwise the
// fleet plateaus at "however many channels one 90s pass can probe".
func (c *Coordinator) checkChaturbateLive(ctx context.Context, username string) LivenessResult {
	c.cbLiveMu.Lock()
	if c.cbLiveCache == nil {
		c.cbLiveCache = map[string]cbLiveEntry{}
	} else if e, ok := c.cbLiveCache[username]; ok {
		ttl := 2 * time.Minute
		if e.res == LivenessUnknown {
			ttl = 20 * time.Second
		}
		if time.Since(e.at) < ttl {
			c.cbLiveMu.Unlock()
			return e.res
		}
	}
	c.cbLiveMu.Unlock()

	var res LivenessResult
	if c.LiveCheck != nil {
		res = c.LiveCheck.CheckLive(ctx, "chaturbate", username)
	} else {
		res = LivenessUnknown
	}

	c.cbLiveMu.Lock()
	c.cbLiveCache[username] = cbLiveEntry{res: res, at: time.Now()}
	c.cbLiveMu.Unlock()
	return res
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

func fetchStripchatLive(client *http.Client, username string) (live, known bool) {
	// Try the v2 cam API first — this is the working endpoint used by
	// StreaMonitor and other scrapers. Stripchat removed __PRELOADED_STATE__
	// from their SSR pages around August 2026.
	if l, k := fetchStripchatLiveV2(client, username); k {
		return l, k
	}

	// Fall back to SSR page parsing (legacy).
	return fetchStripchatLiveSSR(client, username)
}

// fetchStripchatLiveV2 queries the Stripchat v2 cam API and returns live status.
func fetchStripchatLiveV2(client *http.Client, username string) (live, known bool) {
	cloakClient := &http.Client{Transport: internal.CreateTransport()}
	apiURL := fmt.Sprintf("https://stripchat.com/api/front/v2/models/username/%s/cam?uniq=%s", username, controllerUniq())
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return false, false
	}
	req.Header.Set("User-Agent", controllerUA)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", "https://stripchat.com")
	req.Header.Set("Referer", "https://stripchat.com/")

	resp, err := cloakClient.Do(req)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, true
	}
	if resp.StatusCode != http.StatusOK {
		return false, false
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, false
	}

	var data struct {
		User struct {
			User struct {
				Status   string `json:"status"`
				IsLive   bool   `json:"isLive"`
				IsOnline bool   `json:"isOnline"`
			} `json:"user"`
		} `json:"user"`
		Cam struct {
		} `json:"cam"`
		Error *struct {
			Code string `json:"code"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		return false, false
	}

	// API-level errors (e.g. "NOT_FOUND").
	if data.Error != nil {
		if data.Error.Code == "NOT_FOUND" {
			return false, true
		}
		return false, false
	}

	m := data.User.User
	// No cam data = model doesn't exist or is offline.
	if m.Status == "" && !m.IsLive && !m.IsOnline {
		return false, true
	}
	return m.IsLive && m.Status == "public", true
}

// controllerUniq generates a random alphanumeric string to defeat CDN caching.
func controllerUniq() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 16)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[n.Int64()]
	}
	return string(b)
}

// fetchStripchatLiveSSR is the legacy SSR parser for window.__PRELOADED_STATE__.
func fetchStripchatLiveSSR(client *http.Client, username string) (live, known bool) {
	cloakClient := &http.Client{Transport: internal.CreateTransport()}
	pageURL := "https://stripchat.com/" + username
	req, err := http.NewRequest("GET", pageURL, nil)
	if err != nil {
		return false, false
	}
	req.Header.Set("User-Agent", controllerUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := cloakClient.Do(req)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, true
	}
	if resp.StatusCode != http.StatusOK {
		return false, false
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, false
	}
	html := string(b)

	// Try multiple known SSR state variable names.
	for _, stateMarker := range []string{"window.__PRELOADED_STATE__", "window.__INITIAL_STATE__", "window.__APP_STATE__"} {
		idx := strings.Index(html, stateMarker)
		if idx < 0 {
			continue
		}
		start := idx + len(stateMarker)
		for start < len(html) && (html[start] == ' ' || html[start] == '=') {
			start++
		}
		end := findControllerJSONEnd(html, start)
		if end < 0 {
			continue
		}
		var state struct {
			ViewCamBase struct {
				Model struct {
					Status   string `json:"status"`
					IsLive   bool   `json:"isLive"`
					IsOnline bool   `json:"isOnline"`
				} `json:"model"`
			} `json:"viewCamBase"`
		}
		if err := json.Unmarshal([]byte(html[start:end]), &state); err != nil {
			continue
		}
		m := state.ViewCamBase.Model
		if m.Status == "" && !m.IsLive && !m.IsOnline {
			return false, true
		}
		return m.IsLive && m.Status == "public", true
	}

	return false, false
}

// findControllerJSONEnd returns the index after the closing } of the JSON object
// starting at pos. Returns -1 if not found.
func findControllerJSONEnd(s string, pos int) int {
	if pos >= len(s) || s[pos] != '{' {
		return -1
	}
	depth := 0
	inStr := false
	escape := false
	for i := pos; i < len(s); i++ {
		c := s[i]
		if escape {
			escape = false
			continue
		}
		if c == '\\' && inStr {
			escape = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

// balanceSite distributes the ENTIRE pool (live + offline) of one site evenly
// across the active nodes, so that NO channel is ever left unassigned.
//
// Assignment is a pure, deterministic equal split — it does NOT depend on
// liveness. The pool is sorted by username and handed out in contiguous blocks
// so the first (N%M) nodes get base+1 channels and the rest get base; this is
// stable across cycles (no churn on a healthy fleet) and reproducible. Every
// channel ends up on an active node, so even an offline/unknown channel is
// placed and simply records when it goes live.
//
// Two channels are deliberately NOT relocated: a channel actively recording on a
// LIVE node (moving it would lose the in-progress recording) and a channel on a
// HELD (briefly-offline, within the grace window) node (a transient blip must
// not trigger a mass reassignment). Everything else is moved onto its equal slot,
// so the fleet converges to an exact even split with zero unassigned channels.
func (c *Coordinator) balanceSite(site string, all []database.ChannelAssignment, active []database.Node, activeSet map[string]bool, reclaimSet map[string]bool, heldSet map[string]bool, doRebalance bool, renewLease func() bool) {
	// Rows held for a briefly offline node are deliberately outside this sweep.
	// The rest of the pool is balanced only by count, which preserves stable
	// ownership while still ensuring no channel is left unassigned.
	var pool []database.ChannelAssignment
	for _, ca := range all {
		if (site == "" || ca.Site == site) && !heldSet[ca.AssignedNode] {
			pool = append(pool, ca)
		}
	}
	if len(active) == 0 || len(pool) == 0 {
		return
	}

	// Deterministic order so the split is stable across cycles (no churn).
	sort.Slice(pool, func(i, j int) bool {
		if pool[i].Site == pool[j].Site {
			return pool[i].Username < pool[j].Username
		}
		return pool[i].Site < pool[j].Site
	})

	sorted := append([]database.Node{}, active...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NodeID < sorted[j].NodeID })
	targets := equalSplitCounts(len(pool), sorted)
	counts := make(map[string]int, len(sorted))
	for _, ca := range pool {
		if activeSet[ca.AssignedNode] {
			counts[ca.AssignedNode]++
		}
	}

	// First fill empty slots. This runs even without a membership rebalance so
	// newly discovered channels cannot remain unassigned.
	mutations := 0
	for _, ca := range pool {
		if ca.AssignedNode != "" {
			continue
		}
		want := pickUnderTarget(sorted, targets, counts)
		if want == "" {
			continue
		}
		if mutations > 0 && mutations%10 == 0 && !renewLease() {
			log.Printf("[controller] lease lost during assignment sweep; stopping")
			return
		}
		if ok, err := c.Client.ClaimSpecificChannel(ca.Username, ca.Site, want); err != nil {
			log.Printf("[controller] claim %s/%s -> %s error: %v", ca.Site, ca.Username, want, err)
			continue
		} else if ok {
			counts[want]++
			mutations++
		}
	}
	if !doRebalance {
		return
	}

	// Then move only from an overloaded/dead node to an under-target node.
	// A fresh recording is never moved; it simply occupies one of its node's
	// slots until the stream ends.
	for _, ca := range pool {
		cur := ca.AssignedNode
		if cur == "" || heldSet[cur] || (ca.Status == "recording" && activeSet[cur]) {
			continue
		}
		if activeSet[cur] && counts[cur] <= targets[cur] {
			continue
		}
		want := pickUnderTarget(sorted, targets, counts)
		if want == "" || want == cur {
			continue
		}
		if mutations > 0 && mutations%10 == 0 && !renewLease() {
			log.Printf("[controller] lease lost during assignment sweep; stopping")
			return
		}
		if err := c.Client.ReassignChannel(ca.Username, ca.Site, cur, want); err != nil {
			log.Printf("[controller] reassign %s/%s %s -> %s error: %v", ca.Site, ca.Username, cur, want, err)
			continue
		}
		if activeSet[cur] {
			counts[cur]--
		}
		counts[want]++
		mutations++
	}
}

func equalSplitCounts(total int, nodes []database.Node) map[string]int {
	targets := make(map[string]int, len(nodes))
	if len(nodes) == 0 {
		return targets
	}
	base, remainder := total/len(nodes), total%len(nodes)
	for i, n := range nodes {
		targets[n.NodeID] = base
		if i < remainder {
			targets[n.NodeID]++
		}
	}
	return targets
}

// equalSplitTarget assigns the first remainder nodes one additional channel.
// It is safe when there are fewer channels than nodes.
func equalSplitTarget(i, total int, nodes []database.Node) string {
	base := total / len(nodes)
	rem := total % len(nodes)
	if i < rem*(base+1) {
		return nodes[i/(base+1)].NodeID
	}
	return nodes[rem+(i-rem*(base+1))/base].NodeID
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
