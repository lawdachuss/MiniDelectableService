package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/notifier"
	"github.com/teacat/chaturbate-dvr/server"
)

// stuckPoolPayload wraps assignment JSON rows in the /api/pool response shape.
func stuckPoolPayload(assignments ...string) string {
	return `{"mode":"pooled","assignments":[` + strings.Join(assignments, ",") + `]}`
}

// newStuckPauseCoordinator returns a pooled coordinator for the stuck-pause
// tests with an empty observation map.
func newStuckPauseCoordinator(nodeID string) *Coordinator {
	return &Coordinator{
		NodeID:         nodeID,
		Mode:           entity.PoolModePooled,
		stuckPauseSeen: map[string]int{},
	}
}

// TestStuckPauseCheckDetectsAndNotifies verifies the full monitor path: the
// fleet leader queries each online node's /api/pool, flags channels that are
// paused but still assigned (and not uploading/pending), and fires a
// notification only after the channel survives stuckPauseConfirmCycles
// consecutive checks.
func TestStuckPauseCheckDetectsAndNotifies(t *testing.T) {
	var nodeHits int64
	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&nodeHits, 1)
		fmt.Fprint(w, stuckPoolPayload(
			// Paused + idle + assigned to node-a → genuinely stuck.
			`{"username":"stuck1","site":"chaturbate","assigned_node":"node-a","status":"claimed","paused":true,"uploading":false,"pending":false}`,
			// Paused but actively uploading → legitimate processing, never flagged.
			`{"username":"busy","site":"chaturbate","assigned_node":"node-a","status":"claimed","paused":true,"uploading":true,"pending":false}`,
			// Paused with queued pipeline work → legitimate, never flagged.
			`{"username":"queued","site":"chaturbate","assigned_node":"node-a","status":"claimed","paused":true,"uploading":false,"pending":true}`,
			// Paused but not paused-on-this-node semantics are per-owner only:
			// this one belongs to node-b, so when node-a's pool is read it must
			// NOT be attributed to node-a (it IS stuck on node-b).
			`{"username":"other","site":"stripchat","assigned_node":"node-b","status":"claimed","paused":true,"uploading":false,"pending":false}`,
			// Manually paused by the user → intentional, never flagged as stuck.
			`{"username":"manual_pause","site":"chaturbate","assigned_node":"node-a","status":"claimed","paused":true,"uploading":false,"pending":false,"pause_reason":"manual"}`,
			// Not paused at all → skipped.
			`{"username":"active","site":"chaturbate","assigned_node":"node-a","status":"recording","paused":false,"uploading":false,"pending":false}`,
		))
	}))
	defer nodeSrv.Close()

	var ntfyHits int64
	ntfySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&ntfyHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ntfySrv.Close()

	oldCfg := server.Config
	server.Config = &entity.Config{NtfyURL: ntfySrv.URL, NtfyTopic: "alerts"}
	defer func() { server.Config = oldCfg }()
	notifier.Default.ResetCooldown(notifier.KeyStuckPause)

	c := newStuckPauseCoordinator("node-a")
	nodes := []database.Node{
		{NodeID: "node-a", Status: "online", WebURL: nodeSrv.URL},
		{NodeID: "node-b", Status: "online", WebURL: nodeSrv.URL},
	}

	// ── Cycle 1: observed but below the confirmation threshold ──
	c.runStuckPauseCheckWith(context.Background(), nodes)
	if got := atomic.LoadInt64(&ntfyHits); got != 0 {
		t.Fatalf("notification fired after a single observation (ntfy hits=%d)", got)
	}
	if got := c.stuckPauseSeen["node-a/chaturbate/stuck1"]; got != 1 {
		t.Fatalf("stuck1 seen count = %d, want 1", got)
	}
	if got := c.stuckPauseSeen["node-b/stripchat/other"]; got != 1 {
		t.Fatalf("other seen count = %d, want 1", got)
	}
	// busy/queued/active/manual_pause must never be tracked; stuck1 must only
	// be attributed to node-a (its owner), never node-b.
	for _, k := range []string{"node-a/chaturbate/busy", "node-a/chaturbate/queued", "node-a/chaturbate/active", "node-a/chaturbate/manual_pause", "node-b/chaturbate/stuck1"} {
		if _, ok := c.stuckPauseSeen[k]; ok {
			t.Fatalf("unexpected stuck-pause tracking for %q: %v", k, c.stuckPauseSeen)
		}
	}
	if len(c.stuckPauseSeen) != 2 {
		t.Fatalf("stuckPauseSeen = %v, want exactly 2 tracked keys", c.stuckPauseSeen)
	}

	// ── Cycle 2: confirmed → notification fires ──
	c.runStuckPauseCheckWith(context.Background(), nodes)
	if got := atomic.LoadInt64(&ntfyHits); got != 1 {
		t.Fatalf("ntfy hits = %d, want 1 after confirmation", got)
	}
	if got := atomic.LoadInt64(&nodeHits); got != 4 {
		t.Fatalf("node api hits = %d, want 4 (2 nodes × 2 cycles)", got)
	}
}

// TestStuckPauseCheckNonLeaderSkips verifies only the lowest-id ONLINE node
// runs the fleet scan — other nodes must not probe their peers.
func TestStuckPauseCheckNonLeaderSkips(t *testing.T) {
	var nodeHits int64
	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&nodeHits, 1)
		fmt.Fprint(w, stuckPoolPayload(
			`{"username":"stuck1","site":"chaturbate","assigned_node":"node-a","status":"claimed","paused":true}`,
		))
	}))
	defer nodeSrv.Close()

	oldCfg := server.Config
	server.Config = &entity.Config{}
	defer func() { server.Config = oldCfg }()

	c := newStuckPauseCoordinator("node-b")
	nodes := []database.Node{
		{NodeID: "node-a", Status: "online", WebURL: nodeSrv.URL},
		{NodeID: "node-b", Status: "online", WebURL: nodeSrv.URL},
	}

	c.runStuckPauseCheckWith(context.Background(), nodes)
	if got := atomic.LoadInt64(&nodeHits); got != 0 {
		t.Fatalf("non-leader node-b performed %d probes, want 0", got)
	}
	if len(c.stuckPauseSeen) != 0 {
		t.Fatalf("non-leader must not track stuck channels, got %v", c.stuckPauseSeen)
	}

	// A node with no web_url must not be probed even when it is the leader.
	c2 := newStuckPauseCoordinator("node-a")
	nodesNoURL := []database.Node{{NodeID: "node-a", Status: "online", WebURL: ""}}
	c2.runStuckPauseCheckWith(context.Background(), nodesNoURL)
	if got := atomic.LoadInt64(&nodeHits); got != 0 {
		t.Fatalf("leader without web_url probed %d times, want 0", got)
	}
}

// TestStuckPauseCheckPrunesRecovered verifies a channel that stops being stuck
// (resumed or released) is forgotten — its observation counter resets and no
// notification fires.
func TestStuckPauseCheckPrunesRecovered(t *testing.T) {
	var payload atomic.Value
	payload.Store(stuckPoolPayload(
		`{"username":"stuck1","site":"chaturbate","assigned_node":"node-a","status":"claimed","paused":true,"uploading":false,"pending":false}`,
	))
	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, payload.Load().(string))
	}))
	defer nodeSrv.Close()

	var ntfyHits int64
	ntfySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&ntfyHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ntfySrv.Close()

	oldCfg := server.Config
	server.Config = &entity.Config{NtfyURL: ntfySrv.URL, NtfyTopic: "alerts"}
	defer func() { server.Config = oldCfg }()
	notifier.Default.ResetCooldown(notifier.KeyStuckPause)

	c := newStuckPauseCoordinator("node-a")
	nodes := []database.Node{{NodeID: "node-a", Status: "online", WebURL: nodeSrv.URL}}

	// Cycle 1: observed once (below threshold).
	c.runStuckPauseCheckWith(context.Background(), nodes)
	if got := c.stuckPauseSeen["node-a/chaturbate/stuck1"]; got != 1 {
		t.Fatalf("seen count = %d, want 1", got)
	}

	// Cycle 2: the channel is no longer paused → recovered, counter pruned.
	payload.Store(stuckPoolPayload(
		`{"username":"stuck1","site":"chaturbate","assigned_node":"node-a","status":"claimed","paused":false}`,
	))
	c.runStuckPauseCheckWith(context.Background(), nodes)
	if len(c.stuckPauseSeen) != 0 {
		t.Fatalf("recovered channel still tracked: %v", c.stuckPauseSeen)
	}
	if got := atomic.LoadInt64(&ntfyHits); got != 0 {
		t.Fatalf("notification fired for a recovered channel (ntfy hits=%d)", got)
	}
}

// TestStuckPauseCheckFetchErrorIsolated verifies one unreachable node does not
// abort the scan — the other node is still checked and a stuck channel there
// is still detected across cycles.
func TestStuckPauseCheckFetchErrorIsolated(t *testing.T) {
	downSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	downURL := downSrv.URL
	downSrv.Close() // simulate an unreachable node

	var upHits int64
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&upHits, 1)
		fmt.Fprint(w, stuckPoolPayload(
			`{"username":"stuck1","site":"chaturbate","assigned_node":"node-b","status":"claimed","paused":true}`,
		))
	}))
	defer upSrv.Close()

	oldCfg := server.Config
	server.Config = &entity.Config{}
	defer func() { server.Config = oldCfg }()

	c := newStuckPauseCoordinator("node-a")
	nodes := []database.Node{
		{NodeID: "node-a", Status: "online", WebURL: downURL},
		{NodeID: "node-b", Status: "online", WebURL: upSrv.URL},
	}

	c.runStuckPauseCheckWith(context.Background(), nodes)
	if got := atomic.LoadInt64(&upHits); got != 1 {
		t.Fatalf("healthy node probed %d times, want 1", got)
	}
	if got := c.stuckPauseSeen["node-b/chaturbate/stuck1"]; got != 1 {
		t.Fatalf("stuck channel on healthy node not tracked (seen=%v)", c.stuckPauseSeen)
	}
}
