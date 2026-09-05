package coordinator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
)

// ctrlDBRecord captures one request the fake controller-cycle Supabase saw.
type ctrlDBRecord struct {
	method string
	path   string
	body   string
}

// fakeCtrlDB serves the endpoints runControllerCycle touches and models the
// server-side semantics of the stale-recording-lease reset (including the
// protected-owner exclusion added to the PATCH filter), so the test can prove
// a protected node's recording is never handed to another node end to end.
type fakeCtrlDB struct {
	nodes       []database.Node
	assignments []database.ChannelAssignment // in-memory table the PATCH mutates

	reqs []ctrlDBRecord
}

func (f *fakeCtrlDB) handler(w http.ResponseWriter, r *http.Request) {
	body := []byte{}
	if r.Body != nil {
		if b, err := io.ReadAll(r.Body); err == nil {
			body = b
		}
	}
	f.reqs = append(f.reqs, ctrlDBRecord{method: r.Method, path: r.URL.RequestURI(), body: string(body)})

	// The real client prepends /rest/v1 to every path, so match on containment
	// rather than prefix.
	switch {
	case r.Method == "POST" && strings.Contains(r.URL.Path, "/rpc/claim_controller_lease"):
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(true)
		return
	case r.Method == "GET" && strings.Contains(r.URL.Path, "/app_settings"):
		writeJSON(w, []map[string]interface{}{}) // no assigner_config override → defaults
		return
	case r.Method == "GET" && strings.Contains(r.URL.Path, "/nodes"):
		writeJSON(w, f.nodes)
		return
	case r.Method == "GET" && strings.Contains(r.URL.Path, "/channel_assignments"):
		writeJSON(w, f.assignments)
		return
	case r.Method == "PATCH" && strings.Contains(r.URL.Path, "/channel_assignments"):
		// Stale-recording-lease reset. Model the real DB: only rows whose owner
		// is NOT excluded by the assigned_node=not.in.(...) filter are reset.
		f.resetStaleRecording(r)
		writeJSON(w, []database.ChannelAssignment{})
		return
	default:
		// Any other RPC would be an unexpected assignment write — make the test
		// fail loudly if one arrives.
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// resetStaleRecording applies the reset PATCH semantics to f.assignments and
// records whether the protected owner's row survived (i.e. was not matched).
func (f *fakeCtrlDB) resetStaleRecording(r *http.Request) {
	q := r.URL.Query()
	excluded := map[string]bool{}
	if v := q.Get("assigned_node"); strings.HasPrefix(v, "not.in.(") && strings.HasSuffix(v, ")") {
		for _, n := range strings.Split(v[len("not.in.("):len(v)-1], ",") {
			excluded[n] = true
		}
	}
	kept := f.assignments[:0]
	for _, ca := range f.assignments {
		if ca.Status == "recording" && !excluded[ca.AssignedNode] {
			ca.Status = "claimed" // matched → would be handed back to the pool
		}
		kept = append(kept, ca)
	}
	f.assignments = kept
}

func (f *fakeCtrlDB) saw(method, pathContains string) bool {
	for _, rec := range f.reqs {
		if rec.method == method && strings.Contains(rec.path, pathContains) {
			return true
		}
	}
	return false
}

func newFakeCtrlClient(t *testing.T, fake *fakeCtrlDB) *database.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	return database.NewClient(srv.URL, "test-key")
}

// TestControllerCycleNeverHandsOffProtectedRecording drives a full
// runControllerCycle where node-b is alive (online, fresh heartbeat —
// therefore protected) but its channel X carries a STALE recording marker.
// The controller must not reset X (the protected-owner exclusion is part of
// the reset PATCH) and must not reassign or reclaim it: the owner is alive,
// so nothing about X may move.
func TestControllerCycleNeverHandsOffProtectedRecording(t *testing.T) {
	now := time.Now().UTC()
	hb := now.Add(-30 * time.Second).Format(time.RFC3339Nano)         // node heartbeat: fresh
	staleMarker := now.Add(-5 * time.Minute).Format(time.RFC3339Nano) // channel heartbeat: stale (> 2m TTL)

	fake := &fakeCtrlDB{
		nodes: []database.Node{
			{NodeID: "node-a", Status: "online", LastHeartbeat: hb}, // the lease-holding leader
			{NodeID: "node-b", Status: "online", LastHeartbeat: hb}, // alive → protected owner
		},
		assignments: []database.ChannelAssignment{
			{Username: "X", Site: "chaturbate", AssignedNode: "node-b", Status: "recording", LastHeartbeat: staleMarker, IsLive: true},
		},
	}
	client := newFakeCtrlClient(t, fake)

	coord := &Coordinator{NodeID: "node-a", Client: client}
	coord.runControllerCycle()

	// The leader lease was acquired once (and only once — no renewal needed
	// because nothing was mutated).
	if !fake.saw("POST", "/rpc/claim_controller_lease") {
		t.Fatalf("expected the controller lease to be acquired")
	}

	// The stale-recording reset ran and carried the protected-owner exclusion
	// for the alive node-b into the server-side filter.
	var resetPath string
	for _, rec := range fake.reqs {
		if rec.method == "PATCH" && strings.Contains(rec.path, "status=eq.recording") {
			resetPath = rec.path
		}
	}
	if resetPath == "" {
		t.Fatalf("expected the stale-recording-lease reset PATCH; requests=%+v", fake.reqs)
	}
	// The exclusion must carry BOTH protected owners (node-a, the leader, and
	// node-b, the recording's owner). clearStaleRecordingLeases builds the list
	// by iterating a Go map, so the order inside not.in.(...) is
	// nondeterministic — compare as a set.
	excl := ""
	if i := strings.Index(resetPath, "assigned_node=not.in.("); i >= 0 {
		rest := resetPath[i+len("assigned_node=not.in.("):]
		if j := strings.Index(rest, ")"); j >= 0 {
			excl = rest[:j]
		}
	}
	got := map[string]bool{}
	for _, n := range strings.Split(excl, ",") {
		got[n] = true
	}
	if !got["node-a"] || !got["node-b"] || len(got) != 2 {
		t.Fatalf("reset must exclude the protected owners node-a and node-b, got %q: %s", excl, resetPath)
	}

	// The exclusion did its job server-side: X (owned by protected node-b) was
	// NOT reset, so it was never handed back to the pool or to another node.
	if len(fake.assignments) != 1 ||
		fake.assignments[0].AssignedNode != "node-b" ||
		fake.assignments[0].Status != "recording" {
		t.Fatalf("protected node-b's recording X must stay recording on node-b, got %+v", fake.assignments)
	}

	// And the cycle made no assignment writes at all (no claim/reassign/release).
	if fake.saw("POST", "/rpc/reassign_channel") ||
		fake.saw("POST", "/rpc/claim_specific_channel") ||
		fake.saw("POST", "/rpc/claim_channels") {
		t.Fatalf("controller must not move or reclaim a protected node's recording; requests=%+v", fake.reqs)
	}
}
