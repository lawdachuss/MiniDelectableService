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

// syncDBRecord captures one request the fake assignment-sync Supabase received.
type syncDBRecord struct {
	method string
	path   string
	body   string
}

// fakeSyncDB stands in for the PostgREST endpoints the assignment-sync loop
// uses: node-assignment GET, single-row GET, re-pin PATCH, and the
// mark/reset RPCs. Every request is recorded for assertions.
type fakeSyncDB struct {
	nodeAssignments []database.ChannelAssignment // response to assigned_node=eq.<me>
	singleRow       []database.ChannelAssignment // response to a username+site single-row GET (empty = no row)

	reqs []syncDBRecord
}

func (f *fakeSyncDB) handler(w http.ResponseWriter, r *http.Request) {
	body := []byte{}
	if r.Body != nil {
		if b, err := io.ReadAll(r.Body); err == nil {
			body = b
		}
	}
	f.reqs = append(f.reqs, syncDBRecord{method: r.Method, path: r.URL.RequestURI(), body: string(body)})

	switch r.Method {
	case "GET":
		q := r.URL.Query()
		if q.Get("assigned_node") != "" {
			writeJSON(w, f.nodeAssignments)
			return
		}
		if q.Get("username") != "" {
			writeJSON(w, f.singleRow)
			return
		}
		writeJSON(w, []database.ChannelAssignment{})
	case "PATCH":
		// ReassertAssignmentNode — body carries the new assigned_node.
		w.WriteHeader(http.StatusOK)
		writeJSON(w, []database.ChannelAssignment{})
	default:
		// RPCs (mark_channel_recording / reset_channel_status) return void → 204.
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (f *fakeSyncDB) record(method, pathContains, bodyContains string) *syncDBRecord {
	for i := range f.reqs {
		rec := &f.reqs[i]
		if rec.method == method && strings.Contains(rec.path, pathContains) {
			if bodyContains == "" || strings.Contains(rec.body, bodyContains) {
				return rec
			}
		}
	}
	return nil
}

func (f *fakeSyncDB) count(method, pathContains string) int {
	n := 0
	for _, rec := range f.reqs {
		if rec.method == method && strings.Contains(rec.path, pathContains) {
			n++
		}
	}
	return n
}

func newFakeSyncClient(t *testing.T, fake *fakeSyncDB) *database.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	return database.NewClient(srv.URL, "test-key")
}

// syncChanStub is a configurable in-memory ChannelManager for sync-loop tests.
type syncChanStub struct {
	local map[string]*syncChannelState
	// removed records usernames passed to RemoveChannelForReassignment.
	removed []string
	// started records usernames passed to CreateChannelFromAssignment.
	started []string
}

type syncChannelState struct {
	site      string
	recording bool
}

func newSyncChanStub() *syncChanStub {
	return &syncChanStub{local: map[string]*syncChannelState{}}
}

func (m *syncChanStub) CreateChannelFromAssignment(ca *database.ChannelAssignment) error {
	m.started = append(m.started, ca.Username)
	return nil
}
func (m *syncChanStub) RemoveChannelForReassignment(username string) error {
	m.removed = append(m.removed, username)
	return nil
}
func (m *syncChanStub) GetLocalChannels() []string {
	var out []string
	for u := range m.local {
		out = append(out, u)
	}
	return out
}
func (m *syncChanStub) LocalChannelSite(username string) (string, bool) {
	if ch, ok := m.local[username]; ok {
		return ch.site, true
	}
	return "", false
}
func (m *syncChanStub) HasPendingSegments(username string) bool { return false }
func (m *syncChanStub) IsRecording(username string) bool {
	if ch, ok := m.local[username]; ok {
		return ch.recording
	}
	return false
}
func (m *syncChanStub) ManualPausedChannels() []ChannelPause { return nil }
func (m *syncChanStub) CFBlockedCount() int                  { return 0 }
func (m *syncChanStub) RequestCookieRefresh()                {}

func hasSubstr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// TestAssignmentSyncRepinsReassignedRecording: a channel this node is actively
// recording but whose row was reassigned to node-b must be re-pinned back to
// this node (so node-b never starts a duplicate) and re-marked recording; the
// local stop is deferred. A non-recording lost channel is stopped normally.
func TestAssignmentSyncRepinsReassignedRecording(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{}, // X is no longer assigned to us
		singleRow: []database.ChannelAssignment{
			{Username: "X", Site: "chaturbate", AssignedNode: "node-b", Status: "claimed"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["X"] = &syncChannelState{site: "chaturbate", recording: true}
	stub.local["Y"] = &syncChannelState{site: "chaturbate", recording: false}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.runAssignmentSyncCycleWith(client)

	// Y is not assigned to us and not recording → stopped.
	if !hasSubstr(stub.removed, "Y") {
		t.Fatalf("non-recording lost channel Y was not removed (removed=%v)", stub.removed)
	}
	// X is recording → never torn down.
	if hasSubstr(stub.removed, "X") {
		t.Fatalf("actively recording channel X must not be removed mid-file")
	}
	// X's row was reassigned to node-b → we must re-pin it to node-a.
	repin := fake.record("PATCH", "/channel_assignments?username=eq.X", `"assigned_node":"node-a"`)
	if repin == nil {
		t.Fatalf("expected a re-pin PATCH re-asserting X to node-a; requests=%+v", fake.reqs)
	}
	// The re-pin must be conditional on the observed owner (node-b): if the row
	// moved again between the read and the PATCH, the PATCH must no-op rather
	// than resurrect a released/paused channel.
	if !strings.Contains(repin.path, "assigned_node=eq.node-b") {
		t.Fatalf("re-pin PATCH must be filtered by assigned_node=eq.node-b (the observed owner); path=%s", repin.path)
	}
	// After re-pinning, X must be re-affirmed as recording (fresh heartbeat pin).
	if fake.record("POST", "/rpc/mark_channel_recording", `"p_username":"X"`) == nil {
		t.Fatalf("expected X to be re-marked recording after re-pin; requests=%+v", fake.reqs)
	}
	// Y is gone from the local set, so nothing else may be marked or reset.
	if fake.record("POST", "/rpc/mark_channel_recording", `"p_username":"Y"`) != nil {
		t.Fatalf("non-local Y must not be marked recording")
	}
	if !coord.recordingMarked[recMarkKey{site: "chaturbate", username: "X"}] {
		t.Fatalf("X should be tracked as marked recording after the cycle")
	}
}

// TestAssignmentSyncDoesNotRepinReleasedChannel: when the row was RELEASED to
// unassigned (user pause/removal, Cloudflare shed) rather than reassigned, the
// recording node must NOT re-pin it — it finishes the file and is removed by a
// later cycle.
func TestAssignmentSyncDoesNotRepinReleasedChannel(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{},
		singleRow: []database.ChannelAssignment{
			{Username: "X", Site: "chaturbate", AssignedNode: "", Status: "unassigned"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["X"] = &syncChannelState{site: "chaturbate", recording: true}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.runAssignmentSyncCycleWith(client)

	if hasSubstr(stub.removed, "X") {
		t.Fatalf("recording channel X must not be removed mid-file even when released")
	}
	if fake.count("PATCH", "/channel_assignments") != 0 {
		t.Fatalf("released-to-unassigned channel must NOT be re-pinned; requests=%+v", fake.reqs)
	}
	// X is not ours in the DB and not re-pinned → its row must not be refreshed.
	if fake.record("POST", "/rpc/mark_channel_recording", `"p_username":"X"`) != nil {
		t.Fatalf("row not owned by us must not be marked recording")
	}
}

// TestAssignmentSyncSkipsUnownedRecordingWithoutRow: if the row is gone
// entirely while we still record, we defer the stop and touch nothing (no
// re-pin, no mark) — the row is not ours to refresh.
func TestAssignmentSyncSkipsUnownedRecordingWithoutRow(t *testing.T) {
	fake := &fakeSyncDB{nodeAssignments: []database.ChannelAssignment{}}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["X"] = &syncChannelState{site: "chaturbate", recording: true}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.runAssignmentSyncCycleWith(client)

	if hasSubstr(stub.removed, "X") {
		t.Fatalf("recording channel X must not be removed when its row is missing")
	}
	if fake.count("PATCH", "/channel_assignments") != 0 {
		t.Fatalf("missing row must not be re-pinned")
	}
	if fake.record("POST", "/rpc/mark_channel_recording", `"p_username":"X"`) != nil {
		t.Fatalf("missing row must not be marked recording")
	}
}

// TestAssignmentSyncResetsOwnFinishedRecordingMarker: a channel this node
// still owns but no longer records, and that this node marked 'recording'
// earlier in the process, must be reset to 'claimed' exactly once — the
// controller never resets markers on a live owner, so the owner cleans up
// after itself (otherwise the finished recording stays 'recording' with a
// stale is_live until the node goes offline).
func TestAssignmentSyncResetsOwnFinishedRecordingMarker(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{
			{Username: "Z", Site: "chaturbate", AssignedNode: "node-a", Status: "claimed"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["Z"] = &syncChannelState{site: "chaturbate", recording: false}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client,
		recordingMarked: map[recMarkKey]bool{recMarkKey{site: "chaturbate", username: "Z"}: true}}
	coord.runAssignmentSyncCycleWith(client)

	reset := fake.record("POST", "/rpc/reset_channel_status", `"p_username":"Z"`)
	if reset == nil {
		t.Fatalf("expected owner-side reset of finished recording Z to claimed; requests=%+v", fake.reqs)
	}
	if !strings.Contains(reset.body, `"p_status":"claimed"`) {
		t.Fatalf("reset body = %s, want p_status=claimed", reset.body)
	}
	if coord.recordingMarked[recMarkKey{site: "chaturbate", username: "Z"}] {
		t.Fatalf("Z should no longer be tracked as marked recording after the reset")
	}
	if hasSubstr(stub.removed, "Z") {
		t.Fatalf("owned idle channel Z must not be removed")
	}
	// An idle channel must not be marked as if it were recording.
	if fake.record("POST", "/rpc/mark_channel_recording", `"p_username":"Z"`) != nil {
		t.Fatalf("idle channel Z must not be marked recording")
	}
}

// TestAssignmentSyncMarksRecordingOwnedChannel: a channel still assigned to us
// and actively recording is re-marked each cycle and tracked.
func TestAssignmentSyncMarksRecordingOwnedChannel(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{
			{Username: "X", Site: "chaturbate", AssignedNode: "node-a", Status: "recording"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["X"] = &syncChannelState{site: "chaturbate", recording: true}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.runAssignmentSyncCycleWith(client)

	if fake.record("POST", "/rpc/mark_channel_recording", `"p_username":"X"`) == nil {
		t.Fatalf("expected X (owned + recording) to be marked recording; requests=%+v", fake.reqs)
	}
	if !coord.recordingMarked[recMarkKey{site: "chaturbate", username: "X"}] {
		t.Fatalf("X should be tracked as marked recording")
	}
	if hasSubstr(stub.removed, "X") {
		t.Fatalf("owned recording channel X must not be removed")
	}
}

// TestAssignmentSyncClearsGhostMarkerMovedAway: this node marked G 'recording'
// earlier, but the row moved to node-b while the re-pin could not run (DB
// unreachable — the same wedge that let the lease reset move the row). G is no
// longer recorded locally and the row is still 'recording' with a STALE
// heartbeat (node-b is not refreshing it), so the marker is ours to clear —
// otherwise it would sit 'recording' on node-b for its whole session, since
// the controller never resets markers on a live owner.
func TestAssignmentSyncClearsGhostMarkerMovedAway(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{},
		singleRow: []database.ChannelAssignment{
			{Username: "G", Site: "chaturbate", AssignedNode: "node-b", Status: "recording",
				LastHeartbeat: time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339Nano)},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub() // nothing local: G was already removed
	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client,
		recordingMarked: map[recMarkKey]bool{recMarkKey{site: "chaturbate", username: "G"}: true}}
	coord.runAssignmentSyncCycleWith(client)

	reset := fake.record("POST", "/rpc/reset_channel_status", `"p_username":"G"`)
	if reset == nil {
		t.Fatalf("expected ghost marker G to be reset to claimed; requests=%+v", fake.reqs)
	}
	if !strings.Contains(reset.body, `"p_status":"claimed"`) {
		t.Fatalf("reset body = %s, want p_status=claimed", reset.body)
	}
	if coord.recordingMarked[recMarkKey{site: "chaturbate", username: "G"}] {
		t.Fatalf("G should no longer be tracked after the ghost-marker clear")
	}
}

// TestAssignmentSyncKeepsGhostMarkerWhenNewOwnerCapturing: the row moved to
// node-b and node-b IS actively capturing it (fresh heartbeat refreshed by
// node-b's own mark phase). This node must NOT clear the marker — doing so
// would unpin node-b's live capture and let the controller hand the row to a
// third node mid-file. The local entry is dropped as moot (node-b manages it
// now), but no reset RPC may fire.
func TestAssignmentSyncKeepsGhostMarkerWhenNewOwnerCapturing(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{},
		singleRow: []database.ChannelAssignment{
			{Username: "G", Site: "chaturbate", AssignedNode: "node-b", Status: "recording",
				LastHeartbeat: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client,
		recordingMarked: map[recMarkKey]bool{recMarkKey{site: "chaturbate", username: "G"}: true}}
	coord.runAssignmentSyncCycleWith(client)

	if fake.record("POST", "/rpc/reset_channel_status", `"p_username":"G"`) != nil {
		t.Fatalf("must NOT reset G while node-b is actively recording it; requests=%+v", fake.reqs)
	}
	if coord.recordingMarked[recMarkKey{site: "chaturbate", username: "G"}] {
		t.Fatalf("G's local marker should be dropped as moot now that node-b owns it")
	}
}
