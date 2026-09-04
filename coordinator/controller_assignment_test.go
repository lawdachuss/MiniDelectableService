package coordinator

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
)

func TestEqualSplitTargetHandlesMoreNodesThanChannels(t *testing.T) {
	nodes := make([]database.Node, 18)
	for i := range nodes {
		nodes[i].NodeID = fmt.Sprintf("node-%02d", i+1)
	}

	counts := make(map[string]int)
	for i := 0; i < 19; i++ {
		counts[equalSplitTarget(i, 19, nodes)]++
	}
	for _, node := range nodes {
		want := 1
		if node.NodeID == "node-01" {
			want = 2
		}
		if got := counts[node.NodeID]; got != want {
			t.Fatalf("%s received %d channels, want %d", node.NodeID, got, want)
		}
	}
}

func TestRecordingLeaseFreshness(t *testing.T) {
	now := time.Now().UTC()
	if !recordingLeaseFresh(now.Add(-recordingLeaseTTL+time.Second).Format(time.RFC3339Nano), now) {
		t.Fatal("fresh recording lease considered stale")
	}
	if recordingLeaseFresh(now.Add(-recordingLeaseTTL-time.Second).Format(time.RFC3339Nano), now) {
		t.Fatal("expired recording lease considered fresh")
	}
	if recordingLeaseFresh("not-a-time", now) {
		t.Fatal("invalid recording lease considered fresh")
	}
}

// testNode constructs a Node helper for balance/hasMovableImbalance tests.
func testNode(id string, deadline *time.Time) database.Node {
	return database.Node{NodeID: id, Status: "online", SessionDeadline: deadline}
}

// TestHasMovableImbalanceIgnoresPinnedRecordingOnMigratingNode verifies that a
// deadline-migrating node's in-progress recording is NOT counted toward an
// imbalance (it stays pinned to finish on its owner), while its claimed slot is
// still loaded into the pool for redistribution.
func TestHasMovableImbalanceIgnoresPinnedRecordingOnMigratingNode(t *testing.T) {
	deadline := time.Now().Add(2 * time.Minute) // within the migration window
	active := []database.Node{testNode("node-a", nil), testNode("node-b", &deadline)}
	activeSet := map[string]bool{"node-a": true, "node-b": true}
	heldSet := map[string]bool{}
	// Both nodes are online and recently heartbeating → their recordings are
	// protected. node-b is also deadline-migrating: its claimed channels must
	// leave, but a recording on it must not be moved.
	protected := map[string]bool{"node-a": true, "node-b": true}

	all := []database.ChannelAssignment{
		{Username: "rec1", AssignedNode: "node-b", Status: "recording"},
		{Username: "idle1", AssignedNode: "node-b", Status: "claimed"},
		{Username: "idle2", AssignedNode: "node-a", Status: "claimed"},
	}

	c := &Coordinator{}
	// node-b's recording is excluded from the pool, so node-a's single channel
	// vs node-b's single movable channel are exactly balanced → no imbalance.
	if c.hasMovableImbalance(all, active, activeSet, heldSet, protected) {
		t.Fatal("pinned recording on a migrating node should not create an imbalance")
	}
}

// TestHasMovableImbalanceSeesImbalanceWhenProtectedNodeHasNoMovableWork
// guarantees that dropping a genuine movable channel out of a protected set
// surfaces an imbalance (i.e., we did not permanently pin everything).
func TestHasMovableImbalanceDetectsRealImbalance(t *testing.T) {
	active := []database.Node{testNode("node-a", nil), testNode("node-b", nil)}
	activeSet := map[string]bool{"node-a": true, "node-b": true}
	heldSet := map[string]bool{}
	protected := map[string]bool{} // neither recording → both fully movable

	all := []database.ChannelAssignment{
		{Username: "idle1", AssignedNode: "node-b", Status: "claimed"},
		{Username: "idle2", AssignedNode: "node-b", Status: "claimed"},
		{Username: "idle3", AssignedNode: "node-a", Status: "claimed"},
	}
	c := &Coordinator{}
	if !c.hasMovableImbalance(all, active, activeSet, heldSet, protected) {
		t.Fatal("imbalance (2 vs 1) should be detected when work is movable")
	}
}

func TestPickLiveRebalanceMoveNeverMovesARecording(t *testing.T) {
	active := map[string]bool{"node-a": true, "node-b": true}
	// node-a is over share (2 rec vs fair 1). It owns a live-claimed channel
	// (movable) AND an in-progress recording. The move must pick the live-claimed
	// channel and NEVER the recording.
	all := []database.ChannelAssignment{
		{Username: "aLiveMove", AssignedNode: "node-a", Status: "claimed", IsLive: true},
		{Username: "aRec", AssignedNode: "node-a", Status: "recording", IsLive: true},
	}
	rec := map[string]int{"node-a": 2, "node-b": 0}
	m := pickLiveRebalanceMove(all, active, rec, 1)
	if m == nil {
		t.Fatal("expected a move: node-a is over share and owns a live-claimed channel")
	}
	if m.ca.Username != "aLiveMove" {
		t.Fatalf("pickLiveRebalanceMove picked %q, want the live-claimed channel (never a recording)", m.ca.Username)
	}
	if m.ca.Status == "recording" {
		t.Fatalf("pickLiveRebalanceMove returned a recording channel: %+v", m.ca)
	}
}

func TestPickLiveRebalanceMoveRequiresLiveNotRecordingCandidate(t *testing.T) {
	active := map[string]bool{"node-a": true, "node-b": true}
	rec := map[string]int{"node-a": 3, "node-b": 0}
	// Node-a is heavily over share (fair 2), but the over-share channel it owns
	// is OFFLINE (not live) — nothing is live-and-not-recording, so no move.
	all := []database.ChannelAssignment{
		{Username: "a1", AssignedNode: "node-a", Status: "claimed", IsLive: false},
	}
	if m := pickLiveRebalanceMove(all, active, rec, 2); m != nil {
		t.Fatalf("no live-and-not-recording channel should be moved, got %+v", m.ca)
	}
}

func TestPickLiveRebalanceMoveRelievesOverShareNodeToColdest(t *testing.T) {
	active := map[string]bool{"node-a": true, "node-b": true, "node-c": true}
	rec := map[string]int{"node-a": 3, "node-b": 1, "node-c": 0}
	all := []database.ChannelAssignment{
		// node-a is over share (3 vs fair 2); node-c is coldest (0).
		{Username: "aLive", AssignedNode: "node-a", Status: "claimed", IsLive: true},
		// A recording on node-a must remain untouched.
		{Username: "aRec", AssignedNode: "node-a", Status: "recording", IsLive: true},
	}
	m := pickLiveRebalanceMove(all, active, rec, 2)
	if m == nil {
		t.Fatal("expected a move: node-a is over share and owns a live claimed channel")
	}
	if m.src != "node-a" {
		t.Fatalf("source = %s, want node-a", m.src)
	}
	if m.dst != "node-c" {
		t.Fatalf("dst = %s, want node-c (coldest)", m.dst)
	}
	if m.ca.Status == "recording" {
		t.Fatal("must never move a recording")
	}
}

func TestPickLiveRebalanceMoveTieBreaksDestByNodeID(t *testing.T) {
	active := map[string]bool{"node-a": true, "node-b": true, "node-c": true}
	rec := map[string]int{"node-a": 3, "node-b": 0, "node-c": 0}
	all := []database.ChannelAssignment{
		{Username: "aLive", AssignedNode: "node-a", Status: "claimed", IsLive: true},
	}
	// node-b and node-c both have 0 recordings (tie) and both below fair 2;
	// node-b sorts first.
	m := pickLiveRebalanceMove(all, active, rec, 2)
	if m == nil || m.dst != "node-b" {
		t.Fatalf("dst = %v, want node-b (tie-break by node_id)", m)
	}
}

func TestPickLiveRebalanceMoveNilWhenBalanced(t *testing.T) {
	active := map[string]bool{"node-a": true, "node-b": true}
	rec := map[string]int{"node-a": 1, "node-b": 1}
	all := []database.ChannelAssignment{
		{Username: "aLive", AssignedNode: "node-a", Status: "claimed", IsLive: true},
	}
	if m := pickLiveRebalanceMove(all, active, rec, 1); m != nil {
		t.Fatalf("node-a at fair share should not shed, got %+v", m.ca)
	}
}

func TestPickLiveRebalanceMoveSkipsUnassignedAndNonActiveNodes(t *testing.T) {
	active := map[string]bool{"node-b": true}
	rec := map[string]int{"node-b": 0}
	all := []database.ChannelAssignment{
		{Username: "orphan", AssignedNode: "", Status: "claimed", IsLive: true},
		{Username: "deadOwner", AssignedNode: "node-x", Status: "claimed", IsLive: true},
	}
	if m := pickLiveRebalanceMove(all, active, rec, 1); m != nil {
		t.Fatalf("channels on unassigned/off-node must never be moved, got %+v", m.ca)
	}
}

// TestFleetSignatureIncludesMigratingNode verifies that a node entering the
// deadline-migration window produces a distinct fleet signature, so the
// controller triggers exactly one reassignment when it enters/exits.
func TestFleetSignatureIncludesMigratingNode(t *testing.T) {
	now := time.Now()
	active := []database.Node{testNode("node-a", nil), testNode("node-b", nil)}

	sigNormal := fleetSignature(active, nil, nil)
	if strings.Contains(sigNormal, "M:node-b") {
		t.Fatalf("normal signature should not mark node-b as migrating: %q", sigNormal)
	}

	migrating := []database.Node{testNode("node-b", &now)}
	sigMigrating := fleetSignature(active, migrating, nil)
	if !strings.Contains(sigMigrating, "M:node-b") {
		t.Fatalf("migrating signature should mark node-b: %q", sigMigrating)
	}
	if sigNormal == sigMigrating {
		t.Fatal("fleet signature must differ when a node enters the migration window")
	}
}
