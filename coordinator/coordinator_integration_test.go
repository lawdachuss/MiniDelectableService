//go:build integration

package coordinator

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// These integration tests require a real Supabase project.
// Run with: go test -tags=integration -run TestIntegration ./coordinator/
// Set SUPABASE_URL and SUPABASE_API_KEY env vars to point to a test Supabase project.

func skipIfNoSupabase(t *testing.T) *database.Client {
	t.Helper()
	url := os.Getenv("SUPABASE_URL")
	key := os.Getenv("SUPABASE_API_KEY")
	if url == "" || key == "" {
		t.Skip("Skipping integration test: set SUPABASE_URL and SUPABASE_API_KEY")
	}
	return database.NewClient(url, key)
}

const testNodeID = "coordinator-int-test-node"

func cleanupAssignments(t *testing.T, client *database.Client, ids ...string) {
	t.Helper()
	for _, id := range ids {
		_ = client.ReleaseNodeChannels(id)
	}
	// Also clean up any assignments created during the test
	if assignments, err := client.GetAllAssignments(); err == nil {
		for _, a := range assignments {
			if a.AssignedNode == testNodeID || a.Username == testNodeID {
				_ = client.DeleteAssignment(a.Username, a.Site)
			}
		}
	}
}

func cleanupNode(t *testing.T, client *database.Client) {
	t.Helper()
	_ = client.DeleteAssignment(testNodeID, "chaturbate")
	_ = client.DeleteAssignment(testNodeID, "stripchat")
	// Mark node offline (can't delete — FK reference)
	_ = client.UpdateNodeStatus(testNodeID, "offline")
}

// TestIntegrationRegisterAndHeartbeat verifies node registration and heartbeat.
func TestIntegrationRegisterAndHeartbeat(t *testing.T) {
	client := skipIfNoSupabase(t)

	coord := New(client, &mockChannelManager{})
	coord.NodeID = testNodeID
	coord.Mode = entity.PoolModePooled
	defer cleanupNode(t, client)

	// Register
	coord.Register()

	// Verify node exists
	node, err := client.GetNode(testNodeID)
	if err != nil {
		t.Fatalf("GetNode error: %v", err)
	}
	if node.Status != "online" {
		t.Errorf("node status = %q, want online", node.Status)
	}

	// Heartbeat
	err = client.HeartbeatNode(testNodeID, 3)
	if err != nil {
		t.Fatalf("HeartbeatNode error: %v", err)
	}

	// Verify heartbeat updated
	node, err = client.GetNode(testNodeID)
	if err != nil {
		t.Fatalf("GetNode error: %v", err)
	}
	if node.CurrentLoad != 3 {
		t.Errorf("current_load = %d, want 3", node.CurrentLoad)
	}
}

// TestIntegrationClaimChannels verifies atomic channel claiming.
func TestIntegrationClaimChannels(t *testing.T) {
	client := skipIfNoSupabase(t)
	defer cleanupAssignments(t, client, testNodeID)
	defer cleanupNode(t, client)

	// Register node
	coord := New(client, &mockChannelManager{})
	coord.NodeID = testNodeID
	coord.Mode = entity.PoolModePooled
	coord.Register()

	// Create test assignments for this node.  Claiming does NOT filter on
	// is_live: a DVR claims channels from the pool and records them when they
	// actually go live, so offline channels must be claimable too.
	assignments := []database.ChannelAssignment{
		{Username: testNodeID + "-ch1", Site: "chaturbate", Status: "unassigned", IsLive: true, Framerate: 60, Resolution: 1080},
		{Username: testNodeID + "-ch2", Site: "stripchat", Status: "unassigned", IsLive: false, Framerate: 30, Resolution: 720},
		{Username: testNodeID + "-ch3", Site: "chaturbate", Status: "unassigned", IsLive: false, Framerate: 60, Resolution: 2160},
	}
	if err := client.BulkInsertAssignments(assignments); err != nil {
		t.Fatalf("BulkInsertAssignments error: %v", err)
	}
	defer func() {
		for _, a := range assignments {
			_ = client.DeleteAssignment(a.Username, a.Site)
		}
	}()

	// Claim up to 2 channels (regardless of is_live).  The claim_channels RPC
	// randomizes order (ORDER BY RANDOM(), SKIP LOCKED), so ANY 2 unassigned
	// channels may be claimed — exactly two must be (limit honored), both
	// assigned to us, and none double-claimed.  Because this runs against a live
	// pool that may contain foreign unassigned channels (e.g. a real channel
	// waiting to be claimed), a claimed row may occasionally be a foreign one;
	// the assertions below are written to be correct either way.
	claimed, err := client.ClaimChannels(testNodeID, 2)
	if err != nil {
		t.Fatalf("ClaimChannels error: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("ClaimChannels returned %d, want 2", len(claimed))
	}

	// Verify claimed channels are assigned to our node
	for _, c := range claimed {
		if c.AssignedNode != testNodeID {
			t.Errorf("claimed %s assigned to %q, want %q", c.Username, c.AssignedNode, testNodeID)
		}
		if c.Status != "claimed" {
			t.Errorf("claimed %s status = %q, want claimed", c.Username, c.Status)
		}
	}

	// Of our 3 test channels, exactly (2 - foreignClaimed) must have been
	// claimed, where foreignClaimed is how many non-test rows the RPC picked
	// (only possible when a foreign unassigned channel exists in the pool).
	claimedSet := make(map[string]bool, len(claimed))
	foreignClaimed := 0
	for _, c := range claimed {
		claimedSet[c.Username] = true
		if !strings.HasPrefix(c.Username, testNodeID+"-") {
			foreignClaimed++
		}
	}
	channels := []struct {
		username string
		site     string
	}{
		{testNodeID + "-ch1", "chaturbate"},
		{testNodeID + "-ch2", "stripchat"},
		{testNodeID + "-ch3", "chaturbate"},
	}
	testClaimed := 0
	for _, ch := range channels {
		a, err := client.GetAssignment(ch.username, ch.site)
		if err != nil {
			t.Fatalf("GetAssignment(%s) error: %v", ch.username, err)
		}
		if a != nil && a.AssignedNode == testNodeID {
			testClaimed++
			if !claimedSet[ch.username] {
				t.Errorf("%s assigned to us but not in claimed response", ch.username)
			}
		}
	}
	if want := 2 - foreignClaimed; testClaimed != want {
		t.Errorf("%d of our test channels claimed, want %d (foreign rows claimed: %d)", testClaimed, want, foreignClaimed)
	}
}

// TestIntegrationFairShareAcrossNodes verifies that multiple nodes get a fair share.
func TestIntegrationFairShareAcrossNodes(t *testing.T) {
	client := skipIfNoSupabase(t)

	nodeA := testNodeID + "-a"
	nodeB := testNodeID + "-b"

	// Register both nodes and heartbeat them so GetAliveNodes sees them as alive.
	// Register() alone relies on the last_heartbeat column default, which only
	// fires on a fresh INSERT — on a re-run against an existing node row the
	// upsert merge leaves the stale heartbeat, so the node looks dead and
	// fair-share computes 0.  Heartbeating mirrors what the heartbeat loop does
	// in production.
	for _, id := range []string{nodeA, nodeB} {
		coord := New(client, &mockChannelManager{})
		coord.NodeID = id
		coord.Mode = entity.PoolModePooled
		coord.Register()
		if err := client.HeartbeatNode(id, 0); err != nil {
			t.Fatalf("HeartbeatNode(%s) error: %v", id, err)
		}
	}
	defer func() {
		_ = client.UpdateNodeStatus(nodeA, "offline")
		_ = client.UpdateNodeStatus(nodeB, "offline")
	}()

	// Create 6 live unassigned channels
	var chans []database.ChannelAssignment
	for i := 0; i < 6; i++ {
		chans = append(chans, database.ChannelAssignment{
			Username:   fmt.Sprintf("%s-ch%d", testNodeID, i),
			Site:       "chaturbate",
			Status:     "unassigned",
			IsLive:     true,
			Framerate:  60,
			Resolution: 1080,
		})
	}
	if err := client.BulkInsertAssignments(chans); err != nil {
		t.Fatalf("BulkInsertAssignments error: %v", err)
	}
	defer func() {
		for _, a := range chans {
			_ = client.DeleteAssignment(a.Username, a.Site)
		}
	}()

	// Simulate fair-share claim by each node.
	// With 2 alive nodes and 6 live channels, fair share = ceil(6/2) = 3 each.
	// The pool may also contain foreign unassigned channels (real channels
	// waiting to be claimed) — the RPC randomizes, so a foreign row may win a
	// slot.  We track how many foreign rows each node claimed so the final
	// assertion stays correct either way.
	foreignClaimed := 0
	for _, id := range []string{nodeA, nodeB} {
		stats, err := client.GetAssignmentStats()
		if err != nil {
			t.Fatalf("GetAssignmentStats error: %v", err)
		}
		fairShare := 0
		if stats.TotalAliveNodes > 0 {
			fairShare = (stats.TotalLiveChannels + stats.TotalAliveNodes - 1) / stats.TotalAliveNodes
		}
		claimed, err := client.ClaimChannels(id, fairShare)
		if err != nil {
			t.Fatalf("ClaimChannels for %s error: %v", id, err)
		}
		for _, c := range claimed {
			if !strings.HasPrefix(c.Username, testNodeID+"-") {
				foreignClaimed++
				// Release foreign rows immediately — they belong to the real pool.
				if err := client.ReleaseChannel(c.Username, c.Site); err != nil {
					t.Logf("warning: could not release foreign row %s: %v", c.Username, err)
				}
			}
		}
		t.Logf("Node %s claimed %d channels (fair share = %d, foreign: %d)", id, len(claimed), fairShare, foreignClaimed)
	}

	// Verify total claimed = 6 (no double-claiming), minus any foreign rows the
	// RPC grabbed (which we released).  No test channel may be claimed twice.
	all, err := client.GetAllAssignments()
	if err != nil {
		t.Fatalf("GetAllAssignments error: %v", err)
	}
	totalClaimed := 0
	for _, a := range all {
		if a.AssignedNode == nodeA || a.AssignedNode == nodeB {
			// Only count our test channels
			for i := 0; i < 6; i++ {
				if a.Username == fmt.Sprintf("%s-ch%d", testNodeID, i) {
					totalClaimed++
					break
				}
			}
		}
	}
	// Every claim slot that did NOT go to a foreign row must have gone to one of
	// our 6 test channels (each test channel is claimed at most once).  Use >=
	// rather than == because fairShare is computed from the LIVE pool: if a real
	// production channel is live mid-test, fairShare inflates, claim slots exceed
	// the candidate count, and ALL test channels get claimed (totalClaimed = 6
	// while 6 - foreignClaimed < 6).  The invariant that must always hold is that
	// no claim slot was wasted on a foreign row at the expense of a test channel.
	if totalClaimed < 6-foreignClaimed {
		t.Errorf("total test channels claimed = %d, want >= %d (foreign rows claimed: %d)", totalClaimed, 6-foreignClaimed, foreignClaimed)
	}

	// Verify no channel is assigned to both nodes
	claimedBy := map[string]string{}
	for _, a := range all {
		if a.AssignedNode == "" {
			continue
		}
		if prev, ok := claimedBy[a.Username]; ok && prev != a.AssignedNode {
			t.Errorf("split-brain: %s claimed by both %s and %s", a.Username, prev, a.AssignedNode)
		}
		claimedBy[a.Username] = a.AssignedNode
	}
}

// TestIntegrationNodeStatusTransitions tests online → draining → offline.
func TestIntegrationNodeStatusTransitions(t *testing.T) {
	client := skipIfNoSupabase(t)
	defer cleanupNode(t, client)

	coord := New(client, &mockChannelManager{})
	coord.NodeID = testNodeID
	coord.Mode = entity.PoolModePooled
	coord.Register()

	if err := client.UpdateNodeStatus(testNodeID, "draining"); err != nil {
		t.Fatalf("UpdateNodeStatus draining error: %v", err)
	}
	node, _ := client.GetNode(testNodeID)
	if node.Status != "draining" {
		t.Errorf("status = %q, want draining", node.Status)
	}

	if err := client.UpdateNodeStatus(testNodeID, "offline"); err != nil {
		t.Fatalf("UpdateNodeStatus offline error: %v", err)
	}
	node, _ = client.GetNode(testNodeID)
	if node.Status != "offline" {
		t.Errorf("status = %q, want offline", node.Status)
	}
}

// TestIntegrationDeadlineMigrationPrimitives verifies the DB layer behind the
// deadline-migration loop on real PostgREST: finding nodes with an imminent
// session_deadline and atomically reassigning a channel off them.
func TestIntegrationDeadlineMigrationPrimitives(t *testing.T) {
	client := skipIfNoSupabase(t)

	deadlineNode := testNodeID + "-deadline"
	targetNode := testNodeID + "-target"
	defer func() {
		_ = client.UpdateNodeStatus(deadlineNode, "offline")
		_ = client.UpdateNodeStatus(targetNode, "offline")
	}()

	// Node whose session deadline is ~5 minutes away (imminent for a 10m window).
	dl := time.Now().Add(5 * time.Minute).UTC()
	if err := client.UpsertNode(&database.Node{
		NodeID:          deadlineNode,
		Hostname:        "test-host-deadline",
		Status:          "online",
		SessionDeadline: &dl,
	}); err != nil {
		t.Fatalf("UpsertNode(deadline) error: %v", err)
	}
	if err := client.HeartbeatNode(deadlineNode, 1); err != nil {
		t.Fatalf("HeartbeatNode(deadline) error: %v", err)
	}

	// Healthy target node (no deadline).
	if err := client.UpsertNode(&database.Node{
		NodeID:   targetNode,
		Hostname: "test-host-target",
		Status:   "online",
	}); err != nil {
		t.Fatalf("UpsertNode(target) error: %v", err)
	}
	if err := client.HeartbeatNode(targetNode, 0); err != nil {
		t.Fatalf("HeartbeatNode(target) error: %v", err)
	}

	// A channel assigned to the deadline node.
	assignment := database.ChannelAssignment{
		Username:     testNodeID + "-mig-ch",
		Site:         "chaturbate",
		AssignedNode: deadlineNode,
		Status:       "claimed",
		IsLive:       true,
		Framerate:    60,
		Resolution:   1080,
	}
	if err := client.BulkInsertAssignments([]database.ChannelAssignment{assignment}); err != nil {
		t.Fatalf("BulkInsertAssignments error: %v", err)
	}
	defer func() { _ = client.DeleteAssignment(assignment.Username, assignment.Site) }()

	// GetNodesWithImminentDeadline must surface the deadline node.
	imminent, err := client.GetNodesWithImminentDeadline(10 * time.Minute)
	if err != nil {
		t.Fatalf("GetNodesWithImminentDeadline error: %v", err)
	}
	found := false
	for _, n := range imminent {
		if n.NodeID == deadlineNode {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("deadline node %q not in imminent list (window 10m, deadline +5m)", deadlineNode)
	}

	// ReassignChannel moves the channel off the deadline node atomically.
	if err := client.ReassignChannel(assignment.Username, assignment.Site, deadlineNode, targetNode); err != nil {
		t.Fatalf("ReassignChannel error: %v", err)
	}
	got, err := client.GetAssignment(assignment.Username, assignment.Site)
	if err != nil {
		t.Fatalf("GetAssignment error: %v", err)
	}
	if got == nil || got.AssignedNode != targetNode {
		assigned := "<nil>"
		if got != nil {
			assigned = got.AssignedNode
		}
		t.Fatalf("assignment assigned to %q, want %q", assigned, targetNode)
	}
}

// TestIntegrationReaperEligibility verifies that channels on dead nodes are reclaimable.
func TestIntegrationReaperEligibility(t *testing.T) {
	client := skipIfNoSupabase(t)
	defer cleanupAssignments(t, client, testNodeID)
	defer cleanupNode(t, client)

	// Register node
	_ = client.UpsertNode(&database.Node{
		NodeID:   testNodeID,
		Hostname: "test-host",
		Status:   "online",
		// Set heartbeat to far in the past
		LastHeartbeat: time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339),
	})

	// Assign a channel to this dead node
	assignment := database.ChannelAssignment{
		Username:     testNodeID + "-dead-ch",
		Site:         "chaturbate",
		AssignedNode: testNodeID,
		Status:       "claimed",
		IsLive:       true,
		Framerate:    60,
		Resolution:   1080,
	}
	if err := client.BulkInsertAssignments([]database.ChannelAssignment{assignment}); err != nil {
		t.Fatalf("BulkInsertAssignments error: %v", err)
	}
	defer func() {
		_ = client.DeleteAssignment(assignment.Username, assignment.Site)
	}()

	// Get dead nodes using 180s timeout (standard reaper timeout)
	deadIDs, err := client.GetDeadNodes(180 * time.Second)
	if err != nil {
		t.Fatalf("GetDeadNodes error: %v", err)
	}

	found := false
	for _, id := range deadIDs {
		if id == testNodeID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("node %q should be in dead list (heartbeat > 180s old)", testNodeID)
	}

	// Reclaim channels from dead node
	reclaimed, err := client.ReclaimChannels(testNodeID)
	if err != nil {
		t.Fatalf("ReclaimChannels error: %v", err)
	}
	if reclaimed == 0 {
		t.Error("expected at least 1 channel to be reclaimed")
	}
}
