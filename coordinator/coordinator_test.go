package coordinator

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// ============================================================================
// Mock implementations
// ============================================================================

type mockClient struct {
	*database.Client // embed nil to satisfy interface with only overridden methods

	stats             *database.AssignmentStats
	aliveNodes        []database.Node
	imminentNodes     []database.Node
	assignmentsByNode map[string][]database.ChannelAssignment
	reassignCalls     []reassignCall
	specificClaims    []reassignCall
	releasedExcess    []database.ChannelAssignment
}

type reassignCall struct {
	username, site, fromNode, toNode string
}

func (m *mockClient) GetAssignmentStats() (*database.AssignmentStats, error) {
	return m.stats, nil
}

func (m *mockClient) GetAliveNodes() ([]database.Node, error) {
	return m.aliveNodes, nil
}

func (m *mockClient) GetNodesWithImminentDeadline(window time.Duration) ([]database.Node, error) {
	return m.imminentNodes, nil
}

func (m *mockClient) CountMyAssignments(nodeID string) (int, error) {
	return len(m.assignmentsByNode[nodeID]), nil
}

func (m *mockClient) GetNodeAssignments(nodeID string) ([]database.ChannelAssignment, error) {
	return m.assignmentsByNode[nodeID], nil
}

func (m *mockClient) ReassignChannel(username, site, fromNode, toNode string) error {
	m.reassignCalls = append(m.reassignCalls, reassignCall{username, site, fromNode, toNode})
	return nil
}

func (m *mockClient) RepairOrphanedAssignments() (int, error) {
	return 0, nil
}

func (m *mockClient) ClaimSpecificChannel(username, site, nodeID string) (bool, error) {
	m.specificClaims = append(m.specificClaims, reassignCall{username, site, "", nodeID})
	return true, nil
}

func (m *mockClient) ClaimOfflineChannels(nodeID string, limit int) ([]database.ChannelAssignment, error) {
	return nil, nil
}

func (m *mockClient) ClaimLiveChannels(nodeID string, limit int) ([]database.ChannelAssignment, error) {
	return nil, nil
}

func (m *mockClient) ReleaseExcessOfflineChannels(nodeID string, limit int) ([]database.ChannelAssignment, error) {
	return m.releasedExcess, nil
}

type mockChannelManager struct {
	created      []*database.ChannelAssignment
	removed      []string
	manualPaused []ChannelPause
	cfBlocked    int
}

func (m *mockChannelManager) CreateChannelFromAssignment(ca *database.ChannelAssignment) error {
	m.created = append(m.created, ca)
	return nil
}

func (m *mockChannelManager) CFBlockedCount() int {
	return m.cfBlocked
}

func (m *mockChannelManager) RequestCookieRefresh() {} // no-op in tests

func (m *mockChannelManager) RemoveChannelForReassignment(username string) error {
	// Mirror the guard in manager.Manager: a channel the user explicitly
	// paused is never discarded here — it stays parked+paused locally.
	for _, mc := range m.manualPaused {
		if mc.Username == username {
			return nil
		}
	}
	m.removed = append(m.removed, username)
	return nil
}

func (m *mockChannelManager) GetLocalChannels() []string {
	var list []string
	for _, ca := range m.created {
		list = append(list, ca.Username)
	}
	return list
}

func (m *mockChannelManager) HasPendingSegments(username string) bool {
	return false
}

func (m *mockChannelManager) ManualPausedChannels() []ChannelPause {
	return m.manualPaused
}

// ============================================================================
// Tests
// ============================================================================

func TestDetectNodeID(t *testing.T) {
	tests := []struct {
		name     string
		envs     map[string]string
		expected string
	}{
		{
			name:     "from NODE_ID env",
			envs:     map[string]string{"NODE_ID": "my-custom-node"},
			expected: "my-custom-node",
		},
		{
			name:     "from GITHUB_REPOSITORY with dashed suffix",
			envs:     map[string]string{"GITHUB_REPOSITORY": "owner/MiniDelectableService-node-a"},
			expected: "a",
		},
		{
			name:     "simple repo name (no dashes) — slash replaced with dash",
			envs:     map[string]string{"GITHUB_REPOSITORY": "you/myrepo"},
			expected: "you-myrepo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear env first
			os.Unsetenv("NODE_ID")
			os.Unsetenv("GITHUB_REPOSITORY")

			for k, v := range tt.envs {
				os.Setenv(k, v)
			}

			got := detectNodeID()
			if got != tt.expected {
				t.Errorf("detectNodeID() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestPoolMode(t *testing.T) {
	tests := []struct {
		name     string
		envVal   string
		expected string
	}{
		{"empty defaults to isolated", "", entity.PoolModeIsolated},
		{"explicit isolated", "isolated", entity.PoolModeIsolated},
		{"pooled", "pooled", entity.PoolModePooled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("CHANNEL_POOL_MODE", tt.envVal)
			got := channelPoolMode()
			if got != tt.expected {
				t.Errorf("channelPoolMode() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestIsPooled(t *testing.T) {
	c := &Coordinator{Mode: entity.PoolModePooled}
	if !c.IsPooled() {
		t.Error("expected IsPooled() = true for pooled mode")
	}

	c2 := &Coordinator{Mode: entity.PoolModeIsolated}
	if c2.IsPooled() {
		t.Error("expected IsPooled() = false for isolated mode")
	}
}

func TestConfigFromAssignment(t *testing.T) {
	ca := &database.ChannelAssignment{
		Username:    "testuser",
		Site:        "chaturbate",
		Framerate:   60,
		Resolution:  2160,
		Pattern:     "videos/{{.Username}}_...",
		MaxDuration: 60,
		Compress:    true,
	}

	conf := ConfigFromAssignment(ca)
	if conf.Username != "testuser" {
		t.Errorf("Username = %q, want %q", conf.Username, "testuser")
	}
	if conf.Site != "chaturbate" {
		t.Errorf("Site = %q, want %q", conf.Site, "chaturbate")
	}
	if conf.Framerate != 60 {
		t.Errorf("Framerate = %d, want %d", conf.Framerate, 60)
	}
	if conf.Resolution != 2160 {
		t.Errorf("Resolution = %d, want %d", conf.Resolution, 2160)
	}
	if !conf.Compress {
		t.Error("expected Compress = true")
	}
}

// TestConfigFromAssignment_EmptyPattern ensures a channel_assignments row
// with an empty pattern (the column's DB default) still yields a channel
// config that can generate filenames — the missing Sanitize() here caused
// fleet-wide "filename pattern \"\" rendered an empty name" errors that
// blocked recording entirely for newly-claimed channels.
func TestConfigFromAssignment_EmptyPattern(t *testing.T) {
	ca := &database.ChannelAssignment{
		Username: "empty_pattern_user",
		Site:     "chaturbate",
		Pattern:  "",
	}

	conf := ConfigFromAssignment(ca)
	if conf.Pattern == "" {
		t.Fatal("expected Sanitize() to fill the default pattern, got empty")
	}
	if conf.Resolution == 0 {
		t.Error("expected Sanitize() to fill default resolution")
	}
	if conf.Framerate == 0 {
		t.Error("expected Sanitize() to fill default framerate")
	}
}

func TestMarshalUnmarshalPool(t *testing.T) {
	pool := []*entity.ChannelConfig{
		{Username: "alice", Site: "chaturbate", Framerate: 60, Resolution: 2160},
		{Username: "bob", Site: "stripchat", Framerate: 30, Resolution: 1080},
	}

	data, err := MarshalPool(pool)
	if err != nil {
		t.Fatalf("MarshalPool error: %v", err)
	}

	got, err := UnmarshalPool(data)
	if err != nil {
		t.Fatalf("UnmarshalPool error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d items, want 2", len(got))
	}

	if got[0].Username != "alice" {
		t.Errorf("got[0].Username = %q, want %q", got[0].Username, "alice")
	}
	if got[1].Username != "bob" {
		t.Errorf("got[1].Username = %q, want %q", got[1].Username, "bob")
	}
}

func TestCurrentLoad(t *testing.T) {
	// Coordinator with no client returns 0
	c := &Coordinator{NodeID: "test-node"}
	load := c.currentLoad()
	if load != 0 {
		t.Errorf("currentLoad() = %d, want 0 (no client)", load)
	}
}

func TestFairShareCalculation(t *testing.T) {
	tests := []struct {
		name         string
		totalLive    int
		totalNodes   int
		expectedFair int
	}{
		{"20 channels, 5 nodes", 20, 5, 4},
		{"7 channels, 3 nodes", 7, 3, 3},
		{"1 channel, 3 nodes", 1, 3, 1},
		{"0 channels, 3 nodes", 0, 3, 0},
		{"3 channels, 0 nodes", 3, 0, 0},
		{"5 channels, 1 node", 5, 1, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fairShare := 0
			if tt.totalNodes > 0 {
				fairShare = (tt.totalLive + tt.totalNodes - 1) / tt.totalNodes
			}
			if fairShare != tt.expectedFair {
				t.Errorf("fairShare = %d, want %d", fairShare, tt.expectedFair)
			}
		})
	}
}

// TestLiveClaimBudget verifies the live fair-share budget: how many live
// channels a node may still claim = ceil(totalLive/aliveNodes) minus its own
// live count, clamped at 0. This is the fix for live channels being swept
// wholesale by one node — each node claims live channels only up to its share.
func TestLiveClaimBudget(t *testing.T) {
	tests := []struct {
		name          string
		myLiveCount   int
		totalLive     int
		totalNodes    int
		expectedBudget int
	}{
		{"30 live, 3 nodes, node holds 0 → 10", 0, 30, 3, 10},
		{"30 live, 3 nodes, node holds 10 → 0 (at share)", 10, 30, 3, 0},
		{"30 live, 3 nodes, node holds 15 → 0 (over share, sticky)", 15, 30, 3, 0},
		{"1 live, 3 nodes, node holds 0 → 1 (ceil rounds up)", 0, 1, 3, 1},
		{"7 live, 3 nodes, node holds 2 → 1", 2, 7, 3, 1},
		{"7 live, 3 nodes, node holds 3 → 0", 3, 7, 3, 0},
		{"0 live, 3 nodes → 0 (nothing to claim)", 0, 0, 3, 0},
		{"3 live, 1 node → 3 (single node takes all)", 0, 3, 1, 3},
		{"3 live, 0 nodes → 3 (defensive: treat as 1)", 0, 3, 0, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := liveClaimBudget(tt.myLiveCount, tt.totalLive, tt.totalNodes)
			if got != tt.expectedBudget {
				t.Errorf("liveClaimBudget(%d, %d, %d) = %d, want %d",
					tt.myLiveCount, tt.totalLive, tt.totalNodes, got, tt.expectedBudget)
			}
		})
	}
}

// TestStartStop verifies that Start and Stop don't panic when in pooled mode.
func TestStartStop(t *testing.T) {
	os.Setenv("CHANNEL_POOL_MODE", "pooled")
	defer os.Unsetenv("CHANNEL_POOL_MODE")

	mgr := &mockChannelManager{}
	c := New(nil, mgr)
	c.LiveCheck = nil // no live check for test

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should not panic
	c.Start(ctx)

	// Stop should not panic
	c.Stop()
}

// TestStartStopIsolated verifies that Start and Stop are no-ops in isolated mode.
func TestStartStopIsolated(t *testing.T) {
	os.Setenv("CHANNEL_POOL_MODE", "isolated")
	defer os.Unsetenv("CHANNEL_POOL_MODE")

	c := New(nil, nil)
	ctx := context.Background()

	c.Start(ctx)
	// Start sets started=true even in isolated mode (prevents double-start)
	// but all goroutines are skipped.
	c.Stop()

	if c.started != true {
		t.Error("expected started to be true after Start()")
	}
}

// TestStopIdempotent verifies Stop() is safe to call twice — the double-close
// guard prevents a panic on the second call.
func TestStopIdempotent(t *testing.T) {
	os.Setenv("CHANNEL_POOL_MODE", "pooled")
	defer os.Unsetenv("CHANNEL_POOL_MODE")

	c := New(nil, &mockChannelManager{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Start(ctx)
	c.Stop()
	c.Stop() // must not panic
}

// TestCycleGuardPreventsOverlap verifies that a second tryRun is skipped while
// the first is still executing — the guard that prevents overlapping cycles
// when the DB is slow (a slow cycle must not stack a concurrent one).
func TestCycleGuardPreventsOverlap(t *testing.T) {
	g := &cycleGuard{}

	entered := make(chan struct{})
	release := make(chan struct{})
	executed := 0

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		g.tryRun("test", func() {
			executed++
			close(entered)
			<-release
		})
	}()

	<-entered // first run is inside fn

	// Second run while the first is still blocked must be skipped.
	if g.tryRun("test", func() { executed++ }) {
		t.Fatal("second tryRun executed while the first was still running")
	}

	close(release)
	<-firstDone

	if executed != 1 {
		t.Fatalf("fn executed %d times, want 1", executed)
	}
}

// TestCycleGuardPanicRecovery verifies a panicking cycle is caught (no crash
// propagates), the guard is released, and subsequent cycles still run — a
// single bad cycle must not crash the node or permanently wedge the guard.
//
// Note: tryRun returns false for a panicking fn (the panic unwinds before the
// `return true`), so the return value is not asserted for that run — only that
// the panic was contained and the guard recovered.
func TestCycleGuardPanicRecovery(t *testing.T) {
	g := &cycleGuard{}

	g.tryRun("test", func() { panic("boom") }) // must not crash the test

	ran := false
	if !g.tryRun("test", func() { ran = true }) {
		t.Fatal("second tryRun should execute after panic recovery released the guard")
	}
	if !ran {
		t.Fatal("second tryRun did not run its function")
	}
}

// TestCycleGuardLastRunTime verifies lastRunTime is zero before the first run
// and set after a run — the timestamp the health check uses to detect stalls.
func TestCycleGuardLastRunTime(t *testing.T) {
	g := &cycleGuard{}
	if !g.lastRunTime().IsZero() {
		t.Fatal("lastRunTime should be zero before the first run")
	}

	g.tryRun("test", func() {})
	if g.lastRunTime().IsZero() {
		t.Fatal("lastRunTime should be set after a run")
	}
}

// TestComputeSessionDeadline verifies the node deadline derivation used by the
// deadline-migration loop: explicit SESSION_DURATION wins, CI runners get a
// 335m buffer, and permanent nodes get no deadline at all.
func TestComputeSessionDeadline(t *testing.T) {
	os.Unsetenv("SESSION_DURATION")
	os.Unsetenv("GITHUB_RUN_ID")

	t.Run("explicit SESSION_DURATION", func(t *testing.T) {
		os.Setenv("SESSION_DURATION", "2h")
		defer os.Unsetenv("SESSION_DURATION")

		dl := computeSessionDeadline()
		if dl == nil {
			t.Fatal("expected a deadline for SESSION_DURATION")
		}
		if got := time.Until(*dl); got < 119*time.Minute || got > 121*time.Minute {
			t.Fatalf("deadline %.0fm away, want ~120m", got.Minutes())
		}
	})

	t.Run("CI runner gets 335m buffer", func(t *testing.T) {
		os.Setenv("GITHUB_RUN_ID", "12345")
		defer os.Unsetenv("GITHUB_RUN_ID")

		dl := computeSessionDeadline()
		if dl == nil {
			t.Fatal("expected a deadline for GITHUB_RUN_ID")
		}
		if got := time.Until(*dl); got < 334*time.Minute || got > 336*time.Minute {
			t.Fatalf("deadline %.0fm away, want ~335m", got.Minutes())
		}
	})

	t.Run("permanent node has no deadline", func(t *testing.T) {
		if dl := computeSessionDeadline(); dl != nil {
			t.Fatalf("expected nil deadline, got %v", dl)
		}
	})
}

// TestLeastLoaded verifies the shared spread helper picks the candidate with
// the smallest load (ties resolved by first-in-slice) and reflects load changes.
func TestLeastLoaded(t *testing.T) {
	candidates := []database.Node{
		{NodeID: "a", CurrentLoad: 3},
		{NodeID: "b", CurrentLoad: 1},
		{NodeID: "c", CurrentLoad: 5},
	}
	load := map[string]int{"a": 3, "b": 1, "c": 5}
	if got := leastLoaded(candidates, load); got.NodeID != "b" {
		t.Fatalf("least loaded = %q, want b", got.NodeID)
	}

	// Load map reflects moves: after bumping b, a becomes least loaded.
	load["b"] = 6
	if got := leastLoaded(candidates, load); got.NodeID != "a" {
		t.Fatalf("least loaded after bump = %q, want a", got.NodeID)
	}

	// Spread: once z's load exceeds y's, y becomes least loaded.
	load = map[string]int{"y": 2, "z": 0}
	candidates = []database.Node{
		{NodeID: "y", CurrentLoad: 2},
		{NodeID: "z", CurrentLoad: 0},
	}
	if got := leastLoaded(candidates, load); got.NodeID != "z" {
		t.Fatalf("least loaded = %q, want z", got.NodeID)
	}
	load["z"] = 3
	if got := leastLoaded(candidates, load); got.NodeID != "y" {
		t.Fatalf("least loaded after spread = %q, want y", got.NodeID)
	}
}

// TestRunOfflineShuffleCycle verifies a node over fair-share moves exactly its
// excess OFFLINE channel to the least-loaded alive node, and never touches
// live or recording channels.
func TestRunOfflineShuffleCycle(t *testing.T) {
	mock := &mockClient{
		stats: &database.AssignmentStats{TotalPoolChannels: 4},
		aliveNodes: []database.Node{
			{NodeID: "node-a", CurrentLoad: 3},
			{NodeID: "node-b", CurrentLoad: 0},
			{NodeID: "node-c", CurrentLoad: 1},
		},
		assignmentsByNode: map[string][]database.ChannelAssignment{
			"node-a": {
				{Username: "off1", Site: "chaturbate", Status: "claimed", IsLive: false},
				{Username: "off2", Site: "chaturbate", Status: "claimed", IsLive: false},
				{Username: "live1", Site: "stripchat", Status: "recording", IsLive: true},
			},
		},
	}
	mgr := &mockChannelManager{}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: mgr}

	c.runOfflineShuffleCycleWith(mock)

	// myLoad=3 (all three count), fairShare=ceil(4/3)=2 → moveCount=1. Only the
	// offline, non-local channel (off1) is movable; live1 is recording and skipped.
	if len(mock.reassignCalls) != 1 {
		t.Fatalf("expected 1 reassign, got %d: %+v", len(mock.reassignCalls), mock.reassignCalls)
	}
	call := mock.reassignCalls[0]
	if call.username != "off1" || call.fromNode != "node-a" || call.toNode != "node-b" {
		t.Fatalf("unexpected reassign call: %+v", call)
	}
	if len(mgr.removed) != 1 || mgr.removed[0] != "off1" {
		t.Fatalf("expected local channel off1 removed, got %v", mgr.removed)
	}
}

// mockChannelManagerWithPending is a mockChannelManager that reports
// pending segments for specific usernames, so the shuffle can be
// verified to skip them.
type mockChannelManagerWithPending struct {
	mockChannelManager
	pendingSet map[string]bool
}

func (m *mockChannelManagerWithPending) HasPendingSegments(username string) bool {
	return m.pendingSet[username]
}

// TestRunOfflineShuffleCycleSkipsPending verifies that the shuffle
// never moves a channel that has pending recording segments on the
// local node, even when the channel is otherwise offline.
func TestRunOfflineShuffleCycleSkipsPending(t *testing.T) {
	mock := &mockClient{
		stats: &database.AssignmentStats{TotalPoolChannels: 2},
		aliveNodes: []database.Node{
			{NodeID: "node-a", CurrentLoad: 2},
			{NodeID: "node-b", CurrentLoad: 0},
		},
		assignmentsByNode: map[string][]database.ChannelAssignment{
			"node-a": {
				{Username: "offline_with_pending", Site: "chaturbate", Status: "claimed", IsLive: false},
				{Username: "offline_clean", Site: "chaturbate", Status: "claimed", IsLive: false},
			},
		},
	}
	mgr := &mockChannelManagerWithPending{
		pendingSet: map[string]bool{"offline_with_pending": true},
	}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: mgr}

	c.runOfflineShuffleCycleWith(mock)

	// fairShare=ceil(2/2)=1, myLoad=2 → moveCount=1. Only
	// offline_clean should be moved; offline_with_pending is
	// protected by the pending-segment guard.
	if len(mock.reassignCalls) != 1 {
		t.Fatalf("expected 1 reassign, got %d: %+v", len(mock.reassignCalls), mock.reassignCalls)
	}
	if mock.reassignCalls[0].username != "offline_clean" {
		t.Fatalf("expected offline_clean to be shuffled, got %q", mock.reassignCalls[0].username)
	}
	if mock.reassignCalls[0].toNode != "node-b" {
		t.Fatalf("expected move to node-b, got %q", mock.reassignCalls[0].toNode)
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}

// TestRunDeadlineMigrationCycle verifies ALL channels of an imminent-deadline
// node (including live+recording) are reassigned to the least-loaded healthy
// node, spreading across candidates.
func TestRunDeadlineMigrationCycle(t *testing.T) {
	mock := &mockClient{
		imminentNodes: []database.Node{{NodeID: "node-x", SessionDeadline: timePtr(time.Now().Add(5 * time.Minute))}},
		aliveNodes: []database.Node{
			{NodeID: "node-x", CurrentLoad: 5},
			{NodeID: "node-y", CurrentLoad: 2},
			{NodeID: "node-z", CurrentLoad: 0},
		},
		assignmentsByNode: map[string][]database.ChannelAssignment{
			"node-x": {
				{Username: "live1", Site: "chaturbate", Status: "recording", IsLive: true},
				{Username: "off1", Site: "stripchat", Status: "claimed", IsLive: false},
			},
		},
	}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: &mockChannelManager{}}

	c.runDeadlineMigrationCycleWith(mock)

	if len(mock.reassignCalls) != 2 {
		t.Fatalf("expected 2 reassigns, got %d: %+v", len(mock.reassignCalls), mock.reassignCalls)
	}
	for _, call := range mock.reassignCalls {
		if call.fromNode != "node-x" {
			t.Fatalf("reassign from %q, want node-x: %+v", call.fromNode, call)
		}
		if call.toNode == "node-x" || call.toNode == "node-a" {
			t.Fatalf("reassign to invalid target %q: %+v", call.toNode, call)
		}
	}
	// Both go to node-z (initially least loaded); node-y must never receive one.
	for _, call := range mock.reassignCalls {
		if call.toNode != "node-z" {
			t.Fatalf("expected all moves to node-z, got %+v", call)
		}
	}
}

// TestRunDeadlineMigrationCycleSkipsPastDeadline guards against the root cause
// of the fleet-wide churn: a node whose session_deadline has already passed but
// which is still alive and heartbeating must NOT be drained — the old behavior
// migrated its channels every 60s while its claim loop re-claimed them,
// pinning channels to no node and overloading the migration targets.
func TestRunDeadlineMigrationCycleSkipsPastDeadline(t *testing.T) {
	mock := &mockClient{
		imminentNodes: []database.Node{
			{NodeID: "node-x", SessionDeadline: timePtr(time.Now().Add(-2 * time.Hour))},
			{NodeID: "node-y", SessionDeadline: timePtr(time.Now().Add(-15 * time.Minute))},
			{NodeID: "node-nil-deadline"},
		},
		aliveNodes: []database.Node{
			{NodeID: "node-x", CurrentLoad: 10},
			{NodeID: "node-y", CurrentLoad: 10},
			{NodeID: "node-nil-deadline", CurrentLoad: 10},
			{NodeID: "node-target", CurrentLoad: 0},
		},
		assignmentsByNode: map[string][]database.ChannelAssignment{
			"node-x":              {{Username: "ch1", Site: "chaturbate", Status: "claimed", IsLive: false}},
			"node-y":              {{Username: "ch2", Site: "chaturbate", Status: "claimed", IsLive: false}},
			"node-nil-deadline":   {{Username: "ch3", Site: "chaturbate", Status: "claimed", IsLive: false}},
		},
	}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: &mockChannelManager{}}

	c.runDeadlineMigrationCycleWith(mock)

	if len(mock.reassignCalls) != 0 {
		t.Fatalf("expected 0 reassigns for past/nil deadlines, got %d: %+v", len(mock.reassignCalls), mock.reassignCalls)
	}
}

// TestRebalanceReclaimsManualPauses verifies the session-boundary rebalance
// re-claims channels the user explicitly paused after every assignment is
// released, so they keep a DB claim on this node and stay parked+paused
// locally instead of being grabbed by another node and recording again.
func TestRebalanceReclaimsManualPauses(t *testing.T) {
	mock := &mockClient{}
	mgr := &mockChannelManager{
		manualPaused: []ChannelPause{
			{Username: "manual_user", Site: "chaturbate"},
			{Username: "manual_two", Site: "stripchat"},
		},
	}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: mgr}

	if got := c.reclaimManualPausedChannelsWith(mock, mgr.manualPaused); got != 2 {
		t.Fatalf("reclaimed = %d, want 2", got)
	}
	if len(mock.specificClaims) != 2 {
		t.Fatalf("specific claims = %+v, want 2", mock.specificClaims)
	}
	for _, call := range mock.specificClaims {
		if call.toNode != "node-a" {
			t.Fatalf("re-claim target = %q, want node-a (%+v)", call.toNode, call)
		}
	}
}

// TestRunClaimCycleReclaimsManualPausedAfterExcessRelease verifies that when
// the claim cycle's excess-offline sweep (a raw DB release that has no notion
// of local state) catches a user-paused channel, the channel is immediately
// re-claimed for this node and its local object is NOT removed — the automatic
// load rebalance never hands a manual pause back to the pool, where another
// node would claim it and record over the user's pause.
func TestRunClaimCycleReclaimsManualPausedAfterExcessRelease(t *testing.T) {
	mock := &mockClient{
		stats: &database.AssignmentStats{
			TotalPoolChannels: 4,
			TotalAliveNodes:   2,
			TotalLiveChannels: 0,
		},
		assignmentsByNode: map[string][]database.ChannelAssignment{
			"node-a": {
				{Username: "manual_user", Site: "chaturbate", Status: "claimed", IsLive: false},
				{Username: "off1", Site: "chaturbate", Status: "claimed", IsLive: false},
				{Username: "off2", Site: "stripchat", Status: "claimed", IsLive: false},
			},
		},
		releasedExcess: []database.ChannelAssignment{
			{Username: "manual_user", Site: "chaturbate", Status: "claimed", IsLive: false},
		},
	}
	mgr := &mockChannelManager{
		manualPaused: []ChannelPause{
			{Username: "manual_user", Site: "chaturbate"},
			{Username: "manual_two", Site: "stripchat"}, // NOT swept — must not be re-claimed
		},
	}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: mgr}

	c.runClaimCycleWith(mock)

	// fairShare=ceil(4/2)=2, myOfflineCount=3 > 2 → excess=1 → the mock returns
	// manual_user as released. Only the SWEPT manual pause is re-claimed (a
	// still-assigned manual channel must not trigger a bogus re-claim).
	if len(mock.specificClaims) != 1 {
		t.Fatalf("expected exactly 1 re-claim (only the swept channel), got %d: %+v", len(mock.specificClaims), mock.specificClaims)
	}
	call := mock.specificClaims[0]
	if call.username != "manual_user" || call.toNode != "node-a" {
		t.Fatalf("unexpected re-claim call: %+v", call)
	}
	// And no local parked channel may be removed (the guard in
	// RemoveChannelForReassignment keeps manual-paused channels).
	if len(mgr.removed) != 0 {
		t.Fatalf("manual-paused channel should not be removed, got %v", mgr.removed)
	}
}

// TestRunClaimCycleStarvedSheds verifies the Cloudflare-starved shed: when
// enough channels are simultaneously blocked (IP flagged, nothing can record),
// the claim cycle releases all but a small probe set back to the pool and does
// NOT claim anything, so healthy nodes can record the channels the starved
// node was hoarding while retrying every minute.
func TestRunClaimCycleStarvedSheds(t *testing.T) {
	a := func(u string) database.ChannelAssignment {
		return database.ChannelAssignment{Username: u, Site: "chaturbate", Status: "claimed", IsLive: false}
	}
	released := []database.ChannelAssignment{a("off1"), a("off2"), a("off3")}
	mock := &mockClient{
		stats: &database.AssignmentStats{
			TotalPoolChannels: 12,
			TotalAliveNodes:   2,
			TotalLiveChannels: 0,
		},
		assignmentsByNode: map[string][]database.ChannelAssignment{
			"node-a": {a("off1"), a("off2"), a("off3"), a("off4"), a("off5"), a("off6")},
		},
		releasedExcess: released,
	}
	mgr := &mockChannelManager{cfBlocked: 6}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: mgr}

	c.runClaimCycleWith(mock)

	// 6 offline, probe set 3 → 3 shed. Without the starved branch this cycle
	// would be a no-op (myOfflineCount == maxOfflineAllowed, no claims) — the
	// removals prove the starved shed ran.
	if len(mgr.removed) != 3 {
		t.Fatalf("expected 3 shed channels removed locally, got %v", mgr.removed)
	}
	for _, u := range []string{"off1", "off2", "off3"} {
		found := false
		for _, r := range mgr.removed {
			if r == u {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected %s among removed, got %v", u, mgr.removed)
		}
	}
}

// TestRunDeadlineMigrationSkipsManualPaused verifies that a channel the user
// explicitly paused on this node is exempt from this node's own deadline drain:
// migrating it would hand it to another node, which would recreate it as a
// fresh recording channel and record over the user's pause.
func TestRunDeadlineMigrationSkipsManualPaused(t *testing.T) {
	mock := &mockClient{
		imminentNodes: []database.Node{
			{NodeID: "node-a", SessionDeadline: timePtr(time.Now().Add(5 * time.Minute))},
		},
		aliveNodes: []database.Node{
			{NodeID: "node-a", CurrentLoad: 5},
			{NodeID: "node-b", CurrentLoad: 0},
		},
		assignmentsByNode: map[string][]database.ChannelAssignment{
			"node-a": {
				{Username: "manual_user", Site: "chaturbate", Status: "claimed", IsLive: false},
				{Username: "off1", Site: "chaturbate", Status: "claimed", IsLive: false},
			},
		},
	}
	mgr := &mockChannelManager{
		manualPaused: []ChannelPause{
			{Username: "manual_user", Site: "chaturbate"},
		},
	}
	c := &Coordinator{NodeID: "node-a", Mode: entity.PoolModePooled, Manager: mgr}

	c.runDeadlineMigrationCycleWith(mock)

	// off1 migrates to node-b; the manual-paused channel stays on node-a.
	if len(mock.reassignCalls) != 1 {
		t.Fatalf("expected 1 reassign, got %d: %+v", len(mock.reassignCalls), mock.reassignCalls)
	}
	call := mock.reassignCalls[0]
	if call.username != "off1" || call.toNode != "node-b" {
		t.Fatalf("unexpected reassign call: %+v", call)
	}
}

// TestOwnDeadlineImminent verifies the claim-cycle self-drain guard: imminent
// only inside the pre-deadline migration window; zero, far-future, and already-
// passed deadlines never pause claiming (a node that outlives its deadline
// resumes claiming since migration skips past deadlines).
func TestOwnDeadlineImminent(t *testing.T) {
	c := &Coordinator{}
	if c.ownDeadlineImminent() {
		t.Fatal("zero deadline must not be imminent")
	}

	c.ownDeadline = time.Now().Add(2 * time.Hour)
	if c.ownDeadlineImminent() {
		t.Fatal("far-future deadline must not be imminent")
	}

	c.ownDeadline = time.Now().Add(10 * time.Minute)
	if !c.ownDeadlineImminent() {
		t.Fatal("deadline within migration window must be imminent")
	}

	c.ownDeadline = time.Now().Add(-1 * time.Hour)
	if c.ownDeadlineImminent() {
		t.Fatal("already-passed deadline must not be imminent (node resumes claiming)")
	}
}

// TestCheckCycleHealthWarnsOnStall verifies the health check logs a warning
// when a cycle has not run within maxCycleStall, and stays silent for guards
// that have never run (startup noise suppression).
func TestCheckCycleHealthWarnsOnStall(t *testing.T) {
	var buf bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(oldWriter)

	c := &Coordinator{}

	stalled := &cycleGuard{}
	stalled.lastRun = time.Now().Add(-maxCycleStall - time.Minute)
	c.checkCycleHealth("claim", stalled)
	if !strings.Contains(buf.String(), "HEALTH WARNING: claim cycle last ran") {
		t.Fatalf("expected HEALTH WARNING for stalled cycle, got: %q", buf.String())
	}

	buf.Reset()
	c.checkCycleHealth("claim", &cycleGuard{}) // never ran
	if buf.String() != "" {
		t.Fatalf("unexpected warning for never-run guard: %q", buf.String())
	}
}
