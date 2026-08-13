package coordinator

import (
	"fmt"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// mockHoardDB implements dbHoardRebalance for unit tests.
type mockHoardDB struct {
	alive        []database.Node
	assignments  []database.ChannelAssignment
	releaseCalls []hoardReleaseCall
	releaseN     int
	releaseErr   error
}

type hoardReleaseCall struct {
	nodeID  string
	exclude []string
}

func (m *mockHoardDB) GetAliveNodes() ([]database.Node, error) {
	return m.alive, nil
}

func (m *mockHoardDB) GetAllAssignments() ([]database.ChannelAssignment, error) {
	return m.assignments, nil
}

func (m *mockHoardDB) ReleaseNodeOfflineChannels(nodeID string, exclude []string) (int, error) {
	m.releaseCalls = append(m.releaseCalls, hoardReleaseCall{nodeID: nodeID, exclude: exclude})
	return m.releaseN, m.releaseErr
}

// fleet18 builds the pre-fix fleet shape: 18 alive nodes (node-01 is the
// smallest, hence the designated actor), node-18 hoarding 904/907 channels
// (893 offline + 11 live), the rest near-empty. fairShare = ceil(907/18) = 51,
// threshold = max(102, 101) = 102.
func fleet18() ([]database.Node, []database.ChannelAssignment) {
	var alive []database.Node
	for i := 1; i <= 18; i++ {
		alive = append(alive, database.Node{NodeID: fmt.Sprintf("node-%02d", i)})
	}

	var assignments []database.ChannelAssignment
	for i := 0; i < 893; i++ {
		assignments = append(assignments, database.ChannelAssignment{
			Username: fmt.Sprintf("off_hoard_%04d", i), Site: "chaturbate", Status: "claimed", IsLive: false, AssignedNode: "node-18",
		})
	}
	for i := 0; i < 11; i++ {
		assignments = append(assignments, database.ChannelAssignment{
			Username: fmt.Sprintf("live_hoard_%02d", i), Site: "chaturbate", Status: "recording", IsLive: true, AssignedNode: "node-18",
		})
	}
	assignments = append(assignments,
		database.ChannelAssignment{Username: "live15a", Site: "chaturbate", Status: "recording", IsLive: true, AssignedNode: "node-15"},
		database.ChannelAssignment{Username: "live15b", Site: "chaturbate", Status: "recording", IsLive: true, AssignedNode: "node-15"},
		database.ChannelAssignment{Username: "off17", Site: "chaturbate", Status: "claimed", IsLive: false, AssignedNode: "node-17"},
	)
	return alive, assignments
}

func newHoardCoordinator(nodeID string, mgr ChannelManager) *Coordinator {
	return &Coordinator{NodeID: nodeID, Mode: entity.PoolModePooled, Manager: mgr}
}

func TestHoardRebalanceReleasesHoarder(t *testing.T) {
	alive, assignments := fleet18()
	mock := &mockHoardDB{alive: alive, assignments: assignments, releaseN: 893}
	c := newHoardCoordinator("node-01", &mockChannelManager{})

	c.runHoardRebalanceCycleWith(mock)

	if len(mock.releaseCalls) != 1 {
		t.Fatalf("release calls = %d, want 1 (node-18 hoarding)", len(mock.releaseCalls))
	}
	if mock.releaseCalls[0].nodeID != "node-18" {
		t.Errorf("released node = %q, want node-18", mock.releaseCalls[0].nodeID)
	}
}

func TestHoardRebalanceNonDesignatedSkips(t *testing.T) {
	alive, assignments := fleet18()
	mock := &mockHoardDB{alive: alive, assignments: assignments, releaseN: 893}

	// node-18 is the hoarder but NOT the smallest online node — it must not act.
	c := newHoardCoordinator("node-18", &mockChannelManager{})
	c.runHoardRebalanceCycleWith(mock)

	if len(mock.releaseCalls) != 0 {
		t.Fatalf("non-designated node issued %d release(s), want 0", len(mock.releaseCalls))
	}
}

func TestHoardRebalanceBalancedNoAction(t *testing.T) {
	alive := []database.Node{
		{NodeID: "node-01", CurrentLoad: 3},
		{NodeID: "node-02", CurrentLoad: 3},
		{NodeID: "node-03", CurrentLoad: 3},
	}
	var assignments []database.ChannelAssignment
	for _, n := range alive {
		for i := 0; i < 3; i++ {
			assignments = append(assignments, database.ChannelAssignment{
				Username: fmt.Sprintf("%s_%d", n.NodeID, i), Site: "chaturbate", Status: "claimed", IsLive: false, AssignedNode: n.NodeID,
			})
		}
	}
	mock := &mockHoardDB{alive: alive, assignments: assignments}
	c := newHoardCoordinator("node-01", &mockChannelManager{})

	c.runHoardRebalanceCycleWith(mock)

	if len(mock.releaseCalls) != 0 {
		t.Fatalf("balanced pool triggered %d release(s), want 0", len(mock.releaseCalls))
	}
}

func TestHoardRebalanceExcludesPausedChannels(t *testing.T) {
	alive, assignments := fleet18()
	mock := &mockHoardDB{alive: alive, assignments: assignments, releaseN: 893}
	mgr := &mockChannelManager{
		manualPaused: []ChannelPause{{Username: "paused_user", Site: "chaturbate"}},
	}
	c := newHoardCoordinator("node-01", mgr)

	c.runHoardRebalanceCycleWith(mock)

	if len(mock.releaseCalls) != 1 {
		t.Fatalf("release calls = %d, want 1", len(mock.releaseCalls))
	}
	if len(mock.releaseCalls[0].exclude) != 1 || mock.releaseCalls[0].exclude[0] != "paused_user" {
		t.Errorf("paused channels not passed as exclusions: %v", mock.releaseCalls[0].exclude)
	}
}

// TestHoardRebalanceIgnoresLiveAndRecording verifies that live/recording
// channels are not counted as offline excess, so a node holding only live
// channels is never swept by the net.
func TestHoardRebalanceIgnoresLiveAndRecording(t *testing.T) {
	alive := []database.Node{
		{NodeID: "node-01", CurrentLoad: 0},
		{NodeID: "node-02", CurrentLoad: 0},
	}
	var assignments []database.ChannelAssignment
	// node-02 holds 200 live+recording channels (sticky by design) and only
	// 5 offline — well under the threshold.
	for i := 0; i < 200; i++ {
		assignments = append(assignments, database.ChannelAssignment{
			Username: fmt.Sprintf("live_%03d", i), Site: "chaturbate", Status: "recording", IsLive: true, AssignedNode: "node-02",
		})
	}
	for i := 0; i < 5; i++ {
		assignments = append(assignments, database.ChannelAssignment{
			Username: fmt.Sprintf("off_%03d", i), Site: "chaturbate", Status: "claimed", IsLive: false, AssignedNode: "node-02",
		})
	}
	// fairShare = ceil(205/2) = 103 → threshold = max(206, 153) = 206.
	mock := &mockHoardDB{alive: alive, assignments: assignments}
	c := newHoardCoordinator("node-01", &mockChannelManager{})

	c.runHoardRebalanceCycleWith(mock)

	if len(mock.releaseCalls) != 0 {
		t.Fatalf("live/recording channels counted as excess: %d release(s)", len(mock.releaseCalls))
	}
}

func TestHoardRebalanceInactiveSkips(t *testing.T) {
	alive, assignments := fleet18()
	mock := &mockHoardDB{alive: alive, assignments: assignments, releaseN: 893}
	c := newHoardCoordinator("node-01", &mockChannelManager{})
	c.draining = true // node shutting down must not move channels around

	c.runHoardRebalanceCycleWith(mock)

	if len(mock.releaseCalls) != 0 {
		t.Fatalf("draining node issued %d release(s), want 0", len(mock.releaseCalls))
	}
}
