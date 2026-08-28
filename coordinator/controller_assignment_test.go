package coordinator

import (
	"fmt"
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
