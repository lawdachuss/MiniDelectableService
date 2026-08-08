package manager

import (
	"sort"
	"testing"

	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func TestChannelSortPriorityOrdersRecordingPausedOffline(t *testing.T) {
	channels := []*entity.ChannelInfo{
		{Username: "offline"},
		{Username: "paused_offline", IsPaused: true},
		{Username: "recording", IsOnline: true},
		{Username: "reconnecting", IsConnecting: true},
		{Username: "paused_live", IsPaused: true, IsOnline: true},
	}

	sort.Slice(channels, func(i, j int) bool {
		pi, pj := channelSortPriority(channels[i]), channelSortPriority(channels[j])
		if pi != pj {
			return pi < pj
		}
		return channels[i].Username < channels[j].Username
	})

	got := make([]string, len(channels))
	for i, ch := range channels {
		got[i] = ch.Username
	}
	want := []string{"recording", "paused_live", "paused_offline", "reconnecting", "offline"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sort order = %v, want %v", got, want)
		}
	}
}

// TestCreateChannelFromAssignmentResumesPausedChannel guards against the
// "some channels automatically got paused" failure mode: a channel whose
// local object is PAUSED (left over from a session boundary, a UI pause, or an
// interrupted Stop during a handoff) but which is still assigned to this node
// must be reactivated when the coordinator re-claims it — silently returning
// would leave it paused forever with nothing ever resuming it.
func TestCreateChannelFromAssignmentResumesPausedChannel(t *testing.T) {
	// The channel's background goroutines (Publisher) call server.Manager, and
	// the resumed monitor loop reads server.Config — point both at safe test
	// values so no nil deref can crash the test binary.
	oldMgr, oldCfg := server.Manager, server.Config
	defer func() { server.Manager, server.Config = oldMgr, oldCfg }()

	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	server.Manager = m
	server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/", CFChannelThreshold: 5}

	// A paused channel object already in the local map (e.g. left over from a
	// session boundary that paused everything before the re-claim).
	conf := &entity.ChannelConfig{Username: "paused_user", Site: "chaturbate"}
	conf.Sanitize()
	ch := channel.New(conf)
	ch.Config.IsPaused.Store(true)
	m.Channels.Store(conf.Username, ch)

	ca := &database.ChannelAssignment{
		Username:   "paused_user",
		Site:       "chaturbate",
		Status:     "claimed",
		Framerate:  60,
		Resolution: 1080,
	}

	if err := m.CreateChannelFromAssignment(ca); err != nil {
		t.Fatalf("CreateChannelFromAssignment: %v", err)
	}

	got, ok := m.Channels.Load("paused_user")
	if !ok {
		t.Fatal("channel missing from map after re-claim")
	}
	if got.(*channel.Channel).Config.IsPaused.Load() {
		t.Fatal("CreateChannelFromAssignment should resume a paused-but-still-assigned channel")
	}
}
