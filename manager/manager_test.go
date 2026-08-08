package manager

import (
	"sort"
	"strings"
	"testing"
	"time"

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

	// A paused channel object already in the local map, paused automatically
	// by a session boundary (e.g. left over before the re-claim).
	conf := &entity.ChannelConfig{Username: "paused_user", Site: "chaturbate"}
	conf.Sanitize()
	ch := channel.New(conf)
	ch.PauseWithReason(entity.PauseReasonBoundary)
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
	// The resume must be visible in the node web UI: the channel is marked
	// AutoResumedFromPause (drives the badge) and the browser log carries the
	// recovery line.
	if !got.(*channel.Channel).ExportInfo().AutoResumedFromPause {
		t.Fatal("auto-resumed channel should be marked AutoResumedFromPause for the UI badge")
	}
	// The browser log is written by the channel's async Publisher goroutine,
	// so poll briefly for the recovery line instead of asserting synchronously.
	chAfter := got.(*channel.Channel)
	var foundRecovery bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range chAfter.ExportInfo().Logs {
			if strings.Contains(l, "stuck-pause recovery") {
				foundRecovery = true
				break
			}
		}
		if foundRecovery {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !foundRecovery {
		t.Fatalf("expected a browser-visible stuck-pause recovery log line, got: %v", chAfter.ExportInfo().Logs)
	}
}

// TestCreateChannelFromAssignmentLeavesManualPause verifies the claim/reconcile
// path NEVER overrides a user's explicit UI pause: a channel paused with a
// MANUAL reason stays paused even when it is re-claimed from an assignment.
func TestCreateChannelFromAssignmentLeavesManualPause(t *testing.T) {
	oldMgr, oldCfg := server.Manager, server.Config
	defer func() { server.Manager, server.Config = oldMgr, oldCfg }()

	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	server.Manager = m
	server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/", CFChannelThreshold: 5}

	conf := &entity.ChannelConfig{Username: "manual_user", Site: "chaturbate"}
	conf.Sanitize()
	ch := channel.New(conf)
	ch.PauseWithReason(entity.PauseReasonManual)
	m.Channels.Store(conf.Username, ch)

	ca := &database.ChannelAssignment{
		Username:   "manual_user",
		Site:       "chaturbate",
		Status:     "claimed",
		Framerate:  60,
		Resolution: 1080,
	}

	if err := m.CreateChannelFromAssignment(ca); err != nil {
		t.Fatalf("CreateChannelFromAssignment: %v", err)
	}

	got, _ := m.Channels.Load("manual_user")
	if !got.(*channel.Channel).Config.IsPaused.Load() {
		t.Fatal("CreateChannelFromAssignment must NOT resume a manually paused channel")
	}
	if got.(*channel.Channel).PauseReason() != entity.PauseReasonManual {
		t.Fatalf("manual pause reason lost after re-claim: got %q", got.(*channel.Channel).PauseReason())
	}
}

// TestResumeAllChannelsSkipsManualPause verifies the session restart resumes
// automatic (boundary) pauses but leaves channels the user explicitly paused.
func TestResumeAllChannelsSkipsManualPause(t *testing.T) {
	oldMgr, oldCfg := server.Manager, server.Config
	defer func() { server.Manager, server.Config = oldMgr, oldCfg }()

	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	server.Manager = m
	server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/", CFChannelThreshold: 5}

	mk := func(name string, reason entity.PauseReason) {
		conf := &entity.ChannelConfig{Username: name, Site: "chaturbate"}
		conf.Sanitize()
		ch := channel.New(conf)
		ch.PauseWithReason(reason)
		m.Channels.Store(name, ch)
	}
	mk("manual_user", entity.PauseReasonManual)
	mk("boundary_user", entity.PauseReasonBoundary)

	m.ResumeAllChannels()

	manual, _ := m.Channels.Load("manual_user")
	if !manual.(*channel.Channel).Config.IsPaused.Load() {
		t.Fatal("ResumeAllChannels must leave a manually paused channel paused")
	}
	boundary, _ := m.Channels.Load("boundary_user")
	if boundary.(*channel.Channel).Config.IsPaused.Load() {
		t.Fatal("ResumeAllChannels must resume an automatic (boundary) pause")
	}
}
