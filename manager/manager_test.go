package manager

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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
	var m *Manager
	defer func() {
		// Best-effort cancel of the resumed monitor. Do NOT wait for it: an
		// in-flight HTTP attempt through httpcloak can ignore the cancel for
		// many seconds, so waiting would hang the suite. Leaked goroutines
		// may still read the globals, so restore to safe non-nil values
		// instead of nil (see restoreTestGlobalsSafe).
		if m != nil {
			m.StopAllChannels()
		}
		restoreTestGlobalsSafe(oldMgr, oldCfg, m)
	}()

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

// TestCreateChannelFromAssignmentSkipsDuplicateRecording verifies the
// duplicate-recording guard: a DB assignment already status='recording' that
// this node is NOT actively recording must NOT be started, otherwise a channel
// reassigned here mid-recording by an external autopilot would spawn a second,
// overlapping recording on top of the one owned by the source node.
func TestCreateChannelFromAssignmentSkipsDuplicateRecording(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.StopAllChannels()

	ca := &database.ChannelAssignment{
		Username:   "dup_user",
		Site:       "chaturbate",
		Status:     "recording",
		Framerate:  60,
		Resolution: 1080,
	}
	if err := m.CreateChannelFromAssignment(ca); err != nil {
		t.Fatalf("CreateChannelFromAssignment returned error: %v", err)
	}
	for _, u := range m.GetLocalChannels() {
		if u == "dup_user" {
			t.Fatalf("duplicate recording channel must not be started, but it was added to local channels")
		}
	}
}

// restoreTestGlobalsSafe restores server.Manager/server.Config to their
// pre-test values, falling back to safe non-nil substitutes when the originals
// were nil. Channel Monitor/Publisher goroutines spawned by a test can outlive
// the test body and keep reading these globals; restoring to nil lets a
// late-scheduled goroutine nil-deref and crash the whole test binary.
func restoreTestGlobalsSafe(oldMgr server.IManager, oldCfg *entity.Config, m *Manager) {
	server.Manager = oldMgr
	if server.Manager == nil {
		server.Manager = m
	}
	server.Config = oldCfg
	if server.Config == nil {
		server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/"}
	}
}

// TestReportSessionCut verifies the early session-cut detector: a handful of
// distinct channels reporting the CDN-403/404 + failing-probe signature within
// the window triggers a cookie re-mint (so the rest of the node's channels
// never 404), while isolated or expired reports do not.
func TestReportSessionCut(t *testing.T) {
	oldMgr, oldCfg := server.Manager, server.Config
	var m *Manager
	defer func() { restoreTestGlobalsSafe(oldMgr, oldCfg, m) }()
	// ReportCFBlock reads server.Config.CFGlobalThreshold unconditionally,
	// so the test must install a non-nil Config (other tests leave a
	// fallback behind, but this test must stand alone).
	server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/", CFChannelThreshold: 5}

	newManager := func() (*Manager, *int32) {
		m, err := New()
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		var calls int32
		m.SetCookieRefreshFunc(func() { atomic.AddInt32(&calls, 1) })
		return m, &calls
	}

	waitForRefresh := func(calls *int32, want int32) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if atomic.LoadInt32(calls) >= want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		if got := atomic.LoadInt32(calls); got != want {
			t.Fatalf("cookie refresh calls = %d, want %d", got, want)
		}
	}

	t.Run("isolated reports below threshold do not refresh", func(t *testing.T) {
		m, calls := newManager()
		m.ReportSessionCut("a")
		m.ReportSessionCut("b")
		time.Sleep(100 * time.Millisecond)
		if got := atomic.LoadInt32(calls); got != 0 {
			t.Fatalf("cookie refresh calls = %d, want 0 (below threshold)", got)
		}
	})

	t.Run("threshold of distinct channels triggers refresh once", func(t *testing.T) {
		m, calls := newManager()
		m.ReportSessionCut("a")
		m.ReportSessionCut("b")
		m.ReportSessionCut("c") // 3rd distinct channel -> re-mint
		waitForRefresh(calls, 1)
		// Duplicate reports and more channels stay bounded by the refresh
		// rate limit (10 min), so the recorder must not fire again.
		m.ReportSessionCut("a")
		m.ReportSessionCut("d")
		m.ReportSessionCut("e")
		time.Sleep(100 * time.Millisecond)
		if got := atomic.LoadInt32(calls); got != 1 {
			t.Fatalf("cookie refresh calls = %d, want 1 (rate-limited)", got)
		}
	})

	t.Run("expired reports are pruned and do not count", func(t *testing.T) {
		m, calls := newManager()
		m.ReportSessionCut("a")
		m.ReportSessionCut("b")
		// Age the reports out of the window: a later report from a single
		// channel must not combine with stale ones to cross the threshold.
		m.sessionCutMu.Lock()
		m.sessionCutAt["a"] = time.Now().Add(-10 * time.Minute)
		m.sessionCutAt["b"] = time.Now().Add(-10 * time.Minute)
		m.sessionCutMu.Unlock()
		m.ReportSessionCut("c")
		time.Sleep(100 * time.Millisecond)
		if got := atomic.LoadInt32(calls); got != 0 {
			t.Fatalf("cookie refresh calls = %d, want 0 (stale reports pruned)", got)
		}
	})

	t.Run("CF-block bursts feed the shared detector", func(t *testing.T) {
		m, calls := newManager()
		// Two CF-blocked channels alone are below the threshold.
		m.ReportCFBlock("a")
		m.ReportCFBlock("b")
		time.Sleep(100 * time.Millisecond)
		if got := atomic.LoadInt32(calls); got != 0 {
			t.Fatalf("cookie refresh calls = %d, want 0 (below threshold)", got)
		}
		// A third distinct CF-blocked channel crosses it.
		m.ReportCFBlock("c")
		waitForRefresh(calls, 1)
	})

	t.Run("session cuts and CF blocks combine in one detector", func(t *testing.T) {
		m, calls := newManager()
		m.ReportSessionCut("a")
		m.ReportCFBlock("b")
		time.Sleep(100 * time.Millisecond)
		if got := atomic.LoadInt32(calls); got != 0 {
			t.Fatalf("cookie refresh calls = %d, want 0 (below threshold)", got)
		}
		// Third distinct channel via the OTHER signature -> same detector fires.
		m.ReportCFBlock("c")
		waitForRefresh(calls, 1)
	})

	t.Run("ResetCFBlock prunes the shared window", func(t *testing.T) {
		m, calls := newManager()
		m.ReportCFBlock("a")
		m.ReportCFBlock("b")
		m.ResetCFBlock("a")
		m.ResetCFBlock("b")
		// A single new report must not combine with the pruned ones to
		// cross the threshold.
		m.ReportCFBlock("c")
		time.Sleep(100 * time.Millisecond)
		if got := atomic.LoadInt32(calls); got != 0 {
			t.Fatalf("cookie refresh calls = %d, want 0 (reset pruned window)", got)
		}
	})
}

// TestCreateChannelFromAssignmentLeavesManualPause verifies the claim/reconcile
// path NEVER overrides a user's explicit UI pause: a channel paused with a
// MANUAL reason stays paused even when it is re-claimed from an assignment.
func TestCreateChannelFromAssignmentLeavesManualPause(t *testing.T) {
	oldMgr, oldCfg := server.Manager, server.Config
	var m *Manager
	defer func() { restoreTestGlobalsSafe(oldMgr, oldCfg, m) }()

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
	var m *Manager
	defer func() { restoreTestGlobalsSafe(oldMgr, oldCfg, m) }()

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

// TestResolveRunDeadline verifies the RUN_DEADLINE env parsing: unset and
// unparsable values yield the zero time (restarts unrestricted, e.g. local
// dev), while valid Unix seconds resolve to that exact instant.
func TestResolveRunDeadline(t *testing.T) {
	t.Setenv("RUN_DEADLINE", "")
	if !resolveRunDeadline().IsZero() {
		t.Fatal("expected zero deadline when RUN_DEADLINE unset")
	}
	t.Setenv("RUN_DEADLINE", "garbage")
	if !resolveRunDeadline().IsZero() {
		t.Fatal("expected zero deadline for unparsable RUN_DEADLINE")
	}
	t.Setenv("RUN_DEADLINE", "0")
	if !resolveRunDeadline().IsZero() {
		t.Fatal("expected zero deadline for non-positive RUN_DEADLINE")
	}
	want := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	t.Setenv("RUN_DEADLINE", strconv.FormatInt(want.Unix(), 10))
	if got := resolveRunDeadline(); !got.Equal(want) {
		t.Fatalf("deadline = %s, want %s", got, want)
	}
}

// TestStartSessionRefusesPastRunDeadline verifies the final-drain rule: with
// RUN_DEADLINE set, StartSession must refuse a session whose full duration
// would overrun the runner's hard deadline (the VM dies at run end, so the
// recordings would be lost), and accept one that fits before it.
func TestStartSessionRefusesPastRunDeadline(t *testing.T) {
	oldMgr, oldCfg := server.Manager, server.Config
	var m *Manager
	defer func() {
		if m != nil {
			m.StopSession()
		}
		os.Remove("upload-complete.flag") // sessionLoop writes this in cwd
		restoreTestGlobalsSafe(oldMgr, oldCfg, m)
	}()

	// Refuse: a 2h session starting now would overrun a deadline 1h away.
	t.Setenv("RUN_DEADLINE", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	server.Manager = m
	server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/", CFChannelThreshold: 5}

	m.StartSession(2 * time.Hour)
	if _, active := m.SessionInfo(); active {
		t.Fatal("StartSession must refuse a session that would overrun RUN_DEADLINE")
	}

	// Accept: a 1h session fits before a deadline 3h away. The deadline is
	// published by the session loop goroutine, so poll briefly for it.
	t.Setenv("RUN_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Hour).Unix(), 10))
	m.StartSession(time.Hour)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, active := m.SessionInfo(); active {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("StartSession should start a session that fits before RUN_DEADLINE")
}

// TestCreateChannelFromAssignmentDeclinesAfterFinalDrain verifies that once
// the session is stopped (final drain on an ephemeral runner, or graceful
// shutdown), the manager declines new assignments instead of creating channels
// that would record into a doomed tail killed with the VM.
func TestCreateChannelFromAssignmentDeclinesAfterFinalDrain(t *testing.T) {
	oldMgr, oldCfg := server.Manager, server.Config
	var m *Manager
	defer func() {
		if m != nil {
			m.StopAllChannels()
		}
		restoreTestGlobalsSafe(oldMgr, oldCfg, m)
	}()

	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	server.Manager = m
	server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/", CFChannelThreshold: 5}

	// Control: before the stop, an assignment creates the channel.
	ca := &database.ChannelAssignment{
		Username: "early_claim", Site: "chaturbate", Status: "claimed",
		Framerate: 60, Resolution: 1080,
	}
	if err := m.CreateChannelFromAssignment(ca); err != nil {
		t.Fatalf("CreateChannelFromAssignment: %v", err)
	}
	if _, ok := m.Channels.Load("early_claim"); !ok {
		t.Fatal("assignment before the stop should create the channel")
	}

	// Stop the session permanently (final-drain state) and re-claim.
	m.StopSession()
	ca2 := &database.ChannelAssignment{
		Username: "late_claim", Site: "chaturbate", Status: "claimed",
		Framerate: 60, Resolution: 1080,
	}
	if err := m.CreateChannelFromAssignment(ca2); err != nil {
		t.Fatalf("CreateChannelFromAssignment: %v", err)
	}
	if _, ok := m.Channels.Load("late_claim"); ok {
		t.Fatal("channel must NOT be created when the session is stopped (final drain)")
	}
}

// TestShouldTriggerEarlyFinalDrain verifies the early-drain decision: a
// multi-GB backlog that can't finish before the run deadline must trigger,
// while a small backlog, no deadline, or no pending bytes never does.
func TestShouldTriggerEarlyFinalDrain(t *testing.T) {
	now := time.Now()
	deadline := now.Add(30 * time.Minute)

	// 10 GB pending, no throughput signal yet → default 10 MB/s → ETA ~17 min
	// + 20 min margin = 37 min > 30 min remaining → trigger.
	if !shouldTriggerEarlyFinalDrain(now, deadline, 10_000_000_000, 0) {
		t.Fatal("multi-GB backlog near the deadline must trigger the early drain")
	}
	// Same backlog with healthy real throughput (50 MB/s → ETA ~3.3 min + 20
	// min margin = 23 min < 30 min) → no trigger.
	if shouldTriggerEarlyFinalDrain(now, deadline, 10_000_000_000, 50_000_000) {
		t.Fatal("backlog draining at healthy throughput must not trigger")
	}
	// Small backlog fits easily.
	if shouldTriggerEarlyFinalDrain(now, deadline, 100_000_000, 10_000_000) {
		t.Fatal("small backlog must not trigger")
	}
	// No run deadline (local dev) → never trigger.
	if shouldTriggerEarlyFinalDrain(now, time.Time{}, 10_000_000_000, 0) {
		t.Fatal("no run deadline must not trigger")
	}
	// No pending bytes → never trigger.
	if shouldTriggerEarlyFinalDrain(now, deadline, 0, 0) {
		t.Fatal("no pending bytes must not trigger")
	}
}

