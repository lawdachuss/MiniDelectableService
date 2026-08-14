package coordinator

import (
	"context"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// fakeLiveCheckDB records live-check bookkeeping against a fixed assignment set.
type fakeLiveCheckDB struct {
	assignments  []database.ChannelAssignment
	recorded     []string // site/username marked recording
	released     []string // site/username released back to the pool
	setLive      [][2]string
	notLiveCalls int
}

func (f *fakeLiveCheckDB) GetAllAssignments() ([]database.ChannelAssignment, error) {
	return f.assignments, nil
}

func (f *fakeLiveCheckDB) MarkChannelRecording(username, site string) error {
	f.recorded = append(f.recorded, site+"/"+username)
	return nil
}

func (f *fakeLiveCheckDB) ReleaseChannel(username, site string) error {
	f.released = append(f.released, site+"/"+username)
	return nil
}

func (f *fakeLiveCheckDB) SetChannelsLive(pairs [][2]string) error {
	f.setLive = pairs
	return nil
}

func (f *fakeLiveCheckDB) SetChannelsNotLive(pairs [][2]string) error {
	f.notLiveCalls++
	return nil
}

// stubLiveness returns a canned CheckLive result per run.
type stubLiveness struct {
	results []LivenessResult
	idx     int
}

func (s *stubLiveness) CheckLive(ctx context.Context, site, username string) LivenessResult {
	r := LivenessOffline
	if s.idx < len(s.results) {
		r = s.results[s.idx]
	}
	s.idx++
	return r
}

func (s *stubLiveness) allOffline() *stubLiveness {
	return &stubLiveness{results: []LivenessResult{LivenessOffline}}
}

func newLiveCheckCoordinator(mgr ChannelManager) *Coordinator {
	return &Coordinator{
		NodeID:        "node-a",
		Manager:       mgr,
		liveCheckMiss: make(map[string]int),
	}
}

func withCleanServerConfig(t *testing.T, fn func()) {
	t.Helper()
	old := server.Config
	server.Config = &entity.Config{Interval: 1, Domain: "http://127.0.0.1:1/"}
	defer func() { server.Config = old }()
	fn()
}

func recordingAssignment(user, site, node string) database.ChannelAssignment {
	return database.ChannelAssignment{Username: user, Site: site, Status: "recording", AssignedNode: node}
}

func TestRunLiveCheckLiveChannelKeepsRecording(t *testing.T) {
	withCleanServerConfig(t, func() {
		db := &fakeLiveCheckDB{assignments: []database.ChannelAssignment{
			recordingAssignment("livegirl", "chaturbate", "node-a"),
		}}
		c := newLiveCheckCoordinator(&mockChannelManager{})

		c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessLive}})

		if len(db.recorded) != 1 || db.recorded[0] != "chaturbate/livegirl" {
			t.Fatalf("live channel should be marked recording, got recorded=%v", db.recorded)
		}
		if len(db.released) != 0 {
			t.Fatalf("live channel must never be released, got %v", db.released)
		}
		if len(db.setLive) != 1 || db.setLive[0] != [2]string{"livegirl", "chaturbate"} {
			t.Fatalf("live pair should be reported, got %v", db.setLive)
		}
	})
}

func TestRunLiveCheckOfflineChannelReleasesAfterDebounce(t *testing.T) {
	withCleanServerConfig(t, func() {
		db := &fakeLiveCheckDB{assignments: []database.ChannelAssignment{
			recordingAssignment("gonegirl", "chaturbate", "node-a"),
		}}
		c := newLiveCheckCoordinator(&mockChannelManager{})

		// Cycle 1: a single definitive-offline observation must NOT release.
		c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessOffline}})
		if len(db.released) != 0 {
			t.Fatalf("single offline cycle must not release (debounce), got %v", db.released)
		}

		// Cycle 2: consecutive offline observation reaches the streak → release.
		c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessOffline}})
		if len(db.released) != 1 || db.released[0] != "chaturbate/gonegirl" {
			t.Fatalf("channel offline for %d cycles should be released, got %v", liveCheckReleaseStreak, db.released)
		}

		// Cycle 3: nothing left to release.
		c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessOffline}})
		if len(db.released) != 1 {
			t.Fatalf("already-released channel must not be released again, got %v", db.released)
		}
	})
}

func TestRunLiveCheckUnknownNeverReleases(t *testing.T) {
	withCleanServerConfig(t, func() {
		db := &fakeLiveCheckDB{assignments: []database.ChannelAssignment{
			recordingAssignment("flaky", "chaturbate", "node-a"),
		}}
		c := newLiveCheckCoordinator(&mockChannelManager{})

		// The exact failure mode being guarded: a probe that keeps erroring /
		// returning an ambiguous status must never pause a live recording.
		for i := 0; i < 5; i++ {
			c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessUnknown}})
		}
		if len(db.released) != 0 {
			t.Fatalf("unknown probe results must never release a channel, got %v", db.released)
		}
		if len(db.recorded) != 0 {
			t.Fatalf("unknown probe results must not mark recording either, got %v", db.recorded)
		}
	})
}

func TestRunLiveCheckOfflineThenLiveResetsStreak(t *testing.T) {
	withCleanServerConfig(t, func() {
		db := &fakeLiveCheckDB{assignments: []database.ChannelAssignment{
			recordingAssignment("bouncing", "chaturbate", "node-a"),
		}}
		c := newLiveCheckCoordinator(&mockChannelManager{})

		// offline, then live (streak reset), then a single offline — must NOT
		// reach the release threshold.
		for _, r := range []LivenessResult{LivenessOffline, LivenessLive, LivenessOffline} {
			c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{r}})
		}
		if len(db.released) != 0 {
			t.Fatalf("offline→live→offline must not accumulate a release streak, got %v", db.released)
		}

		// A second consecutive offline after the reset DOES release.
		c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessOffline}})
		if len(db.released) != 1 {
			t.Fatalf("2 consecutive offlines after a live reset should release, got %v", db.released)
		}
	})
}

func TestRunLiveCheckManualPauseNeverReleased(t *testing.T) {
	withCleanServerConfig(t, func() {
		db := &fakeLiveCheckDB{assignments: []database.ChannelAssignment{
			recordingAssignment("mypause", "chaturbate", "node-a"),
		}}
		mgr := &mockChannelManager{manualPaused: []ChannelPause{{Username: "mypause", Site: "chaturbate"}}}
		c := newLiveCheckCoordinator(mgr)

		for i := 0; i < liveCheckReleaseStreak+2; i++ {
			c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessOffline}})
		}
		if len(db.released) != 0 {
			t.Fatalf("user-paused channel must never be released, got %v", db.released)
		}
	})
}

func TestRunLiveCheckNonRecordingChannelNotReleased(t *testing.T) {
	withCleanServerConfig(t, func() {
		// A "claimed" (not recording) offline channel stays assigned — only
		// recording channels are released so a node never pins a channel that
		// isn't even capturing.
		assigned := database.ChannelAssignment{Username: "parked", Site: "stripchat", Status: "claimed", AssignedNode: "node-a"}
		db := &fakeLiveCheckDB{assignments: []database.ChannelAssignment{assigned}}
		c := newLiveCheckCoordinator(&mockChannelManager{})

		for i := 0; i < liveCheckReleaseStreak+1; i++ {
			c.runLiveCheckWith(db, &stubLiveness{results: []LivenessResult{LivenessOffline}})
		}
		if len(db.released) != 0 {
			t.Fatalf("non-recording offline channel must not be released, got %v", db.released)
		}
	})
}

func TestRunLiveCheckMissStreakIsPerChannel(t *testing.T) {
	withCleanServerConfig(t, func() {
		// Two recording channels on this node; both report offline but only one
		// reaches the streak on the same cycle.
		db := &fakeLiveCheckDB{assignments: []database.ChannelAssignment{
			recordingAssignment("alpha", "chaturbate", "node-a"),
			recordingAssignment("beta", "stripchat", "node-a"),
		}}
		c := newLiveCheckCoordinator(&mockChannelManager{})
		check := &stubLiveness{results: []LivenessResult{
			LivenessOffline, LivenessOffline, // cycle 1
			LivenessOffline, LivenessUnknown, // cycle 2: beta unknown → streak reset
		}}

		c.runLiveCheckWith(db, check)
		c.runLiveCheckWith(db, check)

		if len(db.released) != 1 || db.released[0] != "chaturbate/alpha" {
			t.Fatalf("only alpha should be released, got %v", db.released)
		}
	})
}
