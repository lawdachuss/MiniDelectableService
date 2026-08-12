package coordinator

import (
	"os"
	"testing"
	"time"
)

// TestHeartbeatWatchdogCheck verifies the hang-detection predicate: only a
// non-zero, non-draining, stale-enough last-success time trips the watchdog.
func TestHeartbeatWatchdogCheck(t *testing.T) {
	now := time.Now()

	if d := heartbeatWatchdogCheck(time.Time{}, false, false); d != 0 {
		t.Fatalf("never-heartbeated (zero) should not trip, got %v", d)
	}
	if d := heartbeatWatchdogCheck(now.Add(-2*time.Minute), false, false); d != 0 {
		t.Fatalf("recent heartbeat should not trip, got %v", d)
	}
	if d := heartbeatWatchdogCheck(now.Add(-10*time.Minute), true, false); d != 0 {
		t.Fatalf("draining node should not trip (graceful shutdown), got %v", d)
	}
	if d := heartbeatWatchdogCheck(now.Add(-10*time.Minute), false, true); d != 0 {
		t.Fatalf("fenced node should not trip (DB outage — fence handles recovery, exit would cause a restart storm), got %v", d)
	}
	if d := heartbeatWatchdogCheck(now.Add(-10*time.Minute), false, false); d <= 0 {
		t.Fatalf("stale heartbeat should trip, got %v", d)
	}
	// Boundary: slightly under the threshold must NOT trip; at/over it trips.
	// (time.Since includes a few microseconds of drift, so an exact-now.Add(-stale)
	// would already read as over — assert the under case only.)
	if d := heartbeatWatchdogCheck(now.Add(-heartbeatWatchdogStale+time.Second), false, false); d != 0 {
		t.Fatalf("just-under-threshold should not trip, got %v", d)
	}
}

// TestHeartbeatWatchdogActCI verifies that on a CI runner (GITHUB_RUN_ID set)
// the watchdog force-exits, and that it only logs on a permanent node.
func TestHeartbeatWatchdogActCI(t *testing.T) {
	oldExit := heartbeatFatalExit
	defer func() { heartbeatFatalExit = oldExit }()

	exited := false
	heartbeatFatalExit = func(code int) { exited = true }

	c := &Coordinator{}
	stale := 10 * time.Minute

	os.Setenv("GITHUB_RUN_ID", "test-run")
	defer os.Unsetenv("GITHUB_RUN_ID")

	c.heartbeatWatchdogAct(stale)
	if !exited {
		t.Fatal("expected watchdog to force-exit on CI runner")
	}
}

func TestHeartbeatWatchdogActPermanent(t *testing.T) {
	oldExit := heartbeatFatalExit
	defer func() { heartbeatFatalExit = oldExit }()

	exited := false
	heartbeatFatalExit = func(code int) { exited = true }

	c := &Coordinator{}
	stale := 10 * time.Minute

	os.Unsetenv("GITHUB_RUN_ID")

	c.heartbeatWatchdogAct(stale)
	if exited {
		t.Fatal("permanent node must NOT force-exit")
	}
}
