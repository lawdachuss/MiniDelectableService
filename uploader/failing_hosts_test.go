package uploader

import (
	"errors"
	"testing"
)

// failingHostStub builds a MultiHostUploader with one fake host whose upload
// function always returns the given error (or succeeds when err is nil).
func failingHostStub(err error) *MultiHostUploader {
	return &MultiHostUploader{
		log: &nilLogger{},
		hosts: map[string]uploaderFunc{
			"TestHost": func(_ string, _ ProgressFunc) (string, error) {
				if err != nil {
					return "", err
				}
				return "https://example.test/video", nil
			},
		},
	}
}

// TestFailingHostDisabledAfterConsecutiveFailures: a generic (non-dead,
// non-storage-full) failure repeated on consecutive files must disable the
// host once the threshold is reached, so the failing host is never retried
// again this run. The first threshold-1 failures must NOT disable it (a single
// blip stays retryable).
func TestFailingHostDisabledAfterConsecutiveFailures(t *testing.T) {
	boom := errors.New("HTTP 503: temporarily overloaded")
	u := failingHostStub(boom)

	for i := 1; i < failingHostsThreshold; i++ {
		results := u.UploadSelected("f.mp4", []string{"TestHost"})
		if len(results) != 1 || results[0].Error == nil {
			t.Fatalf("attempt %d: expected a failure result", i)
		}
		if u.isHostDisabled("TestHost") {
			t.Fatalf("attempt %d: host disabled before reaching threshold %d", i, failingHostsThreshold)
		}
	}

	// The Nth consecutive failure crosses the threshold.
	results := u.UploadSelected("f.mp4", []string{"TestHost"})
	if len(results) != 1 || results[0].Error == nil {
		t.Fatalf("attempt %d: expected a failure result", failingHostsThreshold)
	}
	if !u.isHostDisabled("TestHost") {
		t.Fatalf("host still enabled after %d consecutive failures", failingHostsThreshold)
	}

	// From now on the host is skipped entirely: no upload function runs, so
	// the result set is empty rather than another failed attempt.
	if got := u.UploadSelected("f.mp4", []string{"TestHost"}); len(got) != 0 {
		t.Fatalf("disabled host must be skipped, got %d result(s): %#v", len(got), got)
	}
}

// TestHostStreakResetsOnSuccess: one success in a string of failures resets
// the consecutive-failure counter, so intermittent failures never accumulate
// into a disable.
func TestHostStreakResetsOnSuccess(t *testing.T) {
	u := failingHostStub(nil) // succeeds
	// Build a streak just under the threshold, then succeed.
	u.recordHostFailure("TestHost")
	u.recordHostFailure("TestHost")
	u.UploadSelected("f.mp4", []string{"TestHost"}) // success resets the streak

	if got := u.recordHostFailure("TestHost"); got != 1 {
		t.Fatalf("streak after success + 1 failure = %d, want 1 (not accumulated)", got)
	}
	if u.isHostDisabled("TestHost") {
		t.Fatal("host must not be disabled while the streak keeps resetting")
	}
}

// TestDeadHostDisablesImmediately: isHostDead failures (dial tcp / connection
// refused / 500) still disable on the FIRST failure — the consecutive-failure
// path is only for errors that do not already classify as dead or rate-limited.
func TestDeadHostDisablesImmediately(t *testing.T) {
	u := failingHostStub(errors.New(`dial tcp 1.2.3.4:443: connectex: A connection attempt failed`))

	results := u.UploadSelected("f.mp4", []string{"TestHost"})
	if len(results) != 1 || results[0].Error == nil {
		t.Fatalf("expected a failure result")
	}
	if !u.isHostDisabled("TestHost") {
		t.Fatal("dead host must be disabled on the first failure")
	}
	if got := u.UploadSelected("f.mp4", []string{"TestHost"}); len(got) != 0 {
		t.Fatalf("disabled host must be skipped, got %d result(s)", len(got))
	}
}

// TestFailingHostThresholdValues pins the threshold so a refactor cannot
// silently make the fleet retry broken hosts all day.
func TestFailingHostThresholdValues(t *testing.T) {
	if failingHostsThreshold < 2 {
		t.Fatalf("failingHostsThreshold = %d, want >= 2 (a single transient blip must not disable a host)", failingHostsThreshold)
	}
	if failingHostsThreshold > 5 {
		t.Fatalf("failingHostsThreshold = %d, want <= 5 (retrying a broken host burns minutes per file)", failingHostsThreshold)
	}
}
