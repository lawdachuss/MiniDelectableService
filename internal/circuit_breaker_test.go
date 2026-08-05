package internal

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsExpectedChannelError verifies that normal per-channel states are
// classified as expected (so they never feed the global breaker/rate limiter)
// and that genuine upstream failures are NOT.
func TestIsExpectedChannelError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"offline", ErrChannelOffline, true},
		{"offline wrapped", fmt.Errorf("get stream: %w", ErrChannelOffline), true},
		{"private", ErrPrivateStream, true},
		{"private wrapped", fmt.Errorf("forbidden: %w", ErrPrivateStream), true},
		{"hidden", ErrHiddenStream, true},
		{"not found", ErrNotFound, true},
		{"not found wrapped", fmt.Errorf("get bytes: %w", ErrNotFound), true},
		{"age verification", ErrAgeVerification, true},
		{"password required", ErrRoomPasswordRequired, true},
		{"geo blocked", ErrGeoBlocked, true},
		// Genuine upstream failures must still feed the breaker:
		{"cloudflare blocked", ErrCloudflareBlocked, false},
		{"circuit breaker open", ErrCircuitBreakerOpen, false},
		{"generic error", errors.New("connection reset"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsExpectedChannelError(tc.err); got != tc.want {
				t.Errorf("IsExpectedChannelError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestErrCircuitBreakerOpenIsNotChannelOffline guards against the sentinel
// regressing into ErrChannelOffline, which would bench channels for the full
// Interval instead of retrying in seconds after the breaker cooldown.
func TestErrCircuitBreakerOpenIsNotChannelOffline(t *testing.T) {
	if errors.Is(ErrCircuitBreakerOpen, ErrChannelOffline) {
		t.Fatal("ErrCircuitBreakerOpen must not wrap ErrChannelOffline")
	}
}

// TestReportChaturbateFailureUnlessExpectedSkipsExpected verifies that
// expected per-channel states never feed the breaker while genuine failures do.
func TestReportChaturbateFailureUnlessExpectedSkipsExpected(t *testing.T) {
	// Reset the shared breaker to a known-closed state.
	chaturbateBreaker.state.Store(int32(StateClosed))
	chaturbateBreaker.resetCounters()

	// Expected error: must not touch counters.
	ReportChaturbateFailureUnlessExpected(ErrPrivateStream)
	if n := chaturbateBreaker.failures.Load(); n != 0 {
		t.Fatalf("expected-error feedback incremented breaker failures: %d", n)
	}

	// Genuine failure: must be counted.
	ReportChaturbateFailureUnlessExpected(errors.New("connection reset"))
	if n := chaturbateBreaker.failures.Load(); n != 1 {
		t.Fatalf("genuine failure feedback = %d failures, want 1", n)
	}
}
