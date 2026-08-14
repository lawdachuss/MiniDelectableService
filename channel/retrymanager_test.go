package channel

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryManagerDoWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	defer rm.Stop()

	var calls int32
	err := rm.DoWithRetry("test-1", func() error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryManagerDoWithRetry_RetriesAndSucceeds(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	defer rm.Stop()

	var calls int32
	err := rm.DoWithRetry("test-2", func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return errors.New("transient error")
		}
		return nil
	}, WithMaxAttempts(3), WithBaseBackoff(10*time.Millisecond))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryManagerDoWithRetry_ExhaustsRetries(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	defer rm.Stop()

	var calls int32
	err := rm.DoWithRetry("test-3", func() error {
		atomic.AddInt32(&calls, 1)
		return errors.New("persistent error")
	}, WithMaxAttempts(3), WithBaseBackoff(10*time.Millisecond))
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryManagerDoWithRetry_RespectsMaxAttempts(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	defer rm.Stop()

	var calls int32
	err := rm.DoWithRetry("test-4", func() error {
		atomic.AddInt32(&calls, 1)
		return errors.New("always fails")
	}, WithMaxAttempts(2), WithBaseBackoff(10*time.Millisecond))
	if err == nil {
		t.Fatal("expected error")
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestRetryManagerDoWithRetry_StopDrainsQueue(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()

	// Submit a job that will never succeed
	err := rm.DoWithRetry("test-5", func() error {
		return errors.New("fail")
	}, WithMaxAttempts(10), WithBaseBackoff(10*time.Millisecond))
	if err == nil {
		t.Fatal("expected error")
	}

	// Now stop the manager
	if err := rm.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestRetryManagerDoWithRetry_AfterStopReturnsErrorNotPanic(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	rm.Stop()

	// DoWithRetry after Stop must return an error — never panic with
	// "send on closed channel" (the pre-fix behavior).
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DoWithRetry after Stop panicked: %v", r)
		}
	}()
	err := rm.DoWithRetry("post-stop", func() error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error after Stop, got nil")
	}
}

func TestRetryManagerDoWithRetry_UploadSem(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	defer rm.Stop()

	var semAcquired int32

	// Temporarily replace UploadSem with a small one for testing
	oldSem := UploadSem
	UploadSem = make(chan struct{}, 1)
	defer func() { UploadSem = oldSem }()

	// Acquire the sem slot so the RetryManager worker will block on acquire
	UploadSem <- struct{}{}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This should block until the sem is released
		err := rm.DoWithRetry("test-6", func() error {
			atomic.AddInt32(&semAcquired, 1)
			return nil
		}, WithUploadSem(), WithMaxAttempts(1))
		if err != nil {
			t.Errorf("DoWithRetry failed: %v", err)
		}
	}()

	// Give the worker time to try acquiring the sem
	time.Sleep(50 * time.Millisecond)

	// Release the sem so the worker can proceed
	<-UploadSem

	wg.Wait()
	if atomic.LoadInt32(&semAcquired) != 1 {
		t.Fatal("expected UploadSem to be acquired")
	}
}

func TestRetryManagerSchedule_ExecutesJob(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	defer rm.Stop()

	var executed int32
	rm.Schedule("test-schedule", func() error {
		atomic.AddInt32(&executed, 1)
		return nil
	})

	// Wait for the job to execute
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&executed) != 1 {
		t.Fatal("expected job to execute")
	}
}

func TestRetryManagerSchedule_StoppedManagerIgnoresJobs(t *testing.T) {
	rm := NewRetryManager()
	rm.Start()
	rm.Stop()

	var executed int32
	rm.Schedule("test-stopped", func() error {
		atomic.AddInt32(&executed, 1)
		return nil
	})

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&executed) != 0 {
		t.Fatal("expected job to be ignored after stop")
	}
}

func TestRetryManagerSingleton(t *testing.T) {
	// Reset the singleton for this test
	globalRetryManager = nil
	retryManagerOnce = sync.Once{}

	rm1 := GetRetryManager()
	rm2 := GetRetryManager()
	if rm1 != rm2 {
		t.Fatal("GetRetryManager should return the same singleton")
	}

	StopRetryManager()
	globalRetryManager = nil
	retryManagerOnce = sync.Once{}
}
