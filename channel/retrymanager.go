package channel

import (
	"fmt"
	"sync"
	"time"
)

// RetryManager runs retryable jobs in the background without blocking the
// main pipeline or recording goroutines. It respects the global UploadSem
// for upload-type jobs and provides graceful shutdown.
type RetryManager struct {
	mu       sync.Mutex
	queue    chan *retryJob
	workers  sync.WaitGroup
	stopCh   chan struct{}
	stopped  bool
	numWorkers int
}

type retryJob struct {
	id          string
	fn          func() error
	requiresSem bool
	maxAttempts int
	attempt     int
	baseBackoff time.Duration
	priority    int
	enqueuedAt  time.Time
	resultCh    chan error
}

const (
	defaultMaxAttempts = 3
	defaultBaseBackoff = 5 * time.Second
	maxBackoff         = 10 * time.Minute
	numWorkers         = 2
)

// NewRetryManager creates a new RetryManager.
func NewRetryManager() *RetryManager {
	return &RetryManager{
		queue:      make(chan *retryJob, 1024),
		stopCh:     make(chan struct{}),
		numWorkers: numWorkers,
	}
}

// Start begins processing jobs.
func (rm *RetryManager) Start() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.stopped {
		return
	}
	for i := 0; i < rm.numWorkers; i++ {
		rm.workers.Add(1)
		go rm.worker(i)
	}
}

// Stop gracefully shuts down the RetryManager. It stops accepting new jobs,
// drains the queue, and waits for all workers to finish their current job.
func (rm *RetryManager) Stop() error {
	rm.mu.Lock()
	if rm.stopped {
		rm.mu.Unlock()
		return nil
	}
	rm.stopped = true
	close(rm.queue)
	rm.mu.Unlock()

	rm.workers.Wait()
	return nil
}

// Schedule adds a job to the retry queue. The job will be executed in the
// background with up to maxAttempts retries and exponential backoff.
// If requiresSem is true, the job will acquire UploadSem before running.
func (rm *RetryManager) Schedule(id string, fn func() error, opts ...RetryOption) {
	rm.mu.Lock()
	if rm.stopped {
		rm.mu.Unlock()
		return
	}
	rm.mu.Unlock()

	job := &retryJob{
		id:          id,
		fn:          fn,
		maxAttempts: defaultMaxAttempts,
		attempt:     0,
		baseBackoff: defaultBaseBackoff,
		enqueuedAt:  time.Now(),
	}
	for _, opt := range opts {
		opt(job)
	}

	select {
	case rm.queue <- job:
	case <-rm.stopCh:
	}
}

// RetryOption configures a retry job.
type RetryOption func(*retryJob)

// WithMaxAttempts sets the maximum number of attempts (default 3).
func WithMaxAttempts(n int) RetryOption {
	return func(j *retryJob) { j.maxAttempts = n }
}

// WithBaseBackoff sets the base backoff duration (default 5s).
func WithBaseBackoff(d time.Duration) RetryOption {
	return func(j *retryJob) { j.baseBackoff = d }
}

// WithUploadSem makes the job acquire UploadSem before executing.
func WithUploadSem() RetryOption {
	return func(j *retryJob) { j.requiresSem = true }
}

// WithPriority sets job priority (higher = more urgent).
func WithPriority(p int) RetryOption {
	return func(j *retryJob) { j.priority = p }
}

func (rm *RetryManager) worker(id int) {
	defer rm.workers.Done()

	for {
		select {
		case job, ok := <-rm.queue:
			if !ok {
				return
			}
			rm.executeJob(job)
		case <-rm.stopCh:
			return
		}
	}
}

func (rm *RetryManager) executeJob(job *retryJob) {
	job.attempt++

	var semReleased func()
	if job.requiresSem {
		UploadSem <- struct{}{}
		semReleased = func() { <-UploadSem }
		defer semReleased()
	}

	err := job.fn()
	if err == nil {
		if job.resultCh != nil {
			job.resultCh <- nil
			close(job.resultCh)
		}
		return
	}

	if job.attempt >= job.maxAttempts {
		if job.resultCh != nil {
			job.resultCh <- err
			close(job.resultCh)
		}
		return
	}

	backoff := job.baseBackoff * time.Duration(1<<uint(job.attempt-1))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	// Re-queue with delay
	go func() {
		timer := time.NewTimer(backoff)
		defer timer.Stop()
		select {
		case <-timer.C:
			rm.mu.Lock()
			stopped := rm.stopped
			rm.mu.Unlock()
			if !stopped {
				select {
				case rm.queue <- job:
				case <-rm.stopCh:
				}
			}
		case <-rm.stopCh:
			if job.resultCh != nil {
				job.resultCh <- fmt.Errorf("retry manager stopped")
				close(job.resultCh)
			}
		}
	}()
}

// globalRetryManager is the singleton instance.
var globalRetryManager *RetryManager
var retryManagerOnce sync.Once

// GetRetryManager returns the global RetryManager instance, starting it on first access.
func GetRetryManager() *RetryManager {
	retryManagerOnce.Do(func() {
		globalRetryManager = NewRetryManager()
		globalRetryManager.Start()
	})
	return globalRetryManager
}

// StopRetryManager stops the global RetryManager. Used during shutdown.
func StopRetryManager() error {
	if globalRetryManager == nil {
		return nil
	}
	return globalRetryManager.Stop()
}

// DoWithRetry executes fn with retry logic in the background and waits for
// the result. Returns the final error (nil on success). The function runs
// in a background worker, freeing the caller from managing retry loops.
// If requiresSem is true, UploadSem is acquired during execution.
func (rm *RetryManager) DoWithRetry(id string, fn func() error, opts ...RetryOption) error {
	resultCh := make(chan error, 1)
	job := &retryJob{
		id:          id,
		fn:          fn,
		maxAttempts: defaultMaxAttempts,
		attempt:     0,
		baseBackoff: defaultBaseBackoff,
		resultCh:    resultCh,
		enqueuedAt:  time.Now(),
	}
	for _, opt := range opts {
		opt(job)
	}

	select {
	case rm.queue <- job:
	case <-rm.stopCh:
		return fmt.Errorf("retry manager stopped")
	}

	// Wait for result
	return <-resultCh
}

// ScheduleRetry is a convenience wrapper for GetRetryManager().Schedule().
func ScheduleRetry(id string, fn func() error, opts ...RetryOption) {
	GetRetryManager().Schedule(id, fn, opts...)
}

// DoWithRetry is a convenience wrapper for GetRetryManager().DoWithRetry().
func DoWithRetry(id string, fn func() error, opts ...RetryOption) error {
	return GetRetryManager().DoWithRetry(id, fn, opts...)
}