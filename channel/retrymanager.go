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
	mu         sync.Mutex
	queue      chan *retryJob
	workers    sync.WaitGroup
	stopCh     chan struct{}
	stopped    bool
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
)

var (
	// numWorkers is how many retry jobs execute concurrently.  The global
	// UploadSem and the per-host upload semaphores are the real throughput
	// governors, so the worker pool must be large enough to actually
	// saturate them — a 2-worker pool serialized every video upload on a
	// node behind a massive backlog, which delayed stageSaveMetadata (and
	// therefore thumbnail persistence) for hours while the queue drained.
	// VM-sized at startup via SetRetryWorkers (config.VMSizedConcurrency);
	// this default is the 2-vCPU GitHub-hosted runner baseline, which fills
	// both per-host tiers — GoFile's higher 12-slot cap and the 8-slot cap
	// of the other hosts.  DoWithRetry callers still wait on their result
	// channel, so jobs simply queue when the pool is busy.
	numWorkers = 12
)

// SetRetryWorkers overrides the retry worker pool size.  Call at startup
// before any upload jobs are scheduled — the global RetryManager is created
// lazily on first use, so this always applies.
func SetRetryWorkers(n int) {
	if n > 0 {
		numWorkers = n
	}
}

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
	close(rm.stopCh)
	rm.mu.Unlock()

	rm.workers.Wait()
	return nil
}

// enqueue adds a job to the queue unless the manager has been stopped.
// The stopped check and the channel send happen under the same lock that
// Stop() holds while closing the queue, so a send can never race the close —
// a send racing Stop() would panic with "send on closed channel".
func (rm *RetryManager) enqueue(job *retryJob) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.stopped {
		return false
	}
	select {
	case rm.queue <- job:
		return true
	case <-rm.stopCh:
		return false
	}
}

// failResult delivers an error to a job's result channel (if any) without
// blocking, then closes it.  The result channel is buffered with capacity 1,
// so the send never blocks; the default case guards against double delivery.
func (rm *RetryManager) failResult(job *retryJob, err error) {
	if job.resultCh == nil {
		return
	}
	select {
	case job.resultCh <- err:
	default:
	}
	close(job.resultCh)
}

// Schedule adds a job to the retry queue. The job will be executed in the
// background with up to maxAttempts retries and exponential backoff.
// If requiresSem is true, the job will acquire UploadSem before running.
// Jobs scheduled after Stop() are silently dropped.
func (rm *RetryManager) Schedule(id string, fn func() error, opts ...RetryOption) {
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

	rm.enqueue(job)
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

// worker runs a single retry worker goroutine.  Workers exit only when the
// queue is closed AND drained, so Stop() reliably finishes every already-
// queued job (drain semantics) before returning.
func (rm *RetryManager) worker(id int) {
	defer rm.workers.Done()

	for {
		job, ok := <-rm.queue
		if !ok {
			return
		}
		rm.executeJob(job)
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

	// Re-queue with delay.  The re-queue goes through enqueue(), which
	// re-checks stopped under the same lock Stop() uses to close the queue,
	// so a timer firing in the instant before Stop() can no longer panic
	// with "send on closed channel".  A job aborted by Stop() is reported
	// to its result channel so DoWithRetry callers never hang at shutdown.
	go func() {
		timer := time.NewTimer(backoff)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-rm.stopCh:
			rm.failResult(job, fmt.Errorf("retry manager stopped"))
			return
		}
		if !rm.enqueue(job) {
			rm.failResult(job, fmt.Errorf("retry manager stopped"))
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

	if !rm.enqueue(job) {
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
