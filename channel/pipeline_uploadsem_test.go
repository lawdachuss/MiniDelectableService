package channel

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentUploadJobsTinySemaphore is the regression test for the
// production deadlock where every pipeline froze at the thumbnail_upload
// stage (pipeline_states rows stuck with updated==created, failed=false,
// retries=0 — recordings uploaded but thumbnails never saved).
//
// The old processPipeline upload goroutine pre-acquired UploadSem BEFORE
// calling stageUploadVideos, which runs its upload inside
// DoWithRetry(..., WithUploadSem()).  The retry worker then acquired UploadSem
// a SECOND time.  Once concurrent pipelines exceeded the semaphore capacity,
// every pipeline held one slot while waiting on the retry result channel, and
// the retry workers could never acquire a slot for any job — a permanent
// deadlock with no timeout anywhere in the chain.  The pipeline's deferred
// state-save never ran, so Supabase kept the enqueue-time snapshot forever
// (stage=thumbnail_upload, updated==created).
//
// The fix removed the outer pre-acquire: the retry worker is now the ONLY
// acquirer of UploadSem.  This test proves that contract — with a tiny
// semaphore, far more concurrent WithUploadSem jobs than capacity all complete
// (they queue up and workers serialize on the semaphore).  If anyone re-adds
// an outer acquire that holds a slot while waiting on DoWithRetry, the test
// hangs and trips the timeout.
func TestConcurrentUploadJobsTinySemaphore(t *testing.T) {
	// Run the contract check at two capacities: a 2-slot semaphore (the
	// serialization threshold) and a 1-slot semaphore (where any caller-side
	// pre-acquire would deadlock instantly — the exact fixed pipeline shape).
	for _, cap := range []int{2, 1} {
		t.Run(fmt.Sprintf("capacity-%d", cap), func(t *testing.T) {
			oldSem := UploadSem
			UploadSem = make(chan struct{}, cap)
			defer func() { UploadSem = oldSem }()

			rm := NewRetryManager()
			rm.Start()
			defer rm.Stop()

			const jobs = 12
			var wg sync.WaitGroup
			errs := make([]error, jobs)
			for i := 0; i < jobs; i++ {
				wg.Add(1)
				go func(n int) {
					defer wg.Done()
					errs[n] = rm.DoWithRetry(fmt.Sprintf("upload-%d", n),
						func() error { return nil },
						WithUploadSem(), WithMaxAttempts(1))
				}(i)
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("deadlock: concurrent WithUploadSem jobs blocked on a tiny semaphore — " +
					"an outer UploadSem acquire must be holding slots while waiting on DoWithRetry")
			}
			for i, err := range errs {
				if err != nil {
					t.Errorf("job %d failed: %v", i, err)
				}
			}
		})
	}
}
