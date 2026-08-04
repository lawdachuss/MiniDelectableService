package channel

// UploadSem limits how many video files may upload at the same time.
// Each file still fans out to the configured hosts, and each host has its own
// per-host concurrency cap (see uploader.SetHostConcurrency), so the effective
// ceiling is the more restrictive of the two.  100 concurrent file uploads
// keeps large backlogs draining quickly without overloading the box.
var UploadSem = make(chan struct{}, 100)

// SetUploadConcurrency replaces the global upload semaphore with a new
// capacity.  Call once at startup before any upload goroutines begin; in-flight
// acquires on the old channel are drained naturally.
func SetUploadConcurrency(n int) {
	if n <= 0 {
		return
	}
	UploadSem = make(chan struct{}, n)
}
