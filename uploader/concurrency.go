package uploader

import "sync"

// Per-host concurrency caps.  These are the primary throughput throttle for
// uploads: each host only accepts a bounded number of simultaneous requests
// before it starts rate-limiting (HTTP 429).  The defaults are tuned for
// maximum throughput while staying under the rate-limit thresholds; when a
// host does respond 429 the uploader already backs off and retries.
const (
	// defaultGoFileConcurrency is higher because GoFile's fleet tolerates
	// more parallel uploads than the smaller file hosts.
	defaultGoFileConcurrency = 12
	// defaultHostConcurrency applies to every other configured host.
	defaultHostConcurrency = 8
)

var (
	hostSemMu sync.RWMutex
	hostSems  = map[string]chan struct{}{
		// Video upload hosts
		"GoFile":        make(chan struct{}, defaultGoFileConcurrency),
		"VOE.sx":        make(chan struct{}, defaultHostConcurrency),
		"Streamtape":    make(chan struct{}, defaultHostConcurrency),
		"Mixdrop":       make(chan struct{}, defaultHostConcurrency),
		"Vidara":        make(chan struct{}, defaultHostConcurrency),
		"AnonMP4":       make(chan struct{}, defaultHostConcurrency),
		// Image upload hosts (thumbnails, sprites, previews)
		"Catbox":        make(chan struct{}, defaultHostConcurrency),
		"Pixhost":       make(chan struct{}, defaultHostConcurrency),
		"freeimage.host": make(chan struct{}, defaultHostConcurrency),
		"ImgChest":  make(chan struct{}, defaultHostConcurrency),
		"Imgbox":    make(chan struct{}, defaultHostConcurrency),
		"ImgBB":     make(chan struct{}, defaultHostConcurrency),
	}
)

// acquireHostSem blocks until an upload slot for the named host is free and
// returns a release function to call when the upload finishes.  Unknown hosts
// return a no-op so they are never accidentally throttled.
func acquireHostSem(host string) func() {
	hostSemMu.RLock()
	sem := hostSems[host]
	hostSemMu.RUnlock()
	if sem == nil {
		return func() {}
	}
	sem <- struct{}{}
	return func() { <-sem }
}

// SetHostConcurrency replaces the per-host upload semaphores with a new
// capacity so operators can trade concurrency for rate-limit headroom.
// Call at startup before any upload goroutines are running.
func SetHostConcurrency(perHost int) {
	if perHost <= 0 {
		return
	}
	hostSemMu.Lock()
	defer hostSemMu.Unlock()
	for h := range hostSems {
		hostSems[h] = make(chan struct{}, perHost)
	}
}

// SetHostConcurrencyTiered replaces the per-host upload semaphores with
// distinct capacities: GoFile (whose fleet tolerates more parallel uploads)
// gets goFile slots, every other host gets other.  Used by the VM-sized
// startup tuning (config.VMSizedConcurrency); an operator who sets
// --upload-host-concurrency explicitly uses SetHostConcurrency instead.
// Call at startup before any upload goroutines are running.
func SetHostConcurrencyTiered(goFile, other int) {
	if goFile <= 0 {
		goFile = defaultGoFileConcurrency
	}
	if other <= 0 {
		other = defaultHostConcurrency
	}
	hostSemMu.Lock()
	defer hostSemMu.Unlock()
	hostSems["GoFile"] = make(chan struct{}, goFile)
	for h := range hostSems {
		if h != "GoFile" {
			hostSems[h] = make(chan struct{}, other)
		}
	}
}
