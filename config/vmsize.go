package config

import "runtime"

// VM-sized concurrency pools.
//
// The fleet runs on GitHub-hosted runners, which come in FIXED VM sizes
// (standard windows-latest = 2 vCPU / 7 GB RAM; larger runners = 4/8/16/32/64
// vCPU).  Pools hardcoded for a 2-core box undershoot a larger runner, and
// oversized pools would thrash a 2-core one.  These functions derive every
// parallelism knob from the VM's vCPU count, anchored so 2 vCPU yields exactly
// the tuned production baseline:
//
//	2 vCPU:  12 retry workers · 100 UploadSem · 12 GoFile / 8 other host slots · 3 pipelines/channel
//	4 vCPU:  24               · 200           · 24        / 16                · 6
//	8 vCPU:  32 (cap)         · 400 (cap)     · 32 (cap)  / 24 (cap)          · 12
//	16 vCPU: 32               · 400           · 32        / 24                · 16 (cap)
//
// Host-facing caps stay bounded: they reflect the upload hosts' rate-limit
// tolerance (429 backoff), not VM resources, so they never exceed sane
// ceilings even on huge runners.
const (
	minRetryWorkers      = 12
	maxRetryWorkers      = 32
	minUploadSem         = 100
	maxUploadSem         = 400
	minGoFileConcurrency = 12
	maxGoFileConcurrency = 32
	minOtherConcurrency  = 8
	maxOtherConcurrency  = 24
	minPipelineWorkers   = 3
	maxPipelineWorkers   = 16
)

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// sizedFor computes the concurrency pools for a VM with n vCPUs.  Exposed for
// tests; VMSizedConcurrency calls it with the real CPU count.
func sizedFor(n int) (retryWorkers, uploadSem, goFile, other, pipelineWorkers int) {
	retryWorkers = clampInt(n*6, minRetryWorkers, maxRetryWorkers)
	uploadSem = clampInt(n*50, minUploadSem, maxUploadSem)
	goFile = clampInt(n*6, minGoFileConcurrency, maxGoFileConcurrency)
	other = clampInt(n*4, minOtherConcurrency, maxOtherConcurrency)
	pipelineWorkers = clampInt(n*3/2, minPipelineWorkers, maxPipelineWorkers)
	return
}

// VMSizedConcurrency returns the parallelism pools for the current VM,
// derived from runtime.NumCPU(): retry workers, UploadSem, GoFile per-host
// cap, other-hosts per-host cap, and pipelines per channel queue.
func VMSizedConcurrency() (retryWorkers, uploadSem, goFile, other, pipelineWorkers int) {
	return sizedFor(runtime.NumCPU())
}
