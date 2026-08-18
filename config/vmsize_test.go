package config

import "testing"

// TestVMSizedConcurrencyAnchorsAtTwoVCPU verifies a 2-vCPU runner (the
// standard GitHub-hosted windows-latest VM) gets exactly the tuned production
// baseline: 12 retry workers, 100 UploadSem, 12 GoFile / 8 other host slots,
// 3 pipelines per channel.
func TestVMSizedConcurrencyAnchorsAtTwoVCPU(t *testing.T) {
	r, s, g, o, p := sizedFor(2)
	if r != 12 || s != 100 || g != 12 || o != 8 || p != 3 {
		t.Fatalf("2 vCPU = retry %d, sem %d, gofile %d, other %d, pipelines %d; want 12, 100, 12, 8, 3",
			r, s, g, o, p)
	}
}

// TestVMSizedConcurrencyScales verifies larger runners scale up monotonically
// and stay within the ceilings.
func TestVMSizedConcurrencyScales(t *testing.T) {
	prev := 0
	for _, n := range []int{2, 4, 8, 16, 32} {
		r, s, g, o, p := sizedFor(n)
		if r < prev || r > maxRetryWorkers {
			t.Fatalf("%d vCPU: retry workers %d out of range (prev %d, cap %d)", n, r, prev, maxRetryWorkers)
		}
		if g > maxGoFileConcurrency || o > maxOtherConcurrency {
			t.Fatalf("%d vCPU: host caps out of range (gofile %d, other %d)", n, g, o)
		}
		if p > maxPipelineWorkers {
			t.Fatalf("%d vCPU: pipeline workers %d exceeds cap %d", n, p, maxPipelineWorkers)
		}
		if s < r {
			t.Fatalf("%d vCPU: UploadSem %d smaller than retry workers %d", n, s, r)
		}
		prev = r
	}
}

// TestVMSizedConcurrencyFloors verifies tiny VMs never go below the minimums
// (a 1-vCPU box still gets the conservative baseline).
func TestVMSizedConcurrencyFloors(t *testing.T) {
	r, s, g, o, p := sizedFor(1)
	if r != minRetryWorkers || s != minUploadSem || g != minGoFileConcurrency || o != minOtherConcurrency || p != minPipelineWorkers {
		t.Fatalf("1 vCPU = retry %d, sem %d, gofile %d, other %d, pipelines %d; want floors %d, %d, %d, %d, %d",
			r, s, g, o, p, minRetryWorkers, minUploadSem, minGoFileConcurrency, minOtherConcurrency, minPipelineWorkers)
	}
}
