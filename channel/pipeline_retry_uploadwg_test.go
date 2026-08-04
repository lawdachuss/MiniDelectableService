package channel

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// TestPipelineFailureReleasesUploadWg verifies that a pipeline which fails
// during processing releases its UploadWg token exactly once.
//
// Regression for the leak where processPipeline scheduled a retry (marking
// p.retried synchronously) BEFORE the Done() gate read the flag, so the first
// failed processing skipped Done() and ch.UploadWg.Wait() in ProcessPending
// deadlocked the channel shutdown forever.
//
// Hermetic by design: the path does not exist on disk, so stageUploadVideos
// fails immediately with "file not found" — no network I/O (GoFile is always
// registered even with no config keys).
func TestPipelineFailureReleasesUploadWg(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{}

	dir := t.TempDir()

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "alice"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	pq := NewPipelineQueue(ch)

	pq.EnqueueFile(filepath.Join(dir, "alice_2025-01-01_12-00-00.mp4"))
	pq.Stop() // drains the queue: the worker must fully process + Done() the pipeline

	done := make(chan struct{})
	go func() {
		ch.UploadWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("UploadWg never reached zero after a failed pipeline — token leaked")
	}
}

// TestScheduleRetryKeepsRecording verifies scheduleRetry never touches the
// recording on disk: a transient failure must leave the file available for the
// retry, and cancelling the retry (Stop) must not delete it either.
func TestScheduleRetryKeepsRecording(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "alice_2025-01-01_12-00-00.mp4")
	if err := os.WriteFile(path, []byte("recording"), 0o666); err != nil {
		t.Fatalf("write: %v", err)
	}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "alice"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	pq := NewPipelineQueue(ch)
	p := &Pipeline{
		FileHash: "hash-1",
		FilePath: path,
		Filename: filepath.Base(path),
		Username: "alice",
		Retries:  1,
		Links:    map[string]string{},
	}

	if !pq.scheduleRetry(p) {
		t.Fatal("scheduleRetry returned false for an existing recording")
	}
	if !p.retried {
		t.Fatal("scheduleRetry did not mark the pipeline as retried")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("recording was removed by scheduleRetry: %v", err)
	}

	// Stopping the queue cancels the pending retry without touching the file.
	pq.Stop()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("recording was removed when the retry was cancelled: %v", err)
	}

	// scheduleRetry must not count an UploadWg token: retries are free.
	done := make(chan struct{})
	go func() {
		ch.UploadWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduleRetry leaked an UploadWg token")
	}
}

// TestScheduleRetryDropsVanishedFile verifies scheduleRetry deletes the state
// row (not the file — there is no file left) when the recording vanished
// externally, so it never schedules a pointless retry loop.
func TestScheduleRetryDropsVanishedFile(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "alice"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	pq := NewPipelineQueue(ch)
	p := &Pipeline{
		FileHash: "hash-ghost",
		FilePath: filepath.Join(t.TempDir(), "ghost.mp4"),
		Filename: "ghost.mp4",
		Username: "alice",
		Retries:  1,
		Links:    map[string]string{},
	}

	if pq.scheduleRetry(p) {
		t.Fatal("scheduleRetry returned true for a vanished recording")
	}
	if p.retried {
		t.Fatal("scheduleRetry marked a vanished pipeline as retried")
	}
}
