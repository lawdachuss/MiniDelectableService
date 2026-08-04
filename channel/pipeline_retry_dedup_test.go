package channel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// TestPipelineRetryRoundTrip verifies that the Retries counter survives a
// toDBState -> pipelineFromDBState round trip, which is what lets ResumePending
// observe how many times a failed pipeline has already been retried.
//
// Regression for the bug where failed pipelines retried forever on every
// restart with no bound — there was no retry counter persisted at all.
func TestPipelineRetryRoundTrip(t *testing.T) {
	p := &Pipeline{
		FileHash:     "abc123",
		FilePath:     "/tmp/x.mp4",
		Filename:     "alice_2025-01-01_12-00-00.mp4",
		Username:     "alice",
		CurrentStage: StageThumbnailUpload,
		Failed:       true,
		LastError:    "boom",
		Retries:      3,
		Links:        map[string]string{},
	}

	state := p.toDBState()
	if state.Retries != 3 {
		t.Fatalf("toDBState: Retries = %d, want 3", state.Retries)
	}

	restored := pipelineFromDBState(state)
	if restored.Retries != 3 {
		t.Fatalf("pipelineFromDBState: Retries = %d, want 3", restored.Retries)
	}
	if !restored.Failed || restored.LastError != "boom" {
		t.Fatalf("pipelineFromDBState: Failed=%v LastError=%q, want true/boom", restored.Failed, restored.LastError)
	}
}

// TestPipelineStateRetriesJSONField guards the JSON tag name so the Supabase
// column mapping stays stable ("retries").
func TestPipelineStateRetriesJSONField(t *testing.T) {
	s := database.PipelineState{Retries: 7}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"retries":7`) {
		t.Fatalf("expected JSON to contain \"retries\":7, got %s", b)
	}
}

// TestPipelineQueueContainsHash exercises the dedup helper directly.
func TestPipelineQueueContainsHash(t *testing.T) {
	pq := &PipelineQueue{}
	pq.pipelines = []*Pipeline{
		{FileHash: "hash-a"},
		{FileHash: "hash-b"},
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if !pq.containsHash("hash-a") {
		t.Error("containsHash(hash-a) = false, want true")
	}
	if !pq.containsHash("hash-b") {
		t.Error("containsHash(hash-b) = false, want true")
	}
	if pq.containsHash("hash-c") {
		t.Error("containsHash(hash-c) = true, want false")
	}
	if pq.containsHash("") {
		t.Error("containsHash('') = true, want false (empty hash never matches)")
	}
}

// TestPipelineQueueDedup verifies that EnqueueFile drops a duplicate when a
// pipeline for the same file hash is already queued.  Without this guard, two
// pipelines for one file would race on the same upload journal and double-add
// to UploadWg.
func TestPipelineQueueDedup(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{}

	dir := t.TempDir()
	path := filepath.Join(dir, "alice_2025-01-01_12-00-00.mp4")
	if err := os.WriteFile(path, []byte("video"), 0o666); err != nil {
		t.Fatalf("write: %v", err)
	}

	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "alice"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	pq := NewPipelineQueue(ch)

	// Enqueue twice; the second must be dropped as a duplicate.  Use the
	// monotonic accepted-counter rather than len(pipelines), because workers
	// may already have dequeued the first pipeline for processing.
	pq.EnqueueFile(path)
	pq.EnqueueFile(path)

	if got := pq.EnqueuedCount(); got != 1 {
		t.Fatalf("expected 1 accepted pipeline after duplicate enqueue, got %d", got)
	}
}
