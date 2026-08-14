package channel

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
)

// TestCleanupLockedLogsEndReason verifies that when a recording file is closed,
// cleanupLocked logs WHY it ended. Each production path sets closeReason before
// closing (offline in Monitor, session-expiry in RecordStream, rotation in
// handleSegmentForMonitor), and the queued-for-post-processing message must
// carry that reason through to the log channel.
func TestCleanupLockedLogsEndReason(t *testing.T) {
	cases := []struct {
		name   string
		reason string // closeReason as set by the production call sites
	}{
		{"offline", "channel went offline"},
		{"session expiry", "stream session expired (no new segments)"},
		{"rotation", "max duration or filesize reached"},
		{"unset defaults to unknown", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ch := &Channel{
				LogCh:  make(chan string, 256),
				Config: &entity.ChannelConfig{Username: "alice"},
			}

			// A real non-empty recording file, as if the stream wrote segments.
			path := filepath.Join(t.TempDir(), "alice_2026-08-14_10-00-00.mp4")
			if err := os.WriteFile(path, []byte("fake-mp4-bytes"), 0644); err != nil {
				t.Fatalf("write recording: %v", err)
			}
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatalf("open recording: %v", err)
			}
			ch.File = f
			ch.Duration = 30 * 60 // 30 minutes of recorded stream
			ch.Filesize = 4096

			if c.reason != "" {
				ch.setCloseReason(c.reason)
			}

			// cleanupLocked is called with fileMu held in production; mirror that.
			ch.fileMu.Lock()
			err = ch.cleanupLocked()
			ch.fileMu.Unlock()
			if err != nil {
				t.Fatalf("cleanupLocked: %v", err)
			}

			// The non-empty file must be queued for post-processing.
			if len(ch.pendingFiles) != 1 {
				t.Fatalf("pendingFiles = %d, want 1", len(ch.pendingFiles))
			}
			if ch.pendingFiles[0].videoPath != path {
				t.Errorf("queued path = %q, want %q", ch.pendingFiles[0].videoPath, path)
			}

			// The reason is consumed after logging so the next file starts fresh.
			ch.fileMu.RLock()
			gotReason := ch.closeReason
			ch.fileMu.RUnlock()
			if gotReason != "" {
				t.Errorf("closeReason after cleanup = %q, want empty (consumed)", gotReason)
			}

			// The log message must carry the end reason.
			select {
			case msg := <-ch.LogCh:
				wantReason := c.reason
				if wantReason == "" {
					wantReason = "unknown"
				}
				if !strings.Contains(msg, "ended: "+wantReason) {
					t.Errorf("log message %q does not contain end reason %q", msg, wantReason)
				}
				if !strings.Contains(msg, "duration: 0:30:00") {
					t.Errorf("log message %q missing recorded duration", msg)
				}
			default:
				t.Fatal("no log message emitted for queued file")
			}
		})
	}
}

// TestCleanupLockedEmptyFileNoReasonLog verifies the empty-file path: a
// recording that never received segments is removed (not queued) and no
// post-processing log is emitted.
func TestCleanupLockedEmptyFileNoReasonLog(t *testing.T) {
	ch := &Channel{
		LogCh:  make(chan string, 256),
		Config: &entity.ChannelConfig{Username: "alice"},
	}
	ch.setCloseReason("channel went offline")

	path := filepath.Join(t.TempDir(), "alice_2026-08-14_10-00-00.mp4")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("write empty recording: %v", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open recording: %v", err)
	}
	ch.File = f

	ch.fileMu.Lock()
	err = ch.cleanupLocked()
	ch.fileMu.Unlock()
	if err != nil {
		t.Fatalf("cleanupLocked: %v", err)
	}

	if len(ch.pendingFiles) != 0 {
		t.Fatalf("pendingFiles = %d, want 0 (empty file must not be queued)", len(ch.pendingFiles))
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("empty file still exists after cleanup: %v", err)
	}
	select {
	case msg := <-ch.LogCh:
		t.Errorf("unexpected log for empty file: %q", msg)
	default:
	}
}
