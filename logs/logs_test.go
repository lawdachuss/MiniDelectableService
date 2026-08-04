package logs

import (
	"log"
	"os"
	"testing"
	"time"
)

func TestInstallCapturesOutput(t *testing.T) {
	// Reset package state so the test is idempotent even if a previous run
	// already installed the redirect.
	installMu.Lock()
	installed = false
	Default.mu.Lock()
	Default.entries = nil
	Default.next = 0
	Default.mu.Unlock()
	installMu.Unlock()

	Install()

	fmtOut(t, "hello from stdout")
	logFatalFree(t, "hello from stderr")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(Default.Entries()) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	entries := Default.Entries()
	if len(entries) < 2 {
		t.Fatalf("expected >=2 captured entries, got %d", len(entries))
	}
	var sawOut, sawErr bool
	for _, e := range entries {
		if e.Stream == "stdout" && e.Line == "hello from stdout" {
			sawOut = true
		}
		if e.Stream == "stderr" && len(e.Line) >= 17 && e.Line[len(e.Line)-17:] == "hello from stderr" {
			sawErr = true
		}
	}
	if !sawOut || !sawErr {
		t.Fatalf("did not capture both streams: out=%v err=%v entries=%+v", sawOut, sawErr, entries)
	}

	// Incremental poll: entries after index 0 should exclude the first line.
	after := Default.After(1)
	if len(after) >= len(entries) {
		t.Fatalf("After(1) should return fewer entries than full set: %d >= %d", len(after), len(entries))
	}
}

// fmtOut writes to os.Stdout (redirected after Install).
func fmtOut(t *testing.T, s string) {
	t.Helper()
	if _, err := os.Stdout.WriteString(s + "\n"); err != nil {
		t.Fatalf("stdout write: %v", err)
	}
}

// logFatalFree writes via the stdlib logger without calling Fatal.
func logFatalFree(t *testing.T, s string) {
	t.Helper()
	log.Printf("%s", s)
}
