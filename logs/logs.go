// Package logs captures all stdout/stderr output of the process into a
// bounded ring buffer and exposes it to the web UI (GET /api/logs, /logs).
//
// Install() redirects the process's stdout/stderr file descriptors through
// OS pipes, teeing every line both to their original destinations (terminal,
// CI log, redirected file) and into an in-memory ring buffer. It also
// re-points the stdlib log package at the redirected stderr so log.Printf /
// log.Fatal output is captured as well.
package logs

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// Entry is a single captured log line.
type Entry struct {
	Index  uint64    `json:"index"`
	Time   time.Time `json:"time"`
	Stream string    `json:"stream"`
	Line   string    `json:"line"`
}

// Buffer is a thread-safe bounded buffer of log entries. When full, the
// oldest entries are dropped so memory usage stays constant.
type Buffer struct {
	mu      sync.Mutex
	entries []Entry
	max     int
	next    uint64
}

// NewBuffer returns a buffer that keeps at most max entries.
func NewBuffer(max int) *Buffer {
	if max <= 0 {
		max = 5000
	}
	return &Buffer{max: max}
}

func (b *Buffer) add(stream string, p []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for _, raw := range bytes.Split(p, []byte{'\n'}) {
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		if len(b.entries) >= b.max {
			drop := len(b.entries) - b.max + 1
			b.entries = append([]Entry(nil), b.entries[drop:]...)
		}
		b.entries = append(b.entries, Entry{
			Index:  b.next,
			Time:   now,
			Stream: stream,
			Line:   string(raw),
		})
		b.next++
	}
}

// Write appends p to the buffer tagged as the "app" stream. It implements
// io.Writer so arbitrary code can feed the log viewer programmatically.
func (b *Buffer) Write(p []byte) (int, error) {
	b.add("app", p)
	return len(p), nil
}

// Entries returns a copy of all entries currently in the buffer.
func (b *Buffer) Entries() []Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Entry, len(b.entries))
	copy(out, b.entries)
	return out
}

// After returns the entries with Index >= after (useful for incremental polls).
func (b *Buffer) After(after uint64) []Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	idx := 0
	for idx < len(b.entries) && b.entries[idx].Index < after {
		idx++
	}
	out := make([]Entry, len(b.entries)-idx)
	copy(out, b.entries[idx:])
	return out
}

// Total returns the total number of entries ever written to the buffer.
func (b *Buffer) Total() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.next
}

// Default is the process-wide capture buffer exposed by /api/logs.
var Default = NewBuffer(5000)

var (
	installMu sync.Mutex
	installed bool
	origOut   = os.Stdout
	origErr   = os.Stderr
)

// Install redirects os.Stdout/os.Stderr through capture pipes so all process
// output is recorded in Default while still reaching the original sinks. It
// is idempotent and safe to call from multiple goroutines.
func Install() {
	installMu.Lock()
	defer installMu.Unlock()
	if installed {
		return
	}
	if r, w, err := os.Pipe(); err == nil {
		os.Stdout = w
		go teePipe(r, origOut, Default, "stdout")
	}
	if r, w, err := os.Pipe(); err == nil {
		os.Stderr = w
		go teePipe(r, origErr, Default, "stderr")
	}
	// The stdlib log package cached the original os.Stderr at init, so point
	// it at the redirected stderr explicitly to capture log.Printf output.
	log.SetOutput(os.Stderr)
	installed = true
}

// teePipe drains lines from the read end of a redirected pipe, recording each
// into the buffer (tagged with stream) and forwarding it to the original sink
// exactly once, complete lines at a time, to avoid interleaved writes.
func teePipe(r io.Reader, orig io.Writer, buf *Buffer, stream string) {
	br := bufio.NewReader(r)
	var pending []byte
	for {
		chunk, err := br.ReadBytes('\n')
		if len(chunk) > 0 {
			if bytes.HasSuffix(chunk, []byte{'\n'}) {
				buf.add(stream, chunk)
				line := append(append([]byte{}, pending...), chunk...)
				pending = pending[:0]
				orig.Write(line)
			} else {
				pending = append(pending, chunk...)
			}
		}
		if err != nil {
			if len(pending) > 0 {
				buf.add(stream, pending)
				orig.Write(pending)
			}
			return
		}
	}
}
