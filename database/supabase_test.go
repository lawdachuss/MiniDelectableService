package database

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// reqRecord captures one request the fake Supabase received.
type reqRecord struct {
	method string
	path   string // raw RequestURI (escaped), what actually hits the proxy
	body   string
}

// fakeSupabase is an in-memory stand-in for the PostgREST API. GET requests
// return `limit` fabricated channel rows; PATCH requests echo back the rows
// matched by username=in.(...) so the client's decode path is exercised.
type fakeSupabase struct {
	mu        sync.Mutex
	reqs      []reqRecord
	failPatch int // when > 0, the Nth PATCH (1-based) responds HTTP 400
}

func (f *fakeSupabase) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	f.mu.Lock()
	f.reqs = append(f.reqs, reqRecord{method: r.Method, path: r.URL.RequestURI(), body: string(body)})
	patchNo := 0
	for _, rec := range f.reqs {
		if rec.method == "PATCH" {
			patchNo++
		}
	}
	shouldFail := r.Method == "PATCH" && f.failPatch > 0 && patchNo == f.failPatch
	f.mu.Unlock()

	if shouldFail {
		// Plain 400 — non-retryable in requestWithRetry, so the test stays fast.
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"message":"boom"}`)
		return
	}

	q := r.URL.Query()
	switch r.Method {
	case "GET":
		limit := 0
		if v := q.Get("limit"); v != "" {
			fmt.Sscanf(v, "%d", &limit)
		}
		if limit > 100 {
			limit = 100 // keep the fake cheap for limit=50000 fetches
		}
		rows := make([]ChannelAssignment, 0, limit)
		for i := 0; i < limit; i++ {
			rows = append(rows, ChannelAssignment{Username: fmt.Sprintf("u%03d", i), Site: "chaturbate"})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rows)
	case "PATCH":
		// Return the rows matched by username=in.(...) when the filter is present.
		var rows []ChannelAssignment
		if v := q.Get("username"); strings.HasPrefix(v, "in.(") && strings.HasSuffix(v, ")") {
			inner := v[len("in.(") : len(v)-1]
			if inner != "" {
				for _, name := range strings.Split(inner, ",") {
					rows = append(rows, ChannelAssignment{Username: name, Site: "chaturbate"})
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rows)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func newTestClient(t *testing.T, fake *fakeSupabase) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	return &Client{URL: srv.URL, APIKey: "test-key", client: srv.Client()}
}

func (f *fakeSupabase) patchRequests() []reqRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []reqRecord
	for _, rec := range f.reqs {
		if rec.method == "PATCH" {
			out = append(out, rec)
		}
	}
	return out
}

// usernamesFromInFilter extracts the usernames carried by a username=in.(...)
// filter in a request path.
func usernamesFromInFilter(path string) []string {
	idx := strings.Index(path, "?")
	if idx < 0 {
		return nil
	}
	q, err := url.ParseQuery(path[idx+1:])
	if err != nil {
		return nil
	}
	v := q.Get("username")
	if !strings.HasPrefix(v, "in.(") || !strings.HasSuffix(v, ")") {
		return nil
	}
	inner := v[len("in.(") : len(v)-1]
	if inner == "" {
		return nil
	}
	return strings.Split(inner, ",")
}

func makePairs(n int) [][2]string {
	pairs := make([][2]string, n)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("model_%03d", i), "chaturbate"}
	}
	return pairs
}

// maxFilterURLLen is a generous bound: the real proxy limit is ~8KB, so any
// URL the client builds must stay far under it (a regression here would have
// shipped the fleet-wide HTTP 414 deadlock again).
const maxFilterURLLen = 3000

// ============================================================================
// ReleaseExcessOfflineChannels / ReleaseExcessChannels
// ============================================================================

func TestReleaseNodeOfflineChannels(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	n, err := c.ReleaseNodeOfflineChannels("node-18", nil)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if n == 0 {
		t.Fatal("expected a non-zero release count from the fake GET")
	}

	// Exactly one GET (count) + one PATCH (release), and the PATCH filter is a
	// tiny filter-based URL — no giant username=in.(...) list.
	var getPath, patchPath string
	for _, rec := range fake.reqs {
		if rec.method == "GET" {
			getPath = rec.path
		} else if rec.method == "PATCH" {
			patchPath = rec.path
		}
	}
	if getPath == "" || patchPath == "" {
		t.Fatalf("expected one GET and one PATCH, got GET=%q PATCH=%q", getPath, patchPath)
	}
	if strings.Contains(patchPath, "username=in.(") {
		t.Errorf("release PATCH must not carry a username in-list: %s", patchPath)
	}
	if len(patchPath) > maxFilterURLLen {
		t.Errorf("release PATCH URL is %d bytes (> %d, 414 risk)", len(patchPath), maxFilterURLLen)
	}
	if strings.Contains(patchPath, "select=") {
		t.Errorf("release PATCH should not carry select=, got: %s", patchPath)
	}
	for _, want := range []string{"assigned_node=eq.node-18", "is_live=eq.false", "status=neq.recording"} {
		if !strings.Contains(patchPath, want) {
			t.Errorf("release PATCH missing %q: %s", want, patchPath)
		}
	}
}

func TestReleaseNodeOfflineChannelsExcludesPaused(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	if _, err := c.ReleaseNodeOfflineChannels("node-18", []string{"paused_user"}); err != nil {
		t.Fatalf("release: %v", err)
	}

	for _, rec := range fake.reqs {
		if rec.method == "PATCH" {
			if !strings.Contains(rec.path, "username=not.in.(paused_user)") {
				t.Errorf("release PATCH must exclude paused usernames, got: %s", rec.path)
			}
		}
	}
}

func TestReleaseExcessOfflineChannelsChunked(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	const limit = 100
	released, err := c.ReleaseExcessOfflineChannels("node-18", limit)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if len(released) != limit {
		t.Fatalf("released %d rows, want %d", len(released), limit)
	}

	patchReqs := fake.patchRequests()
	if want := (limit + releaseBatchSize - 1) / releaseBatchSize; len(patchReqs) != want {
		t.Fatalf("PATCH count = %d, want %d (chunked)", len(patchReqs), want)
	}

	seen := map[string]bool{}
	for i, rec := range patchReqs {
		if len(rec.path) > maxFilterURLLen {
			t.Errorf("PATCH %d URL is %d bytes (> %d, 414 risk): %s", i, len(rec.path), maxFilterURLLen, rec.path)
		}
		names := usernamesFromInFilter(rec.path)
		if len(names) > releaseBatchSize {
			t.Errorf("PATCH %d carries %d usernames, batch cap is %d", i, len(names), releaseBatchSize)
		}
		for _, n := range names {
			if seen[n] {
				t.Errorf("username %s released more than once", n)
			}
			seen[n] = true
		}
	}
	if len(seen) != limit {
		t.Errorf("distinct released usernames = %d, want %d", len(seen), limit)
	}
}

func TestReleaseExcessOfflineChannelsSingleBatch(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	released, err := c.ReleaseExcessOfflineChannels("node-1", 20)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if len(released) != 20 {
		t.Fatalf("released %d rows, want 20", len(released))
	}
	if got := len(fake.patchRequests()); got != 1 {
		t.Fatalf("PATCH count = %d, want 1 (fits in one batch)", got)
	}
}

func TestReleaseExcessOfflineChannelsZeroLimit(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	released, err := c.ReleaseExcessOfflineChannels("node-1", 0)
	if err != nil || released != nil {
		t.Fatalf("expected (nil, nil), got (released=%v, err=%v)", released, err)
	}
	if len(fake.reqs) != 0 {
		t.Fatalf("expected no requests for limit 0, got %d", len(fake.reqs))
	}
}

func TestReleaseExcessOfflineChannelsPartialFailure(t *testing.T) {
	fake := &fakeSupabase{failPatch: 2} // second batch fails
	c := newTestClient(t, fake)

	released, err := c.ReleaseExcessOfflineChannels("node-18", 100)
	if err == nil {
		t.Fatal("expected an error when a batch fails")
	}
	if !strings.Contains(err.Error(), "batch 2") {
		t.Errorf("error should name the failed batch, got: %v", err)
	}
	if len(released) != releaseBatchSize {
		t.Fatalf("released %d rows on partial failure, want %d (only the first chunk)", len(released), releaseBatchSize)
	}
}

func TestReleaseExcessChannelsChunked(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	// 60 offline + 40 online = 100 targets → 3 PATCH batches.
	released, err := c.ReleaseExcessChannels("node-18", 100)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if len(released) != 100 {
		t.Fatalf("released %d rows, want 100", len(released))
	}
	patchReqs := fake.patchRequests()
	if want := (100 + releaseBatchSize - 1) / releaseBatchSize; len(patchReqs) != want {
		t.Fatalf("PATCH count = %d, want %d", len(patchReqs), want)
	}
	for i, rec := range patchReqs {
		if len(rec.path) > maxFilterURLLen {
			t.Errorf("PATCH %d URL is %d bytes (> %d, 414 risk)", i, len(rec.path), maxFilterURLLen)
		}
	}
}

// ============================================================================
// SetChannelsLive / SetChannelsNotLive
// ============================================================================

func TestSetChannelsLiveChunked(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	const pairs = 65 // 65 / 30 → 3 batches
	if err := c.SetChannelsLive(makePairs(pairs)); err != nil {
		t.Fatalf("SetChannelsLive: %v", err)
	}

	patchReqs := fake.patchRequests()
	if want := (pairs + livenessBatchSize - 1) / livenessBatchSize; len(patchReqs) != want {
		t.Fatalf("PATCH count = %d, want %d", len(patchReqs), want)
	}
	for i, rec := range patchReqs {
		if len(rec.path) > maxFilterURLLen {
			t.Errorf("PATCH %d URL is %d bytes (> %d, 414 risk): %s", i, len(rec.path), maxFilterURLLen, rec.path)
		}
		if !strings.Contains(rec.path, "or=") {
			t.Errorf("PATCH %d missing or= filter: %s", i, rec.path)
		}
	}
}

func TestSetChannelsNotLiveClearsThenSets(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	const pairs = 65
	if err := c.SetChannelsNotLive(makePairs(pairs)); err != nil {
		t.Fatalf("SetChannelsNotLive: %v", err)
	}

	patchReqs := fake.patchRequests()
	// 1 clear-all PATCH + ceil(65/30) set-live PATCHes.
	if want := 1 + (pairs+livenessBatchSize-1)/livenessBatchSize; len(patchReqs) != want {
		t.Fatalf("PATCH count = %d, want %d (1 clear + %d set-live)", len(patchReqs), want, (pairs+livenessBatchSize-1)/livenessBatchSize)
	}

	// First PATCH must be the tiny clear-all (no or= filter, no giant not.or).
	if strings.Contains(patchReqs[0].path, "or=") {
		t.Errorf("clear-all PATCH should have no or= filter, got: %s", patchReqs[0].path)
	}
	if len(patchReqs[0].path) > maxFilterURLLen {
		t.Errorf("clear-all PATCH URL is %d bytes (> %d)", len(patchReqs[0].path), maxFilterURLLen)
	}

	for i := 1; i < len(patchReqs); i++ {
		if !strings.Contains(patchReqs[i].path, "or=") {
			t.Errorf("set-live PATCH %d missing or= filter: %s", i, patchReqs[i].path)
		}
		if len(patchReqs[i].path) > maxFilterURLLen {
			t.Errorf("set-live PATCH %d URL is %d bytes (> %d, 414 risk)", i, len(patchReqs[i].path), maxFilterURLLen)
		}
	}
}

func TestSetChannelsNotLiveEmptyClearsAll(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	if err := c.SetChannelsNotLive(nil); err != nil {
		t.Fatalf("SetChannelsNotLive(nil): %v", err)
	}
	patchReqs := fake.patchRequests()
	if len(patchReqs) != 1 {
		t.Fatalf("PATCH count = %d, want 1 (clear-all only)", len(patchReqs))
	}
	if strings.Contains(patchReqs[0].path, "or=") {
		t.Errorf("clear-all PATCH should have no or= filter, got: %s", patchReqs[0].path)
	}
}
