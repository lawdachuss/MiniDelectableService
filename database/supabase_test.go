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
	"time"
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

// ============================================================================
// ResetStaleRecordingAssignments (protected-owner exclusion)
// ============================================================================

// TestResetStaleRecordingAssignmentsExcludesProtectedNodes verifies that a
// stale recording marker whose owner is in protectedNodeIDs is never touched:
// the PATCH filter must exclude those nodes server-side, so a live node's
// in-progress recording cannot be unpinned and handed to a second node just
// because its assignment-sync heartbeat refresh lagged.
func TestResetStaleRecordingAssignmentsExcludesProtectedNodes(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	before := time.Now().UTC().Add(-2 * time.Minute)
	if _, err := c.ResetStaleRecordingAssignments(before, []string{"node-a", "node-b"}); err != nil {
		t.Fatalf("ResetStaleRecordingAssignments error: %v", err)
	}

	reqs := fake.patchRequests()
	if len(reqs) != 1 {
		t.Fatalf("got %d PATCH requests, want 1", len(reqs))
	}
	path := reqs[0].path
	if !strings.Contains(path, "status=eq.recording") {
		t.Fatalf("PATCH missing recording filter: %s", path)
	}
	if !strings.Contains(path, "last_heartbeat.is.null") {
		t.Fatalf("PATCH missing null-heartbeat clause: %s", path)
	}
	if !strings.Contains(path, "assigned_node=not.in.(node-a,node-b)") {
		t.Fatalf("PATCH missing protected-owner exclusion: %s", path)
	}
	if reqs[0].body != `{"status":"claimed"}` {
		t.Fatalf("PATCH body = %s, want {\"status\":\"claimed\"}", reqs[0].body)
	}
}

// TestResetStaleRecordingAssignmentsNoProtectedNodes verifies that with an
// empty protected list the reset keeps its original behaviour (no exclusion
// filter), so a genuinely dead runner's leftover marker is still cleaned up.
func TestResetStaleRecordingAssignmentsNoProtectedNodes(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	before := time.Now().UTC().Add(-2 * time.Minute)
	if _, err := c.ResetStaleRecordingAssignments(before, nil); err != nil {
		t.Fatalf("ResetStaleRecordingAssignments error: %v", err)
	}
	reqs := fake.patchRequests()
	if len(reqs) != 1 {
		t.Fatalf("got %d PATCH requests, want 1", len(reqs))
	}
	if strings.Contains(reqs[0].path, "assigned_node=") {
		t.Fatalf("empty protected list must not add an exclusion filter: %s", reqs[0].path)
	}
}

// TestResetStaleRecordingAssignmentsEmptyProtectedSameAsNil treats an empty
// (non-nil) protected list exactly like nil — the controller builds the list
// from a map that is often empty at cold start.
func TestResetStaleRecordingAssignmentsEmptyProtectedSameAsNil(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	before := time.Now().UTC().Add(-2 * time.Minute)
	if _, err := c.ResetStaleRecordingAssignments(before, []string{}); err != nil {
		t.Fatalf("ResetStaleRecordingAssignments error: %v", err)
	}
	reqs := fake.patchRequests()
	if len(reqs) != 1 {
		t.Fatalf("got %d PATCH requests, want 1", len(reqs))
	}
	if strings.Contains(reqs[0].path, "assigned_node=") {
		t.Fatalf("empty protected list must not add an exclusion filter: %s", reqs[0].path)
	}
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

func TestDeleteChannelsNotInChunked(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	// The fake serves 100 existing channels (u000..u099); keeping 2 means 98
	// must be deleted → ceil(98/40) = 3 DELETE batches.
	if err := c.DeleteChannelsNotIn([]string{"u000", "u001"}); err != nil {
		t.Fatalf("DeleteChannelsNotIn: %v", err)
	}

	var deletes []reqRecord
	for _, rec := range fake.reqs {
		if rec.method == "DELETE" {
			deletes = append(deletes, rec)
		}
	}
	if want := (98 + releaseBatchSize - 1) / releaseBatchSize; len(deletes) != want {
		t.Fatalf("DELETE count = %d, want %d", len(deletes), want)
	}
	for i, rec := range deletes {
		if len(rec.path) > maxFilterURLLen {
			t.Errorf("DELETE %d URL is %d bytes (> %d, 414 risk)", i, len(rec.path), maxFilterURLLen)
		}
		if strings.Contains(rec.path, "not.in.(") {
			t.Errorf("DELETE %d must delete by in.(toDelete), not not.in.(keep): %s", i, rec.path)
		}
		if !strings.Contains(rec.path, "username=in.(") {
			t.Errorf("DELETE %d missing username=in.(...) filter: %s", i, rec.path)
		}
	}
}

func TestDeleteChannelsNotInEmptyDeletesAll(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	if err := c.DeleteChannelsNotIn(nil); err != nil {
		t.Fatalf("DeleteChannelsNotIn(nil): %v", err)
	}
	// Empty keep-list = delete everything: exactly one filter-less DELETE.
	if len(fake.reqs) != 1 || fake.reqs[0].method != "DELETE" || fake.reqs[0].path != "/rest/v1/channels" {
		t.Errorf("expected exactly one DELETE to /rest/v1/channels, got %+v", fake.reqs)
	}
}

func TestDeleteChannelsNotInAllKeptNoDeletes(t *testing.T) {
	fake := &fakeSupabase{}
	c := newTestClient(t, fake)

	// Keep all 100 fake channels → nothing to delete.
	keep := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		keep = append(keep, fmt.Sprintf("u%03d", i))
	}
	if err := c.DeleteChannelsNotIn(keep); err != nil {
		t.Fatalf("DeleteChannelsNotIn: %v", err)
	}
	for _, rec := range fake.reqs {
		if rec.method == "DELETE" {
			t.Errorf("no channels are stale — got unexpected DELETE: %s", rec.path)
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

// ============================================================================
// TouchChannelRecordings (owned-but-idle last_recorded_at refresh)
// ============================================================================

// fakeTouchDB records touch RPC payloads and can simulate HTTP 204 (void RPC),
// a PGRST202 (RPC not in schema cache), or a plain error response.
type fakeTouchDB struct {
	mu       sync.Mutex
	reqs     []reqRecord
	respCode int
	respBody string
}

func (f *fakeTouchDB) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.reqs = append(f.reqs, reqRecord{method: r.Method, path: r.URL.RequestURI(), body: string(body)})
	code, respBody := f.respCode, f.respBody
	f.mu.Unlock()

	if code != 0 {
		w.WriteHeader(code)
		fmt.Fprint(w, respBody)
		return
	}
	// Default: behave like a void SECURITY DEFINER RPC (204 No Content).
	w.WriteHeader(http.StatusNoContent)
}

func newTouchTestClient(t *testing.T, fake *fakeTouchDB) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	return &Client{URL: srv.URL, APIKey: "test-key", client: srv.Client()}
}

// TestTouchChannelRecordingsPayloadShape verifies the RPC path, the node id,
// and that every (username, site) pair arrives in the JSON payload.
func TestTouchChannelRecordingsPayloadShape(t *testing.T) {
	fake := &fakeTouchDB{}
	c := newTouchTestClient(t, fake)

	channels := []ChannelAssignment{
		{Username: "alice", Site: "chaturbate"},
		{Username: "bob", Site: "stripchat"},
	}
	if err := c.TouchChannelRecordings("node-9", channels); err != nil {
		t.Fatalf("TouchChannelRecordings: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.reqs) != 1 {
		t.Fatalf("request count = %d, want 1", len(fake.reqs))
	}
	rec := fake.reqs[0]
	if rec.method != "POST" || !strings.HasPrefix(rec.path, "/rest/v1/rpc/touch_channel_recordings") {
		t.Fatalf("unexpected request: %s %s", rec.method, rec.path)
	}
	for _, want := range []string{`"p_node_id":"node-9"`, `"username":"alice"`, `"site":"chaturbate"`, `"username":"bob"`, `"site":"stripchat"`} {
		if !strings.Contains(rec.body, want) {
			t.Errorf("payload missing %s: %s", want, rec.body)
		}
	}
}

// TestTouchChannelRecordingsChunks verifies the 200-pair-per-RPC chunking.
func TestTouchChannelRecordingsChunks(t *testing.T) {
	fake := &fakeTouchDB{}
	c := newTouchTestClient(t, fake)

	const n = 450 // ceil(450/200) = 3 chunks
	channels := make([]ChannelAssignment, n)
	for i := range channels {
		channels[i] = ChannelAssignment{Username: fmt.Sprintf("u%03d", i), Site: "chaturbate"}
	}
	if err := c.TouchChannelRecordings("node-1", channels); err != nil {
		t.Fatalf("TouchChannelRecordings: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.reqs) != 3 {
		t.Fatalf("request count = %d, want 3 chunks", len(fake.reqs))
	}
}

// TestTouchChannelRecordingsEmptyNoRequest verifies an empty set is a no-op.
func TestTouchChannelRecordingsEmptyNoRequest(t *testing.T) {
	fake := &fakeTouchDB{}
	c := newTouchTestClient(t, fake)

	if err := c.TouchChannelRecordings("node-1", nil); err != nil {
		t.Fatalf("TouchChannelRecordings(nil): %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.reqs) != 0 {
		t.Fatalf("request count = %d, want 0", len(fake.reqs))
	}
}

// TestTouchChannelRecordings204 verifies the void-RPC (204 No Content) path
// decodes cleanly — the mark/reset RPCs return 204 in production.
func TestTouchChannelRecordings204(t *testing.T) {
	fake := &fakeTouchDB{respCode: http.StatusNoContent}
	c := newTouchTestClient(t, fake)

	if err := c.TouchChannelRecordings("node-1", []ChannelAssignment{{Username: "a", Site: "chaturbate"}}); err != nil {
		t.Fatalf("TouchChannelRecordings with 204: %v", err)
	}
}

// TestTouchChannelRecordingsPGRST202SkipsSilently verifies the degradation
// path: a project that has not applied the migration returns PGRST202 and the
// call must be swallowed — staleness visibility is best-effort.
func TestTouchChannelRecordingsPGRST202SkipsSilently(t *testing.T) {
	fake := &fakeTouchDB{
		respCode: http.StatusBadRequest,
		respBody: `{"code":"PGRST202","message":"Could not find the function"}`,
	}
	c := newTouchTestClient(t, fake)

	if err := c.TouchChannelRecordings("node-1", []ChannelAssignment{{Username: "a", Site: "chaturbate"}}); err != nil {
		t.Fatalf("PGRST202 must be swallowed, got: %v", err)
	}
}

// TestTouchChannelRecordingsErrorSurfaced verifies non-PGRST202 failures are
// returned so the caller can log them.
func TestTouchChannelRecordingsErrorSurfaced(t *testing.T) {
	fake := &fakeTouchDB{
		respCode: http.StatusBadRequest,
		respBody: `{"message":"boom"}`,
	}
	c := newTouchTestClient(t, fake)

	if err := c.TouchChannelRecordings("node-1", []ChannelAssignment{{Username: "a", Site: "chaturbate"}}); err == nil {
		t.Fatalf("expected an error for a plain 400")
	}
}
