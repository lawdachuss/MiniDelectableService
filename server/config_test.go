package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
)

// fakeSupabase is an in-memory app_settings store that mimics the Supabase
// REST endpoints saveJSONSetting / loadJSONSetting rely on (GET/POST/PATCH).
type fakeSupabase struct {
	mu sync.Mutex
	m  map[string]json.RawMessage
}

func newFakeSupabase() *fakeSupabase { return &fakeSupabase{m: map[string]json.RawMessage{}} }

// keyFromQuery extracts the key from "?key=eq.<key>..." (RawQuery form).
func (f *fakeSupabase) keyFromQuery(rawQuery string) string {
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ""
	}
	k := q.Get("key")
	if len(k) > 3 && k[:3] == "eq." {
		return k[3:]
	}
	return k
}

func (f *fakeSupabase) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		switch r.Method {
		case http.MethodGet:
			k := f.keyFromQuery(r.URL.RawQuery)
			if v, ok := f.m[k]; ok {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`[{"key":` + mustJSON(k) + `,"value":` + string(v) + `}]`))
				return
			}
			w.Write([]byte(`[]`))
		case http.MethodPost:
			var body struct {
				Key   string          `json:"key"`
				Value json.RawMessage `json:"value"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.m[body.Key] = body.Value
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"key":` + mustJSON(body.Key) + `,"value":` + string(body.Value) + `}]`))
		case http.MethodPatch:
			var body struct {
				Value json.RawMessage `json:"value"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			k := f.keyFromQuery(r.URL.RawQuery)
			f.m[k] = body.Value
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"key":` + mustJSON(k) + `,"value":` + string(body.Value) + `}]`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return mux
}

func mustJSON(s string) string { b, _ := json.Marshal(s); return string(b) }

// TestPerNodeCookieSaveLoadRoundTrip verifies the end-to-end split: cookies go
// to the per-node key (dvr_settings:<node_id>), upload creds stay global, and
// LoadSettings restores both from the right keys.
func TestPerNodeCookieSaveLoadRoundTrip(t *testing.T) {
	ts := httptest.NewServer(newFakeSupabase().handler())
	defer ts.Close()

	oldID := os.Getenv("NODE_ID")
	os.Setenv("NODE_ID", "node-99")
	defer os.Setenv("NODE_ID", oldID)

	oldConfig := Config
	Config = &entity.Config{
		SupabaseURL:     ts.URL,
		SupabaseAPIKey:  "test-key",
		Cookies:         "cf_clearance=NODE_COOKIE; csrftoken=tok",
		CfClearance:     "NODE_COOKIE",
		Csrftoken:       "tok",
		UserAgent:       "UA",
		VoeSXAPIKey:     "voe-key",
		StreamtapeKey:   "st-key",
		StreamtapeLogin: "st-login",
		AffiliateWM:     "wm-123",
	}
	defer func() { Config = oldConfig }()

	if err := SaveSettings(); err != nil {
		t.Fatalf("SaveSettings: %v", err)
	}

	// Per-node key must hold cookies, never upload creds.
	nodeKey := CookieSettingsKey()
	if nodeKey != "dvr_settings:node-99" {
		t.Fatalf("CookieSettingsKey = %q, want dvr_settings:node-99", nodeKey)
	}
	perNode := LoadSettingsFromDBKey(nodeKey)
	if perNode == nil {
		t.Fatalf("per-node key %q not written", nodeKey)
	}
	var node persistedSettings
	if err := json.Unmarshal(perNode, &node); err != nil {
		t.Fatalf("unmarshal per-node blob: %v", err)
	}
	if node.Cookies != "cf_clearance=NODE_COOKIE; csrftoken=tok" {
		t.Errorf("per-node cookies = %q", node.Cookies)
	}
	if node.CfClearance != "NODE_COOKIE" {
		t.Errorf("per-node cf_clearance = %q", node.CfClearance)
	}
	if node.UserAgent != "UA" {
		t.Errorf("per-node user_agent = %q", node.UserAgent)
	}
	if node.VoeSXAPIKey != "" || node.StreamtapeKey != "" || node.AffiliateWM != "" {
		t.Errorf("per-node blob leaked upload creds: %+v", node)
	}

	// Global key must hold creds, never cookies.
	global := LoadSettingsFromDB()
	if global == nil {
		t.Fatalf("global dvr_settings not written")
	}
	var g persistedSettings
	if err := json.Unmarshal(global, &g); err != nil {
		t.Fatalf("unmarshal global blob: %v", err)
	}
	if g.VoeSXAPIKey != "voe-key" || g.StreamtapeKey != "st-key" {
		t.Errorf("global creds = voe:%q st:%q", g.VoeSXAPIKey, g.StreamtapeKey)
	}
	if g.AffiliateWM != "wm-123" {
		t.Errorf("global creds affiliate_wm = %q, want wm-123", g.AffiliateWM)
	}
	if g.Cookies != "" || g.CfClearance != "" {
		t.Errorf("global key leaked cookies: %+v", g)
	}

	// Reset cookies in memory, then LoadSettings from DB and confirm restore.
	Config.Cookies = ""
	Config.CfClearance = ""
	Config.Csrftoken = ""
	Config.VoeSXAPIKey = ""
	Config.StreamtapeKey = ""
	Config.AffiliateWM = ""
	if err := LoadSettings(); err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if Config.Cookies != "cf_clearance=NODE_COOKIE; csrftoken=tok" {
		t.Errorf("after load cookies = %q", Config.Cookies)
	}
	if Config.VoeSXAPIKey != "voe-key" {
		t.Errorf("after load voesx = %q", Config.VoeSXAPIKey)
	}
	if Config.StreamtapeKey != "st-key" {
		t.Errorf("after load streamtape = %q", Config.StreamtapeKey)
	}
	if Config.AffiliateWM != "wm-123" {
		t.Errorf("after load affiliate_wm = %q, want wm-123", Config.AffiliateWM)
	}
}

// TestStaggerSessionDurationDeterministic verifies the offset is stable for a
// given node ID and stays within [0, spread).
func TestStaggerSessionDurationDeterministic(t *testing.T) {
	t.Setenv("NODE_ID", "node-7")
	t.Setenv("GITHUB_RUN_ID", "")
	base := 320 * time.Minute
	a := staggerSessionDuration(base)
	b := staggerSessionDuration(base)
	if a != b {
		t.Fatalf("stagger not deterministic: %s vs %s", a, b)
	}
	if a < base || a >= base+permanentSessionStaggerSpread {
		t.Fatalf("staggered %s out of [%s, %s)", a, base, base+permanentSessionStaggerSpread)
	}
}

// TestStaggerSessionDurationSpreadsNodes verifies distinct node IDs get spread
// across the stagger range instead of all landing on the same offset.
func TestStaggerSessionDurationSpreadsNodes(t *testing.T) {
	t.Setenv("GITHUB_RUN_ID", "")
	base := 320 * time.Minute
	seen := map[time.Duration]bool{}
	for i := 1; i <= 24; i++ {
		t.Setenv("NODE_ID", fmt.Sprintf("node-%d", i))
		seen[staggerSessionDuration(base)-base] = true
	}
	if len(seen) < 12 {
		t.Errorf("expected >=12 distinct offsets across 24 node IDs, got %d", len(seen))
	}
}

// TestStaggerSessionDurationCI verifies CI runners get only a tiny offset so
// the staggered duration never crosses the 348m self-cancel / 360m hard kill.
func TestStaggerSessionDurationCI(t *testing.T) {
	t.Setenv("NODE_ID", "node-7")
	t.Setenv("GITHUB_RUN_ID", "12345")
	base := 335 * time.Minute
	d := staggerSessionDuration(base)
	if d-base >= ciSessionStaggerSpread {
		t.Fatalf("CI offset %s exceeds spread %s", d-base, ciSessionStaggerSpread)
	}
	if d > 348*time.Minute {
		t.Fatalf("CI staggered duration %s crosses the 348m self-cancel buffer", d)
	}
}

// TestApplyCentralSessionDurationStaggers verifies the resolved duration (from
// env, central, or CI fallback) gets staggered and the string field stays
// consistent with the parsed value.
func TestApplyCentralSessionDurationStaggers(t *testing.T) {
	t.Setenv("NODE_ID", "node-3")
	t.Setenv("GITHUB_RUN_ID", "")
	oldConfig := Config
	defer func() { Config = oldConfig }()
	Config = &entity.Config{SessionDuration: "5h20m", SessionDurationParsed: 320 * time.Minute}

	ApplyCentralSessionDuration()

	if Config.SessionDurationParsed <= 320*time.Minute {
		t.Fatalf("expected staggered duration > 5h20m, got %s", Config.SessionDurationParsed)
	}
	if Config.SessionDurationParsed >= 320*time.Minute+permanentSessionStaggerSpread {
		t.Fatalf("staggered duration %s exceeds base+spread", Config.SessionDurationParsed)
	}
	if got, _ := time.ParseDuration(Config.SessionDuration); got != Config.SessionDurationParsed {
		t.Fatalf("SessionDuration string %q != parsed %s", Config.SessionDuration, Config.SessionDurationParsed)
	}
}

// TestApplyCentralSessionDurationContinuous verifies a node with no session
// duration stays continuous (0) — never staggered into a deadline.
func TestApplyCentralSessionDurationContinuous(t *testing.T) {
	t.Setenv("NODE_ID", "node-3")
	t.Setenv("GITHUB_RUN_ID", "")
	oldConfig := Config
	defer func() { Config = oldConfig }()
	Config = &entity.Config{}

	ApplyCentralSessionDuration()

	if Config.SessionDurationParsed != 0 {
		t.Fatalf("no-duration node got %s, want 0 (continuous)", Config.SessionDurationParsed)
	}
}

// TestSyncNodeEnvironmentAfterDotenv proves NodeID()/CookieSettingsKey() pick
// up NODE_ID even though package init() runs before .env is loaded.
func TestSyncNodeEnvironmentAfterEnv(t *testing.T) {
	oldID := os.Getenv("NODE_ID")
	oldRepo := os.Getenv("GITHUB_REPOSITORY")

	os.Setenv("NODE_ID", "")
	os.Setenv("GITHUB_REPOSITORY", "lawdachuss/node-12")
	syncNodeEnvironment()
	if got := NodeID(); got != "12" {
		t.Errorf("NodeID after .env-sync = %q, want 12", got)
	}
	if got := CookieSettingsKey(); got != "dvr_settings:12" {
		t.Errorf("CookieSettingsKey = %q, want dvr_settings:12", got)
	}

	os.Setenv("NODE_ID", oldID)
	os.Setenv("GITHUB_REPOSITORY", oldRepo)
	syncNodeEnvironment()
}
