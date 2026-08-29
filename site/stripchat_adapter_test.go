package site

import (
	"encoding/json"
	"testing"
)

// TestParsePageState verifies the live-model page state JSON parses fully.
func TestParsePageState(t *testing.T) {
	raw := `{
		"viewCamBase": {
			"model": {
				"username": "Kira_Queen",
				"status": "public",
				"isLive": true,
				"isOnline": true,
				"broadcastGender": "female",
				"previewUrlThumbBig": "https://img.doppiocdn.org/preview.jpg",
				"snapshotTimestamp": 1788009810
			}
		},
		"viewCam": {
			"streamName": "abc_123",
			"isCamActive": true,
			"viewServers": {"flashphoner-hls": "srv1"},
			"topic": "#fun night"
		}
	}`

	var state scPageState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatalf("unmarshal page state failed: %v", err)
	}

	m := state.ViewCamBase.Model
	cam := state.ViewCam

	if m.Username != "Kira_Queen" || !m.IsLive || m.Status != "public" {
		t.Fatalf("unexpected model data: %+v", m)
	}
	if !cam.IsCamActive || cam.StreamName != "abc_123" {
		t.Fatalf("unexpected cam data: %+v", cam)
	}
	if cam.ViewServers["flashphoner-hls"] != "srv1" {
		t.Fatalf("viewServers not parsed: %+v", cam.ViewServers)
	}
}

// TestFindJSONObjectEnd tests JSON object boundary detection.
func TestFindJSONObjectEnd(t *testing.T) {
	s := `window.__PRELOADED_STATE__ = {"a": {"b": 1}, "c": "}"}; window.other = 1;`
	pos := 29 // start of {
	end := findJSONObjectEnd(s, pos)
	if end < 0 {
		t.Fatalf("expected to find end of JSON object")
	}
	jsonStr := s[pos:end]
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
		t.Fatalf("extracted JSON invalid: %s (err: %v)", jsonStr, err)
	}
}

// TestMapGender verifies gender string normalization.
func TestMapGender(t *testing.T) {
	cases := map[string]string{
		"female": "f",
		"male":   "m",
		"couple": "c",
		"trans":  "t",
		"other":  "other",
	}
	for in, want := range cases {
		if got := mapGender(in); got != want {
			t.Errorf("mapGender(%q) = %q, want %q", in, got, want)
		}
	}
}

