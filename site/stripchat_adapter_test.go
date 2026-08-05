package site

import (
	"encoding/json"
	"testing"
)

// TestParseCamObjectForm verifies the live-model object shape parses fully.
func TestParseCamObjectForm(t *testing.T) {
	raw := json.RawMessage(`{"streamName":"abc_123","isCamActive":true,"viewServers":{"flashphoner-hls":"srv1"},"broadcastSettings":{"broadcastType":"public"},"topic":"#fun night"}`)
	cam := parseCam(raw)
	if !cam.IsCamActive || cam.StreamName != "abc_123" {
		t.Fatalf("unexpected cam: %+v", cam)
	}
	if cam.ViewServers["flashphoner-hls"] != "srv1" {
		t.Fatalf("viewServers not parsed: %+v", cam.ViewServers)
	}
	if cam.Topic != "#fun night" {
		t.Fatalf("topic not parsed: %+v", cam)
	}
}

// TestParseCamArrayForm verifies the idle/offline array shape ([]) yields an
// inactive cam instead of a parse error, and that null/missing are safe.
func TestParseCamArrayForm(t *testing.T) {
	if cam := parseCam(json.RawMessage(`[]`)); cam.IsCamActive {
		t.Fatalf("empty array must yield inactive cam, got %+v", cam)
	}
	if cam := parseCam(json.RawMessage(`null`)); cam.IsCamActive {
		t.Fatalf("null must yield inactive cam, got %+v", cam)
	}
	if cam := parseCam(nil); cam.IsCamActive {
		t.Fatalf("missing cam must yield inactive cam, got %+v", cam)
	}
}

// TestParseCamArrayWithPayload verifies an array of cam payloads falls back to
// the first element.
func TestParseCamArrayWithPayload(t *testing.T) {
	raw := json.RawMessage(`[{"streamName":"x","isCamActive":true}]`)
	cam := parseCam(raw)
	if !cam.IsCamActive || cam.StreamName != "x" {
		t.Fatalf("first array element should win, got %+v", cam)
	}
}

// TestCamResponseUnmarshalWithArrayCam is the exact production failure: the
// Stripchat API now returns cam:[] for idle models, which used to abort the
// whole response unmarshal (\"cannot unmarshal array into Go struct field
// camResponse.cam\"). The response must now decode and keep the user data.
func TestCamResponseUnmarshalWithArrayCam(t *testing.T) {
	var resp camResponse
	if err := json.Unmarshal([]byte(`{"cam":[],"user":{"user":{"username":"m","isOnline":false,"status":"offline"}}}`), &resp); err != nil {
		t.Fatalf("unmarshal with array cam failed: %v", err)
	}
	if resp.User.User.Username != "m" || resp.User.User.Status != "offline" {
		t.Fatalf("user data not parsed: %+v", resp.User.User)
	}
}

// TestCamResponseUnmarshalWithObjectCam verifies the live shape still parses.
func TestCamResponseUnmarshalWithObjectCam(t *testing.T) {
	var resp camResponse
	if err := json.Unmarshal([]byte(`{"cam":{"streamName":"abc","isCamActive":true},"user":{"user":{"username":"m","isOnline":true}}}`), &resp); err != nil {
		t.Fatalf("unmarshal with object cam failed: %v", err)
	}
	cam := parseCam(resp.Cam)
	if !cam.IsCamActive || cam.StreamName != "abc" {
		t.Fatalf("object cam not parsed: %+v", cam)
	}
}
