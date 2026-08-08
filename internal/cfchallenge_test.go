package internal

import (
	"net/http"
	"testing"
)

// TestIsCloudflareChallenge verifies the shared challenge detector used by the
// GET and POST API paths: 429/503/410 statuses are always treated as a block
// (rate-limit pages), Cloudflare body markers match on any status, and a bare
// 403 without markers stays a private show (expected per-channel state).
func TestIsCloudflareChallenge(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"403 with challenge title", http.StatusForbidden, "<title>Just a moment...</title>", true},
		{"403 with ellipsis title", http.StatusForbidden, "<title>Just a moment…</title>", true},
		{"403 with attention required", http.StatusForbidden, "<title>Attention Required! | Cloudflare</title>", true},
		{"403 with cf-chl", http.StatusForbidden, `id="cf-chl-widget"`, true},
		{"403 with challenge-platform", http.StatusForbidden, "challenge-platform script", true},
		{"403 enable javascript", http.StatusForbidden, "Enable JavaScript and cookies to continue", true},
		{"bare 403 is a private show", http.StatusForbidden, "{\"room_status\":\"private\"}", false},
		{"429 always a block", http.StatusTooManyRequests, "", true},
		{"429 with challenge page", http.StatusTooManyRequests, "<title>Just a moment...</title>", true},
		{"503 always a block", http.StatusServiceUnavailable, "", true},
		{"410 always a block", http.StatusGone, "", true},
		{"404 is not a challenge", http.StatusNotFound, "not found", false},
		{"200 with challenge body", http.StatusOK, "<title>Just a moment...</title>", true},
		{"200 normal JSON", http.StatusOK, `{"hls_source":"https://x"}}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCloudflareChallenge(tt.status, tt.body); got != tt.want {
				t.Errorf("IsCloudflareChallenge(%d, %q) = %v, want %v", tt.status, tt.body, got, tt.want)
			}
		})
	}
}
