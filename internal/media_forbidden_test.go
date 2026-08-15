package internal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// TestMedia403NotPrivateShow guards the classification split between CDN media
// requests and site API requests. A bare 403 from the CDN (HLS playlist,
// segment, thumbnail) is ambiguous — it can be a real private show OR an
// expired HLS session token — so the recorder must NOT treat it as a private
// show (which ended live recordings and benched the channel for the full
// offline interval). Only the site API may classify a 403 as a private show.
func TestMedia403NotPrivateShow(t *testing.T) {
	// SetRequestHeaders reads server.Config for referer/UA; make a minimal one.
	old := server.Config
	server.Config = &entity.Config{Domain: "https://www.cb.xxx/"}
	defer func() { server.Config = old }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("{\"room_status\":\"private\"}"))
	}))
	defer srv.Close()

	ctx := context.Background()

	// CDN media client: bare 403 -> ErrMediaForbidden (not ErrPrivateStream).
	_, err := NewMediaReq().Get(ctx, srv.URL)
	if err == nil {
		t.Fatal("media Get: expected error, got nil")
	}
	if !errors.Is(err, ErrMediaForbidden) {
		t.Errorf("media Get: err = %v, want ErrMediaForbidden", err)
	}
	if errors.Is(err, ErrPrivateStream) {
		t.Errorf("media Get: err = %v, must NOT be classified as private show", err)
	}

	// CDN media client via GetBytesWithTimeout: same classification.
	_, err = NewMediaReq().GetBytesWithTimeout(ctx, srv.URL, 30*time.Second)
	if err == nil {
		t.Fatal("media GetBytesWithTimeout: expected error, got nil")
	}
	if !errors.Is(err, ErrMediaForbidden) {
		t.Errorf("media GetBytesWithTimeout: err = %v, want ErrMediaForbidden", err)
	}

	// Site API client: bare 403 stays a private show (unchanged behavior).
	_, err = NewReq().Get(ctx, srv.URL)
	if err == nil {
		t.Fatal("site Get: expected error, got nil")
	}
	if !errors.Is(err, ErrPrivateStream) {
		t.Errorf("site Get: err = %v, want ErrPrivateStream", err)
	}
}

// TestMedia403CloudflareChallengeStillBlocked verifies a CDN 403 that carries
// Cloudflare challenge markers still reports ErrCloudflareBlocked, not the
// ambiguous media-forbidden error.
func TestMedia403CloudflareChallengeStillBlocked(t *testing.T) {
	old := server.Config
	server.Config = &entity.Config{Domain: "https://www.cb.xxx/"}
	defer func() { server.Config = old }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("<title>Just a moment...</title>"))
	}))
	defer srv.Close()

	_, err := NewMediaReq().Get(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrCloudflareBlocked) {
		t.Errorf("err = %v, want ErrCloudflareBlocked", err)
	}
}
