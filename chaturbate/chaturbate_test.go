package chaturbate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafov/m3u8"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func TestPickPlaylistIncludesDefaultAudioRendition(t *testing.T) {
	t.Parallel()

	master := &m3u8.MasterPlaylist{
		Variants: []*m3u8.Variant{
			{
				URI: "llhls_video.m3u8",
				VariantParams: m3u8.VariantParams{
					Resolution: "1920x1080",
					FrameRate:  60,
					Audio:      "audio-main",
					Alternatives: []*m3u8.Alternative{
						{Type: "AUDIO", GroupId: "audio-main", URI: "audio-en.m3u8", Name: "English", Default: true},
					},
				},
			},
		},
	}

	// The audio rendition is resolved only for LL-HLS/fMP4 streams (the
	// variant URL must signal llhls/.m4s, or the base URL a Stripchat CDN).
	playlist, err := PickPlaylist(master, "https://example.com/master.m3u8", 1080, 60)
	if err != nil {
		t.Fatalf("PickPlaylist() error = %v", err)
	}
	if got, want := playlist.PlaylistURL, "https://example.com/llhls_video.m3u8"; got != want {
		t.Fatalf("PlaylistURL = %q, want %q", got, want)
	}
	if got, want := playlist.AudioPlaylistURL, "https://example.com/audio-en.m3u8"; got != want {
		t.Fatalf("AudioPlaylistURL = %q, want %q", got, want)
	}
	if got, want := playlist.FileExt, ".mp4"; got != want {
		t.Fatalf("FileExt = %q, want %q", got, want)
	}
}

func TestAlternateEdgeURLsPreservesLLHLSToken(t *testing.T) {
	t.Parallel()

	source := "https://edge15-sin.live.mmcdn.com/v1/edge/streams/example/llhls.m3u8?token=abc.def"

	urls := alternateEdgeURLs(source)
	if len(urls) != len(edgeRegions)-1 {
		t.Fatalf("alternateEdgeURLs() returned %d URLs, want %d", len(urls), len(edgeRegions)-1)
	}
	for _, got := range urls {
		if strings.Contains(got, "edge15-sin.live.mmcdn.com") {
			t.Fatalf("alternateEdgeURLs() included original edge: %q", got)
		}
		if !strings.Contains(got, "/llhls.m3u8?token=abc.def") {
			t.Fatalf("alternateEdgeURLs() did not preserve path/token: %q", got)
		}
	}
}

// TestWatchSegmentsContinuesAfterSegmentFetchFailure guards against a
// transient segment fetch failure permanently stalling the recording loop.
// The failed segment (seq 101) is skipped after its fetch attempts fail, but
// recording must continue with later segments (seq 102) on the next poll.
func TestWatchSegmentsContinuesAfterSegmentFetchFailure(t *testing.T) {
	if server.Config == nil {
		server.Config = &entity.Config{}
	}
	server.Config.Debug = false

	playlistBody := strings.Join([]string{
		"#EXTM3U",
		"#EXT-X-VERSION:3",
		"#EXT-X-TARGETDURATION:2",
		"#EXT-X-MEDIA-SEQUENCE:100",
		"#EXTINF:2.000,",
		"seg_1_100_video_abc.m4s",
		"#EXTINF:2.000,",
		"seg_2_101_video_abc.m4s",
		"#EXTINF:2.000,",
		"seg_3_102_video_abc.m4s",
		"",
	}, "\n")

	mux := http.NewServeMux()
	mux.HandleFunc("/playlist.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(playlistBody))
	})
	mux.HandleFunc("/seg_1_100_video_abc.m4s", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("seg-100-data"))
	})
	mux.HandleFunc("/seg_2_101_video_abc.m4s", func(w http.ResponseWriter, _ *http.Request) {
		// Close the connection mid-response so the client gets a real fetch error.
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("hijacker not supported")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		_ = conn.Close()
	})
	mux.HandleFunc("/seg_3_102_video_abc.m4s", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("seg-102-data"))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	pl := &Playlist{
		PlaylistURL: srv.URL + "/playlist.m3u8",
		RootURL:     srv.URL + "/",
	}

	var handlerCalls atomic.Int32
	handler := func(_ []byte, _ float64) error {
		handlerCalls.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- pl.WatchSegments(ctx, handler)
	}()

	// seg_1 (seq 100) must reach the handler, the failed seg_2 (seq 101) must
	// be skipped, and seg_3 (seq 102) must still be recorded on the next poll.
	deadline := time.Now().Add(20 * time.Second)
	for handlerCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	cancel()

	if got := handlerCalls.Load(); got != 2 {
		t.Fatalf("handler called %d times, want 2 (seg_1 + seg_3; failed seg_2 must be skipped)", got)
	}
}
