package internal

import (
	"net/http"
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func TestSetRequestHeadersStripchatSameOrigin(t *testing.T) {
	old := server.Config
	server.Config = &entity.Config{Domain: "https://www.cb.xxx/", UserAgent: ""}
	t.Cleanup(func() { server.Config = old })

	req, err := http.NewRequest("GET", "https://stripchat.com/api/front/v2/models/foo/cam", nil)
	if err != nil {
		t.Fatal(err)
	}
	SetRequestHeaders(req)

	if got := req.Header.Get("Referer"); got != "https://stripchat.com/" {
		t.Errorf("stripchat Referer = %q, want https://stripchat.com/", got)
	}
	if got := req.Header.Get("Origin"); got != "https://stripchat.com" {
		t.Errorf("stripchat Origin = %q, want https://stripchat.com", got)
	}
	if got := req.Header.Get("User-Agent"); got != defaultBrowserUA {
		t.Errorf("stripchat User-Agent = %q, want default browser UA", got)
	}
	if got := req.Header.Get("Sec-Fetch-Site"); got != "same-origin" {
		t.Errorf("stripchat Sec-Fetch-Site = %q, want same-origin", got)
	}
}

func TestSetRequestHeadersChaturbateSameOrigin(t *testing.T) {
	old := server.Config
	server.Config = &entity.Config{Domain: "https://www.cb.xxx/", UserAgent: ""}
	t.Cleanup(func() { server.Config = old })

	req, err := http.NewRequest("GET", "https://www.cb.xxx/api/foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	SetRequestHeaders(req)

	if got := req.Header.Get("Referer"); got != "https://www.cb.xxx/" {
		t.Errorf("chaturbate Referer = %q, want https://www.cb.xxx/", got)
	}
}

func TestSetRequestHeadersConfiguredUAWins(t *testing.T) {
	old := server.Config
	server.Config = &entity.Config{Domain: "https://www.cb.xxx/", UserAgent: "CustomUA/1.0"}
	t.Cleanup(func() { server.Config = old })

	req, err := http.NewRequest("GET", "https://stripchat.com/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	SetRequestHeaders(req)

	if got := req.Header.Get("User-Agent"); got != "CustomUA/1.0" {
		t.Errorf("User-Agent = %q, want CustomUA/1.0", got)
	}
}
