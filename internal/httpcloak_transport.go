package internal

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sardanioss/httpcloak"
	"github.com/teacat/chaturbate-dvr/server"
)

// httpcloakTransport wraps httpcloak.Client as an http.RoundTripper.
// It emulates a Chrome 146 TLS/HTTP2 fingerprint to bypass Cloudflare WAF
// TCP RST that Go's default crypto/tls triggers.
// ECH (Encrypted Client Hello) hides the SNI from network observers for
// better Cloudflare bot scores.
//
// All connections are direct (no proxy). Plain-HTTP requests, CDN hosts and
// Stripchat go through http.DefaultTransport; everything else uses the
// httpcloak client.
type httpcloakTransport struct {
	mu     sync.Mutex
	client *httpcloak.Client
}

// sharedTransportSingleton is a singleton http.RoundTripper for the shared transport.
var sharedTransportSingleton http.RoundTripper
var sharedTransportOnce sync.Once

func getSharedTransport() http.RoundTripper {
	sharedTransportOnce.Do(func() {
		sharedTransportSingleton = &httpcloakTransport{
			client: newCloakClient(),
		}
	})
	return sharedTransportSingleton
}

// newCloakClient creates a new httpcloak client with a Chrome 146 TLS fingerprint.
func newCloakClient() *httpcloak.Client {
	return httpcloak.New("chrome-146-windows", httpcloak.WithTimeout(120*time.Second))
}

// WarmupChaturbate makes an initial request to the configured Chaturbate
// domain to establish TLS session tickets with Cloudflare before any API
// calls are made. This gives subsequent requests TLS session resumption,
// making them look more like a returning browser visitor. Best-effort —
// single attempt.
func WarmupChaturbate(ctx context.Context) {
	warmupURL := strings.TrimRight(server.Config.Domain, "/") + "/"
	if server.Config.Domain == "" {
		warmupURL = "https://www.cb.xxx/"
	}
	req, err := http.NewRequestWithContext(ctx, "HEAD", warmupURL, nil)
	if err != nil {
		return
	}
	SetRequestHeaders(req)
	t, ok := getSharedTransport().(*httpcloakTransport)
	if !ok {
		return
	}
	resp, err := t.RoundTrip(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// WarmupStripchat makes an initial request to stripchat.com to establish TLS
// session tickets before any API calls are made. This is the same idea as
// WarmupChaturbate but for Stripchat's domain.
func WarmupStripchat(ctx context.Context) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", "https://stripchat.com/", nil)
	if err != nil {
		return
	}
	SetRequestHeaders(req)
	t, ok := getSharedTransport().(*httpcloakTransport)
	if !ok {
		return
	}
	resp, err := t.RoundTrip(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// cdnHostSuffixes lists CDN hostname suffixes that serve HLS segments
// with signed URLs (pkey/token). These hosts are routed through
// http.DefaultTransport directly.
var cdnHostSuffixes = []string{
	".doppiocdn.net",
	".doppiocdn.com",
	".live.mmcdn.com",
}

// directHosts lists hosts that should always use http.DefaultTransport.
// NOTE: stripchat.com was previously here but has been removed — Stripchat's
// Cloudflare configuration returns HTTP 418 for Go's native TLS fingerprint.
// All stripchat.com requests now go through the httpcloak Chrome fingerprint client.
var directHosts = []string{}

func isCDNHost(host string) bool {
	host = strings.ToLower(host)
	for _, suffix := range cdnHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

func isDirectHost(host string) bool {
	host = strings.ToLower(host)
	for _, h := range directHosts {
		if host == h || strings.HasSuffix(host, h) {
			return true
		}
	}
	return false
}

// shouldUseDefaultTransport reports whether a request should bypass the
// httpcloak client and go straight through http.DefaultTransport.
func shouldUseDefaultTransport(req *http.Request) bool {
	return req.URL.Scheme == "http" || isCDNHost(req.URL.Host) || isDirectHost(req.URL.Host)
}

// RoundTrip implements http.RoundTripper. All connections are direct;
// CDN/HTTP/Stripchat requests bypass the httpcloak client entirely.
func (t *httpcloakTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if shouldUseDefaultTransport(req) {
		return http.DefaultTransport.RoundTrip(req)
	}

	ctx := req.Context()
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	// Prepare request body once
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	t.mu.Lock()
	client := t.client
	t.mu.Unlock()

	cloakReq := &httpcloak.Request{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header,
	}
	if len(bodyBytes) > 0 {
		cloakReq.Body = bytes.NewReader(bodyBytes)
	}

	cloakResp, err := client.Do(ctx, cloakReq)
	if err != nil {
		return nil, err
	}

	body, bodyErr := cloakResp.Bytes()
	if bodyErr != nil {
		cloakResp.Close()
		return nil, bodyErr
	}

	resp := &http.Response{
		StatusCode: cloakResp.StatusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    req,
	}
	if cloakResp.Headers != nil {
		for k, vs := range cloakResp.Headers {
			for _, v := range vs {
				resp.Header.Add(k, v)
			}
		}
	}
	return resp, nil
}
