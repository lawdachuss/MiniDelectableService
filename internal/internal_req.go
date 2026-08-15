package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// sharedTransport is a singleton http.RoundTripper reused across all channels.
// It uses httpcloak's Chrome 146 Windows TLS/HTTP2 fingerprint to bypass
// Cloudflare WAF TCP RST that Go's default crypto/tls triggers.
func sharedTransport() http.RoundTripper {
	return getSharedTransport()
}

// IsCloudflareChallenge reports whether an HTTP response is a Cloudflare
// challenge/block page rather than real content. It matches on the status
// codes Cloudflare uses for challenge/rate-limit pages (403, 429, 503, 410)
// AND on the body markers present in the challenge HTML. The body check is
// intentionally broad: cb.xxx varies the exact title ("Just a moment…",
// "Attention Required! | Cloudflare") and can return the challenge on any
// status, so a single shared helper keeps the GET and POST paths in sync.
func IsCloudflareChallenge(status int, body string) bool {
	// 429 (rate limited) and 503 always mean "upstream rejecting us right
	// now" — treating them as a block backs the channel off instead of
	// hammering the API with per-second retries (seen fleet-wide on cb.xxx
	// serving "Just a moment…" pages at HTTP 429/410 to an over-claiming node).
	if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable || status == http.StatusGone {
		return true
	}
	snippet := body
	if len(snippet) > 4096 {
		snippet = snippet[:4096]
	}
	lower := strings.ToLower(snippet)
	// Body markers are Cloudflare-specific challenge HTML.
	if strings.Contains(lower, "just a moment") ||
		strings.Contains(lower, "attention required") ||
		strings.Contains(lower, "cf-chl") ||
		strings.Contains(lower, "challenge-platform") ||
		strings.Contains(lower, "cf-chl-box") ||
		strings.Contains(lower, "enable javascript") {
		return true
	}
	// A bare 403 is ambiguous: cb.xxx returns it for private shows too, which
	// are an expected per-channel state. Only classify a 403 as a challenge
	// when the body already matched a Cloudflare marker above (handled) — a
	// markerless 403 stays a private show.
	return false
}

// WaitForChaturbateRateLimit blocks until a rate-limit slot is available.
// Call this before every Chaturbate API request to avoid triggering
// Cloudflare's DDoS protection when many channels reconnect simultaneously.
// Uses the adaptive rate limiter that adjusts based on error feedback.
func WaitForChaturbateRateLimit(ctx context.Context) error {
	if chaturbateRateLimiter().Acquire(ctx.Done()) {
		return nil
	}
	return ctx.Err()
}

// ReportChaturbateSuccess notifies the rate limiter and circuit breaker
// of a successful API call, allowing rate to gradually increase.
func ReportChaturbateSuccess() {
	chaturbateRateLimiter().Success()
	chaturbateBreaker.Success()
}

// ReportChaturbateFailure notifies the rate limiter and circuit breaker
// of a failed API call, triggering backoff.
func ReportChaturbateFailure() {
	chaturbateRateLimiter().Failure()
	chaturbateBreaker.Failure()
}

// AllowChaturbateRequest checks the circuit breaker.
// Returns false if the circuit is open and requests should not proceed.
func AllowChaturbateRequest() bool {
	return chaturbateBreaker.Allow()
}

// IsExpectedChannelError reports whether err is a normal per-channel state
// (offline, private, hidden, deleted/404, age-gate, password-gate) rather than
// a genuine upstream failure. Expected states must NOT feed the global circuit
// breaker or adaptive rate limiter — with hundreds of channels, private shows
// and 404s are constant, and counting them trips the breaker for everyone,
// blocking ALL Chaturbate API traffic (recording, room status, profile
// scrapes) for the cooldown period.
func IsExpectedChannelError(err error) bool {
	return errors.Is(err, ErrChannelOffline) ||
		errors.Is(err, ErrPrivateStream) ||
		errors.Is(err, ErrHiddenStream) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrAgeVerification) ||
		errors.Is(err, ErrRoomPasswordRequired) ||
		errors.Is(err, ErrGeoBlocked)
}

// ReportChaturbateFailureUnlessExpected feeds the circuit breaker and adaptive
// rate limiter only for genuine upstream failures. Expected per-channel states
// (private shows, 404s, hidden rooms, etc.) are skipped so a busy channel list
// can never trip the global breaker or drag the API rate down.
func ReportChaturbateFailureUnlessExpected(err error) {
	if IsExpectedChannelError(err) {
		return
	}
	ReportChaturbateFailure()
}

// ChaturbateRate returns the current adaptive rate limit in req/s.
func ChaturbateRate() int {
	return chaturbateRateLimiter().CurrentRate()
}

// ChaturbatePeakRate returns the highest rate reached this session.
func ChaturbatePeakRate() int {
	return chaturbateRateLimiter().PeakRate()
}

// Req represents an HTTP client with customized settings.
type Req struct {
	client  *http.Client
	isMedia bool   // when true, omits browser-spoofing headers not needed for CDN media requests
	referer string // CDN Referer/Origin override; only used when isMedia is true
}

// NewReq creates a new HTTP client for Chaturbate page/API requests.
func NewReq() *Req {
	return &Req{
		client: &http.Client{
			Transport: sharedTransport(),
		},
	}
}

// NewMediaReq creates a new HTTP client for CDN media requests (playlists, segments).
// It omits headers like X-Requested-With that are only needed for page fetches
// and would cause CDN hosts (e.g. mmcdn.com) to reject the request.
func NewMediaReq() *Req {
	return &Req{
		client: &http.Client{
			Transport: sharedTransport(),
		},
		isMedia: true,
	}
}

// NewMediaReqWithReferer creates a media HTTP client that sends the given URL as
// Referer and Origin instead of the Chaturbate defaults. Use this for non-Chaturbate CDNs.
func NewMediaReqWithReferer(referer string) *Req {
	return &Req{
		client: &http.Client{
			Transport: sharedTransport(),
		},
		isMedia: true,
		referer: referer,
	}
}

// CreateTransport returns the shared httpcloak transport (kept for backward compatibility).
func CreateTransport() http.RoundTripper {
	return sharedTransport()
}

// Get sends an HTTP GET request and returns the response as a string.
func (h *Req) Get(ctx context.Context, url string) (string, error) {
	resp, err := h.GetBytes(ctx, url)
	if err != nil {
		return "", fmt.Errorf("get bytes: %w", err)
	}
	return string(resp), nil
}

// GetBytes sends an HTTP GET request and returns the response as a byte slice.
func (h *Req) GetBytes(ctx context.Context, url string) ([]byte, error) {
	req, cancel, err := h.CreateRequest(ctx, url)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("new request: %w", err)
	}
	defer cancel()

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("client do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// Check for Cloudflare protection (challenge page or 403/429/503 status)
	if IsCloudflareChallenge(resp.StatusCode, string(b)) {
		return nil, ErrCloudflareBlocked
	}

	// Check for Age Verification
	if strings.Contains(string(b), "Verify your age") {
		return nil, ErrAgeVerification
	}

	if resp.StatusCode == http.StatusForbidden {
		// A bare 403 from a CDN media endpoint is ambiguous (private show vs
		// expired HLS session token), so it must not be reported as a private
		// show — the recorder would end a live recording and bench the channel
		// for the full offline interval. Only the site API (which knows the
		// room's true room_status) may classify a 403 as a private show.
		if h.isMedia {
			return nil, fmt.Errorf("forbidden: %w", ErrMediaForbidden)
		}
		return nil, fmt.Errorf("forbidden: %w", ErrPrivateStream)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		if strings.Contains(string(b), "password-required") {
			return nil, ErrRoomPasswordRequired
		}
		return nil, fmt.Errorf("unauthorized: %w", ErrPrivateStream)
	}

	if resp.StatusCode != http.StatusOK {
		snippet := string(b)
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("unexpected HTTP %d: %s", resp.StatusCode, snippet)
	}

	return b, nil
}

// Head sends an HTTP HEAD request and returns the status code.
func (h *Req) Head(ctx context.Context, url string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return 0, err
	}
	SetRequestHeaders(req)

	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return resp.StatusCode, nil
}

// GetBytesWithTimeout is like GetBytes but with a caller-specified timeout.
// Large CDN video segments can take a while to read end-to-end, so callers
// that download them can raise the timeout above the default 30s.
func (h *Req) GetBytesWithTimeout(ctx context.Context, url string, timeout time.Duration) ([]byte, error) {
	req, cancel, err := h.CreateRequestWithTimeout(ctx, url, timeout)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("new request: %w", err)
	}
	defer cancel()

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("client do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// Check for Cloudflare protection (challenge page or 403/429/503 status)
	if IsCloudflareChallenge(resp.StatusCode, string(b)) {
		return nil, ErrCloudflareBlocked
	}

	if strings.Contains(string(b), "Verify your age") {
		return nil, ErrAgeVerification
	}

	if resp.StatusCode == http.StatusForbidden {
		if h.isMedia {
			return nil, fmt.Errorf("forbidden: %w", ErrMediaForbidden)
		}
		return nil, fmt.Errorf("forbidden: %w", ErrPrivateStream)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		if strings.Contains(string(b), "password-required") {
			return nil, ErrRoomPasswordRequired
		}
		return nil, fmt.Errorf("unauthorized: %w", ErrPrivateStream)
	}

	if resp.StatusCode != http.StatusOK {
		snippet := string(b)
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("unexpected HTTP %d: %s", resp.StatusCode, snippet)
	}

	return b, nil
}

// CreateRequest constructs an HTTP GET request with necessary headers (30s timeout).
func (h *Req) CreateRequest(ctx context.Context, url string) (*http.Request, context.CancelFunc, error) {
	return h.CreateRequestWithTimeout(ctx, url, 30*time.Second)
}

// CreateRequestWithTimeout is like CreateRequest but with a custom timeout.
func (h *Req) CreateRequestWithTimeout(ctx context.Context, url string, timeout time.Duration) (*http.Request, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	if h.isMedia {
		ctx = context.WithValue(ctx, mediaFlagKey{}, true)
		if h.referer != "" {
			ctx = context.WithValue(ctx, mediaRefererKey{}, h.referer)
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, cancel, err
	}
	SetRequestHeaders(req)
	return req, cancel, nil
}

// SetRequestHeaders applies necessary headers to the request.
func SetRequestHeaders(req *http.Request) {
	if isMediaRequest(req) {
		ref := mediaReferer(req)
		if ref == "" {
			ref = strings.TrimRight(server.Config.Domain, "/") + "/"
			if server.Config.Domain == "" {
				ref = "https://www.cb.xxx/"
			}
		}
		req.Header.Set("Referer", ref)
		req.Header.Set("Origin", strings.TrimRight(ref, "/"))
	} else {
		// X-Requested-With helps bypass Cloudflare on site page fetches.
		// Do NOT send it to CDN media hosts (mmcdn.com) as it may cause rejection.
		req.Header.Set("X-Requested-With", "XMLHttpRequest")

		domain := strings.TrimRight(server.Config.Domain, "/")
		if domain != "" {
			req.Header.Set("Origin", domain)
			req.Header.Set("Referer", domain+"/")
		}
	}

	if server.Config.UserAgent != "" {
		req.Header.Set("User-Agent", strings.TrimSpace(server.Config.UserAgent))
	}
	if server.Config.Cookies != "" {
		cookies := ParseCookies(server.Config.Cookies)
		for name, value := range cookies {
			req.AddCookie(&http.Cookie{Name: name, Value: value})
		}
	}
}

// isMediaRequest reports whether req carries the media client marker.
func isMediaRequest(req *http.Request) bool {
	v, _ := req.Context().Value(mediaFlagKey{}).(bool)
	return v
}

// mediaReferer returns the per-request Referer override, if any.
func mediaReferer(req *http.Request) string {
	v, _ := req.Context().Value(mediaRefererKey{}).(string)
	return v
}

type mediaFlagKey struct{}

type mediaRefererKey struct{}

// ParseCookies converts a cookie string into a map.
// Values are sanitized (quotes, semicolons, backslashes and control bytes
// removed) so a browser-pasted cookie string with quoted values can never
// produce a malformed Cookie header that Cloudflare rejects.  Pairs whose
// value sanitizes to empty are dropped, matching entity.SanitizeCookieString.
func ParseCookies(cookieStr string) map[string]string {
	cookies := make(map[string]string)
	pairs := strings.Split(cookieStr, ";")

	// Iterate over each cookie pair and extract key-value pairs
	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 {
			// Trim spaces around key and value
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			// Sanitize and drop empty/all-invalid values
			if value = entity.SanitizeCookieValue(value); value == "" {
				continue
			}
			// Store cookie name and value in the map
			cookies[key] = value
		}
	}
	return cookies
}
