package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/server"
)

// Shared client for Chaturbate POST API (reuses the shared transport)
var postClient = sync.OnceValue(func() *http.Client {
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: CreateTransport(),
	}
})

// PostChaturbateAPI makes a POST request to Chaturbate API
func PostChaturbateAPI(ctx context.Context, username string) (string, error) {
	apiURL := fmt.Sprintf("%sget_edge_hls_url_ajax/", server.Config.Domain)

	// Build POST data
	postData := url.Values{}
	postData.Set("room_slug", username)
	postData.Set("bandwidth", "high")

	if !AllowChaturbateRequest() {
		return "", ErrCircuitBreakerOpen
	}

	var bodyStr string
	err := retry.Do(func() error {
		if err := WaitForChaturbateRateLimit(ctx); err != nil {
			return err
		}
		if !AllowChaturbateRequest() {
			return ErrCircuitBreakerOpen
		}

		req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBufferString(postData.Encode()))
		if err != nil {
			return err
		}

		// Set headers
		userAgent := server.Config.UserAgent
		if userAgent == "" {
			userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
		}

		req.Header.Set("User-Agent", strings.TrimSpace(userAgent))
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json, text/plain, */*")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Pragma", "no-cache")
		req.Header.Set("Sec-Fetch-Dest", "empty")
		req.Header.Set("Sec-Fetch-Mode", "cors")
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		req.Header.Set("Sec-Ch-Ua", `"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`)
		req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
		req.Header.Set("Sec-Ch-Ua-Platform", `"Windows"`)
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
		domain := strings.TrimRight(server.Config.Domain, "/")
		req.Header.Set("Referer", fmt.Sprintf("%s/%s", domain, username))
		req.Header.Set("Origin", domain)

		sanitized := ""
		if server.Config.Cookies != "" {
			sanitized = strings.Map(func(r rune) rune {
				if r == '\n' || r == '\r' || r == '\t' || r < 32 {
					return -1
				}
				return r
			}, server.Config.Cookies)
			sanitized = strings.TrimSpace(sanitized)
		}
		csrfToken := CSRFTokenForRequest(sanitized)
		req.Header.Set("X-CSRFToken", csrfToken)
		req.Header.Set("Cookie", FormatCookieHeader(sanitized, csrfToken))

		resp, err := postClient().Do(req)
		if err != nil {
			ReportChaturbateFailure()
			return fmt.Errorf("do request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == 404 {
			// Expected state (channel deleted/renamed) — must NOT trip the
			// global breaker or drag the rate limiter down.
			return retry.Unrecoverable(ErrNotFound)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			ReportChaturbateFailure()
			return fmt.Errorf("read body: %w", err)
		}

		bodyStr = string(body)

		if resp.StatusCode == 403 {
			// A 403 can be either an expected private show OR a Cloudflare
			// challenge (site-wide block). The GET path detects the challenge
			// via the body; the POST path must do the same so a real block
			// still feeds the breaker/rate limiter. Private shows are expected
			// per-channel states and must NOT trip the global breaker.
			if strings.Contains(bodyStr, "Just a moment...") {
				ReportChaturbateFailure()
				return retry.Unrecoverable(ErrCloudflareBlocked)
			}
			fmt.Printf("[DEBUG] POST API 403 response for %s: %s\n", username, bodyStr)
			return retry.Unrecoverable(fmt.Errorf("forbidden: %w", ErrPrivateStream))
		}

		if resp.StatusCode != 200 {
			ReportChaturbateFailure()
			fmt.Printf("[DEBUG] POST API status %d for %s: %s\n", resp.StatusCode, username, bodyStr)
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}

		ReportChaturbateSuccess()
		return nil
	},
		retry.Context(ctx),
		retry.Attempts(3),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.FixedDelay),
	)
	if err != nil {
		return "", err
	}

	return bodyStr, nil
}
