package internal

import (
	"strings"
	"testing"
)

func TestCSRFFromCookies(t *testing.T) {
	t.Parallel()
	got := CSRFFromCookies("sessionid=x; csrftoken=real-token; custom=abc")
	if got != "real-token" {
		t.Fatalf("CSRFFromCookies() = %q, want real-token", got)
	}
}

func TestFormatCookieHeaderSingleCSRFToken(t *testing.T) {
	t.Parallel()
	header := FormatCookieHeader("sessionid=x; csrftoken=old", "new-token")
	if CSRFFromCookies(header) != "new-token" {
		t.Fatalf("csrftoken in header = %q, want new-token", CSRFFromCookies(header))
	}
	if stringsCount(header, "csrftoken=") != 1 {
		t.Fatalf("expected one csrftoken, got header: %s", header)
	}
}

func stringsCount(s, sub string) int {
	n := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			n++
		}
	}
	return n
}

// TestParseCookiesSanitizesQuotedValues guards against browser-pasted cookie
// strings whose values are wrapped in literal quotes (e.g. affkey="...").
// Sending those quotes in the Cookie header makes Cloudflare reject it and
// trips the global circuit breaker — values must be sanitized at parse time.
func TestParseCookiesSanitizesQuotedValues(t *testing.T) {
	t.Parallel()
	got := ParseCookies(`sessionid=abc; affkey="quoted-value"; csrftoken=xyz; clean=ok`)
	if got["affkey"] != "quoted-value" {
		t.Fatalf("affkey = %q, want quoted-value (quotes stripped)", got["affkey"])
	}
	if got["sessionid"] != "abc" {
		t.Fatalf("sessionid = %q, want abc", got["sessionid"])
	}
	if got["csrftoken"] != "xyz" {
		t.Fatalf("csrftoken = %q, want xyz", got["csrftoken"])
	}
	if got["clean"] != "ok" {
		t.Fatalf("clean = %q, want ok", got["clean"])
	}
}

// TestParseCookiesStripsInnerInvalidBytes covers quoted values that also
// contain embedded quotes (e.g. a value that itself carries quote characters
// that Go's net/http would otherwise drop with an "invalid byte" warning).
func TestParseCookiesStripsInnerInvalidBytes(t *testing.T) {
	t.Parallel()
	got := ParseCookies(`affkey="a"b"c"`)
	if got["affkey"] != "abc" {
		t.Fatalf("affkey = %q, want abc (embedded quotes stripped)", got["affkey"])
	}
}

// TestFormatCookieHeaderHasNoInvalidBytes is the end-to-end regression guard
// for the Cloudflare block: the final Cookie header built for requests must
// never contain quotes, semicolons, backslashes or control bytes (RFC 6265
// cookie-octet), because a malformed header makes Cloudflare challenge the
// request and trips the global circuit breaker for every channel.
func TestFormatCookieHeaderHasNoInvalidBytes(t *testing.T) {
	t.Parallel()
	// Simulate a browser-pasted cookie string with quoted values (the exact
	// shape that previously produced `invalid byte '"' in Cookie.Value`).
	hdr := FormatCookieHeader(`sessionid="abc"; affkey="quoted"; csrftoken="tok"`, "new-token")
	// `; ` is the valid pair separator; validate each VALUE individually.
	for _, pair := range stringsSplitTrim(hdr) {
		idx := stringsIndexByte(pair, '=')
		if idx < 0 {
			t.Fatalf("malformed pair in header %q", pair)
		}
		val := pair[idx+1:]
		for _, b := range []byte(val) {
			if b < 0x21 || b == '"' || b == ',' || b == ';' || b == '\\' || b >= 0x7f {
				t.Fatalf("cookie value contains invalid byte %q: %q", b, val)
			}
		}
	}
	if stringsCount(hdr, "\"") != 0 {
		t.Fatalf("Cookie header still contains quotes: %q", hdr)
	}
	// The rebuilt header must still carry a single csrftoken.
	if CSRFFromCookies(hdr) != "new-token" {
		t.Fatalf("csrftoken in header = %q, want new-token", CSRFFromCookies(hdr))
	}
}

func stringsSplitTrim(s string) []string {
	parts := strings.Split(s, ";")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func stringsIndexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
