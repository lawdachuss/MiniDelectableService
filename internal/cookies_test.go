package internal

import "testing"

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
