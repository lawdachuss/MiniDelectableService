package entity

import "strings"

// cookieValueInvalid reports whether a single byte is invalid inside a cookie
// value.  RFC 6265 cookie-octet is %x21 / %x23-2B / %x2D-3A / %x3C-5B /
// %x5D-7E — everything else (control bytes, space, `"`, `,`, `;`, `\`, DEL,
// non-ASCII) must not appear in a Cookie header value.
//
// Browser-pasted cookie strings (e.g. DevTools "copy all as string") often
// wrap values in literal quotes: `affkey="..."`.  Sending those quotes in the
// Cookie header makes Cloudflare reject the entire header, which trips the
// global circuit breaker and blocks every channel on the node.  Values are
// sanitized at every parse/load/save boundary so the wire header is always
// well-formed.
func cookieValueInvalid(b byte) bool {
	return b < 0x21 || b == '"' || b == ',' || b == ';' || b == '\\' || b >= 0x7f
}

// SanitizeCookieValue strips invalid bytes from a single cookie value and
// removes any surrounding quotes (browser exports frequently quote values).
// An all-invalid value becomes empty, and callers should drop the pair.
func SanitizeCookieValue(v string) string {
	// Strip a quoted wrapper like `affkey="abc"` -> `abc` before filtering.
	v = strings.Trim(v, `"`)
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); i++ {
		if c := v[i]; !cookieValueInvalid(c) {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// SanitizeCookieString parses a "k=v; k2=v2" cookie string and returns the
// same string with every value sanitized (invalid bytes removed, empty values
// dropped).  Use this when persisting or loading cookie blobs so stored
// settings stay well-formed; the request paths additionally sanitize at parse
// time via ParseCookies.
func SanitizeCookieString(cookieStr string) string {
	if cookieStr == "" {
		return ""
	}
	parts := strings.Split(cookieStr, ";")
	kept := make([]string, 0, len(parts))
	for _, pair := range parts {
		eq := strings.IndexByte(pair, '=')
		if eq < 0 {
			continue
		}
		name := strings.TrimSpace(pair[:eq])
		val := strings.TrimSpace(pair[eq+1:])
		if name == "" {
			continue
		}
		val = SanitizeCookieValue(val)
		if val == "" {
			continue // drop empty / all-invalid values
		}
		kept = append(kept, name+"="+val)
	}
	return strings.Join(kept, "; ")
}
