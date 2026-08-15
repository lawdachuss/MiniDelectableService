package uploader

import "testing"

func TestIsRetryableCatboxError(t *testing.T) {
	cases := []struct {
		name string
		err  string
		want bool
	}{
		{"transient 412 not signed in (observed burst)", "catbox: status 412: Not signed in!", true},
		{"transient 412 invalid uploader (IP block)", "catbox: status 412: Invalid uploader", true},
		{"rate limit 429", "catbox: status 429: too many requests", true},
		{"server error 5xx", "catbox: status 520: something went wrong", true},
		{"network send failure", "catbox: send request: connection reset by peer", true},
		{"stat race", "catbox: stat file: no such file or directory", true},
		{"read failure", "catbox: read response: unexpected EOF", true},
		{"fatal 400 bad request", "catbox: status 400: invalid request", false},
		{"fatal unexpected response", "catbox: unexpected response: not a url", false},
		{"fatal empty response", "catbox: empty response", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableCatboxError(&fakeErr{tc.err}); got != tc.want {
				t.Errorf("isRetryableCatboxError(%q) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }
