package entity

import "testing"

func TestSanitizeCookieValue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
	}{
		{"plain", "plain"},
		{`"quoted"`, "quoted"},           // browser export wrapper quotes
		{`a"b"c`, "abc"},                 // embedded quotes stripped
		{"has;semi", "hassemi"},          // semicolon stripped (would break header)
		{`back\slash`, "backslash"},      // backslash stripped
		{"tab\tnewline\n", "tabnewline"}, // control bytes stripped
		{"", ""},                         // empty stays empty
		{`""`, ""},                       // only quotes -> empty
	}
	for _, c := range cases {
		if got := SanitizeCookieValue(c.in); got != c.want {
			t.Errorf("SanitizeCookieValue(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSanitizeCookieString(t *testing.T) {
	t.Parallel()
	in := `sessionid=abc; affkey="quoted-value"; csrftoken=xyz; broken="a"b"`
	got := SanitizeCookieString(in)
	want := "sessionid=abc; affkey=quoted-value; csrftoken=xyz; broken=ab"
	if got != want {
		t.Fatalf("SanitizeCookieString(%q) = %q, want %q", in, got, want)
	}

	// Empty / all-invalid values are dropped entirely.
	got2 := SanitizeCookieString(`a=""; b=ok`)
	if got2 != "b=ok" {
		t.Fatalf("SanitizeCookieString with empty value = %q, want %q", got2, "b=ok")
	}

	// Empty input is unchanged.
	if got3 := SanitizeCookieString(""); got3 != "" {
		t.Fatalf("SanitizeCookieString(\"\") = %q, want empty", got3)
	}
}
