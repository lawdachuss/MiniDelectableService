package view

import (
	"strings"
	"testing"
)

func TestChannelListItemsUseKeyboardAccessibleButtons(t *testing.T) {
	content, err := FS.ReadFile("templates/index.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}

	html := string(content)
	if !strings.Contains(html, `<button type="button"`) ||
		!strings.Contains(html, `channel-item`) {
		t.Fatal("channel list items should be rendered as native buttons with class channel-item")
	}
}

func TestSidebarHasPausedGroupingAndIndicator(t *testing.T) {
	content, err := FS.ReadFile("templates/index.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	js, err := FS.ReadFile("templates/scripts/app.js")
	if err != nil {
		t.Fatalf("read app.js: %v", err)
	}

	html := string(content)
	for _, want := range []string{
		`data-priority="{{ if and .IsOnline (not .IsPaused) }}0{{ else if .IsPaused }}1`,
		`{{ else if .IsPaused }}Paused`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("index template missing paused sidebar marker %q", want)
		}
	}

	for _, want := range []string{
		`1: 'Paused'`,
	} {
		if !strings.Contains(string(js), want) {
			t.Fatalf("app.js missing paused sidebar marker %q", want)
		}
	}
}
