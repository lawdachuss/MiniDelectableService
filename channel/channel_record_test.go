package channel

import "testing"

func TestIsDefinitiveOfflineStatus(t *testing.T) {
	offline := []string{"offline", "OFFLINE", " Offline ", "away", "AWAY"}
	for _, s := range offline {
		if !isDefinitiveOfflineStatus(s) {
			t.Errorf("isDefinitiveOfflineStatus(%q) = false, want true", s)
		}
	}
	// Anything else — including an empty/ambiguous status seen while we were
	// actively recording — must NOT be treated as a definitive offline so the
	// cycle is merged into the same session instead of being fractured.
	live := []string{"public", "PUBLIC", " Public ", "hidden", "HIDDEN", "", "private", "group", "disconnected"}
	for _, s := range live {
		if isDefinitiveOfflineStatus(s) {
			t.Errorf("isDefinitiveOfflineStatus(%q) = true, want false", s)
		}
	}
}
