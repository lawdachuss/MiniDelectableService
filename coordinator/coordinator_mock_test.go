package coordinator

import "github.com/teacat/chaturbate-dvr/database"

// mockChannelManager is a no-op ChannelManager used by the integration tests.
// It satisfies the ChannelManager interface so the coordinator can be exercised
// without a real recorder.
type mockChannelManager struct{}

func (m *mockChannelManager) CreateChannelFromAssignment(ca *database.ChannelAssignment) error {
	return nil
}
func (m *mockChannelManager) RemoveChannelForReassignment(username string) error { return nil }
func (m *mockChannelManager) GetLocalChannels() []string                         { return nil }
func (m *mockChannelManager) LocalChannelSite(username string) (string, bool)    { return "", false }
func (m *mockChannelManager) HasPendingSegments(username string) bool            { return false }
func (m *mockChannelManager) IsRecording(username string) bool                   { return false }
func (m *mockChannelManager) ManualPausedChannels() []ChannelPause               { return nil }
func (m *mockChannelManager) CFBlockedCount() int                                { return 0 }
func (m *mockChannelManager) RequestCookieRefresh()                              {}
