package server

import (
	"net/http"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
)

var Manager IManager

type IManager interface {
	CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error
	StopChannel(username string) error
	PauseChannel(username string) error
	ResumeChannel(username string) error
	ChannelInfo() []*entity.ChannelInfo
	Publish(name string, ch *entity.ChannelInfo)
	PublishLog(username, line string)
	PublishUploadState()
	Subscriber(w http.ResponseWriter, r *http.Request)
	LoadConfig() error
	SaveConfig() error
	WaitForUploads()
	StopAllChannels()
	WaitForAllChannels()
	StopWatcher()
	StartSession(duration time.Duration)
	StopSession()
	StartWatcher()
	IsFileUploadInFlight(filePath string) bool
	SessionInfo() (time.Duration, bool)
	TriggerSessionStop()
	UploadEntries() *entity.UploadsResponse
	ReportCFBlock(username string)
	ResetCFBlock(username string)
	// ReportSessionCut records that a channel hit the node-wide session-cut
	// signature (CDN HLS 403/404 whose site-API probe also failed). When
	// enough distinct channels report within a short window the manager
	// re-mints cookies early, before the whole node's HLS sessions 404.
	ReportSessionCut(username string)

	// CFBlockedCount returns how many channels are currently in a
	// Cloudflare-blocked state (used by the claim cycle to detect a starved
	// node).
	CFBlockedCount() int
	// ChannelMinDurationBeforeUpload returns the per-channel
	// min-duration-before-upload setting for a live channel (0 when the
	// channel is unknown or the feature is disabled for it).  Orphan and
	// pending-segment flows use this so a channel configured with a 1200s
	// threshold in the pool is gated even when the node's global
	// MIN_DURATION_BEFORE_UPLOAD env var is unset.
	ChannelMinDurationBeforeUpload(username string) int
	// ActiveRecordingFiles returns the absolute paths of files currently being
	// recorded by any channel.  The orphan scan walks the recording directory
	// and uses this to avoid treating a live recording as a stranded orphan.
	ActiveRecordingFiles() []string
	// RequestCookieRefresh triggers a rate-limited cookie re-mint (scripts +
	// Supabase reload) so a Cloudflare-starved node can recover without a
	// restart.
	RequestCookieRefresh()
	// SetCookieRefreshFunc registers the function that re-mints this node's
	// cookies; called by main.go at startup.
	SetCookieRefreshFunc(fn func())
}
