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
}
