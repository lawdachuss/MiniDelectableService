package internal

import "errors"

var (
	ErrChannelExists        = errors.New("channel exists")
	ErrChannelNotFound      = errors.New("channel not found")
	ErrCloudflareBlocked    = errors.New("blocked by Cloudflare; try with `-cookies` and `-user-agent`")
	ErrAgeVerification      = errors.New("age verification required; try with `-cookies` and `-user-agent`")
	ErrChannelOffline       = errors.New("channel offline")
	ErrPrivateStream        = errors.New("channel is in a private show")
	ErrHiddenStream         = errors.New("channel is hidden")
	ErrRoomPasswordRequired = errors.New("room requires a password")
	ErrPaused               = errors.New("channel paused")
	ErrStopped              = errors.New("channel stopped")
	ErrGeoBlocked           = errors.New("stream not accessible (may be geo-blocked)")
	ErrNotFound             = errors.New("not found (404)")
	// ErrStreamStalled is returned when the HLS segment loop makes no forward
	// progress for several consecutive poll cycles.  This usually means the
	// CDN session token embedded in the segment URLs has expired.  The Monitor
	// loop treats it as a soft error: it finalises the current file and
	// re-fetches a fresh HLS URL so recording resumes immediately.
	ErrStreamStalled = errors.New("no new segments downloaded — stream session may have expired; reconnecting")
)
