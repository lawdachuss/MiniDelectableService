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
	// ErrCircuitBreakerOpen is returned when the global Chaturbate API circuit
	// breaker is tripped and requests are being rejected for a cooldown period.
	// It is deliberately NOT wrapped in ErrChannelOffline: the channel itself
	// may be fine — upstream API traffic is throttled globally, so the Monitor
	// loop must treat this as a transient error (retry in seconds) rather than
	// benching the channel for the full Interval.
	ErrCircuitBreakerOpen = errors.New("circuit breaker open")
	// ErrStreamStalled is returned when the HLS segment loop makes no forward
	// progress for several consecutive poll cycles.  This usually means the
	// CDN session token embedded in the segment URLs has expired.  The Monitor
	// loop treats it as a soft error: it finalises the current file and
	// re-fetches a fresh HLS URL so recording resumes immediately.
	ErrStreamStalled = errors.New("no new segments downloaded — stream session may have expired; reconnecting")
	// ErrMediaForbidden is returned when a CDN media endpoint (HLS playlist,
	// segment, thumbnail) answers with a bare 403 that carries no Cloudflare
	// challenge. Unlike the site API — where a bare 403 means the room is in
	// a private show — a CDN 403 is ambiguous: it can be a private show (the
	// public stream stopped) OR an expired HLS session token / dead edge.
	// Treating every CDN 403 as a private show ended live recordings after a
	// handful of failed polls and pushed the channel into the slow offline
	// retry, so recordings rarely reached max duration. The recorder now
	// probes the site API (which knows the room's true state) before deciding.
	ErrMediaForbidden = errors.New("media endpoint forbidden (session expired or stream stopped)")
)
