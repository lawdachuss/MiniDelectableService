package site

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

type StripchatSite struct{}

func NewStripchatSite() *StripchatSite {
	return &StripchatSite{}
}

// scPageState holds the minimal slice of window.__PRELOADED_STATE__ we need.
// Stripchat SSR embeds this JSON in every model page - it is the only endpoint
// reliably reachable from datacenter IPs under Cloudflare bot-scoring.
// The /api/front/v2/models/username/{username}/cam endpoint returns HTTP 418 for
// any non-residential IP regardless of headers or TLS fingerprint.
type scPageState struct {
	ViewCamBase struct {
		Model struct {
			Username           string `json:"username"`
			Status             string `json:"status"`
			IsLive             bool   `json:"isLive"`
			IsOnline           bool   `json:"isOnline"`
			BroadcastGender    string `json:"broadcastGender"`
			PreviewUrlThumbBig string `json:"previewUrlThumbBig"`
			SnapshotTimestamp  int64  `json:"snapshotTimestamp"`
		} `json:"model"`
	} `json:"viewCamBase"`
	ViewCam struct {
		StreamName  string            `json:"streamName"`
		ViewServers map[string]string `json:"viewServers"`
		IsCamActive bool              `json:"isCamActive"`
		Topic       string            `json:"topic"`
	} `json:"viewCam"`
}

// fetchStripchatPage fetches the Stripchat model page and parses embedded SSR state.
// Returns the parsed state and HTTP status (200 = model exists, 404 = not found).
// Uses the shared httpcloak Chrome-fingerprint transport - NOT http.DefaultTransport.
// (stripchat.com was removed from directHosts in httpcloak_transport.go.)
func fetchStripchatPage(ctx context.Context, req *internal.Req, username string) (*scPageState, int, error) {
	pageURL := "https://stripchat.com/" + username

	body, statusCode, err := req.GetBytesWithStatus(ctx, pageURL)
	if err != nil {
		return nil, 0, fmt.Errorf("stripchat: fetch page: %w", err)
	}

	html := string(body)
	const stateMarker = "window.__PRELOADED_STATE__"
	idx := strings.Index(html, stateMarker)
	if idx < 0 {
		return nil, statusCode, fmt.Errorf("stripchat: __PRELOADED_STATE__ not found in page (HTTP %d, len %d)", statusCode, len(html))
	}

	start := idx + len(stateMarker)
	for start < len(html) && (html[start] == ' ' || html[start] == '=') {
		start++
	}
	end := findJSONObjectEnd(html, start)
	if end < 0 {
		return nil, statusCode, fmt.Errorf("stripchat: could not find end of __PRELOADED_STATE__ JSON")
	}

	var state scPageState
	if err := json.Unmarshal([]byte(html[start:end]), &state); err != nil {
		return nil, statusCode, fmt.Errorf("stripchat: parse state: %w", err)
	}
	return &state, statusCode, nil
}

// findJSONObjectEnd returns the index after the closing } of the JSON object
// starting at pos in s. Returns -1 if not found or pos does not point at '{'.
func findJSONObjectEnd(s string, pos int) int {
	if pos >= len(s) || s[pos] != '{' {
		return -1
	}
	depth := 0
	inStr := false
	escape := false
	for i := pos; i < len(s); i++ {
		c := s[i]
		if escape {
			escape = false
			continue
		}
		if c == '\\' && inStr {
			escape = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

func mapGender(g string) string {
	switch g {
	case "female":
		return "f"
	case "male":
		return "m"
	case "couple":
		return "c"
	case "trans":
		return "t"
	default:
		return g
	}
}

// mouflonPDKey returns the manual Stripchat PD key if configured, otherwise
// "auto" so FetchPlaylist resolves the pkey from the master playlist and
// extracts/verifies the pdkey automatically.
func mouflonPDKey() string {
	if server.Config != nil && server.Config.StripchatPDKey != "" {
		return server.Config.StripchatPDKey
	}
	return "auto"
}

func (s *StripchatSite) FetchStream(ctx context.Context, req *internal.Req, username string) (*StreamInfo, error) {
	state, httpStatus, err := fetchStripchatPage(ctx, req, username)
	if err != nil {
		return nil, fmt.Errorf("stripchat: fetch stream: %w", err)
	}

	m := state.ViewCamBase.Model
	cam := state.ViewCam

	// 404 page = model does not exist
	if httpStatus == 404 {
		return nil, internal.ErrNotFound
	}

	// Build room status: Stripchat page state uses "off", "public", "private", etc.
	roomStatus := m.Status
	if roomStatus == "off" || (!m.IsOnline && !m.IsLive) {
		if roomStatus == "" || roomStatus == "off" {
			roomStatus = StatusOffline
		}
	}

	// Parse tags from topic
	var tags []string
	if cam.Topic != "" {
		for _, word := range strings.Fields(cam.Topic) {
			if strings.HasPrefix(word, "#") {
				tag := strings.TrimPrefix(word, "#")
				tag = strings.Trim(tag, ".,!?;:")
				if tag != "" {
					tags = append(tags, tag)
				}
			}
		}
	}

	// Thumbnail URL with cache-buster
	thumbURL := m.PreviewUrlThumbBig
	if thumbURL != "" {
		if strings.Contains(thumbURL, "?") {
			thumbURL = fmt.Sprintf("%s&t=%d", thumbURL, m.SnapshotTimestamp)
		} else {
			thumbURL = fmt.Sprintf("%s?t=%d", thumbURL, m.SnapshotTimestamp)
		}
	}

	info := &StreamInfo{
		RoomStatus:   roomStatus,
		RoomTitle:    cam.Topic,
		Tags:         tags,
		Gender:       mapGender(m.BroadcastGender),
		LiveThumbURL: thumbURL,
		// Stripchat CDNs require a stripchat.com Referer/Origin for media
		// requests, and MOUFLON segment decryption needs a pdkey.
		CDNReferer:   "https://stripchat.com/",
		MouflonPDKey: mouflonPDKey(),
	}

	// Model must be live and in a public show to record
	if !m.IsOnline && !m.IsLive {
		return info, internal.ErrChannelOffline
	}
	if !cam.IsCamActive {
		return info, internal.ErrChannelOffline
	}
	if roomStatus != "public" {
		return info, internal.ErrChannelOffline
	}

	// Build HLS URL from page-embedded stream data
	streamName := cam.StreamName
	if streamName == "" {
		return info, internal.ErrChannelOffline
	}

	var hlsURL string
	if svr, ok := cam.ViewServers["flashphoner-hls"]; ok && svr != "" {
		hlsURL = fmt.Sprintf(
			"https://b-%s.doppiocdn.com/hls/%s/master_%s.m3u8",
			svr, streamName, streamName,
		)
	} else {
		hlsURL = fmt.Sprintf(
			"https://edge-hls.doppiocdn.net/hls/%s/master/%s_auto.m3u8?playlistType=lowLatency",
			streamName, streamName,
		)
	}

	info.HLSSource = hlsURL
	return info, nil
}

// FetchLastBroadcast implements site.Site. Stripchat does not expose a
// last_broadcast timestamp in a usable form, so this returns 0.
func (s *StripchatSite) FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	return 0, nil
}

// FetchProfile implements site.Site. Stripchat exposes no public biocontext-style
// profile endpoint, so this returns nil, nil and callers skip the profile DB write.
func (s *StripchatSite) FetchProfile(ctx context.Context, req *internal.Req, username string) (*database.ChannelProfile, error) {
	return nil, nil
}

func (s *StripchatSite) GetRoomStatus(ctx context.Context, req *internal.Req, username string) (string, error) {
	state, httpStatus, err := fetchStripchatPage(ctx, req, username)
	if err != nil {
		return "", fmt.Errorf("stripchat: get room status: %w", err)
	}

	if httpStatus == 404 {
		return StatusOffline, nil
	}

	m := state.ViewCamBase.Model
	status := m.Status
	if status == "off" || status == "" {
		if !m.IsOnline && !m.IsLive {
			return StatusOffline, nil
		}
		return "unknown", nil
	}
	return status, nil
}

var _ Site = (*StripchatSite)(nil)
