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

type camResponse struct {
	Cam struct {
		StreamName        string            `json:"streamName"`
		IsCamActive       bool              `json:"isCamActive"`
		ViewServers       map[string]string `json:"viewServers"`
		BroadcastSettings struct {
			BroadcastType string `json:"broadcastType"`
		} `json:"broadcastSettings"`
		Topic string `json:"topic"`
	} `json:"cam"`
	User struct {
		User struct {
			ID                 int64  `json:"id"`
			Username           string `json:"username"`
			IsOnline           bool   `json:"isOnline"`
			IsLive             bool   `json:"isLive"`
			Status             string `json:"status"`
			BroadcastGender    string `json:"broadcastGender"`
			PreviewUrlThumbBig string `json:"previewUrlThumbBig"`
			SnapshotTimestamp  int64  `json:"snapshotTimestamp"`
		} `json:"user"`
	} `json:"user"`
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

func (s *StripchatSite) FetchStream(ctx context.Context, req *internal.Req, username string) (*StreamInfo, error) {
	apiURL := fmt.Sprintf("https://stripchat.com/api/front/v2/models/username/%s/cam", username)

	body, err := req.Get(ctx, apiURL)
	if err != nil {
		return nil, fmt.Errorf("stripchat: fetch cam: %w", err)
	}

	var resp camResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, fmt.Errorf("stripchat: parse cam response: %w", err)
	}

	u := resp.User.User

	// Build room status string from Stripchat's status field.
	roomStatus := u.Status
	if !u.IsOnline && !u.IsLive {
		if roomStatus == "" {
			roomStatus = StatusOffline
		}
	}

	tags := []string{}
	if resp.Cam.Topic != "" {
		for _, word := range strings.Fields(resp.Cam.Topic) {
			if strings.HasPrefix(word, "#") {
				tag := strings.TrimPrefix(word, "#")
				tag = strings.Trim(tag, ".,!?;:")
				if tag != "" {
					tags = append(tags, tag)
				}
			}
		}
	}

	// Set LiveThumbURL to the preview image so the frontend renders the
	// <img src="/api/thumb/:username"> tag. The ServeLiveThumb handler will
	// first extract a frame from the recording file via ffmpeg (giving a live
	// stream frame), and fall back to proxying this URL when no recording is
	// active (showing the static profile image rather than nothing at all).
	thumbURL := u.PreviewUrlThumbBig
	if thumbURL != "" {
		if strings.Contains(thumbURL, "?") {
			thumbURL = fmt.Sprintf("%s&t=%d", thumbURL, u.SnapshotTimestamp)
		} else {
			thumbURL = fmt.Sprintf("%s?t=%d", thumbURL, u.SnapshotTimestamp)
		}
	}

	info := &StreamInfo{
		RoomStatus:   roomStatus,
		RoomTitle:    resp.Cam.Topic,
		Tags:         tags,
		Gender:       mapGender(u.BroadcastGender),
		LiveThumbURL: thumbURL,
		// Stripchat CDNs require a stripchat.com Referer/Origin for media
		// requests, and MOUFLON segment decryption needs a pdkey.
		CDNReferer:   "https://stripchat.com/",
		MouflonPDKey: mouflonPDKey(),
	}

	if !u.IsOnline && !u.IsLive {
		return info, internal.ErrChannelOffline
	}
	if !resp.Cam.IsCamActive {
		return info, internal.ErrChannelOffline
	}
	if roomStatus != "public" {
		return info, internal.ErrChannelOffline
	}

	streamName := resp.Cam.StreamName

	var hlsURL string
	if server, ok := resp.Cam.ViewServers["flashphoner-hls"]; ok && server != "" {
		hlsURL = fmt.Sprintf(
			"https://b-%s.doppiocdn.com/hls/%s/master_%s.m3u8",
			server, streamName, streamName,
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

// mouflonPDKey returns the manual Stripchat PD key if configured, otherwise
// "auto" so FetchPlaylist resolves the pkey from the master playlist and
// extracts/verifies the pdkey automatically.
func mouflonPDKey() string {
	if server.Config != nil && server.Config.StripchatPDKey != "" {
		return server.Config.StripchatPDKey
	}
	return "auto"
}

// FetchLastBroadcast implements site.Site. Stripchat's model API does not
// expose a last_broadcast timestamp in a usable form, so this returns 0.
func (s *StripchatSite) FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	return 0, nil
}

// FetchProfile implements site.Site. Stripchat's front API exposes no public
// biocontext-style profile endpoint, so this returns nil, nil and callers skip
// the profile DB write for Stripchat channels.
func (s *StripchatSite) FetchProfile(ctx context.Context, req *internal.Req, username string) (*database.ChannelProfile, error) {
	return nil, nil
}

func (s *StripchatSite) GetRoomStatus(ctx context.Context, req *internal.Req, username string) (string, error) {
	apiURL := fmt.Sprintf("https://stripchat.com/api/front/v2/models/username/%s/cam", username)

	body, err := req.Get(ctx, apiURL)
	if err != nil {
		return "", fmt.Errorf("stripchat: get room status: %w", err)
	}

	var resp camResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return "", fmt.Errorf("stripchat: parse cam response: %w", err)
	}

	status := resp.User.User.Status
	if status == "" {
		if !resp.User.User.IsOnline && !resp.User.User.IsLive {
			return StatusOffline, nil
		}
		return "unknown", nil
	}
	return status, nil
}

var _ Site = (*StripchatSite)(nil)
