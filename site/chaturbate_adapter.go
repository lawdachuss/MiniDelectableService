package site

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

// ChaturbateSite adapts the chaturbate package to the Site interface.
type ChaturbateSite struct{}

func NewChaturbateSite() *ChaturbateSite {
	return &ChaturbateSite{}
}

func (s *ChaturbateSite) FetchStream(ctx context.Context, req *internal.Req, username string) (*StreamInfo, error) {
	var roomInfo chaturbate.APIResponse
	stream, roomStatus, err := chaturbate.FetchStream(ctx, req, username, &roomInfo)
	si := &StreamInfo{
		RoomStatus:       roomStatus,
		RoomTitle:        roomInfo.RoomTitle,
		Tags:             roomInfo.Tags,
		NumUsers:         roomInfo.NumUsers,
		Gender:           roomInfo.BroadcasterGender,
		LiveThumbURL:     fmt.Sprintf("https://thumb.live.mmcdn.com/ri/%s.jpg", username),
		SummaryCardImage: roomInfo.SummaryCardImage,
	}
	if err != nil {
		return si, err
	}
	if stream == nil {
		return si, fmt.Errorf("get stream: %w", internal.ErrChannelOffline)
	}
	si.HLSSource = stream.HLSSource
	return si, nil
}

// FetchLastBroadcast implements site.Site via the biocontext API.
func (s *ChaturbateSite) FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	apiURL := fmt.Sprintf("%sapi/biocontext/%s/", server.Config.Domain, username)
	body, err := req.Get(ctx, apiURL)
	if err != nil {
		return 0, fmt.Errorf("fetch biocontext: %w", err)
	}
	var bio struct {
		LastBroadcast string `json:"last_broadcast"`
	}
	if err := json.Unmarshal([]byte(body), &bio); err != nil {
		return 0, fmt.Errorf("parse biocontext: %w", err)
	}
	if bio.LastBroadcast == "" {
		return 0, nil
	}
	t, err := time.Parse("2006-01-02T15:04:05.999", bio.LastBroadcast)
	if err != nil {
		return 0, fmt.Errorf("parse last_broadcast: %w", err)
	}
	return t.Unix(), nil
}

func (s *ChaturbateSite) GetRoomStatus(ctx context.Context, req *internal.Req, username string) (string, error) {
	apiURL := fmt.Sprintf("%sapi/chatvideocontext/%s/", server.Config.Domain, username)

	if !internal.AllowChaturbateRequest() {
		return "", fmt.Errorf("circuit breaker open: %w", internal.ErrChannelOffline)
	}

	var body string
	err := retry.Do(func() error {
		if err := internal.WaitForChaturbateRateLimit(ctx); err != nil {
			return err
		}
		if !internal.AllowChaturbateRequest() {
			return fmt.Errorf("circuit breaker open: %w", internal.ErrChannelOffline)
		}

		var e error
		body, e = req.Get(ctx, apiURL)
		if e != nil {
			internal.ReportChaturbateFailure()
			return e
		}
		if body == "" {
			internal.ReportChaturbateFailure()
			return fmt.Errorf("empty response body")
		}
		internal.ReportChaturbateSuccess()
		return nil
	},
		retry.Context(ctx),
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.MaxDelay(10*time.Second),
		retry.DelayType(retry.BackOffDelay),
	)
	if err != nil {
		return "", fmt.Errorf("failed to get API response: %w", err)
	}

	var resp chaturbate.APIResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return "", fmt.Errorf("failed to parse API response: %w", err)
	}

	return resp.RoomStatus, nil
}
