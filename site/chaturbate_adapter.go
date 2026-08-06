package site

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/database"
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

// biocontextResponse mirrors the site's api/biocontext/{username}/ payload.
// The recorder only needs a handful of fields; unknown keys are ignored.
// PhotoSets/SocialMedias are json.RawMessage because the site returns arrays
// of objects (e.g. {id, name, cover_url, …}); they are passed through
// verbatim into the JSONB channels columns.
type biocontextResponse struct {
	FollowerCount   int             `json:"follower_count"`
	Location        string          `json:"location"`
	RealName        string          `json:"real_name"`
	BodyDecorations string          `json:"body_decorations"`
	SmokeDrink      string          `json:"smoke_drink"`
	BodyType        string          `json:"body_type"`
	DisplayBirthday string          `json:"display_birthday"`
	DisplayAge      int             `json:"display_age"`
	AboutMe         string          `json:"about_me"`
	WishList        string          `json:"wish_list"`
	FanClubCost     int             `json:"fan_club_cost"`
	Sex             string          `json:"sex"`
	Subgender       string          `json:"subgender"`
	InterestedIn    []string        `json:"interested_in"`
	PhotoSets       json.RawMessage `json:"photo_sets"`
	SocialMedias    json.RawMessage `json:"social_medias"`
	LastBroadcast   string          `json:"last_broadcast"`
	RoomStatus      string          `json:"room_status"`
}

// fetchBiocontext fetches the api/biocontext/{username}/ payload through the
// shared Chaturbate adaptive rate limiter and circuit breaker, exactly like
// the chatvideocontext API. The biocontext endpoint is served by the same
// Cloudflare-protected host, so unthrottled bursts — e.g. 200+ channels all
// scraping profiles / last-broadcast at boot — trigger the "Just a moment..."
// challenge. Failure feedback also feeds the adaptive limiter so it backs off
// for ALL Chaturbate API traffic when the site starts blocking.
func (s *ChaturbateSite) fetchBiocontext(ctx context.Context, req *internal.Req, username string) (string, error) {
	apiURL := fmt.Sprintf("%sapi/biocontext/%s/", server.Config.Domain, username)

	var body string
	err := retry.Do(func() error {
		if err := internal.WaitForChaturbateRateLimit(ctx); err != nil {
			return err
		}
		if !internal.AllowChaturbateRequest() {
			return retry.Unrecoverable(internal.ErrCircuitBreakerOpen)
		}

		var e error
		body, e = req.Get(ctx, apiURL)
		if e != nil {
			internal.ReportChaturbateFailureUnlessExpected(e)
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
		retry.Attempts(3),
		retry.Delay(1*time.Second),
		retry.MaxDelay(10*time.Second),
		retry.DelayType(retry.BackOffDelay),
	)
	if err != nil {
		return "", err
	}
	return body, nil
}

// FetchProfile implements site.Site via the biocontext API, returning the
// model's full public profile so the archive site can display it.
func (s *ChaturbateSite) FetchProfile(ctx context.Context, req *internal.Req, username string) (*database.ChannelProfile, error) {
	body, err := s.fetchBiocontext(ctx, req, username)
	if err != nil {
		return nil, fmt.Errorf("fetch biocontext: %w", err)
	}

	var bio biocontextResponse
	if err := json.Unmarshal([]byte(body), &bio); err != nil {
		return nil, fmt.Errorf("parse biocontext: %w", err)
	}

	if bio.InterestedIn == nil {
		bio.InterestedIn = []string{}
	}
	// A literal `null` in the API response unmarshals into a non-nil
	// json.RawMessage holding "null". Coerce it to nil so omitempty drops it
	// from the PATCH body instead of writing null into a NOT NULL JSONB column.
	if bio.PhotoSets == nil || bytes.Equal(bytes.TrimSpace(bio.PhotoSets), []byte("null")) {
		bio.PhotoSets = nil
	}
	if bio.SocialMedias == nil || bytes.Equal(bytes.TrimSpace(bio.SocialMedias), []byte("null")) {
		bio.SocialMedias = nil
	}

	return &database.ChannelProfile{
		Username:        username,
		FollowerCount:   bio.FollowerCount,
		Location:        bio.Location,
		RealName:        bio.RealName,
		BodyDecorations: bio.BodyDecorations,
		SmokeDrink:      bio.SmokeDrink,
		BodyType:        bio.BodyType,
		DisplayBirthday: bio.DisplayBirthday,
		DisplayAge:      bio.DisplayAge,
		AboutMe:         bio.AboutMe,
		WishList:        bio.WishList,
		FanClubCost:     bio.FanClubCost,
		Sex:             bio.Sex,
		Subgender:       bio.Subgender,
		InterestedIn:    bio.InterestedIn,
		PhotoSets:       bio.PhotoSets,
		SocialMedias:    bio.SocialMedias,
		LastBroadcast:   bio.LastBroadcast,
		RoomStatus:      bio.RoomStatus,
	}, nil
}

// FetchLastBroadcast implements site.Site via the biocontext API.
func (s *ChaturbateSite) FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	body, err := s.fetchBiocontext(ctx, req, username)
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
		return "", internal.ErrCircuitBreakerOpen
	}

	var body string
	err := retry.Do(func() error {
		if err := internal.WaitForChaturbateRateLimit(ctx); err != nil {
			return err
		}
		if !internal.AllowChaturbateRequest() {
			return retry.Unrecoverable(internal.ErrCircuitBreakerOpen)
		}

		var e error
		body, e = req.Get(ctx, apiURL)
		if e != nil {
			internal.ReportChaturbateFailureUnlessExpected(e)
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
		retry.Attempts(3),
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
