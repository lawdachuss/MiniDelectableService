package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// AffiliateModel represents a single model from the affiliate onlinerooms API.
// Endpoint: GET {domain}/affiliates/api/onlinerooms/?format=json&wm={wm}
//
// The endpoint is served by chaturbate.com AND by the cb.xxx domain this
// deployment uses (both are the same platform), so the caller passes the
// configured base domain (server.Config.Domain, default "https://www.cb.xxx/").
type AffiliateModel struct {
	Username      string `json:"username"`
	DisplayName   string `json:"display_name"`
	Gender        string `json:"gender"`
	Age           int    `json:"age"`
	NumUsers      int    `json:"num_users"`
	CurrentShow   string `json:"current_show"`
	ImageURL      string `json:"image_url"`
	ChatRoomURL   string `json:"chat_room_url"`
	RoomSubject   string `json:"room_subject"`
	IsHD          bool   `json:"is_hd"`
	IsNew         bool   `json:"is_new"`
	SecondsOnline int    `json:"seconds_online"`
	Tags          string `json:"tags"`
	Countries     string `json:"countries"`
}

// AffiliateAPIResult caches affiliate API results with a TTL.
type AffiliateAPIResult struct {
	mu        sync.RWMutex
	models    map[string]AffiliateModel
	fetchedAt time.Time
	ttl       time.Duration
}

var (
	affiliateCache   = &AffiliateAPIResult{ttl: 30 * time.Second}
	affiliateClient  = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:    2,
			IdleConnTimeout: 30 * time.Second,
		},
	}
	defaultAffiliateBase = "https://www.cb.xxx/"
)

// FetchAffiliateOnlineModels fetches all currently online models from the
// affiliate onlinerooms API on the given base domain (e.g. "https://www.cb.xxx/").
// Results are cached for ttl duration; a fresh cache returns without a network
// call, and a stale cache is served on error so a transient 5xx never marks
// every model offline. Returns a map keyed by lowercased username.
func FetchAffiliateOnlineModels(ctx context.Context, wmCode, baseURL string) (map[string]AffiliateModel, error) {
	if wmCode == "" {
		return nil, fmt.Errorf("affiliate WM code is empty")
	}
	if baseURL == "" {
		baseURL = defaultAffiliateBase
	}

	affiliateCache.mu.RLock()
	cached := affiliateCache.models
	cachedTime := affiliateCache.fetchedAt
	affiliateCache.mu.RUnlock()

	if cached != nil && time.Since(cachedTime) < affiliateCache.ttl {
		return cached, nil
	}

	affiliateCache.mu.Lock()
	defer affiliateCache.mu.Unlock()

	// Double-check after acquiring write lock
	if affiliateCache.models != nil && time.Since(affiliateCache.fetchedAt) < affiliateCache.ttl {
		return affiliateCache.models, nil
	}

	models, err := fetchAffiliateAPI(ctx, wmCode, baseURL)
	if err != nil {
		// Return stale cache on error if we have it
		if affiliateCache.models != nil {
			return affiliateCache.models, nil
		}
		return nil, err
	}

	affiliateCache.models = models
	affiliateCache.fetchedAt = time.Now()
	return models, nil
}

func fetchAffiliateAPI(ctx context.Context, wmCode, baseURL string) (map[string]AffiliateModel, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	apiURL := fmt.Sprintf("%s/affiliates/api/onlinerooms/?format=json&wm=%s", baseURL, wmCode)

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("affiliate: create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")

	resp, err := affiliateClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("affiliate: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("affiliate: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("affiliate: read body: %w", err)
	}

	var models []AffiliateModel
	if err := json.Unmarshal(body, &models); err != nil {
		return nil, fmt.Errorf("affiliate: parse response: %w", err)
	}

	result := make(map[string]AffiliateModel, len(models))
	for _, m := range models {
		result[strings.ToLower(m.Username)] = m
	}

	return result, nil
}

// CheckAffiliateLive checks a single username against the affiliate API.
// Returns (isLive bool, currentShow string, error). The affiliate list is
// authoritative for offline (a model not in the online list is offline);
// when present, currentShow tells us the type of broadcast.
func CheckAffiliateLive(ctx context.Context, wmCode, baseURL, username string) (bool, string, error) {
	models, err := FetchAffiliateOnlineModels(ctx, wmCode, baseURL)
	if err != nil {
		return false, "", err
	}

	model, found := models[strings.ToLower(username)]
	if !found {
		return false, "offline", nil
	}

	isLive := model.CurrentShow != "away" && model.CurrentShow != "offline"
	return isLive, model.CurrentShow, nil
}

// InvalidateAffiliateCache forces a re-fetch on the next call.
func InvalidateAffiliateCache() {
	affiliateCache.mu.Lock()
	defer affiliateCache.mu.Unlock()
	affiliateCache.models = nil
}
