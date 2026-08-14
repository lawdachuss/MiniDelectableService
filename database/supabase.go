package database

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Client represents a Supabase database client
type Client struct {
	URL    string
	APIKey string
	client *http.Client
}

// NewClient creates a new Supabase client
func NewClient(url, apiKey string) *Client {
	return &Client{
		URL:    url,
		APIKey: apiKey,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

// releaseBatchSize caps how many usernames go into one PostgREST PATCH filter
// URL. Supabase sits behind a proxy with an ~8KB URL limit (HTTP 414 "URI too
// long"); a single PATCH carrying the whole excess list (~850 usernames in the
// wild) was rejected every claim cycle, so an overloaded node could never shed
// channels and the fair-share rebalancer deadlocked the pool (one node hoarding
// ~900 channels while the rest of the fleet sat idle). 40 usernames is well
// under 1KB even with long names.
const releaseBatchSize = 40

// livenessBatchSize caps how many (username, site) pairs go into one
// SetChannelsLive/SetChannelsNotLive or= filter. Each escaped pair is ~45-55
// chars, so 30 pairs stay far below the ~8KB URL limit.
const livenessBatchSize = 30

// ============================================================================
// HTTP HELPERS
// ============================================================================

func (c *Client) request(method, path string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest(method, c.URL+"/rest/v1"+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("apikey", c.APIKey)
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "resolution=merge-duplicates,return=representation")
	}

	return c.client.Do(req)
}

// defaultMaxRetries is used for all Supabase calls unless a call opts into
// more (metadata saves use metadataSaveMaxRetries so a transient outage at
// finalize time cannot strand a recording without its DB row).
const defaultMaxRetries = 3

// metadataSaveMaxRetries applies to the SaveRecordingWithLinks path
// (SaveRecording, GetRecording, SaveUploadLinks): a recording's metadata is
// the last write before the local copy is deleted, so it gets extra attempts.
const metadataSaveMaxRetries = 10

// requestWithRetry executes the request and retries on transient errors:
// - 503 PGRST002 — schema cache rebuilding after migration
// - 400 PGRST204 — column not in schema cache yet (PostgREST needs to refresh)
func (c *Client) requestWithRetry(method, path string, body interface{}) (*http.Response, error) {
	return c.requestWithRetryN(method, path, body, defaultMaxRetries)
}

// requestWithRetryN is requestWithRetry with an explicit retry budget.
func (c *Client) requestWithRetryN(method, path string, body interface{}, maxRetries int) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := c.request(method, path, body)
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				backoff := retryBackoff(attempt)
				fmt.Printf("[WARN] Supabase request failed (attempt %d/%d), retrying in %v: %v\n", attempt+1, maxRetries, backoff, err)
				time.Sleep(backoff)
				continue
			}
			return nil, err
		}

		// Check for transient errors that need retry
		if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 || resp.StatusCode == 400 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			bodyStr := string(bodyBytes)

			// PGRST002: schema cache rebuilding after migration
			if resp.StatusCode == 503 && strings.Contains(bodyStr, "PGRST002") {
				lastErr = fmt.Errorf("HTTP 503: %s", bodyStr)
				backoff := retryBackoff(attempt)
				fmt.Printf("[WARN] Supabase schema cache rebuilding (attempt %d/%d), retrying in %v\n", attempt+1, maxRetries, backoff)
				resp.Body.Close()
				time.Sleep(backoff)
				continue
			}

			// PGRST204: column not yet in PostgREST schema cache
			if resp.StatusCode == 400 && strings.Contains(bodyStr, "PGRST204") {
				lastErr = fmt.Errorf("HTTP 400: %s", bodyStr)
				backoff := retryBackoff(attempt)
				fmt.Printf("[WARN] Supabase schema cache stale — column missing (attempt %d/%d), retrying in %v\n", attempt+1, maxRetries, backoff)
				resp.Body.Close()
				time.Sleep(backoff)
				continue
			}

			// Non-retryable error — return as-is
			if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
				lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyStr)
				resp.Body.Close()
				if attempt < maxRetries-1 {
					backoff := retryBackoff(attempt)
					fmt.Printf("[WARN] Supabase transient HTTP %d (attempt %d/%d), retrying in %v\n", resp.StatusCode, attempt+1, maxRetries, backoff)
					time.Sleep(backoff)
					continue
				}
				return nil, lastErr
			}

			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyStr)
		}

		return resp, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func retryBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<attempt) * 2 * time.Second
}

func (c *Client) get(path string, result interface{}) error {
	return c.getN(path, result, defaultMaxRetries)
}

func (c *Client) getN(path string, result interface{}, maxRetries int) error {
	resp, err := c.requestWithRetryN("GET", path, nil, maxRetries)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

func (c *Client) post(path string, body interface{}, result interface{}) error {
	resp, err := c.requestWithRetry("POST", path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if result != nil && resp.StatusCode != http.StatusNoContent {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

func (c *Client) patch(path string, body interface{}) error {
	resp, err := c.requestWithRetry("PATCH", path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *Client) delete(path string) error {
	resp, err := c.requestWithRetry("DELETE", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// ============================================================================
// CHANNELS
// ============================================================================

type Channel struct {
	ID          string `json:"id,omitempty"`
	Username    string `json:"username"`
	IsPaused    bool   `json:"is_paused"`
	Framerate   int    `json:"framerate"`
	Resolution  int    `json:"resolution"`
	Pattern     string `json:"pattern"`
	MaxDuration int    `json:"max_duration"`
	MaxFilesize int    `json:"max_filesize"`
	Compress    bool   `json:"compress"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

// SaveChannel creates or updates a channel using Supabase's upsert functionality.
// Uses on_conflict to atomically upsert by username, avoiding TOCTOU race conditions.
func (c *Client) SaveChannel(ch *Channel) error {
	resp, err := c.requestWithRetry("POST", "/channels?on_conflict=username", ch)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// ChannelProfile holds the full-profile fields scraped from the site's
// biocontext API and stored in the existing channels table (see
// database/migrate-combined.sql). Only the profile columns are
// touched — config columns (is_paused, framerate, pattern, …) stay intact.
//
// Every field except Username is omitempty so a partial API response never
// clobbers previously-stored values with zeros/empties: missing fields are
// simply left out of the PATCH body and the DB keeps its prior value.
// PhotoSets/SocialMedias are json.RawMessage because the site returns arrays
// of objects (not strings) that we pass through verbatim into the JSONB
// columns.
type ChannelProfile struct {
	Username         string          `json:"username"`
	FollowerCount    int             `json:"follower_count,omitempty"`
	Location         string          `json:"location,omitempty"`
	RealName         string          `json:"real_name,omitempty"`
	BodyDecorations  string          `json:"body_decorations,omitempty"`
	SmokeDrink       string          `json:"smoke_drink,omitempty"`
	BodyType         string          `json:"body_type,omitempty"`
	DisplayBirthday  string          `json:"display_birthday,omitempty"`
	DisplayAge       int             `json:"display_age,omitempty"`
	AboutMe          string          `json:"about_me,omitempty"`
	WishList         string          `json:"wish_list,omitempty"`
	FanClubCost      int             `json:"fan_club_cost,omitempty"`
	Sex              string          `json:"sex,omitempty"`
	Subgender        string          `json:"subgender,omitempty"`
	InterestedIn     []string        `json:"interested_in,omitempty"`
	PhotoSets        json.RawMessage `json:"photo_sets,omitempty"`
	SocialMedias     json.RawMessage `json:"social_medias,omitempty"`
	LastBroadcast    string          `json:"last_broadcast,omitempty"`
	RoomStatus       string          `json:"room_status,omitempty"`
	AvatarURL        string          `json:"avatar_url,omitempty"`
	ProfileScrapedAt string          `json:"profile_scraped_at,omitempty"`
}

// SaveChannelProfile PATCHes the profile columns of the existing channels row
// for the given username, and INSERTs a minimal row when none exists yet.
//
// Rows normally exist for every configured channel (created by
// SaveChannelsToDB), so the PATCH only ever updates profile fields and never
// clobbers recorder config columns. In distributed pool mode, however,
// LoadPooledConfig never syncs the channels table — so a pooled channel has
// no row and the old code dropped every scraped profile with a per-channel
// warning. The fallback INSERT (upsert by username, with created_at — the one
// NOT NULL column without a database default) keeps the profile data instead.
func (c *Client) SaveChannelProfile(p *ChannelProfile) error {
	p.ProfileScrapedAt = time.Now().UTC().Format(time.RFC3339)
	path := "/channels?username=eq." + url.QueryEscape(p.Username)

	// requestWithRetry sets Prefer: return=representation, so the response
	// body lists the rows that were patched — an empty list means no row
	// matched yet.
	resp, err := c.requestWithRetry("PATCH", path, p)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var patched []map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&patched)
	if len(patched) > 0 {
		return nil // row existed, profile columns updated in place
	}

	// No channels row yet (pooled mode) — create one. Every profile column is
	// omitempty and every config column has a database default, so the only
	// schema-required extra field is created_at (BIGINT NOT NULL, no default).
	bodyBytes, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal insert body: %w", err)
	}
	var insert map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &insert); err != nil {
		return fmt.Errorf("unmarshal insert body: %w", err)
	}
	insert["created_at"] = time.Now().Unix()

	// on_conflict=username makes a concurrent SaveChannelsToDB / another
	// scrape safe: if the row appeared between our PATCH and this POST, the
	// upsert just updates the profile columns again.
	insertResp, err := c.requestWithRetry("POST", "/channels?on_conflict=username", insert)
	if err != nil {
		return err
	}
	defer insertResp.Body.Close()
	if insertResp.StatusCode >= 400 {
		b, _ := io.ReadAll(insertResp.Body)
		return fmt.Errorf("insert returned %d: %s", insertResp.StatusCode, string(b))
	}
	fmt.Printf("[DEBUG] SaveChannelProfile(%q): inserted new channels row\n", p.Username)
	return nil
}

// GetChannel retrieves a channel by username
func (c *Client) GetChannel(username string) (*Channel, error) {
	var channels []Channel
	err := c.get(fmt.Sprintf("/channels?username=eq.%s&limit=1", url.QueryEscape(username)), &channels)
	if err != nil {
		return nil, err
	}
	if len(channels) == 0 {
		return nil, fmt.Errorf("channel not found")
	}
	return &channels[0], nil
}

// GetAllChannels retrieves all channels
func (c *Client) GetAllChannels() ([]Channel, error) {
	var channels []Channel
	err := c.get("/channels?order=created_at.desc&limit=50000", &channels)
	return channels, err
}

// ChannelExists reports whether a channel with the given username exists in
// the channels table, matching case-insensitively (cam-site usernames are
// case-insensitive, so "Alice" and "alice" are the same channel).
func (c *Client) ChannelExists(username string) (bool, error) {
	var channels []Channel
	err := c.get(fmt.Sprintf("/channels?username=ilike.%s&limit=1", url.QueryEscape(username)), &channels)
	if err != nil {
		return false, err
	}
	return len(channels) > 0, nil
}

// DeleteChannel removes a channel
func (c *Client) DeleteChannel(username string) error {
	return c.delete(fmt.Sprintf("/channels?username=eq.%s", url.QueryEscape(username)))
}

// deleteByUsernamesChunked DELETEs rows by username in small batches so each
// filter URL stays far below the ~8KB proxy limit (HTTP 414).
func (c *Client) deleteByUsernamesChunked(table string, usernames []string) error {
	for start := 0; start < len(usernames); start += releaseBatchSize {
		end := start + releaseBatchSize
		if end > len(usernames) {
			end = len(usernames)
		}
		batch := usernames[start:end]
		if err := c.delete(fmt.Sprintf("/%s?username=in.(%s)", table, joinEscaped(batch))); err != nil {
			return fmt.Errorf("delete %s batch %d/%d: %w", table, start/releaseBatchSize+1, (len(usernames)+releaseBatchSize-1)/releaseBatchSize, err)
		}
	}
	return nil
}

// DeleteChannelsNotIn removes all channel rows whose username is NOT in the
// provided list. Pass an empty slice to delete all channels.
//
// The old implementation built a single not.in.(keep...) filter listing every
// kept username — at scale that URL exceeds the ~8KB proxy limit (HTTP 414),
// and chunking an exclusion filter is unsafe (each chunk would delete the
// other chunks' keep rows). So we fetch the existing channels, compute the
// exact to-delete set, and DELETE it in small in.(...) batches instead.
func (c *Client) DeleteChannelsNotIn(usernames []string) error {
	if len(usernames) == 0 {
		return c.delete("/channels")
	}

	keep := make(map[string]bool, len(usernames))
	for _, u := range usernames {
		keep[u] = true
	}

	var existing []Channel
	if err := c.get("/channels?select=username&limit=50000", &existing); err != nil {
		return err
	}
	var toDelete []string
	for _, ch := range existing {
		if !keep[ch.Username] {
			toDelete = append(toDelete, ch.Username)
		}
	}
	if len(toDelete) == 0 {
		return nil
	}
	return c.deleteByUsernamesChunked("channels", toDelete)
}

// ============================================================================
// RECORDINGS
// ============================================================================

type Recording struct {
	ID           string   `json:"id,omitempty"`
	ChannelID    string   `json:"channel_id,omitempty"`
	Username     string   `json:"username"`
	Filename     string   `json:"filename"`
	Timestamp    string   `json:"timestamp"`
	RoomTitle    string   `json:"room_title,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	Viewers      int      `json:"viewers"`
	Resolution   string   `json:"resolution,omitempty"`
	Framerate    int      `json:"framerate"`
	Filesize     int64    `json:"filesize"`
	Duration     float64  `json:"duration,omitempty"`
	Gender       string   `json:"gender,omitempty"`
	// EndReason records why the recording stopped (model went offline, stream
	// session expired, max duration/filesize rotation, paused/stopped, session
	// boundary). Empty when unknown (e.g. orphan-recovery uploads).
	EndReason    string   `json:"end_reason,omitempty"`
	ThumbnailURL string   `json:"thumbnail_url,omitempty"`
	SpriteURL    string   `json:"sprite_url,omitempty"`
	PreviewURL   string   `json:"preview_url,omitempty"`
	EmbedURL     string   `json:"embed_url,omitempty"`
	InstanceID   string   `json:"instance_id,omitempty"`
	CreatedAt    string   `json:"created_at,omitempty"`
	UpdatedAt    string   `json:"updated_at,omitempty"`
}

// SaveRecording creates or updates a recording using Supabase's upsert functionality.
// Uses on_conflict to atomically upsert by filename, avoiding TOCTOU race conditions.
// Retries metadataSaveMaxRetries times: a recording's DB row is written before the
// local copy is deleted, so it must survive transient Supabase outages.
func (c *Client) SaveRecording(rec *Recording) error {
	resp, err := c.requestWithRetryN("POST", "/recordings?on_conflict=filename", rec, metadataSaveMaxRetries)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// GetRecording retrieves a recording by filename
func (c *Client) GetRecording(filename string) (*Recording, error) {
	var recordings []Recording
	err := c.getN(fmt.Sprintf("/recordings?filename=eq.%s&limit=1", url.QueryEscape(filename)), &recordings, metadataSaveMaxRetries)
	if err != nil {
		return nil, err
	}
	if len(recordings) == 0 {
		return nil, fmt.Errorf("recording not found")
	}
	return &recordings[0], nil
}

// HasUploadedLinks returns true when the recording identified by filename has
// at least one persisted upload_link.  The recordings row is created at enqueue
// time (SaveRecordingBasics) — BEFORE any upload — so "row exists" is not proof
// that the file's content reached a host.  Disk cleanup must only delete local
// files whose content is actually safe in the cloud.
func (c *Client) HasUploadedLinks(filename string) (bool, error) {
	var recordings []struct {
		UploadLinks []UploadLink `json:"upload_links"`
	}
	err := c.getN(fmt.Sprintf("/recordings?filename=eq.%s&select=upload_links(host)&limit=1",
		url.QueryEscape(filename)), &recordings, metadataSaveMaxRetries)
	if err != nil {
		return false, err
	}
	if len(recordings) == 0 {
		return false, nil
	}
	return len(recordings[0].UploadLinks) > 0, nil
}

// GetRecordingsByUsername retrieves all recordings for a username
func (c *Client) GetRecordingsByUsername(username string) ([]Recording, error) {
	var recordings []Recording
	err := c.get(fmt.Sprintf("/recordings?username=eq.%s&order=timestamp.desc", url.QueryEscape(username)), &recordings)
	return recordings, err
}

// GetAllRecordings retrieves all recordings
func (c *Client) GetAllRecordings() ([]Recording, error) {
	var recordings []Recording
	err := c.get("/recordings?order=timestamp.desc&limit=50000", &recordings)
	return recordings, err
}

// DeleteRecording removes a recording
func (c *Client) DeleteRecording(filename string) error {
	return c.delete(fmt.Sprintf("/recordings?filename=eq.%s", url.QueryEscape(filename)))
}

// DeletePreviewImage removes a preview image by filename
func (c *Client) DeletePreviewImage(filename string) error {
	return c.delete(fmt.Sprintf("/preview_images?filename=eq.%s", url.QueryEscape(filename)))
}

// DeleteUploadLinksByRecordingID removes all upload links for a recording
func (c *Client) DeleteUploadLinksByRecordingID(recordingID string) error {
	return c.delete(fmt.Sprintf("/upload_links?recording_id=eq.%s", url.QueryEscape(recordingID)))
}

// ============================================================================
// UPLOAD LINKS
// ============================================================================

type UploadLink struct {
	ID          string `json:"id,omitempty"`
	RecordingID string `json:"recording_id"`
	Host        string `json:"host"`
	URL         string `json:"url"`
	UploadedAt  string `json:"uploaded_at,omitempty"`
}

// SaveUploadLink creates or updates an upload link.
// Uses on_conflict to atomically upsert by (recording_id, host), making
// repeated calls idempotent and preventing duplicate rows.
func (c *Client) SaveUploadLink(link *UploadLink) error {
	resp, err := c.requestWithRetry("POST", "/upload_links?on_conflict=recording_id,host", link)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// SaveUploadLinks batch-saves all upload links in a single request.
// Uses on_conflict to upsert by (recording_id, host).
// Part of the SaveRecordingWithLinks metadata path, so it retries
// metadataSaveMaxRetries times like SaveRecording/GetRecording.
func (c *Client) SaveUploadLinks(links []UploadLink) error {
	resp, err := c.requestWithRetryN("POST", "/upload_links?on_conflict=recording_id,host", links, metadataSaveMaxRetries)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// GetUploadLinks retrieves all upload links for a recording
func (c *Client) GetUploadLinks(recordingID string) ([]UploadLink, error) {
	var links []UploadLink
	err := c.get(fmt.Sprintf("/upload_links?recording_id=eq.%s", url.QueryEscape(recordingID)), &links)
	return links, err
}

// GetAllUploadLinks retrieves ALL upload links in a single batch query.
// The caller can group by recording_id for O(1) per-recording lookup.
func (c *Client) GetAllUploadLinks() ([]UploadLink, error) {
	var links []UploadLink
	err := c.get("/upload_links?limit=50000", &links)
	return links, err
}

// CountUploadedVideosBelowDuration returns how many recordings with a probed
// duration strictly between 0 and thresholdSeconds seconds have been uploaded
// (i.e. have at least one row in upload_links). Recordings whose duration is
// 0 or NULL (ffprobe miss) are excluded.
//
// It delegates to ListUploadedVideosBelowDuration so both helpers share the
// same two-query logic, threshold guard and 50000-row cap. The two reads are
// not atomic, so a video uploaded in the brief window between them may be
// missed — fine for a monitoring/stats count.
func (c *Client) CountUploadedVideosBelowDuration(thresholdSeconds float64) (int, error) {
	list, err := c.ListUploadedVideosBelowDuration(thresholdSeconds)
	if err != nil {
		return 0, err
	}
	return len(list), nil
}

// RecordingWithLinks is one uploaded recording paired with the URLs it was
// uploaded to, keyed by upload host (e.g. "GoFile", "Streamtape", "Vidara").
type RecordingWithLinks struct {
	ID        string            `json:"id"`
	Username  string            `json:"username"`
	Filename  string            `json:"filename"`
	Timestamp string            `json:"timestamp"`
	Duration  float64           `json:"duration"`
	Filesize  int64             `json:"filesize"`
	Links     map[string]string `json:"links"` // upload host -> URL
}

// ListUploadedVideosBelowDuration returns the recordings with a probed
// duration strictly between 0 and thresholdSeconds seconds that have been
// uploaded (i.e. have at least one row in upload_links), each with its upload
// links keyed by host. Results are ordered by duration ascending (shortest
// first) so the smallest uploads surface first for review. Recordings whose
// duration is 0 or NULL (ffprobe miss) and recordings with no upload links
// are excluded.
//
// Like CountUploadedVideosBelowDuration it uses two batched GETs (upload_links
// then duration-filtered recordings) intersected in memory, because PostgREST's
// embedded-resource join is not usable on deployments whose schema cache lacks
// the recordings → upload_links foreign key (PGRST200). The result is capped
// at 50000 matching recordings, matching the other GetAll* helpers.
func (c *Client) ListUploadedVideosBelowDuration(thresholdSeconds float64) ([]RecordingWithLinks, error) {
	if thresholdSeconds <= 0 || math.IsNaN(thresholdSeconds) || math.IsInf(thresholdSeconds, 0) {
		return nil, nil
	}

	// Step 1: group upload links by recording id.
	links, err := c.GetAllUploadLinks()
	if err != nil {
		return nil, err
	}
	byRecording := make(map[string][]UploadLink)
	for _, l := range links {
		if l.RecordingID != "" {
			byRecording[l.RecordingID] = append(byRecording[l.RecordingID], l)
		}
	}
	if len(byRecording) == 0 {
		return nil, nil
	}

	// Step 2: recordings with 0 < duration < threshold, keeping only uploaded ones.
	// The duration filters run server-side so only eligible rows are transferred.
	path := fmt.Sprintf("/recordings?select=id,username,filename,timestamp,duration,filesize&duration=gt.0&duration=lt.%s&limit=50000",
		strconv.FormatFloat(thresholdSeconds, 'f', -1, 64))
	var recs []Recording
	if err := c.get(path, &recs); err != nil {
		return nil, err
	}

	var out []RecordingWithLinks
	for _, r := range recs {
		recLinks, ok := byRecording[r.ID]
		if !ok || len(recLinks) == 0 {
			continue
		}
		linksMap := make(map[string]string, len(recLinks))
		for _, l := range recLinks {
			if l.Host != "" {
				linksMap[l.Host] = l.URL
			}
		}
		out = append(out, RecordingWithLinks{
			ID:        r.ID,
			Username:  r.Username,
			Filename:  r.Filename,
			Timestamp: r.Timestamp,
			Duration:  r.Duration,
			Filesize:  r.Filesize,
			Links:     linksMap,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Duration < out[j].Duration })
	return out, nil
}

// ============================================================================
// APP SETTINGS
// ============================================================================

type AppSetting struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	UpdatedAt string          `json:"updated_at,omitempty"`
}

// SaveSetting creates or updates an app setting
func (c *Client) SaveSetting(key string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	setting := &AppSetting{
		Key:   key,
		Value: jsonValue,
	}

	// Upsert using Prefer header
	resp, err := c.requestWithRetry("POST", "/app_settings", setting)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// GetSetting retrieves an app setting
func (c *Client) GetSetting(key string, result interface{}) error {
	var settings []AppSetting
	err := c.get(fmt.Sprintf("/app_settings?key=eq.%s&limit=1", url.QueryEscape(key)), &settings)
	if err != nil {
		return err
	}
	if len(settings) == 0 {
		return fmt.Errorf("setting not found")
	}

	return json.Unmarshal(settings[0].Value, result)
}

// ============================================================================
// TUNNELS
// ============================================================================

type Tunnel struct {
	ID         string `json:"id,omitempty"`
	URL        string `json:"url"`
	RunID      int    `json:"run_id"`
	InstanceID string `json:"instance_id,omitempty"`
	IsActive   bool   `json:"is_active"`
	CreatedAt  string `json:"created_at,omitempty"`
	ExpiresAt  string `json:"expires_at,omitempty"`
}

// SaveTunnel creates a new tunnel
func (c *Client) SaveTunnel(tunnel *Tunnel) error {
	var result []Tunnel
	return c.post("/tunnels", tunnel, &result)
}

// GetActiveTunnel retrieves the most recent active tunnel for the given instance
func (c *Client) GetActiveTunnel(instanceID string) (*Tunnel, error) {
	var tunnels []Tunnel
	err := c.get(fmt.Sprintf("/tunnels?is_active=eq.true&instance_id=eq.%s&order=created_at.desc&limit=1", url.QueryEscape(instanceID)), &tunnels)
	if err != nil {
		return nil, err
	}
	if len(tunnels) == 0 {
		return nil, fmt.Errorf("no active tunnel found")
	}
	return &tunnels[0], nil
}

// DeactivateOldTunnels marks all tunnels as inactive for the given instance
func (c *Client) DeactivateOldTunnels(instanceID string) error {
	return c.patch(fmt.Sprintf("/tunnels?is_active=eq.true&instance_id=eq.%s", url.QueryEscape(instanceID)), map[string]interface{}{
		"is_active": false,
	})
}

// ============================================================================
// CHANNEL LOGS
// ============================================================================

type ChannelLog struct {
	ID        string `json:"id,omitempty"`
	ChannelID string `json:"channel_id,omitempty"`
	Username  string `json:"username"`
	LogLevel  string `json:"log_level"`
	Message   string `json:"message"`
	CreatedAt string `json:"created_at,omitempty"`
}

// SaveLog creates a new log entry
func (c *Client) SaveLog(log *ChannelLog) error {
	var result []ChannelLog
	return c.post("/channel_logs", log, &result)
}

// GetLogs retrieves logs for a channel
func (c *Client) GetLogs(username string, limit int) ([]ChannelLog, error) {
	var logs []ChannelLog
	err := c.get(fmt.Sprintf("/channel_logs?username=eq.%s&order=created_at.desc&limit=%d", url.QueryEscape(username), limit), &logs)
	return logs, err
}

// ============================================================================
// PREVIEW IMAGES
// ============================================================================

type PreviewImage struct {
	ID           string `json:"id,omitempty"`
	RecordingID  string `json:"recording_id,omitempty"`
	Filename     string `json:"filename"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
	SpriteURL    string `json:"sprite_url,omitempty"`
	PreviewURL   string `json:"preview_url,omitempty"`
	InstanceID   string `json:"instance_id,omitempty"`
	UploadedAt   string `json:"uploaded_at,omitempty"`
}

// SavePreviewImage creates or updates preview image metadata using Supabase's upsert functionality.
// Uses on_conflict to atomically upsert by filename, avoiding TOCTOU race conditions.
func (c *Client) SavePreviewImage(img *PreviewImage) error {
	resp, err := c.requestWithRetry("POST", "/preview_images?on_conflict=filename", img)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// GetPreviewImage retrieves preview image metadata
func (c *Client) GetPreviewImage(filename string) (*PreviewImage, error) {
	var images []PreviewImage
	err := c.get(fmt.Sprintf("/preview_images?filename=eq.%s&limit=1", url.QueryEscape(filename)), &images)
	if err != nil {
		return nil, err
	}
	if len(images) == 0 {
		return nil, fmt.Errorf("preview image not found")
	}
	return &images[0], nil
}

// GetAllPreviewImages returns all preview images from the database.
func (c *Client) GetAllPreviewImages() ([]PreviewImage, error) {
	var images []PreviewImage
	err := c.get("/preview_images?limit=50000", &images)
	return images, err
}

// ============================================================================
// DISK USAGE
// ============================================================================

type DiskUsage struct {
	ID          string `json:"id,omitempty"`
	NodeID      string `json:"node_id,omitempty"`
	TotalBytes  int64  `json:"total_bytes"`
	UsedBytes   int64  `json:"used_bytes"`
	FreeBytes   int64  `json:"free_bytes"`
	PercentUsed int    `json:"percent_used"`
	RecordedAt  string `json:"recorded_at,omitempty"`
}

// SaveDiskUsage records current disk usage
func (c *Client) SaveDiskUsage(usage *DiskUsage) error {
	var result []DiskUsage
	return c.post("/disk_usage", usage, &result)
}

// GetLatestDiskUsage retrieves the most recent disk usage record
func (c *Client) GetLatestDiskUsage() (*DiskUsage, error) {
	var usages []DiskUsage
	err := c.get("/disk_usage?order=recorded_at.desc&limit=1", &usages)
	if err != nil {
		return nil, err
	}
	if len(usages) == 0 {
		return nil, fmt.Errorf("no disk usage records found")
	}
	return &usages[0], nil
}

// ============================================================================
// UPLOAD JOURNAL
// ============================================================================

type UploadJournal struct {
	ID         string `json:"id,omitempty"`
	FileHash   string `json:"file_hash"`
	Filename   string `json:"filename"`
	Host       string `json:"host"`
	Status     string `json:"status"`
	ErrorMsg   string `json:"error_msg,omitempty"`
	FileSize   int64  `json:"file_size,omitempty"`
	InstanceID string `json:"instance_id,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

// SaveJournalEntry creates or updates an upload journal entry.
// Uses on_conflict to upsert by (file_hash, host).
func (c *Client) SaveJournalEntry(entry *UploadJournal) error {
	resp, err := c.requestWithRetry("POST", "/upload_journal?on_conflict=file_hash,host", entry)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// GetJournalByHash retrieves all journal entries for a given file hash.
func (c *Client) GetJournalByHash(fileHash string) ([]UploadJournal, error) {
	var entries []UploadJournal
	err := c.get(fmt.Sprintf("/upload_journal?file_hash=eq.%s&order=host.asc", url.QueryEscape(fileHash)), &entries)
	return entries, err
}

// GetJournalEntriesByStatus retrieves all journal entries with a given status.
func (c *Client) GetJournalEntriesByStatus(status string) ([]UploadJournal, error) {
	var entries []UploadJournal
	err := c.get(fmt.Sprintf("/upload_journal?status=eq.%s&order=created_at.desc", url.QueryEscape(status)), &entries)
	return entries, err
}

// DeleteJournalByHash removes all journal entries for a file hash (e.g. after local file is deleted).
func (c *Client) DeleteJournalByHash(fileHash string) error {
	return c.delete(fmt.Sprintf("/upload_journal?file_hash=eq.%s", url.QueryEscape(fileHash)))
}

// ============================================================================
// PIPELINE STATES
// ============================================================================
//
// Schema defined in migrate-combined.sql (CREATE TABLE pipeline_states).

type PipelineState struct {
	FileHash     string `json:"file_hash"`
	FilePath     string `json:"file_path"`
	Filename     string `json:"filename"`
	Username     string `json:"username"`
	FileSize     int64  `json:"file_size"`
	CurrentStage string `json:"current_stage"`
	Failed       bool   `json:"failed"`
	LastError    string `json:"last_error,omitempty"`
	ThumbURL     string `json:"thumb_url,omitempty"`
	SpriteURL    string `json:"sprite_url,omitempty"`
	PreviewURL   string `json:"preview_url,omitempty"`
	EmbedURL     string `json:"embed_url,omitempty"`
	LinksJSON    string `json:"links,omitempty"` // JSON-encoded map[string]string
	Retries      int    `json:"retries,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

// SavePipelineState upserts a pipeline state by file_hash.
func (c *Client) SavePipelineState(state *PipelineState) error {
	resp, err := c.requestWithRetry("POST", "/pipeline_states?on_conflict=file_hash", state)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// LoadAllPipelineStates retrieves all pipeline states (for crash recovery on restart).
func (c *Client) LoadAllPipelineStates() ([]PipelineState, error) {
	var states []PipelineState
	err := c.get("/pipeline_states?order=created_at.asc", &states)
	return states, err
}

// DeletePipelineState removes a pipeline state by file hash.
func (c *Client) DeletePipelineState(fileHash string) error {
	return c.delete(fmt.Sprintf("/pipeline_states?file_hash=eq.%s", url.QueryEscape(fileHash)))
}

// ============================================================================
// NODES (distributed shards)
// ============================================================================

// Node represents a worker node in the distributed recording system.
type Node struct {
	NodeID          string `json:"node_id"`
	Hostname        string `json:"hostname"`
	InstanceLabel   string `json:"instance_label"`
	SoftwareVersion string `json:"software_version"`
	Status          string     `json:"status"`
	CurrentLoad     int        `json:"current_load"`
	LastHeartbeat   string     `json:"last_heartbeat,omitempty"`
	WebURL          string     `json:"web_url"`
	SessionDeadline *time.Time `json:"session_deadline,omitempty"`
	CreatedAt       string     `json:"created_at,omitempty"`
	UpdatedAt       string     `json:"updated_at,omitempty"`
}

// UpsertNode registers or updates a node.
func (c *Client) UpsertNode(node *Node) error {
	resp, err := c.requestWithRetry("POST", "/nodes?on_conflict=node_id", node)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// HeartbeatNode updates the last_heartbeat timestamp and current load for a node.
func (c *Client) HeartbeatNode(nodeID string, currentLoad int) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), map[string]interface{}{
		"last_heartbeat": "now()",
		"current_load":   currentLoad,
	})
}

// EnsureNodeOnline sets status=online for a node that is currently offline or
// unknown.  Used by the heartbeat loop to recover from a "stuck offline"
// state (e.g. reaper marked offline during a restart gap).  Does nothing if
// the node is already online or draining.
func (c *Client) EnsureNodeOnline(nodeID string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s&status=neq.online&status=neq.draining", url.QueryEscape(nodeID)), map[string]interface{}{
		"status": "online",
	})
}

// UpdateNodeStatus changes the node's status (online/offline/draining).
func (c *Client) UpdateNodeStatus(nodeID, status string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), map[string]interface{}{
		"status": status,
	})
}

// ResetNodeLoad zeroes a node's current_load. current_load is only ever
// written by the node's own heartbeat, so a node that goes offline or dies
// would otherwise carry a frozen load forever — inflating the dashboard's
// "Total Load" with channels that were already reclaimed. Called by the
// reaper and graceful shutdown after the node's assignments are released.
func (c *Client) ResetNodeLoad(nodeID string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), map[string]interface{}{
		"current_load": 0,
	})
}

// UpdateNodeWebURL sets the public web URL for a node.  Used by the cloudflared
// tunnel reporter so the admin panel's "Visit" link reflects the live tunnel.
func (c *Client) UpdateNodeWebURL(nodeID, webURL string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), map[string]interface{}{
		"web_url": webURL,
	})
}

// GetNode retrieves a single node by ID.
func (c *Client) GetNode(nodeID string) (*Node, error) {
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?node_id=eq.%s&limit=1", url.QueryEscape(nodeID)), &nodes)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return &nodes[0], nil
}

// GetAllNodes returns all registered nodes, ordered by node_id.
func (c *Client) GetAllNodes() ([]Node, error) {
	var nodes []Node
	err := c.get("/nodes?order=node_id.asc", &nodes)
	return nodes, err
}

// GetAliveNodes returns all nodes with status=online and recent heartbeat.
func (c *Client) GetAliveNodes() ([]Node, error) {
	cutoff := time.Now().Add(-180 * time.Second).UTC().Format(time.RFC3339)
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?status=eq.online&last_heartbeat=gt.%s&order=node_id.asc", url.QueryEscape(cutoff)), &nodes)
	return nodes, err
}

// GetDeadNodes returns node IDs whose heartbeat is older than the timeout.
// Includes draining/offline nodes — if a draining node hasn't heartbeated
// inside the timeout it's effectively dead (e.g. killed by the GitHub 6h
// limit), so its channels must be reclaimed or they stay stuck assigned to a
// node that will never release them.
func (c *Client) GetDeadNodes(timeout time.Duration) ([]string, error) {
	cutoff := time.Now().Add(-timeout).UTC().Format(time.RFC3339)
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?last_heartbeat=lt.%s&select=node_id", url.QueryEscape(cutoff)), &nodes)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.NodeID
	}
	return ids, nil
}

// GetNodesWithImminentDeadline returns online nodes whose session_deadline is
// within `window` from now (strictly in the FUTURE). Used to migrate a node's
// channels away BEFORE the node is killed (e.g. GitHub's 6-hour runner limit).
//
// session_deadline=gt.now() is critical: a deadline that has already PASSED
// must not count as "imminent", otherwise the 60s migration cycle keeps
// draining a node that is still alive and heartbeating (its session restart
// simply hasn't fired) while its claim loop re-claims the channels — an
// infinite claim→migrate→reclaim churn that pins channels to no node and
// overloads the migration targets. Past deadlines are the reaper's job: if the
// node truly dies, its channels are reclaimed after the heartbeat timeout.
func (c *Client) GetNodesWithImminentDeadline(window time.Duration) ([]Node, error) {
	cutoff := time.Now().Add(window).UTC().Format(time.RFC3339)
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?session_deadline=not.is.null&session_deadline=gt.now()&session_deadline=lt.%s&status=eq.online&order=node_id.asc",
		url.QueryEscape(cutoff)), &nodes)
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// ReassignChannel atomically moves a channel_assignments row from one node to
// another via the reassign_channel RPC (SELECT ... FOR UPDATE SKIP LOCKED), so
// even when several nodes race to migrate the same channel only one wins.
func (c *Client) ReassignChannel(username, site, fromNode, toNode string) error {
	body := map[string]interface{}{
		"p_username":  username,
		"p_site":      site,
		"p_from_node": fromNode,
		"p_to_node":   toNode,
	}
	resp, err := c.requestWithRetry("POST", "/rpc/reassign_channel", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// ============================================================================
// CHANNEL ASSIGNMENTS
// ============================================================================

// ChannelAssignment represents the assignment of a channel to a node.
type ChannelAssignment struct {
	Username      string `json:"username"`
	Site          string `json:"site"`
	AssignedNode  string `json:"assigned_node,omitempty"`
	Status        string `json:"status"`
	IsLive        bool   `json:"is_live"`
	LiveCheckedAt string `json:"live_checked_at,omitempty"`
	AssignedAt    string `json:"assigned_at,omitempty"`
	LastHeartbeat string `json:"last_heartbeat,omitempty"`
	// Config snapshot
	Framerate               int    `json:"framerate"`
	Resolution              int    `json:"resolution"`
	Pattern                 string `json:"pattern"`
	MaxDuration             int    `json:"max_duration"`
	MaxFilesize             int    `json:"max_filesize"`
	Compress                bool   `json:"compress"`
	MinDurationBeforeUpload int    `json:"min_duration_before_upload"`
	CreatedAt               string `json:"created_at,omitempty"`
	UpdatedAt               string `json:"updated_at,omitempty"`
}

// AssignmentStats holds summary statistics for fair-share calculation.
type AssignmentStats struct {
	TotalPoolChannels  int `json:"total_pool_channels"`
	TotalLiveChannels  int `json:"total_live_channels"`
	TotalUnassigned    int `json:"total_unassigned"`
	TotalAssignedNodes int `json:"total_assigned_nodes"`
	TotalAliveNodes    int `json:"total_alive_nodes"`
}

// claimRPC posts to a claim_* RPC with the standard (p_node_id, p_limit)
// payload and decodes the claimed rows. All claim RPCs use SELECT ... FOR
// UPDATE SKIP LOCKED so two nodes can never claim the same channel
// concurrently (no TOCTOU between a GET and a PATCH).
func (c *Client) claimRPC(rpcName, nodeID string, limit int) ([]ChannelAssignment, error) {
	body := map[string]interface{}{
		"p_node_id": nodeID,
		"p_limit":   limit,
	}

	resp, err := c.requestWithRetry("POST", "/rpc/"+rpcName, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var claimed []ChannelAssignment
	if err := json.NewDecoder(resp.Body).Decode(&claimed); err != nil {
		return nil, fmt.Errorf("decode claimed: %w", err)
	}
	return claimed, nil
}

// ClaimChannels atomically claims up to `limit` unassigned channels for this
// node, regardless of is_live. Kept for compatibility; the claim cycle now
// prefers ClaimOfflineChannels/ClaimLiveChannels so offline budget claims can
// never sweep live channels.
func (c *Client) ClaimChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	return c.claimRPC("claim_channels", nodeID, limit)
}

// ClaimOfflineChannels atomically claims up to `limit` unassigned OFFLINE
// channels (is_live=false) for this node via the claim_offline_channels RPC.
// Used by the claim cycle to fill a node's offline fair-share budget without
// accidentally absorbing live channels that should be spread across nodes.
func (c *Client) ClaimOfflineChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	return c.claimRPC("claim_offline_channels", nodeID, limit)
}

// ClaimLiveChannels atomically claims up to `limit` unassigned LIVE channels
// (is_live=true) for this node via the claim_live_channels RPC. The claim
// cycle uses it to claim live channels only up to the node's live fair share
// (ceil(totalLive / aliveNodes)), so live channels are spread across nodes
// instead of being swept wholesale by whichever node had offline budget room
// after a reclaim.
func (c *Client) ClaimLiveChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	return c.claimRPC("claim_live_channels", nodeID, limit)
}

// ClaimSpecificChannel atomically claims one specific channel for this node.
// Uses the PostgreSQL claim_specific_channel RPC (SELECT ... FOR UPDATE SKIP
// LOCKED) to prevent two nodes from claiming the same channel concurrently.
// Returns true if the channel was successfully claimed, false if it was
// already taken.
func (c *Client) ClaimSpecificChannel(username, site, nodeID string) (bool, error) {
	body := map[string]interface{}{
		"p_username": username,
		"p_site":     site,
		"p_node_id":  nodeID,
	}

	resp, err := c.requestWithRetry("POST", "/rpc/claim_specific_channel", body)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var claimed []ChannelAssignment
	if err := json.NewDecoder(resp.Body).Decode(&claimed); err != nil {
		return false, fmt.Errorf("decode claimed: %w", err)
	}
	return len(claimed) > 0, nil
}

// ReleaseNodeChannels releases all channels currently assigned to a node.
func (c *Client) ReleaseNodeChannels(nodeID string) error {
	return c.patch(fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned", url.QueryEscape(nodeID)),
		map[string]interface{}{
			"assigned_node": nil,
			"status":        "unassigned",
		})
}

// ReleaseNodeOfflineChannels releases a node's OFFLINE channels back to the
// pool with a single filter-based PATCH. Unlike ReleaseExcessOfflineChannels
// there is no username in-list, so the URL stays tiny and can never hit the
// ~8KB proxy limit (HTTP 414) no matter how many channels the node holds. Only
// is_live=false, non-recording channels match, so live broadcasts and
// in-progress recordings are never disturbed. excludeUsernames (e.g.
// user-paused channels) stay assigned to the node. Returns the number of
// channels released.
func (c *Client) ReleaseNodeOfflineChannels(nodeID string, excludeUsernames []string) (int, error) {
	filter := fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&is_live=eq.false&status=neq.recording&status=neq.unassigned",
		url.QueryEscape(nodeID))
	if len(excludeUsernames) > 0 {
		filter += "&username=not.in.(" + joinEscaped(excludeUsernames) + ")"
	}

	// Count what the PATCH will release (drives the caller's log message).
	var matches []ChannelAssignment
	if err := c.get(filter+"&select=username&limit=50000", &matches); err != nil {
		return 0, err
	}
	if len(matches) == 0 {
		return 0, nil
	}

	// Release them. The filter re-evaluates atomically inside the UPDATE, so a
	// channel that went live since the count is not swept.
	if err := c.patch(filter, map[string]interface{}{
		"assigned_node": nil,
		"status":        "unassigned",
	}); err != nil {
		return 0, err
	}
	return len(matches), nil
}

// releaseChunked PATCHes the given usernames back to unassigned for this node
// in small batches (releaseBatchSize per request). The release is idempotent —
// a row already released simply matches nothing — so on a failed batch the
// caller receives the rows released so far plus the error, and the next claim
// cycle picks up the remainder. Without chunking, one giant username=in.(...)
// filter exceeds the ~8KB proxy URL limit (HTTP 414) and the release fails
// wholesale, which is what wedged the fleet's rebalancer.
func (c *Client) releaseChunked(nodeID string, usernames []string) ([]ChannelAssignment, error) {
	var released []ChannelAssignment
	totalBatches := (len(usernames) + releaseBatchSize - 1) / releaseBatchSize
	for start := 0; start < len(usernames); start += releaseBatchSize {
		end := start + releaseBatchSize
		if end > len(usernames) {
			end = len(usernames)
		}
		batch := usernames[start:end]
		batchNo := start/releaseBatchSize + 1

		resp, err := c.requestWithRetry("PATCH",
			fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&username=in.(%s)",
				url.QueryEscape(nodeID), joinEscaped(batch)),
			map[string]interface{}{
				"assigned_node": nil,
				"status":        "unassigned",
			})
		if err != nil {
			return released, fmt.Errorf("release batch %d/%d: %w", batchNo, totalBatches, err)
		}
		if resp.StatusCode >= 400 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return released, fmt.Errorf("release batch %d/%d: HTTP %d: %s", batchNo, totalBatches, resp.StatusCode, string(bodyBytes))
		}
		var batchReleased []ChannelAssignment
		decodeErr := json.NewDecoder(resp.Body).Decode(&batchReleased)
		resp.Body.Close()
		if decodeErr != nil {
			return released, fmt.Errorf("decode released batch %d/%d: %w", batchNo, totalBatches, decodeErr)
		}
		released = append(released, batchReleased...)
	}
	return released, nil
}

// ReleaseExcessChannels releases up to `limit` channels from this node back to unassigned.
// Uses a two-step approach (GET usernames, then PATCH by username) because
// PostgREST PATCH ignores the `limit` parameter.
func (c *Client) ReleaseExcessChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	// Step 1: GET the usernames we want to release.
	// Prioritise releasing offline channels first (is_live=false), then online
	// ones.  Within each group we pick alphabetically to be deterministic.
	var offline, online []ChannelAssignment

	err := c.get(
		fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned&is_live=eq.false&select=username,site&order=username.asc&limit=%d",
			url.QueryEscape(nodeID), limit), &offline)
	if err != nil {
		return nil, err
	}

	remaining := limit - len(offline)
	if remaining > 0 {
		err = c.get(
			fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned&is_live=eq.true&select=username,site&order=username.asc&limit=%d",
				url.QueryEscape(nodeID), remaining), &online)
		if err != nil {
			return nil, err
		}
	}

	target := append(offline, online...)
	if len(target) == 0 {
		return nil, nil
	}

	// Step 2: PATCH only those specific channels, in small batches so the
	// PostgREST filter URL can never blow past the ~8KB proxy limit (HTTP 414).
	usernames := make([]string, len(target))
	for i, ca := range target {
		usernames[i] = ca.Username
	}
	return c.releaseChunked(nodeID, usernames)
}

// ReleaseExcessOfflineChannels releases up to `limit` OFFLINE channels from this
// node back to unassigned. Unlike ReleaseExcessChannels it NEVER releases a
// live or recording channel, so a node's in-progress recordings are left alone
// during fair-share rebalancing. Channels are selected offline-first, then
// alphabetically, to be deterministic.
func (c *Client) ReleaseExcessOfflineChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	if limit <= 0 {
		return nil, nil
	}

	// Offline channels only, and exclude any still marked 'recording' (a node may
	// briefly lag the liveness flag, so this protects a channel that is actually
	// being recorded from being interrupted).
	var offline []ChannelAssignment
	err := c.get(
		fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned&status=neq.recording&is_live=eq.false&select=username,site&order=username.asc&limit=%d",
			url.QueryEscape(nodeID), limit), &offline)
	if err != nil {
		return nil, err
	}
	if len(offline) == 0 {
		return nil, nil
	}

	usernames := make([]string, len(offline))
	for i, ca := range offline {
		usernames[i] = ca.Username
	}
	return c.releaseChunked(nodeID, usernames)
}

// ReleaseChannel releases a single channel back to the pool.
func (c *Client) ReleaseChannel(username, site string) error {
	return c.patch(fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s", url.QueryEscape(username), url.QueryEscape(site)),
		map[string]interface{}{
			"assigned_node": nil,
			"status":        "unassigned",
		})
}

// MarkChannelRecording marks a channel as actively recording on its node and
// bumps its recording heartbeat. Called by the liveness loop for this node's
// live channels so "live+recording" is authoritative in the DB.
func (c *Client) MarkChannelRecording(username, site string) error {
	return c.patch(
		fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s",
			url.QueryEscape(username), url.QueryEscape(site)),
		map[string]interface{}{
			"status":           "recording",
			"last_recorded_at": "now()",
			"last_heartbeat":   "now()",
		})
}

// RepairOrphanedAssignments fixes rows where assigned_node is set but
// status is still 'unassigned'. This can happen if a claim was partially
// rolled back (assigned_node written, status not updated) or if a
// release set status=unassigned without nulling assigned_node. These rows
// are invisible to both ClaimChannels (which requires assigned_node IS NULL)
// and ReleaseExcessChannels (which requires status != unassigned), causing
// a permanent deadlock.
//
// Returns the number of rows repaired.
func (c *Client) RepairOrphanedAssignments() (int, error) {
	// Step 1: count the broken rows
	var orphaned []ChannelAssignment
	err := c.get("/channel_assignments?assigned_node=not.is.null&status=eq.unassigned&select=username&limit=50000", &orphaned)
	if err != nil {
		return 0, err
	}
	if len(orphaned) == 0 {
		return 0, nil
	}

	// Step 2: null out assigned_node on all broken rows
	err = c.patch("/channel_assignments?assigned_node=not.is.null&status=eq.unassigned",
		map[string]interface{}{
			"assigned_node": nil,
		})
	if err != nil {
		return 0, err
	}

	return len(orphaned), nil
}

// DeleteAssignment removes a channel assignment entirely from the pool.
func (c *Client) DeleteAssignment(username, site string) error {
	return c.delete(fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s", url.QueryEscape(username), url.QueryEscape(site)))
}

// GetNodeAssignments returns all channel assignments for a given node.
func (c *Client) GetNodeAssignments(nodeID string) ([]ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.get(fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&order=username.asc", url.QueryEscape(nodeID)), &assignments)
	return assignments, err
}

// GetAssignment returns the assignment for a specific channel.
func (c *Client) GetAssignment(username, site string) (*ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.get(fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s&limit=1",
		url.QueryEscape(username), url.QueryEscape(site)), &assignments)
	if err != nil {
		return nil, err
	}
	if len(assignments) == 0 {
		return nil, nil
	}
	return &assignments[0], nil
}

// GetAssignmentsByStatus returns all assignments with a given status.
func (c *Client) GetAssignmentsByStatus(status string) ([]ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.get(fmt.Sprintf("/channel_assignments?status=eq.%s&order=username.asc", url.QueryEscape(status)), &assignments)
	return assignments, err
}

// GetAllAssignments returns all channel assignments.
func (c *Client) GetAllAssignments() ([]ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.get("/channel_assignments?order=username.asc&limit=50000", &assignments)
	return assignments, err
}

// countRows returns the exact number of rows matching the given PostgREST
// filter path, using a HEAD request with Prefer: count=exact (the total is
// returned in the Content-Range header).  This avoids fetching up to 50k rows
// just to count them and never silently truncates at the fetch limit.
// Transient failures (5xx, 429, 408, network) are retried so a schema-cache
// rebuild or blip doesn't abort the caller's cycle.
func (c *Client) countRows(path string) (int, error) {
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		n, retryable, err := c.countRowsOnce(path)
		if err == nil {
			return n, nil
		}
		if !retryable || attempt >= maxRetries-1 {
			return 0, err
		}
		time.Sleep(retryBackoff(attempt))
	}
	return 0, fmt.Errorf("count failed after %d attempts", maxRetries)
}

func (c *Client) countRowsOnce(path string) (int, bool, error) {
	req, err := http.NewRequest(http.MethodHead, c.URL+"/rest/v1"+path, nil)
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("apikey", c.APIKey)
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Prefer", "count=exact")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, true, err // transport error — retryable
	}
	defer resp.Body.Close()

	if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return 0, true, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return 0, false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// Content-Range: <start>-<end>/<total>  (total is "*" without count=exact)
	cr := resp.Header.Get("Content-Range")
	if cr == "" {
		return 0, false, fmt.Errorf("missing Content-Range header")
	}
	idx := strings.LastIndex(cr, "/")
	if idx < 0 {
		return 0, false, fmt.Errorf("malformed Content-Range: %s", cr)
	}
	total := strings.TrimSpace(cr[idx+1:])
	if total == "*" {
		return 0, false, fmt.Errorf("count not provided")
	}
	n, err := strconv.Atoi(total)
	if err != nil {
		return 0, false, fmt.Errorf("parse Content-Range total %q: %w", total, err)
	}
	return n, false, nil
}

// GetAssignmentStats returns total live channels and total alive nodes for fair-share calculation.
func (c *Client) GetAssignmentStats() (*AssignmentStats, error) {
	stats := &AssignmentStats{}
	var err error

	// Exact counts via HEAD (no 50k-row fetches, no silent truncation).
	if stats.TotalPoolChannels, err = c.countRows("/channel_assignments"); err != nil {
		return nil, err
	}
	if stats.TotalLiveChannels, err = c.countRows("/channel_assignments?is_live=eq.true"); err != nil {
		return nil, err
	}
	if stats.TotalUnassigned, err = c.countRows("/channel_assignments?status=eq.unassigned"); err != nil {
		return nil, err
	}

	// Distinct assigned nodes can't be counted via REST, so fetch and dedupe.
	var assigned []ChannelAssignment
	if err := c.get("/channel_assignments?assigned_node=not.is.null&select=assigned_node&limit=50000", &assigned); err != nil {
		return nil, err
	}
	assignedNodes := make(map[string]bool)
	for _, a := range assigned {
		assignedNodes[a.AssignedNode] = true
	}
	stats.TotalAssignedNodes = len(assignedNodes)

	aliveNodes, err := c.GetAliveNodes()
	if err != nil {
		return nil, err
	}
	stats.TotalAliveNodes = len(aliveNodes)

	return stats, nil
}

// CountMyAssignments returns the number of channels assigned to a node.
// Uses an exact HEAD count rather than loading all rows.
func (c *Client) CountMyAssignments(nodeID string) (int, error) {
	return c.countRows(fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned",
		url.QueryEscape(nodeID)))
}

// compositeOrFilter builds a PostgREST or=(and(a.eq.x,b.eq.y),...) filter for a
// list of composite-key (username, site) pairs.  channel_assignments has a
// composite primary key (username, site), so filters must address both columns
// or they would wrongly touch the same username on the other site.
func compositeOrFilter(pairs [][2]string) string {
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = fmt.Sprintf("and(username.eq.%s,site.eq.%s)",
			url.QueryEscape(p[0]), url.QueryEscape(p[1]))
	}
	return "or=(" + strings.Join(parts, ",") + ")"
}

// SetChannelsLive bulk-updates is_live=true for the given (username, site)
// pairs.  Composite filters ensure a same-named channel on the other site is
// never toggled.  The pair list is chunked because a single or=(...) filter
// listing every live pair can exceed the ~8KB proxy URL limit (HTTP 414) — the
// same failure mode that left is_live flags stale fleet-wide.
func (c *Client) SetChannelsLive(pairs [][2]string) error {
	if len(pairs) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return c.setChannelsLiveChunked(pairs, now)
}

func (c *Client) setChannelsLiveChunked(pairs [][2]string, now string) error {
	for start := 0; start < len(pairs); start += livenessBatchSize {
		end := start + livenessBatchSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[start:end]
		if err := c.patch(
			fmt.Sprintf("/channel_assignments?%s&is_live=eq.false", compositeOrFilter(chunk)),
			map[string]interface{}{
				"is_live":         true,
				"live_checked_at": now,
			}); err != nil {
			return fmt.Errorf("set live batch %d: %w", start/livenessBatchSize+1, err)
		}
	}
	return nil
}

// SetChannelsNotLive bulk-updates is_live=false for every channel EXCEPT the
// given (username, site) pairs.  An empty pair list marks the whole pool
// not-live.
//
// The old implementation built a single PostgREST not.or(...) filter listing
// every live pair — with hundreds of live channels that URL exceeds the ~8KB
// proxy limit (HTTP 414), leaving stale is_live=true rows that make the whole
// pool look live and stall claiming.  The exclusion filter cannot be chunked
// safely (each chunk would clear the other chunks' live flags), so we invert
// it: clear the entire pool with a tiny filter first, then re-mark the live
// pairs in small chunks.  The transient all-not-live window is harmless —
// is_live only biases fair-share claiming (claim RPCs require assigned_node IS
// NULL, so assigned channels are never disturbed) and the liveness loop
// re-runs every cycle.
func (c *Client) SetChannelsNotLive(pairs [][2]string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	// Step 1: clear every live flag — a single tiny PATCH.
	if err := c.patch("/channel_assignments?is_live=eq.true",
		map[string]interface{}{
			"is_live":         false,
			"live_checked_at": now,
		}); err != nil {
		return err
	}
	// Step 2: re-mark the still-live pairs in chunks.
	return c.setChannelsLiveChunked(pairs, now)
}

// ReclaimChannels sets assigned_node=NULL for all channels belonging to a dead node.
// Returns the number of channels reclaimed.
func (c *Client) ReclaimChannels(deadNodeID string) (int, error) {
	// First, count what we'll reclaim
	var assignments []ChannelAssignment
	err := c.get(fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&select=username&limit=50000",
		url.QueryEscape(deadNodeID)), &assignments)
	if err != nil {
		return 0, err
	}
	if len(assignments) == 0 {
		return 0, nil
	}

	// Release them
	if err := c.ReleaseNodeChannels(deadNodeID); err != nil {
		return 0, err
	}
	return len(assignments), nil
}

// BulkInsertAssignments creates channel_assignments rows for channels that don't have one yet.
func (c *Client) BulkInsertAssignments(assignments []ChannelAssignment) error {
	if len(assignments) == 0 {
		return nil
	}
	resp, err := c.requestWithRetry("POST", "/channel_assignments?on_conflict=username,site", assignments)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// ============================================================================
// CHANNEL POOL (shared app_settings key)
// ============================================================================

// PoolKey returns the app_settings key for the shared channel pool.
func PoolKey() string {
	return "channel_pool"
}

// LoadPoolFromDB reads the shared channel pool from app_settings.
func (c *Client) LoadPoolFromDB() ([]byte, error) {
	var settings []AppSetting
	err := c.get(fmt.Sprintf("/app_settings?key=eq.%s&limit=1", PoolKey()), &settings)
	if err != nil {
		return nil, err
	}
	if len(settings) == 0 {
		return nil, nil
	}
	return settings[0].Value, nil
}

// SavePoolToDB writes the shared channel pool to app_settings.
func (c *Client) SavePoolToDB(data []byte) error {
	return c.SaveSetting(PoolKey(), json.RawMessage(data))
}

// GetAllSettingKeys returns all app_settings keys matching a LIKE pattern.
// The prefix should be like "channels_" to get all instance-scoped keys.
func (c *Client) GetAllSettingKeys(likePattern string) ([]string, error) {
	// Supabase REST doesn't support LIKE directly, so we fetch all keys
	// and filter client-side. For typical deployments this is < 100 keys.
	var settings []AppSetting
	err := c.get("/app_settings?select=key&limit=50000", &settings)
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, s := range settings {
		if strings.HasPrefix(s.Key, likePattern) {
			matches = append(matches, s.Key)
		}
	}
	return matches, nil
}

// joinEscaped joins strings with Supabase-compatible CSV escaping.
func joinEscaped(items []string) string {
	escaped := make([]string, len(items))
	for i, item := range items {
		escaped[i] = url.QueryEscape(item)
	}
	return strings.Join(escaped, ",")
}

// ============================================================================
// HEALTH CHECK
// ============================================================================

// HealthCheck verifies the database connection
func (c *Client) HealthCheck() error {
	resp, err := c.request("GET", "/app_settings?key=eq.__healthcheck__&select=key&limit=1", nil)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		return nil
	case 404:
		return fmt.Errorf("app_settings table not found (HTTP 404) — run the SQL migration first")
	case 401, 403:
		return fmt.Errorf("authentication failed (HTTP %d) — check SUPABASE_API_KEY and RLS policies", resp.StatusCode)
	default:
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected response (HTTP %d): %s", resp.StatusCode, string(body))
	}
}
