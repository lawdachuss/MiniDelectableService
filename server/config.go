package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
)

var Config *entity.Config
var ConfigMu sync.RWMutex
var StartTime = time.Now()

type persistedSettings struct {
	Cookies         string `json:"cookies"`
	SessionID       string `json:"sessionid,omitempty"`
	Csrftoken       string `json:"csrftoken,omitempty"`
	CfClearance     string `json:"cf_clearance,omitempty"`
	UserAgent       string `json:"user_agent"`
	VoeSXAPIKey     string `json:"voesx_api_key,omitempty"`
	StreamtapeLogin string `json:"streamtape_login,omitempty"`
	StreamtapeKey   string `json:"streamtape_key,omitempty"`
	MixdropEmail    string `json:"mixdrop_email,omitempty"`
	MixdropToken    string `json:"mixdrop_token,omitempty"`
	VidaraKey       string `json:"vidara_key,omitempty"`
	StripchatPDKey  string `json:"stripchat_pdkey,omitempty"`
}

// SaveSettings writes the runtime cookies and user-agent to Supabase.
func SaveSettings() error {
	ConfigMu.RLock()
	s := persistedSettings{
		Cookies:         Config.Cookies,
		SessionID:       Config.SessionID,
		Csrftoken:       Config.Csrftoken,
		CfClearance:     Config.CfClearance,
		UserAgent:       Config.UserAgent,
		VoeSXAPIKey:     validPersistedValue(Config.VoeSXAPIKey),
		StreamtapeLogin: validPersistedValue(Config.StreamtapeLogin),
		StreamtapeKey:   validPersistedValue(Config.StreamtapeKey),
		MixdropEmail:    validPersistedValue(Config.MixdropEmail),
		MixdropToken:    validPersistedValue(Config.MixdropToken),
		VidaraKey:       validPersistedValue(Config.VidaraKey),
		StripchatPDKey:  validPersistedValue(Config.StripchatPDKey),
	}
	ConfigMu.RUnlock()

	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}

	if err := SaveSettingsToDB(b); err != nil {
		return fmt.Errorf("save settings to Supabase: %w", err)
	}
	return nil
}

// LoadSettings reads persisted cookies and user-agent from Supabase.
func LoadSettings() error {
	b := LoadSettingsFromDB()
	if b == nil {
		return nil
	}

	var s persistedSettings
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("unmarshal settings: %w", err)
	}

	ConfigMu.Lock()
	if s.Cookies != "" {
		Config.Cookies = s.Cookies
	}
	if s.SessionID != "" {
		Config.SessionID = s.SessionID
	}
	if s.Csrftoken != "" {
		Config.Csrftoken = s.Csrftoken
	}
	if s.CfClearance != "" {
		Config.CfClearance = s.CfClearance
	}
	if s.UserAgent != "" {
		Config.UserAgent = s.UserAgent
	}
	if v := validPersistedValue(s.VoeSXAPIKey); v != "" {
		Config.VoeSXAPIKey = v
	}
	if v := validPersistedValue(s.StreamtapeLogin); v != "" {
		Config.StreamtapeLogin = v
	}
	if v := validPersistedValue(s.StreamtapeKey); v != "" {
		Config.StreamtapeKey = v
	}
	if v := validPersistedValue(s.MixdropEmail); v != "" {
		Config.MixdropEmail = v
	}
	if v := validPersistedValue(s.MixdropToken); v != "" {
		Config.MixdropToken = v
	}
	if v := validPersistedValue(s.VidaraKey); v != "" {
		Config.VidaraKey = v
	}
	if v := validPersistedValue(s.StripchatPDKey); v != "" {
		Config.StripchatPDKey = v
	}

	// Parse Config.Cookies back into individual fields if they are empty.
	if Config.Cookies != "" {
		if Config.CfClearance == "" {
			Config.CfClearance = extractCookie(Config.Cookies, "cf_clearance")
		}
		if Config.SessionID == "" {
			Config.SessionID = extractCookie(Config.Cookies, "sessionid")
		}
		if Config.Csrftoken == "" {
			Config.Csrftoken = extractCookie(Config.Cookies, "csrftoken")
		}
	}
	ConfigMu.Unlock()

	return nil
}

func extractCookie(cookieStr, name string) string {
	for _, pair := range strings.Split(cookieStr, ";") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == name {
			return strings.TrimSpace(parts[1])
		}
	}
	return ""
}

// validPersistedValue returns s trimmed, or "" when s is empty, whitespace-only,
// or the placeholder dash ("-"). The web UI and settings blob have previously
// stored "-" for unset API keys; treating it as a real value made startup
// (LoadSettings) overwrite working .env credentials and all uploads failed with
// 401 "invalid api_key". Callers must skip the value when this returns "".
func validPersistedValue(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return ""
	}
	return s
}

// UpdateUploaderCredentials updates upload service credentials and protects concurrent access with a mutex.
func UpdateUploaderCredentials(voeSXAPIKey, streamtapeLogin, streamtapeKey, mixdropEmail, mixdropToken, vidaraKey string) {
	// Ignore placeholder dash values so the web UI can't wipe real .env keys.
	voeSXAPIKey = validPersistedValue(voeSXAPIKey)
	streamtapeLogin = validPersistedValue(streamtapeLogin)
	streamtapeKey = validPersistedValue(streamtapeKey)
	mixdropEmail = validPersistedValue(mixdropEmail)
	mixdropToken = validPersistedValue(mixdropToken)
	vidaraKey = validPersistedValue(vidaraKey)

	ConfigMu.Lock()
	if voeSXAPIKey != "" {
		Config.VoeSXAPIKey = voeSXAPIKey
	}
	if streamtapeLogin != "" {
		Config.StreamtapeLogin = streamtapeLogin
	}
	if streamtapeKey != "" {
		Config.StreamtapeKey = streamtapeKey
	}
	if mixdropEmail != "" {
		Config.MixdropEmail = mixdropEmail
	}
	if mixdropToken != "" {
		Config.MixdropToken = mixdropToken
	}
	if vidaraKey != "" {
		Config.VidaraKey = vidaraKey
	}
	ConfigMu.Unlock()
}
