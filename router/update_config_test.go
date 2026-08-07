package router

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func TestUpdateConfigAcceptsJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server.Config = &entity.Config{}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	body, err := json.Marshal(map[string]string{
		"cookies":      "cf_clearance=test_cf; sessionid=test; csrftoken=abc123",
		"user_agent":   "TestAgent/1.0",
		"affiliate_wm": "wm-router",
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/update_config", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	UpdateConfig(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if server.Config.Cookies != "cf_clearance=test_cf; sessionid=test; csrftoken=abc123" {
		t.Fatalf("Cookies = %q", server.Config.Cookies)
	}
	if server.Config.UserAgent != "TestAgent/1.0" {
		t.Fatalf("UserAgent = %q", server.Config.UserAgent)
	}
	if server.Config.CfClearance != "test_cf" {
		t.Fatalf("CfClearance = %q", server.Config.CfClearance)
	}
	if server.Config.AffiliateWM != "wm-router" {
		t.Fatalf("AffiliateWM = %q", server.Config.AffiliateWM)
	}
}
