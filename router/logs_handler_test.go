package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/logs"
)

func TestLogsAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/logs", LogsAPI)

	logs.Default.Write([]byte("test line one\n"))
	logs.Default.Write([]byte("test line two\n"))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/logs", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		Total uint64 `json:"total"`
		Lines []struct {
			Line string `json:"line"`
		} `json:"lines"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Total == 0 || len(resp.Lines) == 0 {
		t.Fatalf("expected captured log lines, got total=%d lines=%d", resp.Total, len(resp.Lines))
	}
	found := false
	for _, l := range resp.Lines {
		if l.Line == "test line one" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("captured lines did not include test line: %+v", resp.Lines)
	}
}
