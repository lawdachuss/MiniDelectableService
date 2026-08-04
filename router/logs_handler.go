package router

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/logs"
)

// LogsPage renders the log viewer page. The page streams new entries from
// /api/logs via polling, so the buffer never needs to be rendered server-side.
func LogsPage(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.HTML(http.StatusOK, "logs.html", nil)
}

// LogsAPI returns all captured application logs as JSON. When ?after=N is
// given only entries with index >= N are returned, so clients can poll for
// incremental updates.
func LogsAPI(c *gin.Context) {
	c.Header("Cache-Control", "no-store")

	var entries []logs.Entry
	if after, err := strconv.ParseUint(c.Query("after"), 10, 64); err == nil && after > 0 {
		entries = logs.Default.After(after)
	} else {
		entries = logs.Default.Entries()
	}

	c.JSON(http.StatusOK, gin.H{
		"total": logs.Default.Total(),
		"lines": entries,
	})
}
