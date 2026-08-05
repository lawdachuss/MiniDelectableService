package router

import (
	"bytes"
	"html/template"
	"strconv"
	"strings"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/router/view"
)

// loadAllTemplates parses every production template with the exact FuncMap and
// file set used by SetupRouter.
func loadAllTemplates(t *testing.T) *template.Template {
	t.Helper()
	templ := template.New("").Funcs(templateFuncs())
	for _, name := range []string{
		"index.html", "channel_info.html", "videos.html", "video.html",
		"channel.html", "admin.html", "nodes.html", "pool.html", "logs.html",
	} {
		content, err := view.FS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err = templ.New(name).Parse(string(content)); err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
	}
	return templ
}

// renderTemplate executes one template and fails the test if execution errors
// OR the output is truncated (missing the closing tag).  A template action
// referencing a nonexistent field (e.g. the old {{ .VidaraKeyValue }} on
// IndexData) aborts rendering mid-response — the browser then gets a page
// without the trailing <script> block and every inline onclick throws
// "X is not defined".  These tests catch that class of regression.
func renderTemplate(t *testing.T, name string, data any) string {
	t.Helper()
	var buf bytes.Buffer
	if err := loadAllTemplates(t).ExecuteTemplate(&buf, name, data); err != nil {
		t.Fatalf("render %s: %v", name, err)
	}
	out := buf.String()
	if !strings.Contains(out, "</html>") {
		t.Fatalf("%s render truncated: %d bytes, missing </html>\ntail: %s", name, buf.Len(), out[len(out)-300:])
	}
	return out
}

func TestIndexTemplateRendersCompletely(t *testing.T) {
	out := renderTemplate(t, "index.html", &IndexData{
		Config: &entity.Config{
			VidaraKey:       "test-vidara-key",
			CfClearance:     "test-clearance",
			StreamtapeLogin: "test-login",
			StreamtapeKey:   "test-st-key",
			MixdropEmail:    "test@example.com",
			MixdropToken:    "test-md-token",
		},
		Channels: []*entity.ChannelInfo{
			{
				Username:   "testchannel",
				Site:       "chaturbate",
				IsOnline:   true,
				RoomStatus: "public",
				Filename:   "testchannel_2026-08-05_00-00-00.mp4",
			},
		},
		Disk: &entity.DiskInfo{
			Total:   "256.00 GB",
			Used:    "120.50 GB",
			Free:    "135.50 GB",
			Percent: 47,
		},
	})
	if !strings.Contains(out, `name="vidara_key" value="test-vidara-key"`) {
		t.Fatalf("index.html render missing vidara_key value (template action failed?)\noutput tail:\n%s", out[len(out)-400:])
	}
}

func TestAdminTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "admin.html", &AdminData{
		Config: &entity.Config{FFmpegPath: "/usr/bin/ffmpeg"},
		Disk:   &entity.DiskInfo{Total: "1 TB", Used: "1 GB", Free: "999 GB", Percent: 1},
		Channels: []*entity.ChannelInfo{
			{
				Username:       "test",
				Site:           "chaturbate",
				IsOnline:       true,
				Duration:       "01:00",
				Filesize:       "1.2 GB",
				UploadStatus:   "uploading",
				UploadProgress: 42,
				UploadFilename: "f.mp4",
			},
		},
		Uploads: &entity.UploadsResponse{
			Active: []entity.UploadEntry{
				{Channel: "test", Filename: "f.mp4", Status: "uploading", Progress: 42, HostCount: 2, HostTotal: 5, BytesCurrent: 100, BytesTotal: 1000, Speed: "1 MB/s", Hosts: []entity.HostEntry{{Host: "GoFile", Status: "done", Progress: 100, Speed: ""}}},
			},
			Pending: []entity.PendingEntry{{Channel: "test", Filename: "f.mp4", Stage: "upload", Failed: true, Error: "boom"}},
			History: []entity.PendingEntry{{Channel: "test", Filename: "f.mp4", Stage: "upload"}},
		},
		Nodes: []database.Node{
			{NodeID: "node-test", Hostname: "host", Status: "online", CurrentLoad: 1, WebURL: "https://x", LastHeartbeat: "now"},
		},
		Assignments: []database.ChannelAssignment{
			{Username: "test", Site: "chaturbate", AssignedNode: "node-test", Status: "recording", IsLive: true, AssignedAt: "now", Resolution: 1080, Framerate: 60},
		},
		PipelineMap: map[string]ChannelPipelinesEntry{"test": {Username: "test", Queued: 1}},
		OnlineNodes:  1,
		PoolMode:     "pooled",
		MyNodeID:     "node-test",
	})
}

func TestVideosTemplateRendersCompletely(t *testing.T) {
	v := &VideoEntry{
		Username:     "test",
		Filename:     "test_2026-08-05.mp4",
		FullPath:     "videos/test.mp4",
		ThumbnailURL: "https://x/thumb.jpg",
		Links:        map[string]string{"GoFile": "https://gofile.io/d/x"},
		Tags:         []string{"tag"},
		Resolution:   "1080p",
		Framerate:    60,
	}
	renderTemplate(t, "videos.html", &VideosData{
		Config:      &entity.Config{},
		Videos:      []*VideoEntry{v},
		Groups:      []VideoGroup{{Username: "test", Videos: []*VideoEntry{v}}},
		Recommended: []*VideoEntry{v},
	})
}

func TestChannelTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "channel.html", &VideosData{
		Config: &entity.Config{},
		Videos: []*VideoEntry{{Username: "test", Filename: "test.mp4"}},
	})
}

func TestNodesTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "nodes.html", &NodesData{
		Nodes: []database.Node{
			{NodeID: "node-1", Hostname: "h", InstanceLabel: "i", SoftwareVersion: "v", Status: "online", CurrentLoad: 2, LastHeartbeat: "now", WebURL: "https://x", CreatedAt: "t"},
		},
		OnlineCount:  1,
		TotalLoad:    2,
		Mode:         "pooled",
		MyNodeID:     "node-1",
	})
}

// TestOfflineNodeLoadCellHidden verifies the per-node Load cell never shows a
// dead node's frozen current_load (only ever written by the node's own
// heartbeat) — it renders a muted dash instead, on both the nodes page and
// the admin page.
//
// The load metric cards are set to a distinct value (999) so they can't be
// confused with the per-node cell values in the assertions. The online node's
// cell renders "280\n...</td>" (template whitespace), so "\u2265280\n" matches
// exactly one load cell; the offline node's cell renders a dash instead.
func TestOfflineNodeLoadCellHidden(t *testing.T) {
	// Load value chosen to not collide with anything in the templates
	// (admin.html contains max-width:280px in its CSS).
	const load = 283
	nodes := []database.Node{
		{NodeID: "node-16", Status: "online", CurrentLoad: load},
		{NodeID: "node-19", Status: "offline", CurrentLoad: load}, // dead — must not show its load
	}

	checkCells := func(name, out string) {
		t.Helper()
		if !strings.Contains(out, ">999<") {
			t.Fatalf("%s: load metric card missing\n%s", name, out[len(out)-800:])
		}
		// The online node's load cell is the ONLY place the load value may
		// appear (metric cards use 999, no timestamps are set, and the offline
		// node's stale value must be replaced by a dash). Exactly one
		// occurrence proves the offline cell no longer renders the frozen load.
		if got := strings.Count(out, strconv.Itoa(load)); got != 1 {
			t.Fatalf("%s: expected exactly one load cell with %d (online node only), got %d\n%s", name, load, got, out[len(out)-1200:])
		}
		if !strings.Contains(out, "\u2014") {
			t.Fatalf("%s: offline node load cell should render a dash", name)
		}
	}

	checkCells("nodes.html", renderTemplate(t, "nodes.html", &NodesData{
		Nodes:       nodes,
		OnlineCount: 1,
		TotalLoad:   999, // distinct metric value, not a load cell
		Mode:        "pooled",
		MyNodeID:    "node-16",
	}))

	checkCells("admin.html", renderTemplate(t, "admin.html", &AdminData{
		Config:        &entity.Config{FFmpegPath: "/usr/bin/ffmpeg"},
		Disk:          &entity.DiskInfo{Total: "1 TB", Used: "1 GB", Free: "999 GB", Percent: 1},
		Uploads:       &entity.UploadsResponse{},
		Nodes:         nodes,
		OnlineNodes:   1,
		TotalNodeLoad: 999, // distinct metric value, not a load cell
		PoolMode:      "pooled",
		MyNodeID:      "node-16",
	}))
}

func TestPoolTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "pool.html", &PoolData{
		Assignments: []PoolEntry{
			{Username: "test", Site: "chaturbate", AssignedNode: "node-1", Status: "unassigned", Resolution: 1080, Framerate: 30, AssignedAt: "now", Source: "pool"},
		},
		Mode: "pooled",
	})
}

func TestLogsTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "logs.html", nil)
}

// TestSumNodeLoadExcludesOffline verifies that offline nodes' frozen
// current_load (never corrected after death, since only the node's own
// heartbeat writes it) is excluded from the dashboard's Total Load, while
// online and draining nodes still count.
func TestSumNodeLoadExcludesOffline(t *testing.T) {
	nodes := []database.Node{
		{NodeID: "node-16", Status: "online", CurrentLoad: 280},
		{NodeID: "node-17", Status: "online", CurrentLoad: 280},
		{NodeID: "node-18", Status: "draining", CurrentLoad: 280},
		{NodeID: "node-19", Status: "offline", CurrentLoad: 280}, // dead — must not count
	}
	if got := sumNodeLoad(nodes); got != 840 {
		t.Fatalf("sumNodeLoad = %d, want 840 (offline node excluded)", got)
	}
}

func TestSumNodeLoadAllOffline(t *testing.T) {
	nodes := []database.Node{
		{NodeID: "node-19", Status: "offline", CurrentLoad: 280},
	}
	if got := sumNodeLoad(nodes); got != 0 {
		t.Fatalf("sumNodeLoad = %d, want 0 (single offline node)", got)
	}
}
