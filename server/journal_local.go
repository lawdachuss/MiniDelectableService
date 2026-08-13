package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LocalJournalEntry is one host's upload record persisted on the local node.
// Unlike the Supabase upload_journal row, it also carries the download link so
// metadata can be rebuilt when the Supabase write failed during an outage.
type LocalJournalEntry struct {
	Host      string `json:"host"`
	Status    string `json:"status"`
	Link      string `json:"link,omitempty"`
	FileSize  int64  `json:"file_size,omitempty"`
	ErrMsg    string `json:"error_msg,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

var (
	localJournalMu   sync.Mutex
	localJournalDirs = make(map[string]bool) // ensures MkdirAll runs once per dir
)

// localJournalDir returns the directory holding this node's local upload
// journals, creating it if needed.  OutputDir is used when set (the ".pending"
// sibling layout), falling back to "videos".
func localJournalDir() (string, error) {
	dir := "videos"
	if Config != nil && Config.OutputDir != "" {
		dir = Config.OutputDir
	}
	journalDir := filepath.Join(dir, ".journal")
	if err := os.MkdirAll(journalDir, 0777); err != nil {
		return "", err
	}
	return journalDir, nil
}

func localJournalPath(fileHash string) (string, error) {
	dir, err := localJournalDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fileHash+".json"), nil
}

// saveLocalJournalEntry upserts a host's upload record in the local journal.
// This is a local-first write: it never contacts Supabase, so the dedup record
// survives a Supabase outage (the root cause of duplicate re-uploads).  The
// link is stored so recording metadata can be rebuilt without re-uploading.
func saveLocalJournalEntry(fileHash, host, status, link string, fileSize int64, errMsg string) error {
	localJournalMu.Lock()
	defer localJournalMu.Unlock()

	path, err := localJournalPath(fileHash)
	if err != nil {
		return err
	}

	entries := loadLocalJournalUnlocked(path)
	found := false
	now := time.Now().UTC().Format(time.RFC3339)
	for i := range entries {
		if entries[i].Host == host {
			entries[i].Status = status
			entries[i].Link = link
			entries[i].FileSize = fileSize
			entries[i].ErrMsg = errMsg
			entries[i].UpdatedAt = now
			found = true
			break
		}
	}
	if !found {
		entries = append(entries, LocalJournalEntry{
			Host: host, Status: status, Link: link,
			FileSize: fileSize, ErrMsg: errMsg, UpdatedAt: now,
		})
	}

	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadLocalJournalUnlocked reads the local journal file.  Caller holds localJournalMu.
func loadLocalJournalUnlocked(path string) []LocalJournalEntry {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var entries []LocalJournalEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil
	}
	return entries
}

// loadLocalJournal returns all local journal entries for a hash.
func loadLocalJournal(fileHash string) []LocalJournalEntry {
	localJournalMu.Lock()
	defer localJournalMu.Unlock()
	path, err := localJournalPath(fileHash)
	if err != nil {
		return nil
	}
	return loadLocalJournalUnlocked(path)
}

// deleteLocalJournal removes the local journal file for a hash.  A missing
// file is not an error (nothing to clean up).
func deleteLocalJournal(fileHash string) error {
	localJournalMu.Lock()
	defer localJournalMu.Unlock()
	path, err := localJournalPath(fileHash)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// LoadJournalLinks returns the successful download links from the local
// journal for a hash — the data needed to rebuild a recording's metadata
// without re-uploading after a Supabase outage lost the original save.
func LoadJournalLinks(fileHash string) map[string]string {
	entries := loadLocalJournal(fileHash)
	if len(entries) == 0 {
		return nil
	}
	links := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.Status == "success" && e.Link != "" {
			links[e.Host] = e.Link
		}
	}
	if len(links) == 0 {
		return nil
	}
	return links
}

// mergeLocalJournalSuccessHosts returns the set of hosts recorded as
// successfully uploaded in the local journal.
func mergeLocalJournalSuccessHosts(fileHash string, hosts map[string]bool) {
	for _, e := range loadLocalJournal(fileHash) {
		if e.Status == "success" {
			hosts[e.Host] = true
		}
	}
}

// ─── Thumbnail-failure counter (persistent corrupt-file eviction) ──────────
//
// A permanently-corrupt recording (unreadable by ffprobe/ffmpeg) fails
// thumbnail generation on every scan and previously burned disk + image-host
// quota forever with no escape hatch.  This counter persists per-file across
// restarts so that after maxThumbFailures failed generations the file is
// evicted (uploaded + metadata saved + deleted entirely, see
// channel.UploadOrphanedFileEvict) instead of being retried indefinitely.

const MaxThumbFailures = 3

type thumbFailureEntry struct {
	Count      int       `json:"count"`
	LastFailed time.Time `json:"last_failed"`
}

var thumbFailureMu sync.Mutex

func thumbFailurePath() (string, error) {
	dir := "videos"
	if Config != nil && Config.OutputDir != "" {
		dir = Config.OutputDir
	}
	journalDir := filepath.Join(dir, ".journal")
	if err := os.MkdirAll(journalDir, 0777); err != nil {
		return "", err
	}
	return filepath.Join(journalDir, "thumb_failures.json"), nil
}

func loadThumbFailuresUnlocked() map[string]thumbFailureEntry {
	path, err := thumbFailurePath()
	if err != nil {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m map[string]thumbFailureEntry
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}

// RecordThumbFailure increments the persistent failure counter for a video
// file and returns the new count.  The counter is keyed by absolute path so it
// survives restarts and periodic scans.
func RecordThumbFailure(videoPath string) int {
	thumbFailureMu.Lock()
	defer thumbFailureMu.Unlock()

	path, err := thumbFailurePath()
	if err != nil {
		return 0
	}
	m := loadThumbFailuresUnlocked()
	if m == nil {
		m = make(map[string]thumbFailureEntry)
	}
	e := m[videoPath]
	e.Count++
	e.LastFailed = time.Now().UTC()
	m[videoPath] = e

	data, _ := json.Marshal(m)
	tmp := path + ".tmp"
	if werr := os.WriteFile(tmp, data, 0644); werr == nil {
		_ = os.Rename(tmp, path)
	}
	return e.Count
}

// ClearThumbFailure resets the failure counter for a file (thumbnail
// generation succeeded, or the file was evicted/removed).
func ClearThumbFailure(videoPath string) {
	thumbFailureMu.Lock()
	defer thumbFailureMu.Unlock()

	path, err := thumbFailurePath()
	if err != nil {
		return
	}
	m := loadThumbFailuresUnlocked()
	if m == nil {
		return
	}
	if _, ok := m[videoPath]; !ok {
		return
	}
	delete(m, videoPath)
	data, _ := json.Marshal(m)
	tmp := path + ".tmp"
	if werr := os.WriteFile(tmp, data, 0644); werr == nil {
		_ = os.Rename(tmp, path)
	}
}

// ThumbFailures returns a copy of the current failure counts (diagnostics).
func ThumbFailures() map[string]int {
	thumbFailureMu.Lock()
	defer thumbFailureMu.Unlock()
	m := loadThumbFailuresUnlocked()
	out := make(map[string]int, len(m))
	for k, e := range m {
		out[k] = e.Count
	}
	return out
}
