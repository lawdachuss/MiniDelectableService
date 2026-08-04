package channel

import (
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// TestConfiguredUploadHostsIsDelegated is a regression test for a data-loss
// bug: configuredUploadHosts() previously maintained a hand-written list that
// drifted out of sync with the actual uploaders.  IsAlreadyFullyUploaded()
// uses this list to decide whether the watcher may delete the local file, so
// when a host was omitted from the list the watcher could delete the file
// before that host had received it.
//
// The list must now exactly match uploader.NewMultiHostUploader(...).AvailableHosts().
func TestConfiguredUploadHostsIsDelegated(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{
		VoeSXAPIKey:     "key",
		StreamtapeLogin: "user",
		StreamtapeKey:   "pass",
		MixdropEmail:    "a@b.c",
		MixdropToken:    "tok",
		VidaraKey:       "vid-key",
	}

	hosts := configuredUploadHosts()
	has := func(name string) bool {
		for _, h := range hosts {
			if h == name {
				return true
			}
		}
		return false
	}
	for _, want := range []string{"GoFile", "VOE.sx", "Streamtape", "Mixdrop", "Vidara"} {
		if !has(want) {
			t.Errorf("configuredUploadHosts() missing %q; got %v", want, hosts)
		}
	}
}

// TestConfiguredUploadHostsOnlyGoFile confirms the minimal case still works and
// that IsAlreadyFullyUploaded's "len(hosts) == 0 -> false" guard is never hit
// when GoFile (always available) is the only configured host.
func TestConfiguredUploadHostsOnlyGoFile(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{} // no API keys -> only GoFile

	hosts := configuredUploadHosts()
	if len(hosts) != 1 || hosts[0] != "GoFile" {
		t.Fatalf("expected only [GoFile], got %v", hosts)
	}
}
