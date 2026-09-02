package channel

import (
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
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
//
// AnonMP4 is also always-on (no auth), so we allow it.
func TestConfiguredUploadHostsOnlyGoFile(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{} // no API keys in Config

	hosts := configuredUploadHosts()
	// GoFile is always available. AnonMP4 is always available (no auth).
	has := func(name string) bool {
		for _, h := range hosts {
			if h == name {
				return true
			}
		}
		return false
	}
	if !has("GoFile") {
		t.Fatalf("GoFile missing; got %v", hosts)
	}
	// AnonMP4 is always available with no auth required
	if !has("AnonMP4") {
		t.Fatalf("expected always-available host AnonMP4 missing; got %v", hosts)
	}
}

// TestConfiguredUploadHostsHonorsDisabledList verifies that the DISABLED_UPLOAD_HOSTS
// deadlist flows all the way through to configuredUploadHosts(): a deadlisted host is
// neither attempted on upload nor required by IsAlreadyFullyUploaded (both delegate to
// AvailableHosts), so files aren't kept forever waiting for a link we'll never make.
func TestConfiguredUploadHostsHonorsDisabledList(t *testing.T) {
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

	uploader.SetDisabledHosts([]string{"AnonMP4", "Vidara"})
	defer uploader.SetDisabledHosts(nil)

	hosts := configuredUploadHosts()
	has := func(name string) bool {
		for _, h := range hosts {
			if h == name {
				return true
			}
		}
		return false
	}
	for _, disabled := range []string{"AnonMP4", "Vidara"} {
		if has(disabled) {
			t.Errorf("disabled host %q still in configuredUploadHosts(); got %v", disabled, hosts)
		}
	}
	for _, want := range []string{"GoFile", "VOE.sx", "Streamtape", "Mixdrop"} {
		if !has(want) {
			t.Errorf("configuredUploadHosts() missing %q; got %v", want, hosts)
		}
	}
}
