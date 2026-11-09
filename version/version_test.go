package version

import (
	"runtime/debug"
	"testing"
)

// buildInfo builds the record the toolchain embeds: the main module's derived
// version plus the VCS settings. A nil settings map means no VCS record, which
// is what a build with -buildvcs=false or from a tree without .git carries.
func buildInfo(mainVersion string, settings map[string]string) *debug.BuildInfo {
	info := &debug.BuildInfo{}
	info.Main.Version = mainVersion
	for k, v := range settings {
		info.Settings = append(info.Settings, debug.BuildSetting{Key: k, Value: v})
	}
	return info
}

const (
	stampedVersion = "v1.6.7"
	stampedCommit  = "2d99d4b7f4781193c2453a50ced99c6258dc5519"
	vcsRevision    = "d4270dcac280aef3e36e2742d8768e47db900d77"
	pseudoVersion  = "v1.6.7-0.20261104142042-d4270dcac280"
)

var vcsClean = map[string]string{
	"vcs":          "git",
	"vcs.revision": vcsRevision,
	"vcs.time":     "2026-11-04T14:20:42Z",
	"vcs.modified": "false",
}

func TestResolve(t *testing.T) {
	tests := []struct {
		name            string
		version, commit string // what -ldflags left in the vars
		info            *debug.BuildInfo
		ok              bool
		wantVersion     string
		wantCommit      string
	}{
		{
			name:    "both stamped by -ldflags: the VCS record is ignored",
			version: stampedVersion, commit: stampedCommit,
			info:        buildInfo(pseudoVersion, vcsClean),
			ok:          true,
			wantVersion: stampedVersion,
			wantCommit:  stampedCommit,
		},
		{
			name:    "nothing stamped, built inside a git checkout: both come from the VCS record",
			version: unsetVersion, commit: unsetCommit,
			info:        buildInfo(pseudoVersion, vcsClean),
			ok:          true,
			wantVersion: pseudoVersion,
			wantCommit:  vcsRevision,
		},
		{
			name:    "modified checkout: the commit stays the bare revision, the version carries the toolchain's +dirty",
			version: unsetVersion, commit: unsetCommit,
			info: buildInfo(pseudoVersion+"+dirty", map[string]string{
				"vcs.revision": vcsRevision,
				"vcs.modified": "true",
			}),
			ok:          true,
			wantVersion: pseudoVersion + "+dirty",
			wantCommit:  vcsRevision,
		},
		{
			name:    "only the commit stamped (sync_binary.sh shape): the version is still filled in",
			version: unsetVersion, commit: stampedCommit,
			info:        buildInfo(pseudoVersion, vcsClean),
			ok:          true,
			wantVersion: pseudoVersion,
			wantCommit:  stampedCommit,
		},
		{
			name:    "only the version stamped: the commit is still filled in",
			version: stampedVersion, commit: unsetCommit,
			info:        buildInfo(pseudoVersion, vcsClean),
			ok:          true,
			wantVersion: stampedVersion,
			wantCommit:  vcsRevision,
		},
		{
			name:    "no VCS record (-buildvcs=false or no .git): the defaults stay",
			version: unsetVersion, commit: unsetCommit,
			info:        buildInfo(develVersion, nil),
			ok:          true,
			wantVersion: unsetVersion,
			wantCommit:  unsetCommit,
		},
		{
			name:    "no build info at all: the defaults stay",
			version: unsetVersion, commit: unsetCommit,
			info:        nil,
			ok:          false,
			wantVersion: unsetVersion,
			wantCommit:  unsetCommit,
		},
		{
			name:    "an empty main version is not a version",
			version: unsetVersion, commit: unsetCommit,
			info:        buildInfo("", vcsClean),
			ok:          true,
			wantVersion: unsetVersion,
			wantCommit:  vcsRevision,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVersion, gotCommit := resolve(tt.version, tt.commit, tt.info, tt.ok)
			if gotVersion != tt.wantVersion {
				t.Errorf("version = %q, want %q", gotVersion, tt.wantVersion)
			}
			if gotCommit != tt.wantCommit {
				t.Errorf("commit = %q, want %q", gotCommit, tt.wantCommit)
			}
		})
	}
}

// init has already run by the time this test does. Check it against the record
// the toolchain actually embedded in this test binary, whatever that is: with a
// VCS record (none today, since go test does not stamp test binaries) the vars
// must carry it, without one they must still hold the sentinels -X replaces.
func TestInitFollowsTheEmbeddedRecord(t *testing.T) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Skip("no build info in this test binary")
	}
	wantCommit := unsetCommit
	if rev := setting(info, "vcs.revision"); rev != "" {
		wantCommit = rev
	}
	if Commit != wantCommit {
		t.Errorf("Commit = %q, want %q", Commit, wantCommit)
	}
	wantVersion := unsetVersion
	if v := info.Main.Version; v != "" && v != develVersion {
		wantVersion = v
	}
	if Version != wantVersion {
		t.Errorf("Version = %q, want %q", Version, wantVersion)
	}
}
