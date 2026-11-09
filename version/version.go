// Package version exposes the smart-router build identity: a release version
// and the commit the binary was built from.
//
// Both vars are -ldflags -X targets. CI and the release tooling derive
// Version from `git describe --tags --always --dirty` and Commit from the
// build SHA:
//
//	go build -ldflags "\
//	  -X github.com/magma-Devs/smart-router/version.Version=$(git describe --tags --always --dirty) \
//	  -X github.com/magma-Devs/smart-router/version.Commit=$(git rev-parse HEAD)"
//
// A build that skips those flags is not unidentifiable, though. Since Go 1.18
// the toolchain stamps every main package built inside a git checkout with the
// checkout's revision, and since Go 1.24 it derives the main module's version
// from the nearest tag. init reads that record and fills in whichever of the
// two the linker left at its default, so `smartrouter version` names the build
// for a plain `go build ./cmd/smartrouter` as well as for a release.
//
// Only a build with neither stamp (for example one made with -buildvcs=false,
// or from a source tree without .git) still reports "dev" / "none".
//
// A git worktree is the exception to "inside a git checkout": its .git is a
// file, which the toolchain does not take for a checkout. A worktree outside
// any checkout gets no record; one nested inside another checkout gets that
// checkout's record, and so names a commit it was not built from. Stamp a
// worktree build with -X (make build does).
package version

import "runtime/debug"

var (
	Version = "dev"
	Commit  = "none"
)

// The values the linker leaves when -X does not set the var. init treats them
// as "unset"; a stamped value is never overridden.
const (
	unsetVersion = "dev"
	unsetCommit  = "none"
)

// develVersion is what debug.BuildInfo.Main.Version reports when the toolchain
// could not derive a version from VCS tags; it carries no information.
const develVersion = "(devel)"

func init() {
	info, ok := debug.ReadBuildInfo()
	Version, Commit = resolve(Version, Commit, info, ok)
}

// resolve returns the version and commit a binary should report, given the
// -ldflags values and the toolchain's embedded build record. A value set by
// -ldflags wins; an unset one is taken from the VCS record when there is one.
//
// Commit is the bare 40-hex revision, the same shape -ldflags writes, so a
// consumer comparing it to `git rev-parse HEAD` needs no special case. The
// toolchain marks a modified checkout in the version instead (a "+dirty"
// suffix), matching the "-dirty" that `git describe --dirty` adds to a stamped
// Version.
func resolve(version, commit string, info *debug.BuildInfo, ok bool) (string, string) {
	if !ok || info == nil {
		return version, commit
	}
	if commit == unsetCommit {
		if rev := setting(info, "vcs.revision"); rev != "" {
			commit = rev
		}
	}
	if version == unsetVersion {
		if v := info.Main.Version; v != "" && v != develVersion {
			version = v
		}
	}
	return version, commit
}

// setting returns the value of a build setting, or "" when it is absent.
func setting(info *debug.BuildInfo, key string) string {
	for _, s := range info.Settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}
