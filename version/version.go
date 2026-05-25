// Package version exposes the smart-router release version.
//
// Both vars are populated at build time via -ldflags. CI derives Version from
// `git describe --tags --always --dirty` and Commit from the build SHA:
//
//	go build -ldflags "\
//	  -X github.com/Magma-Devs/smart-router/version.Version=$(git describe --tags --always --dirty) \
//	  -X github.com/Magma-Devs/smart-router/version.Commit=$(git rev-parse HEAD)"
//
// Unset-at-build-time defaults are "dev"/"none" so local `go run` and
// `go install` produce identifiable non-release binaries.
package version

var (
	Version = "dev"
	Commit  = "none"
)
