package redisstore

import (
	"os"
	"os/exec"
	"testing"
)

// requireDockerLane gates a docker-backed acceptance lane on its opt-in
// environment variable.
//
// Not opting in skips, as usual. Opting in WITHOUT a reachable docker daemon
// is a hard failure rather than a skip: these lanes are the evidence behind
// the "Local testing lanes" section of docs/RESP-CACHE.md, and `go test` exits
// 0 on a skipped test. An operator running a documented command with docker
// down would otherwise see PASS and tick a box that verified nothing.
func requireDockerLane(t *testing.T, envVar, lane string) {
	t.Helper()
	if os.Getenv(envVar) != "1" {
		t.Skipf("set %s=1 (needs docker) to run %s", envVar, lane)
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Fatalf("%s=1 requests %s, but docker is unavailable (`docker info` failed: %v) — start docker, or unset %s to skip the lane deliberately", envVar, lane, err, envVar)
	}
}
