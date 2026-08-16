package redisstore

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Second)
	}
	return cond()
}

// The UC-2 acceptance proof: a dockerized sentinel topology (primary, replica,
// three sentinels — data plane AND control plane authenticated), the primary
// killed mid-run, and the SAME Store object serving writes again after the
// promotion — reconnection is transparent, no re-construction, no restart.
//
//	RESP_CACHE_TEST_SENTINEL_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestSentinelFailover -v -timeout 5m
//
// Docker-network addressing note: sentinels announce and discover through
// host.docker.internal + host-bound ports, and the client's Dialer rewrites
// every reported host to 127.0.0.1 while keeping the port — in this topology
// the port alone identifies the node, which sidesteps the classic
// sentinel-behind-NAT hostname problem without touching production code.
func TestSentinelFailover(t *testing.T) {
	requireDockerLane(t, "RESP_CACHE_TEST_SENTINEL_DOCKER", "the sentinel failover drill")

	const (
		network      = "sr-sentinel-net"
		primaryPort  = 63791
		replicaPort  = 63792
		dataPass     = "datapass"
		sentinelPass = "sentinelpass"
		masterName   = "mymaster"
	)
	sentinelPorts := []int{26380, 26381, 26382}

	docker := func(args ...string) (string, error) {
		out, err := exec.Command("docker", args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	mustDocker := func(args ...string) string {
		out, err := docker(args...)
		require.NoError(t, err, "docker %s: %s", strings.Join(args, " "), out)
		return out
	}
	cleanup := func() {
		for _, name := range []string{"sr-sent-primary", "sr-sent-replica", "sr-sent-s0", "sr-sent-s1", "sr-sent-s2"} {
			_, _ = docker("rm", "-f", name)
		}
		_, _ = docker("network", "rm", network)
	}
	cleanup() // stale leftovers from an aborted run
	t.Cleanup(cleanup)

	mustDocker("network", "create", network)

	common := []string{"--rm", "-d", "--net", network, "--add-host", "host.docker.internal:host-gateway"}
	run := func(name string, port int, env []string, image string, cmd ...string) {
		args := append([]string{"run", "--name", name, "-p", fmt.Sprintf("0.0.0.0:%d:%d", port, port)}, common...)
		args = append(args, env...)
		args = append(args, image)
		args = append(args, cmd...)
		mustDocker(args...)
	}
	requireRunning := func(name string) {
		state, _ := docker("inspect", "-f", "{{.State.Running}}", name)
		if state != "true" {
			logs, _ := docker("logs", name)
			t.Fatalf("container %s is not running; logs:\n%s", name, logs)
		}
	}

	// Data plane: primary and replica announce themselves through the host
	// bindings so both sentinels and the (host-side) client can reach them.
	run("sr-sent-primary", primaryPort, nil, "valkey/valkey:7.2",
		"valkey-server", "--port", fmt.Sprint(primaryPort),
		"--requirepass", dataPass, "--masterauth", dataPass,
		"--replica-announce-ip", "host.docker.internal", "--replica-announce-port", fmt.Sprint(primaryPort))
	run("sr-sent-replica", replicaPort, nil, "valkey/valkey:7.2",
		"valkey-server", "--port", fmt.Sprint(replicaPort),
		"--replicaof", "host.docker.internal", fmt.Sprint(primaryPort),
		"--requirepass", dataPass, "--masterauth", dataPass,
		"--replica-announce-ip", "host.docker.internal", "--replica-announce-port", fmt.Sprint(replicaPort))

	for i, port := range sentinelPorts {
		// The conf travels through an env var — newlines survive docker -e
		// verbatim, sidestepping shell-quoting entirely.
		conf := fmt.Sprintf(`port %d
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip host.docker.internal
sentinel announce-port %d
sentinel monitor %s host.docker.internal %d 2
sentinel auth-pass %s %s
sentinel down-after-milliseconds %s 2000
sentinel failover-timeout %s 10000
requirepass %s
`, port, port, masterName, primaryPort, masterName, dataPass, masterName, masterName, sentinelPass)
		run(fmt.Sprintf("sr-sent-s%d", i), port, []string{"-e", "SENTINEL_CONF=" + conf}, "valkey/valkey:7.2",
			"sh", "-c", `printf '%s' "$SENTINEL_CONF" > /tmp/sentinel.conf && exec valkey-sentinel /tmp/sentinel.conf`)
	}

	time.Sleep(2 * time.Second)
	for _, name := range []string{"sr-sent-primary", "sr-sent-replica", "sr-sent-s0", "sr-sent-s1", "sr-sent-s2"} {
		requireRunning(name)
	}

	masterAddr := func() string {
		out, _ := docker("exec", "sr-sent-s0", "valkey-cli", "-p", fmt.Sprint(sentinelPorts[0]),
			"-a", sentinelPass, "--no-auth-warning", "sentinel", "get-master-addr-by-name", masterName)
		return out
	}
	if !waitFor(func() bool { return strings.Contains(masterAddr(), fmt.Sprint(primaryPort)) }, 30*time.Second) {
		logs, _ := docker("logs", "sr-sent-s0")
		t.Fatalf("sentinels did not discover the primary; get-master-addr=%q; sentinel logs:\n%s", masterAddr(), logs)
	}

	// The store under test: the production sentinel mapping (both credential
	// domains) plus the test-only port-preserving Dialer.
	cfg := Config{
		Topology:         TopologySentinel,
		Addresses:        []string{"127.0.0.1:26380", "127.0.0.1:26381", "127.0.0.1:26382"},
		MasterName:       masterName,
		Password:         dataPass,
		SentinelPassword: sentinelPass,
		DialTimeout:      2 * time.Second,
		ReadTimeout:      2 * time.Second,
		WriteTimeout:     2 * time.Second,
	}
	require.NoError(t, cfg.Validate())
	opts := cfg.failoverOptions(cfg.Addresses, nil, cfg.credentialsSource(), sentinelPass)
	opts.Dialer = func(ctx context.Context, netw, addr string) (net.Conn, error) {
		_, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return nil, splitErr
		}
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, netw, net.JoinHostPort("127.0.0.1", port))
	}
	store, err := NewWithClient(redis.NewFailoverClient(opts), "sentineltest")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	require.Eventually(t, func() bool {
		return store.SetHeight(ctx, core.HeightKey("ETH1", "pre-failover"), 1, time.Hour) == nil
	}, 30*time.Second, time.Second, "the store must serve through sentinel discovery")
	v, found, err := store.GetHeight(ctx, core.HeightKey("ETH1", "pre-failover"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), v)

	// Kill the primary. Sentinels detect, quorum, and promote the replica; the
	// SAME store must resume serving writes with no re-construction.
	t.Log("stopping the primary; awaiting promotion")
	mustDocker("stop", "sr-sent-primary")

	require.Eventually(t, func() bool {
		return store.SetHeight(ctx, core.HeightKey("ETH1", "post-failover"), 2, time.Hour) == nil
	}, 90*time.Second, time.Second, "the store must resume serving after the promotion, without being rebuilt")

	// The client can resume writing through the promoted node BEFORE sentinel
	// s0's own published view has converged — go-redis rediscovers on failure,
	// while the sentinel updates its master record on its own cadence. Asserting
	// the address immediately therefore races the control plane and can fail
	// even though failover fully succeeded. Wait for the view to converge, then
	// assert on it.
	var lastSeen string
	require.Eventually(t, func() bool {
		lastSeen = masterAddr()
		return strings.Contains(lastSeen, fmt.Sprint(replicaPort))
	}, 60*time.Second, time.Second,
		"sentinels must converge on the promoted replica as the new master (last view: %s)", lastSeen)
	v, found, err = store.GetHeight(ctx, core.HeightKey("ETH1", "post-failover"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), v, "post-failover writes land on the promoted primary")
}
