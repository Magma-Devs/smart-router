package redisstore

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Real-cluster proof: a three-master dockerized cluster reached through a
// SINGLE configuration endpoint (the discovery seed), entries spread across
// hash slots, cross-slot pipelined lookups, and the purge safety property —
// per-master SCAN with single-key UNLINKs deletes exactly one prefix across
// every slot while a second prefix survives untouched.
//
//	RESP_CACHE_TEST_CLUSTER_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestClusterDocker -v -timeout 5m
//
// Addressing uses the same host.docker.internal announcements + test-only
// port-preserving dialer as the sentinel drill.
func TestClusterDocker(t *testing.T) {
	if os.Getenv("RESP_CACHE_TEST_CLUSTER_DOCKER") != "1" {
		t.Skip("set RESP_CACHE_TEST_CLUSTER_DOCKER=1 (needs docker) to run the real-cluster lane")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker unavailable")
	}

	const network = "sr-cluster-net"
	ports := []int{7100, 7101, 7102}

	docker := func(args ...string) (string, error) {
		out, err := exec.Command("docker", args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	mustDocker := func(args ...string) string {
		out, err := docker(args...)
		require.NoError(t, err, "docker %s: %s", strings.Join(args, " "), out)
		return out
	}
	name := func(i int) string { return fmt.Sprintf("sr-clu-%d", i) }
	cleanup := func() {
		for i := range ports {
			_, _ = docker("rm", "-f", name(i))
		}
		_, _ = docker("network", "rm", network)
	}
	cleanup()
	t.Cleanup(cleanup)

	mustDocker("network", "create", network)
	for i, port := range ports {
		busPort := port + 10000
		mustDocker("run", "--rm", "-d", "--name", name(i), "--net", network,
			"--add-host", "host.docker.internal:host-gateway",
			"-p", fmt.Sprintf("0.0.0.0:%d:%d", port, port),
			"-p", fmt.Sprintf("0.0.0.0:%d:%d", busPort, busPort),
			"valkey/valkey:7.2",
			"valkey-server", "--port", fmt.Sprint(port),
			"--cluster-enabled", "yes",
			"--cluster-announce-ip", "host.docker.internal",
			"--cluster-announce-port", fmt.Sprint(port),
			"--cluster-announce-bus-port", fmt.Sprint(busPort),
			"--appendonly", "no", "--protected-mode", "no")
	}

	createArgs := []string{"exec", name(0), "valkey-cli", "--cluster", "create"}
	for _, port := range ports {
		createArgs = append(createArgs, fmt.Sprintf("host.docker.internal:%d", port))
	}
	createArgs = append(createArgs, "--cluster-yes")
	require.Eventually(t, func() bool {
		out, err := docker(createArgs...)
		return err == nil && strings.Contains(out, "[OK] All 16384 slots covered")
	}, 30*time.Second, 2*time.Second, "cluster create must cover all slots")

	require.Eventually(t, func() bool {
		out, _ := docker("exec", name(0), "valkey-cli", "-p", fmt.Sprint(ports[0]), "cluster", "info")
		return strings.Contains(out, "cluster_state:ok")
	}, 30*time.Second, time.Second, "cluster must reach state ok")

	// The store under test: cluster topology seeded with ONE configuration
	// endpoint, plus the test-only port-preserving dialer.
	cfg := Config{
		Topology:     TopologyCluster,
		Addresses:    []string{fmt.Sprintf("127.0.0.1:%d", ports[0])},
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	}
	require.NoError(t, cfg.Validate())
	opts := cfg.clusterOptions(cfg.Addresses, nil, NewStreamingProvider(cfg.credentialsSource()))
	opts.Dialer = func(ctx context.Context, netw, addr string) (net.Conn, error) {
		_, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return nil, splitErr
		}
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, netw, net.JoinHostPort("127.0.0.1", port))
	}
	clusterClient := redis.NewClusterClient(opts)

	store, err := NewWithClient(clusterClient, "sr")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	foreign, err := NewWithClient(clusterClient, "other")
	require.NoError(t, err)

	ctx := context.Background()
	require.Eventually(t, func() bool {
		return store.SetHeight(ctx, core.HeightKey("ETH1", "discovery-probe"), 1, time.Hour) == nil
	}, 30*time.Second, time.Second, "the client must serve through configuration-endpoint discovery")

	// Spread writes across slots (both prefixes), enough that multiple masters
	// must own data.
	for i := 0; i < 12; i++ {
		require.NoError(t, store.SetHeight(ctx, core.HeightKey("ETH1", fmt.Sprintf("0xhash-%d", i)), int64(i), time.Hour))
		require.NoError(t, foreign.SetHeight(ctx, core.HeightKey("ETH1", fmt.Sprintf("0xhash-%d", i)), int64(i), time.Hour))
	}
	env := core.NewEnvelope(&relaytypes.RelayReply{Data: []byte(`cluster-entry`)}, nil, false, nil, 100)
	require.NoError(t, store.SetEntry(ctx, core.RelayKey(false, "ETH1", []byte{0x01}, 100), &env, time.Hour))
	envF := core.NewEnvelope(&relaytypes.RelayReply{Data: []byte(`cluster-entry-f`)}, nil, true, nil, 100)
	require.NoError(t, store.SetEntry(ctx, core.RelayKey(true, "ETH1", []byte{0x01}, 100), &envF, time.Hour))

	nodesWithKeys := 0
	for i, port := range ports {
		out := mustDocker("exec", name(i), "valkey-cli", "-p", fmt.Sprint(port), "dbsize")
		if out != "0" {
			nodesWithKeys++
		}
	}
	require.GreaterOrEqual(t, nodesWithKeys, 2, "writes must spread across multiple masters (hash slots)")

	// Cross-slot pipelined lookup: the two finality variants of one identity
	// hash to different slots, fetched in one GetEntries call.
	entries, err := store.GetEntries(ctx, []string{
		core.RelayKey(false, "ETH1", []byte{0x01}, 100),
		core.RelayKey(true, "ETH1", []byte{0x01}, 100),
	})
	require.NoError(t, err)
	require.NotNil(t, entries[0])
	require.NotNil(t, entries[1])

	// The purge safety property, against a REAL sharded keyspace: one prefix
	// vanishes across every master, the other survives in full.
	require.NoError(t, store.Purge(ctx))

	for i, port := range ports {
		out := mustDocker("exec", name(i), "valkey-cli", "-p", fmt.Sprint(port), "--scan", "--pattern", "sr:*")
		require.Empty(t, out, "master %d must hold no sr:* keys after the purge", i)
	}
	surviving := 0
	for i := 0; i < 12; i++ {
		if _, found, _ := foreign.GetHeight(ctx, core.HeightKey("ETH1", fmt.Sprintf("0xhash-%d", i))); found {
			surviving++
		}
	}
	require.Equal(t, 12, surviving, "every foreign-prefix key must survive the purge")
}
