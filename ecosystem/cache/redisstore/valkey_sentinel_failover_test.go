package redisstore

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// The cadence of the router's health probe while its breaker is open, which
// is the only traffic that reaches the client during an outage.
const (
	outageProbeTimeout  = 3 * time.Second
	outageProbeInterval = time.Second
	failedOutageProbe   = outageProbeTimeout + outageProbeInterval
)

// recoveryTime probes the way the router does until a write lands where
// landed finds it, and returns how long that took.
func recoveryTime(t *testing.T, store *Store, landed func(storedKey string) bool, limit time.Duration) time.Duration {
	t.Helper()
	key := core.HeightKey("ETH1", "after")
	start := time.Now()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), outageProbeTimeout)
		err := store.SetHeight(ctx, key, 2, time.Hour)
		cancel()
		elapsed := time.Since(start)
		if err == nil {
			require.True(t, landed(store.key(key)), "the write lands on the primary the Sentinels name")
			return elapsed
		}
		require.Less(t, elapsed, limit, "no write reached the primary; last error: %v", err)
		time.Sleep(outageProbeInterval)
	}
}

const (
	laneNetwork          = "sr-sentinel-net"
	laneMasterName       = "mymaster"
	laneDataPassword     = "placeholder-data-credential"
	laneSentinelPassword = "placeholder-sentinel-credential"
)

// laneNode is one node of the lane: a Valkey store and the Sentinel beside it.
type laneNode struct {
	store, sentinel         string // container names
	storeAddr, sentinelAddr string
}

// sentinelLane is three such nodes on a bridge network. The host reaches the
// containers at their own addresses, as a router pod reaches store pods, so
// the store under test runs the production dialer on what the Sentinels report.
type sentinelLane struct {
	t     *testing.T
	nodes []*laneNode
}

func docker(args ...string) (string, error) {
	out, err := exec.Command("docker", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func mustDocker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := docker(args...)
	require.NoError(t, err, "docker %s: %s", strings.Join(args, " "), out)
	return out
}

func startSentinelLane(t *testing.T) *sentinelLane {
	t.Helper()
	lane := &sentinelLane{t: t}
	for _, letter := range []string{"a", "b", "c"} {
		lane.nodes = append(lane.nodes, &laneNode{store: "sr-sentinel-store-" + letter, sentinel: "sr-sentinel-watch-" + letter})
	}
	cleanup := func() {
		for _, node := range lane.nodes {
			_, _ = docker("rm", "-f", node.store, node.sentinel)
		}
		_, _ = docker("network", "rm", laneNetwork)
	}
	cleanup() // leftovers of an aborted run
	t.Cleanup(cleanup)
	mustDocker(t, "network", "create", laneNetwork)

	address := func(container string, port int) string {
		ip := mustDocker(t, "inspect", "-f", fmt.Sprintf(`{{(index .NetworkSettings.Networks %q).IPAddress}}`, laneNetwork), container)
		return net.JoinHostPort(ip, fmt.Sprint(port))
	}
	for i, node := range lane.nodes {
		args := []string{
			"run", "-d", "--name", node.store, "--net", laneNetwork, valkeyImage,
			"valkey-server", "--requirepass", laneDataPassword, "--masterauth", laneDataPassword,
		}
		if i > 0 {
			primaryHost, primaryPort, _ := net.SplitHostPort(lane.nodes[0].storeAddr)
			args = append(args, "--replicaof", primaryHost, primaryPort)
		}
		mustDocker(t, args...)
		node.storeAddr = address(node.store, 6379)
	}
	primaryHost, primaryPort, _ := net.SplitHostPort(lane.nodes[0].storeAddr)
	for i, node := range lane.nodes {
		// A fixed id, so a restarted Sentinel is the same voter and the others
		// do not count it twice. The file is written afresh at every start.
		conf := fmt.Sprintf(`port 26379
sentinel myid %040x
sentinel monitor %[2]s %[3]s %[4]s 2
sentinel auth-pass %[2]s %[5]s
sentinel down-after-milliseconds %[2]s 2000
sentinel failover-timeout %[2]s 10000
requirepass %[6]s
`, i+1, laneMasterName, primaryHost, primaryPort, laneDataPassword, laneSentinelPassword)
		mustDocker(t, "run", "-d", "--name", node.sentinel, "--net", laneNetwork, "-e", "SENTINEL_CONF="+conf, valkeyImage,
			"sh", "-c", `printf '%s' "$SENTINEL_CONF" > /tmp/sentinel.conf && exec valkey-sentinel /tmp/sentinel.conf`)
		node.sentinelAddr = address(node.sentinel, 26379)
	}

	lane.eventually(30*time.Second, "both replicas follow the primary", func() bool { return lane.replicas(lane.nodes[0]) == 2 })
	lane.awaitQuorum(lane.nodes...)
	return lane
}

func (l *sentinelLane) eventually(limit time.Duration, what string, cond func() bool) {
	l.t.Helper()
	if !waitFor(cond, limit) {
		var logs strings.Builder
		for _, node := range l.nodes {
			out, _ := docker("logs", "--tail", "40", node.sentinel)
			fmt.Fprintf(&logs, "\n--- %s ---\n%s", node.sentinel, out)
		}
		l.t.Fatalf("not within %s: %s%s", limit, what, logs.String())
	}
}

func waitFor(cond func() bool, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return cond()
}

func (l *sentinelLane) ask(node *laneNode, args ...any) (any, error) {
	client := redis.NewSentinelClient(&redis.Options{Addr: node.sentinelAddr, Password: laneSentinelPassword, DialTimeout: time.Second})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := redis.NewCmd(ctx, args...)
	_ = client.Process(ctx, cmd)
	return cmd.Result()
}

// primary is the store the node's Sentinel names, nil while it names none of
// the lane's stores or does not answer.
func (l *sentinelLane) primary(asked *laneNode) *laneNode {
	reply, err := l.ask(asked, "sentinel", "get-master-addr-by-name", laneMasterName)
	parts, ok := reply.([]any)
	if err != nil || !ok || len(parts) != 2 {
		return nil
	}
	named := net.JoinHostPort(fmt.Sprint(parts[0]), fmt.Sprint(parts[1]))
	for _, node := range l.nodes {
		if node.storeAddr == named {
			return node
		}
	}
	return nil
}

// awaitQuorum waits until each of the nodes' Sentinels can reach the two
// others, which is what a failover needs and what a router discovers.
func (l *sentinelLane) awaitQuorum(nodes ...*laneNode) {
	l.t.Helper()
	for _, node := range nodes {
		l.eventually(60*time.Second, node.sentinel+" counts three usable Sentinels", func() bool {
			reply, err := l.ask(node, "sentinel", "ckquorum", laneMasterName)
			return err == nil && strings.HasPrefix(fmt.Sprint(reply), "OK 3 usable Sentinels")
		})
	}
}

// onStore runs fn on a connection of the test's own to the node's store.
func (l *sentinelLane) onStore(node *laneNode, fn func(context.Context, *redis.Client)) {
	client := redis.NewClient(&redis.Options{Addr: node.storeAddr, Password: laneDataPassword, DialTimeout: time.Second})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fn(ctx, client)
}

// replicas is how many replicas the node's store counts as attached, -1
// while it is not a primary.
func (l *sentinelLane) replicas(node *laneNode) int {
	count := -1
	l.onStore(node, func(ctx context.Context, client *redis.Client) {
		if info, err := client.Info(ctx, "replication").Result(); err == nil && strings.Contains(info, "role:master") {
			count = strings.Count(info, "state=online")
		}
	})
	return count
}

// landedOn reports whether a key reached the node's store.
func (l *sentinelLane) landedOn(node *laneNode) func(string) bool {
	return func(storedKey string) (found bool) {
		l.onStore(node, func(ctx context.Context, client *redis.Client) {
			found = client.Exists(ctx, storedKey).Val() == 1
		})
		return found
	}
}

// dropStoreConnections closes, on the server, every connection a go-redis
// client holds to the node's store, so the next operation has to dial and with
// that ask a Sentinel. The Sentinels' and replicas' own connections stay.
func (l *sentinelLane) dropStoreConnections(node *laneNode) {
	l.t.Helper()
	dropped := 0
	l.onStore(node, func(ctx context.Context, admin *redis.Client) {
		self := admin.ClientID(ctx).Val()
		list, err := admin.ClientList(ctx).Result()
		require.NoError(l.t, err)
		for _, line := range strings.Split(list, "\n") {
			var id int64
			if !strings.Contains(line, "lib-name=go-redis") {
				continue
			}
			if _, err := fmt.Sscanf(line, "id=%d ", &id); err != nil || id == self {
				continue
			}
			require.NoError(l.t, admin.Do(ctx, "client", "kill", "id", id).Err())
			dropped++
		}
	})
	require.Positive(l.t, dropped, "the store under test holds a connection to the primary")
}

// newStore builds the store under test the way a router does: New() on a
// config with password files. With one seed the client attaches to that
// Sentinel and learns the others from it.
func (l *sentinelLane) newStore(t *testing.T, seeds ...*laneNode) *Store {
	t.Helper()
	cfg := Config{
		Topology:             TopologySentinel,
		MasterName:           laneMasterName,
		PasswordFile:         writeTempFile(t, "data-pass", laneDataPassword+"\n"),
		SentinelPasswordFile: writeTempFile(t, "sentinel-pass", laneSentinelPassword+"\n"),
	}
	for _, seed := range seeds {
		cfg.Addresses = append(cfg.Addresses, seed.sentinelAddr)
	}
	store, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), outageProbeTimeout)
		defer cancel()
		return store.SetHeight(ctx, core.HeightKey("ETH1", "before"), 1, time.Hour) == nil
	}, 30*time.Second, 100*time.Millisecond, "the store serves through Sentinel discovery")
	return store
}

// The docker-gated Sentinel lane: real Valkey stores and Sentinels, and the
// store under test on the production path (New(), the production dialer, both
// password files). Each scenario ends with a write landing on the primary the
// Sentinels name, within a limit counted in failed health probes.
//
//	RESP_CACHE_TEST_SENTINEL_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestSentinelFailover -v -timeout 5m
//
// Linux only: the host has to reach the containers at their bridge addresses.
func TestSentinelFailover(t *testing.T) {
	requireDockerLane(t, "RESP_CACHE_TEST_SENTINEL_DOCKER", "the sentinel failover drill")
	if runtime.GOOS != "linux" {
		t.Fatalf("the sentinel failover drill dials the containers' bridge addresses, which only a Linux host reaches (this is %s)", runtime.GOOS)
	}
	lane := startSentinelLane(t)
	a, b, c := lane.nodes[0], lane.nodes[1], lane.nodes[2]
	report := func(t *testing.T, elapsed time.Duration) {
		t.Helper()
		t.Logf("a write landed on the primary after %s", elapsed.Round(10*time.Millisecond))
	}

	// The primary is healthy in the first two scenarios. What is at stake is
	// the next connection to it, which the client opens only after asking the
	// Sentinel it is attached to.
	t.Run("a hung sentinel", func(t *testing.T) {
		store := lane.newStore(t, c)
		mustDocker(t, "pause", c.sentinel)
		defer func() { mustDocker(t, "unpause", c.sentinel) }()
		lane.dropStoreConnections(a)
		report(t, recoveryTime(t, store, lane.landedOn(a), 2*failedOutageProbe))
	})

	t.Run("a stopped sentinel", func(t *testing.T) {
		store := lane.newStore(t, c)
		mustDocker(t, "stop", "-t", "2", c.sentinel)
		lane.dropStoreConnections(a)
		report(t, recoveryTime(t, store, lane.landedOn(a), 2*failedOutageProbe))
		mustDocker(t, "start", c.sentinel)
		lane.awaitQuorum(a, b)
	})

	// The node goes with its store, its Sentinel and every connection to them:
	// nothing is refused, nothing answers. The client is attached to the lost
	// Sentinel and holds an idle connection to the lost primary.
	var promoted *laneNode
	t.Run("a lost node", func(t *testing.T) {
		store := lane.newStore(t, a)
		mustDocker(t, "network", "disconnect", laneNetwork, a.store)
		mustDocker(t, "network", "disconnect", laneNetwork, a.sentinel)
		lane.eventually(60*time.Second, "the remaining Sentinels promote a replica", func() bool {
			promoted = lane.primary(b)
			return promoted != nil && promoted != a && lane.primary(c) == promoted
		})
		report(t, recoveryTime(t, store, lane.landedOn(promoted), 3*failedOutageProbe))
	})
	require.NotNil(t, promoted, "the lost-node scenario promoted a replica")

	// One configured Sentinel is gone from the start here, and the client is
	// attached to a live one, which announces the switch.
	t.Run("a stopped primary", func(t *testing.T) {
		last := b
		if promoted == b {
			last = c
		}
		lane.eventually(60*time.Second, "the remaining replica follows the promoted store", func() bool { return lane.replicas(promoted) == 1 })
		store := lane.newStore(t, a, b, c)
		mustDocker(t, "stop", "-t", "2", promoted.store)
		lane.eventually(60*time.Second, "the remaining Sentinels promote the last store", func() bool {
			return lane.primary(b) == last && lane.primary(c) == last
		})
		report(t, recoveryTime(t, store, lane.landedOn(last), 2*failedOutageProbe))
	})
}
