package redisstore

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/stretchr/testify/require"
)

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestConfigValidateMatrix(t *testing.T) {
	valid := Config{Topology: TopologyStandalone, Addresses: []string{"h:6379"}}
	require.NoError(t, valid.Validate())
	require.NoError(t, Config{Addresses: []string{"h:6379"}}.Validate(), "empty topology defaults to standalone")

	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"unknown topology", Config{Topology: "ring", Addresses: []string{"h:1"}}, "unknown topology"},
		{"no addresses", Config{Topology: TopologyStandalone}, "no addresses"},
		{"sentinel without master-name", Config{Topology: TopologySentinel, Addresses: []string{"s:26379"}}, "master-name"},
		{"sentinel creds on standalone", Config{Addresses: []string{"h:1"}, SentinelPassword: "pw"}, "dangling"},
		// MAG-3671: the reverse of the case above. A master-name with the
		// topology line forgotten used to be accepted, and the router dialled the
		// first sentinel as a data node. The message must name master-name — a
		// refusal that does not would pass "the router refuses" and leave the
		// operator exactly as lost.
		{"master-name with topology omitted", Config{Addresses: []string{"s1:26379", "s2:26379"}, MasterName: "mymaster"}, "master-name"},
		{"master-name on cluster", Config{Topology: TopologyCluster, Addresses: []string{"c:6379"}, MasterName: "mymaster"}, "master-name"},
		{"sentinel cred file on cluster", Config{Topology: TopologyCluster, Addresses: []string{"c:6379"}, SentinelPasswordFile: "/p"}, "dangling"},
		{"db on cluster", Config{Topology: TopologyCluster, Addresses: []string{"c:6379"}, DB: 2}, "db selection"},
		{"password and password-file", Config{Addresses: []string{"h:1"}, Password: "a", PasswordFile: "/f"}, "mutually exclusive"},
		{"sentinel password and file", Config{Topology: TopologySentinel, MasterName: "m", Addresses: []string{"s:1"}, SentinelPassword: "a", SentinelPasswordFile: "/f"}, "mutually exclusive"},
		// MAG-3683: a tls block without the switch. One case per key that can
		// make the block look complete, because each on its own reads as "TLS
		// is configured" to whoever wrote it.
		{"tls ca-file without enabled", Config{Addresses: []string{"h:1"}, TLS: TLSConfig{CAFile: "/ca.pem"}}, "tls.enabled"},
		{"tls client keypair without enabled", Config{Addresses: []string{"h:1"}, TLS: TLSConfig{CertFile: "/c.pem", KeyFile: "/k.pem"}}, "tls.enabled"},
		{"tls server-name without enabled", Config{Addresses: []string{"h:1"}, TLS: TLSConfig{ServerName: "cache.internal"}}, "tls.enabled"},
		{"tls insecure-skip-verify without enabled", Config{Addresses: []string{"h:1"}, TLS: TLSConfig{InsecureSkipVerify: true}}, "tls.enabled"},
		// MAG-3631: a lifetime cannot be negative; zero means the default.
		{"negative expiration", Config{Addresses: []string{"h:1"}, Expiration: ExpirationConfig{Finalized: -time.Second}}, "expiration.finalized"},
		{"negative multiplier", Config{Addresses: []string{"h:1"}, Expiration: ExpirationConfig{NonFinalizedMultiplier: -1}}, "expiration.non-finalized-multiplier"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

// The plaintext-credential warning (warnIfCredentialsCrossPlaintext) names the
// credential keys the block sets, data node and sentinel alike, and only the
// keys: what it says is what would cross the network readable, and it must
// never say the values.
func TestConfiguredCredentialKeys(t *testing.T) {
	require.Empty(t, Config{Addresses: []string{"h:1"}}.configuredCredentialKeys(), "nothing configured, nothing to name")
	require.Equal(t, []string{"username", "password"},
		Config{Addresses: []string{"h:1"}, Username: "u", Password: "p"}.configuredCredentialKeys())
	require.Equal(t, []string{"password-file"},
		Config{Addresses: []string{"h:1"}, PasswordFile: "/run/secrets/cache"}.configuredCredentialKeys())
	require.Equal(t, []string{"sentinel-username", "sentinel-password", "sentinel-password-file"},
		Config{Topology: TopologySentinel, MasterName: "m", Addresses: []string{"s:1"}, SentinelUsername: "u", SentinelPassword: "p", SentinelPasswordFile: "/f"}.configuredCredentialKeys())
}

// MAG-3631: the expiration block builds the engine's TTL table the way the
// sidecar builds its own from flags. The case that matters is the chart's: its
// shipped multiplier of 1.5 on settled answers used to be unreachable on a RESP
// backend, so every customer who moved their cache silently went from 90
// minutes to 60.
func TestExpirationConfigPolicy(t *testing.T) {
	require.Equal(t, core.DefaultPolicy(), ExpirationConfig{}.Policy(), "an empty block is the engine's defaults, exactly")

	chart := ExpirationConfig{FinalizedMultiplier: 1.5}.Policy()
	require.Equal(t, 90*time.Minute, chart.Finalized, "the chart's default multiplier on the default hour")
	require.Equal(t, core.DefaultExpirationForNonFinalized, chart.NonFinalized, "an unrelated field keeps its default")

	fullConfig := ExpirationConfig{
		Finalized:              2 * time.Hour,
		FinalizedMultiplier:    1.5,
		NonFinalized:           time.Second,
		NonFinalizedMultiplier: 1.25,
		NodeErrors:             100 * time.Millisecond,
		BlocksHashesToHeights:  24 * time.Hour,
	}
	full := fullConfig.Policy()
	require.Equal(t, 3*time.Hour, full.Finalized, "duration times multiplier, as the sidecar computes it")
	require.Equal(t, 1250*time.Millisecond, full.NonFinalized)
	require.Equal(t, 100*time.Millisecond, full.NodeErrors)
	require.Equal(t, 24*time.Hour, full.BlocksHashesToHeights)

	require.NoError(t, Config{Addresses: []string{"h:1"}, Expiration: fullConfig}.Validate())
}

// Every row of the table is checked as the lifetime it resolves to, against the
// one millisecond a RESP expiry can express (minLifetime). Below that the
// client rounds every write up to 1ms and logs it to stderr, and a product
// under one nanosecond truncates to a zero TTL, which the store writes as a key
// with no expiry at all (Codex review of #405). The row an operator actually
// types is a number without a unit — 3600 for an hour is 3.6µs — so the message
// says so; a tiny multiplier on a sound base is refused without that hint. A
// valid product is applied exactly, and a block that skipped validation is
// clamped to the floor rather than yielding a zero.
func TestExpirationConfigRejectsALifetimeBelowTheStoresPrecision(t *testing.T) {
	validate := func(e ExpirationConfig) error {
		return Config{Addresses: []string{"h:1"}, Expiration: e}.Validate()
	}
	unitless := validate(ExpirationConfig{Finalized: 3600})
	require.ErrorContains(t, unitless, "expiration.finalized is 3.6µs, shorter than the 1ms")
	require.ErrorContains(t, unitless, "3600 is 3.6µs", "the message shows the operator what their number became")
	require.ErrorContains(t, unitless, "such as 3600s", "and how to write it")

	require.ErrorContains(t, validate(ExpirationConfig{NodeErrors: 250, BlocksHashesToHeights: 172800}),
		"expiration.node-errors is 250ns", "rows without a multiplier are held to the same floor, in table order")

	tinyMultiplier := validate(ExpirationConfig{Finalized: time.Hour, FinalizedMultiplier: 1e-12})
	require.ErrorContains(t, tinyMultiplier, "expiration.finalized (1h0m0s) with expiration.finalized-multiplier (1e-12) is 3ns, shorter than the 1ms")
	require.NotContains(t, tinyMultiplier.Error(), "bare number", "the base had a unit; the multiplier is the problem")

	require.ErrorContains(t, validate(ExpirationConfig{Finalized: time.Nanosecond, FinalizedMultiplier: 0.5}),
		"shorter than the 1ms", "a product under one nanosecond is caught by the same floor")
	require.ErrorContains(t, validate(ExpirationConfig{NonFinalizedMultiplier: 1e-15}),
		"expiration.non-finalized (500ms) with expiration.non-finalized-multiplier (1e-15)", "the default base counts too")
	require.ErrorContains(t, validate(ExpirationConfig{Finalized: 100 * time.Hour, FinalizedMultiplier: 1e15}),
		"longer than a duration can hold")

	require.NoError(t, validate(ExpirationConfig{Finalized: time.Millisecond, NodeErrors: time.Millisecond}), "one millisecond is the floor, inclusive")

	halved := ExpirationConfig{Finalized: 2 * time.Second, FinalizedMultiplier: 0.5, NonFinalized: 400 * time.Millisecond, NonFinalizedMultiplier: 0.5}
	require.NoError(t, validate(halved))
	policy := halved.Policy()
	require.Equal(t, time.Second, policy.Finalized)
	require.Equal(t, 200*time.Millisecond, policy.NonFinalized)

	clamped := ExpirationConfig{Finalized: 3600, NonFinalizedMultiplier: 1e-15}.Policy()
	require.Equal(t, time.Millisecond, clamped.Finalized, "a block that skipped validation is clamped to what the store can express, never a zero")
	require.Equal(t, time.Millisecond, clamped.NonFinalized, "every row is clamped, not only the first that failed")
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// The endpoint tracker must name the DATA node, never a sentinel.
//
// go-redis reuses FailoverOptions.Dialer for the sentinel control-plane
// connections — including sentinels DISCOVERED at runtime (SENTINEL sentinels
// reports peers as IPs, so no exclude list built from configured hostnames can
// name them) — and sentinels re-dial at arbitrary times. That would make
// Lava-Cache-Backend, the header a failover is observed through,
// nondeterministic. The fix discriminates by which CLIENT dials: the failover
// client's hook chain (markDataDialsHook, installed by buildClient) stamps
// data-path dials, and the sentinel tracker records marked dials only. This
// test drives exactly that composition: the marked path is the hook's DialHook
// wrapped around the dialer, the unmarked path is the raw dialer — which is
// precisely how go-redis reaches it for sentinel connections.
func TestSentinelTrackerRecordsOnlyMarkedDataDials(t *testing.T) {
	discoveredSentinel := listenLocal(t) // in NO configured list — the exclude-list approach missed these
	master := listenLocal(t)
	sentinelAddr, masterAddr := discoveredSentinel.Addr().String(), master.Addr().String()

	tracker := &endpointTracker{}
	dial := trackingDialerMarkedOnly(nil, time.Second, tracker)
	markedDial := markDataDialsHook{}.DialHook(dial)

	dialOnce := func(dialFunc func(context.Context, string, string) (net.Conn, error), addr string) {
		t.Helper()
		conn, err := dialFunc(context.Background(), "tcp", addr)
		require.NoError(t, err)
		_ = conn.Close()
	}

	dialOnce(dial, sentinelAddr)
	require.Empty(t, tracker.current(),
		"an unmarked (control-plane) dial must not be recorded, whatever its address")

	dialOnce(markedDial, masterAddr)
	require.Equal(t, masterAddr, tracker.current(), "a marked (data) dial is what the tracker names")

	dialOnce(dial, sentinelAddr)
	require.Equal(t, masterAddr, tracker.current(),
		"a later control-plane re-dial — e.g. a runtime-discovered sentinel — must not "+
			"clobber the recorded master; this is the nondeterminism the static exclude list left open")
}

// The wiring half the mechanism test above cannot see: through the REAL New()
// construction, sentinel control-plane dials must leave the endpoint tracker
// untouched. The fake listener stands in for a sentinel — the TCP dial
// SUCCEEDS (so an unmarked-but-recording dialer would note it) but no RESP
// ever comes back, so discovery fails and Ping errors. If buildClient stopped
// installing markDataDialsHook, or the tracker's mark check were dropped,
// ReadEndpoint would name the fake sentinel here. The positive half — data
// dials recorded through the real construction — needs a live master to dial
// and lives in the docker sentinel drill and the sentinel demo lane.
func TestNewSentinelWiringDoesNotRecordControlPlaneDials(t *testing.T) {
	fakeSentinel := listenLocal(t)
	go func() {
		for {
			conn, err := fakeSentinel.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { _, _ = io.Copy(io.Discard, c) }(conn)
		}
	}()

	store, err := New(Config{
		Topology:    TopologySentinel,
		Addresses:   []string{fakeSentinel.Addr().String()},
		MasterName:  "mymaster",
		DialTimeout: 500 * time.Millisecond,
		ReadTimeout: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Error(t, store.Ping(ctx), "a sentinel that never answers cannot yield a master")
	require.Empty(t, store.ReadEndpoint(),
		"control-plane dials through the real construction must never be recorded as the data node")
}

// The plain dialer records everything it dials; standalone and cluster depend
// on that (every dial there is a data dial).
func TestTrackingDialerRecordsEveryDial(t *testing.T) {
	node := listenLocal(t)
	tracker := &endpointTracker{}
	dial := trackingDialer(nil, time.Second, tracker)

	conn, err := dial(context.Background(), "tcp", node.Addr().String())
	require.NoError(t, err)
	_ = conn.Close()

	require.Equal(t, node.Addr().String(), tracker.current())
}

// The sentinel control plane authenticates independently of the data nodes:
// the mapping must carry BOTH credential sets, or discovery against hardened
// sentinels fails before a data connection is ever attempted.
func TestFailoverOptionsMapping(t *testing.T) {
	// Leading whitespace as well as the trailing newline: the control-plane
	// file goes through the same trim as the data-node file (MAG-3685).
	sentinelPwFile := writeTempFile(t, "sentinel-pass", " \nplaceholder-sentinel-credential\n")
	cfg := Config{
		Topology:             TopologySentinel,
		Addresses:            []string{"s1:26379", "s2:26379", "s3:26379"},
		MasterName:           "mymaster",
		Username:             "datauser",
		Password:             "datapass",
		SentinelUsername:     "sentineluser",
		SentinelPasswordFile: sentinelPwFile,
		DB:                   1,
		DialTimeout:          time.Second,
		PoolSize:             7,
	}
	require.NoError(t, cfg.Validate())

	sentinelPassword, err := cfg.sentinelPassword()
	require.NoError(t, err)
	opts := cfg.failoverOptions(cfg.Addresses, nil, cfg.credentialsSource(), sentinelPassword, &endpointTracker{})

	require.Equal(t, "mymaster", opts.MasterName)
	require.Equal(t, cfg.Addresses, opts.SentinelAddrs)
	require.Equal(t, "sentineluser", opts.SentinelUsername)
	require.Equal(t, "placeholder-sentinel-credential", opts.SentinelPassword, "control-plane password comes from the file, trimmed on both sides")
	require.Equal(t, 1, opts.DB)
	require.Equal(t, 7, opts.PoolSize)

	// Sentinel data-node creds resolve per connection attempt (the streaming
	// re-auth manager is not initialized by NewFailoverClient in go-redis
	// v9.22 — see failoverOptions).
	require.Nil(t, opts.StreamingCredentialsProvider)
	require.NotNil(t, opts.CredentialsProviderContext)
	user, pass, err := opts.CredentialsProviderContext(t.Context())
	require.NoError(t, err)
	require.Equal(t, "datauser", user)
	require.Equal(t, "datapass", pass)
}

// The sentinel control-plane file is read by the same reader as the data-node
// file: whitespace on both sides and a leading byte order mark are trimmed,
// and an empty file is refused naming the path and the setting.
func TestSentinelPasswordFileIsReadLikeTheDataNodeFile(t *testing.T) {
	sentinelCfg := func(path string) Config {
		return Config{Topology: TopologySentinel, Addresses: []string{"s1:26379"}, MasterName: "mymaster", SentinelPasswordFile: path}
	}
	for _, tc := range []struct{ name, content, want string }{
		{"trailing newline", "placeholder-sentinel-credential\n", "placeholder-sentinel-credential"},
		{"leading whitespace", "\n placeholder-sentinel-credential\n", "placeholder-sentinel-credential"},
		{"UTF-8 BOM", utf8BOMBytes + "placeholder-sentinel-credential", "placeholder-sentinel-credential"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pw, err := sentinelCfg(writeTempFile(t, "sentinel-pass", tc.content)).sentinelPassword()
			require.NoError(t, err)
			require.Equal(t, tc.want, pw)
		})
	}
	t.Run("empty file is refused naming the path", func(t *testing.T) {
		path := writeTempFile(t, "sentinel-pass", " \n")
		_, err := sentinelCfg(path).sentinelPassword()
		require.ErrorContains(t, err, path)
		require.ErrorContains(t, err, "sentinel-password-file")
	})
}

func TestClusterOptionsMapping(t *testing.T) {
	cfg := Config{
		Topology:  TopologyCluster,
		Addresses: []string{"config-endpoint.cluster.example:6379"},
		Password:  "pw",
	}
	require.NoError(t, cfg.Validate())
	provider := NewStreamingProvider(cfg.credentialsSource())
	opts := cfg.clusterOptions(cfg.Addresses, nil, provider, &endpointTracker{})

	require.Equal(t, cfg.Addresses, opts.Addrs,
		"the configuration endpoint is the discovery seed — never a full node list")
	require.Same(t, provider, opts.StreamingCredentialsProvider.(*StreamingProvider))
}

func TestNewFailsFastOnBadInputs(t *testing.T) {
	_, err := New(Config{Addresses: []string{"h:1"}, PasswordFile: "/does/not/exist"})
	require.ErrorContains(t, err, "/does/not/exist", "unreadable credential file must fail construction naming the file, not first dial")

	_, err = New(Config{Addresses: []string{"h:1"}, TLS: TLSConfig{Enabled: true, CAFile: "/does/not/exist"}})
	require.ErrorContains(t, err, "/does/not/exist", "unreadable CA must fail construction naming the file")

	_, err = New(Config{Addresses: []string{"h:1"}, TLS: TLSConfig{Enabled: true, CertFile: "/only/cert"}})
	require.Error(t, err, "client cert without key must fail construction")

	_, err = New(Config{Addresses: []string{"h:1"}, KeyPrefix: "glob*"})
	require.Error(t, err, "glob-unsafe prefix must fail construction")

	_, err = New(Config{Addresses: []string{"h:1"}, Password: "placeholder-credential", TLS: TLSConfig{CAFile: "/does/not/exist"}})
	require.ErrorContains(t, err, "tls.enabled", "a tls block without the switch must fail construction, not dial in plaintext (MAG-3683)")
}

// The topology an operator is shown must be the one the client is built with.
func TestEffectiveTopologyResolvesTheDefault(t *testing.T) {
	require.Equal(t, TopologyStandalone, Config{}.EffectiveTopology(), "an omitted topology is standalone, and must be reported as such")
	require.Equal(t, TopologySentinel, Config{Topology: TopologySentinel}.EffectiveTopology())
	require.Equal(t, TopologyCluster, Config{Topology: TopologyCluster}.EffectiveTopology())
}
