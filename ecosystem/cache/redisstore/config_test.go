package redisstore

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		{"sentinel cred file on cluster", Config{Topology: TopologyCluster, Addresses: []string{"c:6379"}, SentinelPasswordFile: "/p"}, "dangling"},
		{"db on cluster", Config{Topology: TopologyCluster, Addresses: []string{"c:6379"}, DB: 2}, "db selection"},
		{"password and password-file", Config{Addresses: []string{"h:1"}, Password: "a", PasswordFile: "/f"}, "mutually exclusive"},
		{"sentinel password and file", Config{Topology: TopologySentinel, MasterName: "m", Addresses: []string{"s:1"}, SentinelPassword: "a", SentinelPasswordFile: "/f"}, "mutually exclusive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
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
// connections, so an unfiltered tracking dialer records sentinel addresses
// alongside the master's — last write wins, and sentinels re-dial at arbitrary
// times (discovery, the +switch-master subscription). That makes
// Lava-Cache-Backend, the header a failover is observed through, nondeterministic.
func TestSentinelAddressesAreNotRecordedAsTheDataNode(t *testing.T) {
	sentinel := listenLocal(t)
	master := listenLocal(t)
	sentinelAddr, masterAddr := sentinel.Addr().String(), master.Addr().String()

	tracker := &endpointTracker{}
	dial := trackingDialerExcluding(nil, time.Second, tracker, []string{sentinelAddr})

	dialOnce := func(addr string) {
		t.Helper()
		conn, err := dial(context.Background(), "tcp", addr)
		require.NoError(t, err)
		_ = conn.Close()
	}

	dialOnce(sentinelAddr)
	require.Empty(t, tracker.current(), "a sentinel dial must not be recorded as the data node")

	dialOnce(masterAddr)
	require.Equal(t, masterAddr, tracker.current(), "the data node is what the tracker names")

	dialOnce(sentinelAddr)
	require.Equal(t, masterAddr, tracker.current(),
		"a later sentinel re-dial must not clobber the recorded master — this is the "+
			"nondeterminism that makes the failover header untrustworthy")
}

// The unfiltered dialer still records everything it dials; standalone and
// cluster depend on that.
func TestTrackingDialerRecordsWhenNothingIsExcluded(t *testing.T) {
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
	sentinelPwFile := writeTempFile(t, "sentinel-pass", "placeholder-sentinel-credential\n")
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
	require.Equal(t, "placeholder-sentinel-credential", opts.SentinelPassword, "control-plane password comes from the file, trimmed")
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
	require.Error(t, err, "unreadable credential file must fail construction, not first dial")

	_, err = New(Config{Addresses: []string{"h:1"}, TLS: TLSConfig{Enabled: true, CAFile: "/does/not/exist"}})
	require.Error(t, err, "unreadable CA must fail construction")

	_, err = New(Config{Addresses: []string{"h:1"}, TLS: TLSConfig{Enabled: true, CertFile: "/only/cert"}})
	require.Error(t, err, "client cert without key must fail construction")

	_, err = New(Config{Addresses: []string{"h:1"}, KeyPrefix: "glob*"})
	require.Error(t, err, "glob-unsafe prefix must fail construction")
}
