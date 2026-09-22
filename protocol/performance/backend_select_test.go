// Package performance_test (external — see backend_parity_test.go).
//
// The precedence + rollback regressions of the backend selection: the PRD's
// backwards-compatibility (UC-5) and rollback flows as executable tests.
package performance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const (
	selectEventuallyTimeout = 2 * time.Second
	selectEventuallyTick    = 20 * time.Millisecond
)

func viperFromYAML(t *testing.T, yaml string) *viper.Viper {
	t.Helper()
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yaml)))
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String(performance.RespCacheAddressesFlagName, "", "")
	flags.String(performance.RespCacheTopologyFlagName, "", "")
	flags.String(performance.RespCacheKeyPrefixFlagName, "", "")
	flags.String(performance.CacheFlagName, "", "")
	flags.String(performance.CacheKeyPrefixFlagName, "", "")
	require.NoError(t, v.BindPFlags(flags))
	return v
}

func selectBackend(t *testing.T, yaml string) performance.CacheBackend {
	t.Helper()
	backend, err := performance.SelectCacheBackend(context.Background(), viperFromYAML(t, yaml))
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	return backend
}

// UC-5: no cache configuration at all — the inert typed-nil backend, exactly
// today's behavior.
func TestSelectBackendNeitherConfigured(t *testing.T) {
	backend := selectBackend(t, `metrics-listen-address: "disabled"`)
	grpcClient, ok := backend.(*performance.Cache)
	require.True(t, ok, "unconfigured must remain the gRPC client type (typed-nil)")
	require.Nil(t, grpcClient)
	require.False(t, backend.CacheActive())
	require.NoError(t, backend.Close(), "typed-nil backend is inert, not a panic")
}

// UC-5: only cache-be — the gRPC client, byte-for-byte today's path.
func TestSelectBackendGRPCOnly(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	backend := selectBackend(t, fmt.Sprintf(`cache-be: %q`, addr))
	_, ok := backend.(*performance.Cache)
	require.True(t, ok)
	require.Eventually(t, backend.CacheActive, selectEventuallyTimeout, selectEventuallyTick,
		"the gRPC client must connect to the configured cache server")
}

// resp-cache only — the RESP backend serves end to end.
func TestSelectBackendRespOnly(t *testing.T) {
	mr := miniredis.RunT(t)
	backend := selectBackend(t, fmt.Sprintf(`
resp-cache:
  addresses: [%q]
  key-prefix: selecttest
`, mr.Addr()))
	_, ok := backend.(*performance.RespCache)
	require.True(t, ok)
	require.True(t, backend.CacheActive())

	setForParity(t, backend, false, []byte("select-hash"), nil, []byte(`payload`), 100, 100)
	eventuallyData(t, backend, []byte("select-hash"), nil, 100, 100, false, []byte(`payload`))
	require.NotEmpty(t, mr.Keys(), "entries land in the RESP backend under the configured prefix")
	for _, key := range mr.Keys() {
		require.True(t, strings.HasPrefix(key, "selecttest:"), "key %q must carry the configured prefix", key)
	}
}

// Precedence + rollback: with BOTH configured the RESP backend serves (and the
// gRPC cache stays untouched); removing the resp-cache block reverts to the
// preserved cache-be — the PRD's rollback flow, config-change only.
func TestSelectBackendPrecedenceAndRollback(t *testing.T) {
	grpcAddr := startLoopbackCacheServer(t)
	mr := miniredis.RunT(t)

	both := fmt.Sprintf(`
cache-be: %q
resp-cache:
  addresses: [%q]
  key-prefix: precedence
`, grpcAddr, mr.Addr())
	backend := selectBackend(t, both)
	_, ok := backend.(*performance.RespCache)
	require.True(t, ok, "with both configured, the RESP backend must win")

	setForParity(t, backend, false, []byte("precedence-hash"), nil, []byte(`resp-served`), 100, 100)
	eventuallyData(t, backend, []byte("precedence-hash"), nil, 100, 100, false, []byte(`resp-served`))
	require.NotEmpty(t, mr.Keys(), "the write went to the RESP backend, not the gRPC cache")

	// Rollback = delete the resp-cache block; the preserved cache-be takes
	// over on the next start.
	rolledBack := selectBackend(t, fmt.Sprintf(`cache-be: %q`, grpcAddr))
	grpcClient, ok := rolledBack.(*performance.Cache)
	require.True(t, ok, "removing the resp-cache configuration must revert to the gRPC client")
	require.NotNil(t, grpcClient)
	require.Eventually(t, rolledBack.CacheActive, selectEventuallyTimeout, selectEventuallyTick)
}

// Misconfiguration aborts selection (and therefore startup) instead of
// silently running cacheless.
func TestSelectBackendConfigErrorsAbort(t *testing.T) {
	_, err := performance.SelectCacheBackend(context.Background(), viperFromYAML(t, `
resp-cache:
  key-prefix: dangling-only
`))
	require.ErrorContains(t, err, "dangling")

	_, err = performance.SelectCacheBackend(context.Background(), viperFromYAML(t, `
resp-cache:
  addresses: ["a:6379"]
  key-prefix: "glob*unsafe"
`))
	require.Error(t, err, "a glob-unsafe prefix must abort startup")

	_, err = performance.SelectCacheBackend(context.Background(), viperFromYAML(t, `
cache-be: "a:20100"
cache-be-key-prefix: "glob*unsafe"
`))
	require.ErrorContains(t, err, performance.CacheKeyPrefixFlagName,
		"the gRPC keyspace takes the same character set, and a bad one must abort startup rather than start cacheless")
}

// The gRPC keyspace setting end to end through selection: two routers selected
// against one cache server with different cache-be-key-prefix values share no
// entries, and the debug state names the keyspace (MAG-3521).
func TestSelectBackendGRPCKeyPrefix(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	tenantA := selectBackend(t, fmt.Sprintf("cache-be: %q\ncache-be-key-prefix: tenant-a\n", addr))
	tenantB := selectBackend(t, fmt.Sprintf("cache-be: %q\ncache-be-key-prefix: tenant-b\n", addr))
	require.Eventually(t, tenantA.CacheActive, selectEventuallyTimeout, selectEventuallyTick)
	require.Eventually(t, tenantB.CacheActive, selectEventuallyTimeout, selectEventuallyTick)

	grpcA, ok := tenantA.(*performance.Cache)
	require.True(t, ok)
	require.Equal(t, addr+" prefix=tenant-a (unconfirmed)", grpcA.DebugCacheState().Address,
		"before the first reply nothing has confirmed the server scopes by the prefix")

	hash := []byte("select-prefix-hash")
	setForParity(t, tenantA, false, hash, nil, []byte(`tenant-a`), 100, 100)
	eventuallyData(t, tenantA, hash, nil, 100, 100, false, []byte(`tenant-a`))
	require.Nil(t, getForParity(t, tenantB, hash, nil, 100, 100, false).GetReply(),
		"a router selected with another prefix must not see the entry")
	require.Equal(t, addr+" prefix=tenant-a", grpcA.DebugCacheState().Address,
		"a server that knows the field has echoed it by now")
}

// A cache-be-key-prefix beside a resp-cache block scopes nothing: the RESP
// backend outranks cache-be and the gRPC client is never built. Said at
// startup naming the prefix, since the keyspace in force is the block's
// (MAG-3521 review); the same prefix with no RESP block is the dangling shape,
// reported by the other line.
func TestSelectBackendWarnsWhenTheGRPCPrefixIsOutranked(t *testing.T) {
	mr := miniredis.RunT(t)
	logged := captureLog(t, func() {
		selectBackend(t, fmt.Sprintf("resp-cache:\n  addresses: [%q]\n  key-prefix: in-force\ncache-be: \"127.0.0.1:1\"\ncache-be-key-prefix: outranked\n", mr.Addr()))
	})
	require.Contains(t, logged, "takes precedence, so the gRPC prefix scopes nothing")
	require.Contains(t, logged, `"key-prefix":"outranked"`)
	require.Contains(t, logged, `"resp-cache-key-prefix":"in-force"`)
	require.NotContains(t, logged, "dangling configuration, it scopes nothing", "the outranked shape is not the dangling one")

	logged = captureLog(t, func() {
		selectBackend(t, "cache-be-key-prefix: dangling\n")
	})
	require.Contains(t, logged, "dangling configuration, it scopes nothing")
	require.Contains(t, logged, `"key-prefix":"dangling"`)
}

// MAG-3683, the shape the refusal cannot reach. Credentials with no tls block
// at all are deliberate and stay allowed, and the harm the refusal names — the
// password readable in the first write of every connection — was just as real
// for them and silent: the startup line said tls=false in one word among six.
// The store now warns once at construction, naming the endpoints and which
// credential keys are set. The two controls pin what the warning is about: the
// same credentials under TLS are not warned about, and no credentials are not.
func TestSelectBackendWarnsWhenCredentialsCrossPlaintext(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireUserAuth("cacheuser", "placeholder-cache-credential")
	const warning = "credentials are configured without tls"
	block := func(tlsLines string) string {
		return fmt.Sprintf(`
resp-cache:
  addresses: [%q]
  username: cacheuser
  password: placeholder-cache-credential
%s`, mr.Addr(), tlsLines)
	}

	plaintext := captureLog(t, func() {
		backend := selectBackend(t, block(""))
		require.True(t, backend.CacheActive())
		require.NoError(t, backend.Close())
	})
	require.Contains(t, plaintext, warning, "credentials without tls must be said out loud at startup")
	require.Contains(t, plaintext, `"level":"warn"`)
	require.Contains(t, plaintext, `"credential-keys":"username,password"`, "the warning names which keys are set")
	require.NotContains(t, plaintext, "placeholder-cache-credential", "and never their values")

	// Control: the same credentials under TLS. The handshake fails against the
	// plaintext server, so the probe logs its own failure while the cache lives;
	// closing inside the capture keeps that to this capture.
	encrypted := captureLog(t, func() {
		backend := selectBackend(t, block("  tls:\n    enabled: true\n"))
		require.NoError(t, backend.Close())
	})
	require.NotContains(t, encrypted, warning, "with tls on the credentials do not cross readable, so nothing to warn about")

	// Control: no credentials at all.
	anonymous := captureLog(t, func() {
		backend := selectBackend(t, fmt.Sprintf("\nresp-cache:\n  addresses: [%q]\n", mr.Addr()))
		require.NoError(t, backend.Close())
	})
	require.NotContains(t, anonymous, warning, "with nothing to protect there is nothing to warn about")
}
