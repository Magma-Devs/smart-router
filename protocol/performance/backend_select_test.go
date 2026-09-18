// Package performance_test (external — see backend_parity_test.go).
//
// The precedence + rollback regressions of the backend selection: the PRD's
// backwards-compatibility (UC-5) and rollback flows as executable tests.
package performance_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/protocol/performance"
	zerolog "github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// captureLog swaps the global zerolog sink for a buffer while fn runs and
// returns what was written; lavalog writes through that global logger.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	prev := zerologlog.Logger
	t.Cleanup(func() { zerologlog.Logger = prev })
	var buf bytes.Buffer
	zerologlog.Logger = zerolog.New(&buf)
	fn()
	return buf.String()
}

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
	flags.String(performance.CacheFlagName, "", "")
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
}

// MAG-3671, the second finding: the startup line printed the raw topology
// field, so an omitted topology showed as a blank and the only surface that
// could have revealed a misresolved configuration said nothing. It must name
// the topology the client was actually built with.
func TestSelectBackendLogsTheResolvedTopology(t *testing.T) {
	mr := miniredis.RunT(t)
	logged := captureLog(t, func() {
		backend := selectBackend(t, fmt.Sprintf(`
resp-cache:
  addresses: [%q]
`, mr.Addr()))
		require.True(t, backend.CacheActive())
	})
	require.Contains(t, logged, "resp-cache backend configured")
	require.Contains(t, logged, `"topology":"standalone"`, "an omitted topology is reported as what it resolves to, not as a blank")
}

// MAG-3684: turning off certificate verification was accepted in silence, so a
// deployment running without that check looked exactly like one running with
// it. It is now said at warning level at startup, carried on the configured
// line, and visible on the debug state.
func TestSelectBackendWarnsOnInsecureSkipVerify(t *testing.T) {
	mr := miniredis.RunT(t)
	block := func(insecure bool) string {
		return fmt.Sprintf(`
resp-cache:
  addresses: [%q]
  tls:
    enabled: true
    insecure-skip-verify: %t
`, mr.Addr(), insecure)
	}

	var insecure performance.CacheBackend
	logged := captureLog(t, func() { insecure = selectBackend(t, block(true)) })
	require.Contains(t, logged, "insecure-skip-verify is set", "the setting must be named at startup")
	require.Contains(t, logged, `"level":"warn"`)
	require.Contains(t, logged, `"tls-insecure-skip-verify":"true"`, "and carried on the configured line beside the tls switch")
	respCache, ok := insecure.(*performance.RespCache)
	require.True(t, ok)
	require.Contains(t, respCache.DebugCacheState().Address, "tls=insecure-skip-verify",
		"the state is visible wherever the cache's configuration is reported")

	quiet := captureLog(t, func() { _ = selectBackend(t, block(false)) })
	require.NotContains(t, quiet, "insecure-skip-verify is set", "a verifying configuration is not warned about")
	require.Contains(t, quiet, `"tls-insecure-skip-verify":"false"`)
}
