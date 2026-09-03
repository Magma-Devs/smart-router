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
