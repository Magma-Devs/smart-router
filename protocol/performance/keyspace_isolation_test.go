package performance_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MAG-3521 over the real hop: one cache server, three routers on one chain —
// two with their own key prefix, one on the shared keyspace every router had
// before the setting existed. What the ticket measured on a cluster: the
// second router served the first router's stored answer, a value none of its
// own nodes produce, as an ordinary cache hit.

func newPrefixedGRPCBackend(t *testing.T, addr, keyPrefix string) *performance.Cache {
	t.Helper()
	client, err := performance.InitCacheWithKeyPrefix(context.Background(), addr, keyPrefix)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.Eventually(t, client.CacheActive, 5*time.Second, 10*time.Millisecond)
	return client
}

func TestKeyPrefixIsolatesRoutersOnOneCacheServer(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	routerA := newPrefixedGRPCBackend(t, addr, "tenant-a")
	routerB := newPrefixedGRPCBackend(t, addr, "tenant-b")
	legacy := newPrefixedGRPCBackend(t, addr, "")
	hash := []byte("eth_getBalance-0x1111")

	// Router A's nodes answer 0xc0ffee; nobody else's do. Waiting for A's own
	// read-back is what makes the misses below meaningful: the entry is in the
	// store before anyone else asks.
	setForParity(t, routerA, false, hash, nil, []byte(`0xc0ffee`), 100, 100)
	eventuallyData(t, routerA, hash, nil, 100, 100, false, []byte(`0xc0ffee`))

	// The ticket's step 2: the identical question on another router. It must
	// NOT be answered from A's entry — by concrete block, nor by LATEST, which
	// A's write resolved through A's own chain tip.
	for name, other := range map[string]performance.CacheBackend{"another prefix": routerB, "the shared keyspace": legacy} {
		require.Nil(t, getForParity(t, other, hash, nil, 100, 100, false).GetReply(),
			"%s must not serve tenant-a's entry", name)
		require.Nil(t, getForParity(t, other, hash, nil, spectypes.LATEST_BLOCK, 100, false).GetReply(),
			"%s must not resolve LATEST through tenant-a's chain tip", name)
	}

	// Positive control, the one the ticket demands of the live test too: the
	// other routers' cache paths are live — each serves its own write back...
	setForParity(t, routerB, false, hash, nil, []byte(`0x0`), 100, 100)
	eventuallyData(t, routerB, hash, nil, 100, 100, false, []byte(`0x0`))
	setForParity(t, legacy, false, hash, nil, []byte(`0x1`), 100, 100)
	eventuallyData(t, legacy, hash, nil, 100, 100, false, []byte(`0x1`))
	// ...and none of those writes reached A.
	eventuallyData(t, routerA, hash, nil, 100, 100, false, []byte(`0xc0ffee`))
	eventuallyData(t, routerA, hash, nil, spectypes.LATEST_BLOCK, 100, false, []byte(`0xc0ffee`))
}

// Sticky claims name an upstream by NAME, so a claim made by a router on one
// node set means nothing to a router on another — it is scoped like every
// other key.
func TestKeyPrefixIsolatesStickyClaims(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	routerA := newPrefixedGRPCBackend(t, addr, "tenant-a")
	routerB := newPrefixedGRPCBackend(t, addr, "tenant-b")
	ctx := context.Background()

	won, err := routerA.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "base", "digest-1",
		core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-a", won.Provider)

	_, found, err := routerB.GetStickySession(ctx, "ETH1", "jsonrpc", "base", "digest-1")
	require.NoError(t, err)
	require.False(t, found, "tenant-b has no upstream named node-a and must not inherit the claim")

	claimB, err := routerB.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "base", "digest-1",
		core.StickyPin{Provider: "node-b", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-b", claimB.Provider, "tenant-b's own claim wins in its own keyspace")

	pinA, found, err := routerA.GetStickySession(ctx, "ETH1", "jsonrpc", "base", "digest-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "node-a", pinA.Provider, "tenant-a's claim is untouched")
}

// The debug endpoint renders the keyspace the way the RESP tier does, so an
// operator can see that two routers on one server sit on different prefixes
// without sending traffic.
func TestKeyPrefixIsReportedInDebugState(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	prefixed := newPrefixedGRPCBackend(t, addr, "tenant-a")
	require.Equal(t, addr+" prefix=tenant-a (unconfirmed)", prefixed.DebugCacheState().Address,
		"until the server has answered on this connection, nothing has confirmed it scopes by the prefix")
	require.Equal(t, "tenant-a", prefixed.KeyPrefix())
	require.Equal(t, addr, prefixed.BackendEndpoint(), "the dialled address itself is unchanged")
	getForParity(t, prefixed, []byte("debug-state-hash"), nil, 100, 100, false)
	require.Equal(t, addr+" prefix=tenant-a", prefixed.DebugCacheState().Address,
		"the first reply from a server that knows the field confirms the keyspace")

	legacy := newPrefixedGRPCBackend(t, addr, "")
	require.Equal(t, addr, legacy.DebugCacheState().Address, "no prefix, no suffix — the field reads exactly as before")
	require.Empty(t, legacy.KeyPrefix())
}

// legacyCacheServer stands in for a cache server built before the keyspace
// field existed: the field is dropped on the way in, so nothing is scoped, and
// no reply echoes it. That is the shape MAG-3521's review names — the operator
// set the one setting that must differ between deployments, and got no
// isolation and no sign of it.
type legacyCacheServer struct {
	*cache.RelayerCacheServer
}

func (s *legacyCacheServer) GetRelay(ctx context.Context, req *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	stripped := *req
	stripped.KeyPrefix = ""
	reply, err := s.RelayerCacheServer.GetRelay(ctx, &stripped)
	if reply != nil {
		reply.KeyPrefix = ""
	}
	return reply, err
}

func (s *legacyCacheServer) SetRelay(ctx context.Context, req *pairingtypes.RelayCacheSet) (*emptypb.Empty, error) {
	stripped := *req
	stripped.KeyPrefix = ""
	return s.RelayerCacheServer.SetRelay(ctx, &stripped)
}

func (s *legacyCacheServer) GetStickySession(ctx context.Context, req *pairingtypes.StickySessionGet) (*pairingtypes.StickySessionReply, error) {
	stripped := *req
	stripped.KeyPrefix = ""
	reply, err := s.RelayerCacheServer.GetStickySession(ctx, &stripped)
	if reply != nil {
		reply.KeyPrefix = ""
	}
	return reply, err
}

func (s *legacyCacheServer) SetStickySession(ctx context.Context, req *pairingtypes.StickySessionSet) (*pairingtypes.StickySessionReply, error) {
	stripped := *req
	stripped.KeyPrefix = ""
	reply, err := s.RelayerCacheServer.SetStickySession(ctx, &stripped)
	if reply != nil {
		reply.KeyPrefix = ""
	}
	return reply, err
}

// A server that ignores the prefix is detected from its first reply: the
// router warns once per connection and qualifies the prefix on the debug
// endpoint, and keeps serving. A server that scopes by it confirms it, with no
// warning; a router with no prefix has nothing to confirm on either.
func TestKeyPrefixEchoNamesAServerThatIgnoresIt(t *testing.T) {
	const unisolated = "did not echo it"
	legacyAddr := startLoopbackCacheServerWith(t, func(srv *cache.RelayerCacheServer) pairingtypes.RelayerCacheServer {
		return &legacyCacheServer{RelayerCacheServer: srv}
	})
	hash := []byte("echo-hash")

	prefixed := newPrefixedGRPCBackend(t, legacyAddr, "tenant-a")
	require.Equal(t, legacyAddr+" prefix=tenant-a (unconfirmed)", prefixed.DebugCacheState().Address)
	logged := captureLog(t, func() {
		getForParity(t, prefixed, hash, nil, 100, 100, false)
		getForParity(t, prefixed, hash, nil, 100, 100, false)
		_, _, err := prefixed.GetStickySession(context.Background(), "ETH1", "jsonrpc", "base", "digest-1")
		require.NoError(t, err)
		setForParity(t, prefixed, false, hash, nil, []byte(`0x1`), 100, 100)
	})
	require.Equal(t, 1, strings.Count(logged, unisolated), "one connection, one warning, however many replies came back without the echo:\n%s", logged)
	require.Contains(t, logged, `"key-prefix":"tenant-a"`)
	require.Equal(t, legacyAddr+" prefix=tenant-a (ignored by the cache server)", prefixed.DebugCacheState().Address,
		"the debug endpoint says the prefix isolates nothing on this server")
	// Serving continues: an unisolated cache is still a cache.
	eventuallyData(t, prefixed, hash, nil, 100, 100, false, []byte(`0x1`))

	// The in-process server logs its own debug lines through the same sink, so
	// the controls look for the warning rather than for silence.
	unprefixed := newPrefixedGRPCBackend(t, legacyAddr, "")
	require.NotContains(t, captureLog(t, func() {
		getForParity(t, unprefixed, hash, nil, 100, 100, false)
	}), unisolated, "no prefix, nothing to confirm")
	require.Equal(t, legacyAddr, unprefixed.DebugCacheState().Address)

	// The control: the same traffic against a server that scopes by the prefix
	// confirms it and warns about nothing.
	currentAddr := startLoopbackCacheServer(t)
	confirmed := newPrefixedGRPCBackend(t, currentAddr, "tenant-a")
	require.NotContains(t, captureLog(t, func() {
		getForParity(t, confirmed, hash, nil, 100, 100, false)
		_, _, err := confirmed.GetStickySession(context.Background(), "ETH1", "jsonrpc", "base", "digest-1")
		require.NoError(t, err)
	}), unisolated)
	require.Equal(t, currentAddr+" prefix=tenant-a", confirmed.DebugCacheState().Address)
}
