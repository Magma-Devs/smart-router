package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// observationlessBackend satisfies CacheBackend but not EndpointObservationBackend — the shape
// of a backend that cannot carry the gate, which must poll locally rather than panic.
type observationlessBackend struct{}

func (observationlessBackend) CacheActive() bool { return true }
func (observationlessBackend) GetEntry(context.Context, *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	return &pairingtypes.CacheRelayReply{}, nil
}

func (observationlessBackend) SetEntry(context.Context, *pairingtypes.RelayCacheSet) error {
	return nil
}
func (observationlessBackend) Flush(context.Context) error { return nil }
func (observationlessBackend) Close() error                { return nil }

// The fleet tracker gate is wired against the CAPABILITY, not the gRPC client's concrete type,
// so a router on the RESP backend gets a peer store. A typed-nil *Cache (no --cache-be) and a
// backend without the capability both yield nil — gate off, poll locally.
func TestPeerObservationStore_WiresAnyCapableBackend(t *testing.T) {
	endpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}

	mr := miniredis.RunT(t)
	store, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "sr")
	require.NoError(t, err)
	respCache := performance.NewRespCache(store, core.DefaultPolicy())
	t.Cleanup(func() { _ = respCache.Close() })

	require.NotNil(t, (&RPCSmartRouterServer{sharedState: true, cache: respCache, listenEndpoint: endpoint}).peerObservationStore(),
		"a RESP backend carries observations, so the gate is on")
	require.Nil(t, (&RPCSmartRouterServer{sharedState: false, cache: respCache, listenEndpoint: endpoint}).peerObservationStore(),
		"shared state off disables the gate on every backend")
	require.Nil(t, (&RPCSmartRouterServer{sharedState: true, cache: (*performance.Cache)(nil), listenEndpoint: endpoint}).peerObservationStore(),
		"an unconfigured gRPC cache travels as a typed nil and means no store")
	require.Nil(t, (&RPCSmartRouterServer{sharedState: true, cache: observationlessBackend{}, listenEndpoint: endpoint}).peerObservationStore(),
		"a backend without the capability polls locally")
}
