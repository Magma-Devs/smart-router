package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// tierSpy is a cache tier that answers every lookup with a hit and counts what the relay path
// asks of it. It serves as the primary (CacheBackend) or the secondary (CacheReader).
type tierSpy struct{ gets, sets atomic.Int64 }

func (s *tierSpy) CacheActive() bool { return true }

func (s *tierSpy) GetEntry(context.Context, *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	s.gets.Add(1)
	return &pairingtypes.CacheRelayReply{Reply: &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0xcac4e"}`)}}, nil
}

func (s *tierSpy) SetEntry(context.Context, *pairingtypes.RelayCacheSet) error {
	s.sets.Add(1)
	return nil
}

func (s *tierSpy) Flush(context.Context) error { return nil }
func (s *tierSpy) Close() error                { return nil }

// With --disable-relay-cache neither tier is asked and an upstream answers. The same server
// without the flag serves the primary's entry, so the request is one a cache would answer.
func TestDisableRelayCacheAsksNoTier(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := ethJsonRPCParser(t)
	answer := refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`}

	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("disabled=%t", disabled), func(t *testing.T) {
			hits, server := newRefusalTestServer(t, chainParser, logs, answer, answer)
			primary, secondary := &tierSpy{}, &tierSpy{}
			server.cache, server.secondaryCache = primary, secondary
			server.relayCacheDisabled = disabled

			result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
			require.NoError(t, sendErr)

			if !disabled {
				require.Equal(t, int64(1), primary.gets.Load(), "the primary tier is asked")
				require.Equal(t, [2]int64{0, 0}, loadRefusalHits(hits), "a hit never reaches an upstream")
				require.Contains(t, string(result.Reply.Data), "0xcac4e", "the caller gets the cached answer")
				return
			}
			require.Zero(t, primary.gets.Load(), "the primary tier is not asked")
			require.Zero(t, secondary.gets.Load(), "the secondary tier is not asked")
			require.Equal(t, [2]int64{1, 0}, loadRefusalHits(hits), "the request is answered by an upstream")
			require.Contains(t, string(result.Reply.Data), "0x10", "the caller gets the upstream's answer")
			require.Never(t, func() bool { return primary.sets.Load() > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
				"the upstream's answer is not written to the backend")
		})
	}
}

// The write path on its own: a reply the router caches without the flag never reaches the
// backend with it.
func TestDisableRelayCacheWritesNothing(t *testing.T) {
	reply := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1312d00"}`)

	t.Run("without the flag the reply is written", func(t *testing.T) {
		backend := &recordingCacheBackend{}
		f := newCapFixture(t, backend, 0)
		f.write(reply)
		require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("with the flag it is not", func(t *testing.T) {
		backend := &recordingCacheBackend{}
		f := newCapFixture(t, backend, 0)
		f.rpcss.relayCacheDisabled = true
		f.write(reply)
		require.Never(t, func() bool { return len(backend.recorded()) > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
			"no SetEntry reaches the backend")
	})
}

// The flag turns off relay entries only: the tracker gate's peer store is wired against the
// same backend, with the flag as without it.
func TestDisableRelayCacheKeepsTheTrackerGate(t *testing.T) {
	endpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}
	mr := miniredis.RunT(t)
	store, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "sr")
	require.NoError(t, err)
	respCache := performance.NewRespCache(store, core.DefaultPolicy())
	t.Cleanup(func() { _ = respCache.Close() })

	require.NotNil(t, (&RPCSmartRouterServer{sharedState: true, relayCacheDisabled: true, cache: respCache, listenEndpoint: endpoint}).peerObservationStore(),
		"poll observations still travel through the backend")
	require.Nil(t, (&RPCSmartRouterServer{sharedState: false, relayCacheDisabled: true, cache: respCache, listenEndpoint: endpoint}).peerObservationStore(),
		"without --shared-state the backend carries nothing")
}

// The flag is registered on the router command and off unless given: a deployment that passes it
// to a router without it exits on an unknown flag.
func TestDisableRelayCacheFlagIsRegisteredAndOffByDefault(t *testing.T) {
	flag := CreateRPCSmartRouterCobraCommand().Flags().Lookup(common.DisableRelayCacheFlag)
	require.NotNil(t, flag, "--%s is a router flag", common.DisableRelayCacheFlag)
	require.Equal(t, "disable-relay-cache", flag.Name)
	require.Equal(t, "bool", flag.Value.Type())
	require.Equal(t, "false", flag.DefValue, "relay caching is on unless the flag is given")
}

func TestRelayCacheModeRefusesASecondaryCache(t *testing.T) {
	require.NoError(t, relayCacheModeFrom(false, true, true, true).validate(), "a secondary cache is fine while relay caching is on")
	require.NoError(t, relayCacheModeFrom(true, true, false, true).validate(), "relay caching off with no secondary is fine")

	err := relayCacheModeFrom(true, true, true, true).validate()
	require.Error(t, err, "a tier that only serves cached replies contradicts the flag")
	require.Contains(t, err.Error(), "--"+common.DisableRelayCacheFlag)
	require.Contains(t, err.Error(), performance.SecondaryCacheFlagName)
	require.Contains(t, err.Error(), performance.SecondaryRespCacheViperKey)
}

func TestRelayCacheModeAdvisory(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode        relayCacheMode
		wantMessage string
		wantWarning bool
	}{
		{"flag unset says nothing", relayCacheModeFrom(false, true, false, true), "", false},
		{"no backend: nothing to turn off", relayCacheModeFrom(true, false, false, true), "no cache backend is configured", false},
		{"a backend nothing uses is a warning", relayCacheModeFrom(true, true, false, false), "nothing uses it", true},
		{"shared state only", relayCacheModeFrom(true, true, false, true), "carries shared state only", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, warning := tc.mode.advisory()
			if tc.wantMessage == "" {
				require.Empty(t, message)
			} else {
				require.Contains(t, message, tc.wantMessage)
			}
			require.Equal(t, tc.wantWarning, warning)
		})
	}
}

// /debug/cache-state reports a configured, reachable primary on a router that caches no relay,
// so the flag is on the wire beside the tiers.
func TestCacheStateReportsRelayCacheDisabled(t *testing.T) {
	tier := &stubCacheStateBackend{state: grpcTier("cache-be:20100", boolPtr(true))}

	rr, resp := getCacheState(t, debugMuxDeps{cache: tier, relayCacheDisabled: true})
	require.True(t, resp.RelayCacheDisabled)
	require.True(t, resp.Tiers.Primary.Configured, "the tier is still reported: it carries shared state")
	require.Contains(t, rr.Body.String(), `"relay_cache_disabled":true`)

	rr, resp = getCacheState(t, debugMuxDeps{cache: tier})
	require.False(t, resp.RelayCacheDisabled)
	require.Contains(t, rr.Body.String(), `"relay_cache_disabled":false`, "present when false: absence must not read as off")
}
