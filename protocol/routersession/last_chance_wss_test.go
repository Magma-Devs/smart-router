package routersession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// lcBesideAWebsocketURL adds a ws url after the provider's own: an https url and a wss url per
// provider, the documented ETH1 jsonrpc shape.
func lcBesideAWebsocketURL(t *testing.T, p *ConsumerSessionsWithProvider, wsURL string) *ConsumerSessionsWithProvider {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: wsURL}, 5, "jsonrpc")
	require.NoError(t, err)
	p.Endpoints = append(p.Endpoints, &Endpoint{NetworkAddress: wsURL, Enabled: true, DirectConnections: []DirectRPCConnection{conn}})
	return p
}

// MAG-4829 on the documented ETH1 shape. The provider lists an https url and a wss url, and its https
// url is off. The blocked list is empty, as after another request's release, and this attempt passed
// the provider over. The wss url is never sent a relay, so it stays on, and it must not hide the
// provider from the last chance. A websocket-only provider is not a candidate: no relay can use its
// url.
func TestLastChance_AWssURLDoesNotHideAProviderFromTheLastChance(t *testing.T) {
	p := lcBesideAWebsocketURL(t, lcProvider(t, "primary", "http://127.0.0.1:1"), "ws://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	wsConn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: "ws://127.0.0.1:2"}, 5, "jsonrpc")
	require.NoError(t, err)
	wsOnly := NewConsumerSessionWithProvider("wsonly", []*Endpoint{{
		NetworkAddress: "ws://127.0.0.1:2", Enabled: true, DirectConnections: []DirectRPCConnection{wsConn},
	}}, 999999, 1, int64(1))
	wsOnly.StaticProvider = true
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p, wsOnly}, nil)
	require.Empty(t, csm.ProviderRoutingSnapshot().CurrentlyBlockedProviderAddresses, "premise: nothing is on the blocked list")

	csm.lock.RLock()
	var candidates []string
	for _, candidate := range csm.lastChanceCandidatesLocked() {
		candidates = append(candidates, candidate.PublicAddress)
	}
	csm.lock.RUnlock()
	require.Equal(t, []string{"primary"}, candidates, "the https + wss provider is out of rotation; the websocket-only one is no candidate")

	ignored := &ignoredProviders{
		providers:          map[string]struct{}{"primary": {}, "wsonly": {}}, // both passed over by this attempt
		triedByThisRequest: map[string]struct{}{},
		currentEpoch:       csm.atomicReadCurrentEpoch(),
	}
	got, err := csm.tryGetConsumerSessionWithProviderFromBlockedProviderList(context.Background(), 1, ignored, 10, spectypes.LATEST_BLOCK, "", nil, nil, common.NO_STATE, 0, NewUsedProviders(nil))
	require.NoError(t, err, "the last chance must not end in No pairings available")
	require.Contains(t, got, "primary")
	require.True(t, got["primary"].lastChance)
}

// MAG-4829 end to end on the same shape: many requests at once, the https url off and the wss url
// on. Each one is lent the https url, never the wss one, however the blocked list flips under it.
func TestLastChance_EveryConcurrentRequestIsLentTheHttpsURLBesideAWss(t *testing.T) {
	const rounds, concurrent = 20, 50
	p := lcBesideAWebsocketURL(t, lcProvider(t, "primary", "http://127.0.0.1:1"), "ws://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	type outcome struct {
		err  error
		lent []*Endpoint
	}
	for round := 0; round < rounds; round++ {
		outcomes := make(chan outcome, concurrent)
		start := make(chan struct{})
		for i := 0; i < concurrent; i++ {
			go func() {
				<-start
				sessions, err := lcGet(csm, NewUsedProviders(nil))
				var lent []*Endpoint
				for _, s := range sessions {
					if drsc, ok := s.Session.Connection.(*DirectRPCSessionConnection); ok {
						lent = append(lent, drsc.Endpoint)
					}
					// The node's error, as in MAG-4829: the URL stays off.
					_ = csm.OnSessionFailure(s.Session, errLastChanceRelay)
				}
				outcomes <- outcome{err: err, lent: lent}
			}()
		}
		close(start)
		for i := 0; i < concurrent; i++ {
			got := <-outcomes
			require.NoError(t, got.err, "round %d: a request missed the last chance", round)
			require.Len(t, got.lent, 1, "round %d", round)
			require.Same(t, p.Endpoints[0], got.lent[0], "round %d: the https url is lent, never the wss one", round)
		}
		require.False(t, p.Endpoints[0].IsEnabled())
	}
}
