package rpcsmartrouter

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestBoot_NewWSSubscriptionSkipsAProviderTheRouterTookOut drives the real
// CreateSmartRouterEndpoint, so it pins the wiring as well as the selection (MAG-4161). Two
// primaries each serve an http and a ws url. Once the router takes primary-1's http url out the
// way production does, head-stalled or switched off after repeated failures, a new subscription
// goes to primary-2's ws url. Nothing touches primary-1's ws url: a ws url never takes a relay, so
// it is never taken out itself, and it follows its provider.
func TestBoot_NewWSSubscriptionSkipsAProviderTheRouterTookOut(t *testing.T) {
	takeOut := map[string]func(*routersession.Endpoint){
		"head-stalled": func(ep *routersession.Endpoint) {
			// Stuck on a block no answer can go above, so the probe loop cannot release it
			// while the test runs.
			ep.AddStateReason(routersession.EndpointDisableHeadStalled, time.Now(), math.MaxInt64)
		},
		"switched off": func(ep *routersession.Endpoint) {
			for range routersession.MaxConsecutiveConnectionAttempts {
				ep.MarkUnhealthy(routersession.EndpointDisableNodeError)
			}
		},
	}
	for name, takeOut := range takeOut {
		t.Run(name, func(t *testing.T) {
			u := newFakeUpstreams(t)
			firstHTTP := u.httpURL("/primary-1/" + upArchive)
			firstWS, secondWS := u.wsURL("/primary-1/"+upArchive), u.wsURL("/primary-2/"+upArchive)
			server := bootETH1Endpoint(t, []*routersession.RPCStaticProviderEndpoint{
				{
					Name: "primary-1", ChainID: "ETH1", ApiInterface: "jsonrpc",
					NodeUrls: []common.NodeUrl{{Url: firstHTTP}, {Url: firstWS}},
				},
				{
					Name: "primary-2", ChainID: "ETH1", ApiInterface: "jsonrpc",
					NodeUrls: []common.NodeUrl{{Url: u.httpURL("/primary-2/" + upArchive)}, {Url: secondWS}},
				},
			}, nil, nil)
			manager, ok := server.wsSubscriptionManager.(*DirectWSSubscriptionManager)
			require.True(t, ok, "a chain with ws urls gets the direct WebSocket manager")

			taken := 0
			for _, ep := range server.sessionManager.GetAllDirectRPCEndpoints() {
				if ep.Endpoint.NetworkAddress == firstHTTP {
					takeOut(ep.Endpoint)
					require.False(t, ep.Endpoint.IsUsable())
					taken++
				}
			}
			require.Equal(t, 1, taken, "the session manager holds one endpoint at primary-1's http url")

			lendRank := manager.lendRank.Load()
			require.NotNil(t, lendRank, "boot gives the manager the session manager's ranking of the urls")
			assert.NotEqual(t, inRotationRank, (*lendRank)(firstWS), "primary-1's ws url follows its provider out of rotation")
			assert.Equal(t, inRotationRank, (*lendRank)(secondWS))

			// Two urls in the tier means the optimizer chooses, so a pass on main is luck; every
			// pick here must still land on primary-2.
			for range 20 {
				selected, err := manager.selectEndpoint(context.Background(), "", nil)
				require.NoError(t, err)
				require.Equal(t, secondWS, selected.Url, "a new subscription goes to the provider in rotation")
			}
		})
	}
}

// TestBoot_NewGRPCSubscriptionSkipsAURLTheRouterTookOut is the gRPC side of the boot test above.
// A gRPC url takes relays, so the router takes it out itself: once primary-1's url is head-stalled
// or switched off, a new stream goes to primary-2's.
func TestBoot_NewGRPCSubscriptionSkipsAURLTheRouterTookOut(t *testing.T) {
	takeOut := map[string]func(*routersession.Endpoint){
		"head-stalled": func(ep *routersession.Endpoint) {
			// Stuck on a block no answer can go above, so the probe loop cannot release it
			// while the test runs.
			ep.AddStateReason(routersession.EndpointDisableHeadStalled, time.Now(), math.MaxInt64)
		},
		"switched off": func(ep *routersession.Endpoint) {
			for range routersession.MaxConsecutiveConnectionAttempts {
				ep.MarkUnhealthy(routersession.EndpointDisableNodeError)
			}
		},
	}
	for name, takeOut := range takeOut {
		t.Run(name, func(t *testing.T) {
			upstream := func() string {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				server := grpc.NewServer()
				go func() { _ = server.Serve(listener) }()
				t.Cleanup(server.Stop)
				return listener.Addr().String()
			}
			first, second := upstream(), upstream()
			server := bootEndpoint(t, "GRPCTEST", "grpc", "../../specs/grpctest.json", []*routersession.RPCStaticProviderEndpoint{
				{Name: "primary-1", ChainID: "GRPCTEST", ApiInterface: "grpc", NodeUrls: []common.NodeUrl{{Url: first}}},
				{Name: "primary-2", ChainID: "GRPCTEST", ApiInterface: "grpc", NodeUrls: []common.NodeUrl{{Url: second}}},
			}, nil, nil)
			manager := server.grpcSubscriptionManager
			require.NotNil(t, manager, "a gRPC chain gets the gRPC subscription manager")

			taken := 0
			for _, ep := range server.sessionManager.GetAllDirectRPCEndpoints() {
				if ep.Endpoint.NetworkAddress == first {
					takeOut(ep.Endpoint)
					require.False(t, ep.Endpoint.IsUsable())
					taken++
				}
			}
			require.Equal(t, 1, taken, "the session manager holds one endpoint at primary-1's url")

			lendRank := manager.lendRank.Load()
			require.NotNil(t, lendRank, "boot gives the manager the session manager's ranking of the urls")
			assert.NotEqual(t, inRotationRank, (*lendRank)(first))
			assert.Equal(t, inRotationRank, (*lendRank)(second))

			for range 20 {
				selected, err := manager.selectEndpoint(context.Background(), "", nil)
				require.NoError(t, err)
				require.Equal(t, second, selected.Url, "a new stream goes to the url in rotation")
			}
		})
	}
}
