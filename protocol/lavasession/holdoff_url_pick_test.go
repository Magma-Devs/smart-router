package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/stretchr/testify/require"
)

// MAG-4732: a 429 holds the url that answered it. A provider is held only while every url of it
// that could serve the request is held, and inside a provider the walk hands out a url that is not
// held before one that is. The provider is one entry: its name on one chain and api-interface.

var urlPickNow = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func urlPickRegistry() (*holdoff.Registry, *time.Time) {
	now := urlPickNow
	return holdoff.NewRegistryWithClock(func() time.Time { return now }), &now
}

func urlPickEndpoint(t *testing.T, url, internalPath string) *Endpoint {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: url, InternalPath: internalPath}, 5, "jsonrpc")
	require.NoError(t, err)
	return &Endpoint{NetworkAddress: url, Enabled: true, InternalPath: internalPath, DirectConnections: []DirectRPCConnection{conn}}
}

func urlPickProvider(name string, endpoints ...*Endpoint) *ConsumerSessionsWithProvider {
	p := NewConsumerSessionWithProvider(name, endpoints, 999999, 1, 1)
	p.StaticProvider = true
	return p
}

// urlPickCSM pairs the given providers on one chain and api-interface.
func urlPickCSM(reg *holdoff.Registry, chainID, apiInterface string, providers ...*ConsumerSessionsWithProvider) *ConsumerSessionManager {
	pairing := map[string]*ConsumerSessionsWithProvider{}
	for _, p := range providers {
		pairing[p.PublicLavaAddress] = p
	}
	return &ConsumerSessionManager{
		rateLimitHoldoff: reg,
		rpcEndpoint:      &RPCEndpoint{ChainID: chainID, ApiInterface: apiInterface},
		pairing:          pairing,
	}
}

func pickedURL(t *testing.T, csm *ConsumerSessionManager, address string, internalPath *string) string {
	t.Helper()
	connected, endpoints, _, err := csm.pairing[address].fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickSkipDisabled, false, "", nil, internalPath, csm.urlHeldOff(address))
	require.NoError(t, err)
	require.True(t, connected)
	return endpoints[0].endpoint.NetworkAddress
}

// Url A answered 429, url B never did: the provider is not held, and B serves.
func TestHoldoff_OneHeldURLLeavesTheProviderToItsSibling(t *testing.T) {
	reg, _ := urlPickRegistry()
	csm := urlPickCSM(reg, "ETH1", "jsonrpc", urlPickProvider("chainstack", urlPickEndpoint(t, "http://a.example", ""), urlPickEndpoint(t, "http://b.example", "")))
	reg.RecordRateLimit(csm.holdoffKey("chainstack"), "http://a.example", time.Minute)

	_, held := csm.providerHeldOff("chainstack", "", nil, nil)
	require.False(t, held, "url B never answered 429, so the provider still has a url to send to")
	require.Equal(t, "http://b.example", pickedURL(t, csm, "chainstack", nil), "the held url A is listed first, but B is free")

	csm.rateLimitHoldoff = nil
	require.Equal(t, "http://a.example", pickedURL(t, csm, "chainstack", nil), "without a registry the pick is config order, as before")
}

// Url B had a 429 that has expired, url A is held now: the walk hands out B, not A.
func TestHoldoff_ExpiredSiblingIsPickedBeforeAHeldURL(t *testing.T) {
	reg, now := urlPickRegistry()
	csm := urlPickCSM(reg, "ETH1", "jsonrpc", urlPickProvider("chainstack", urlPickEndpoint(t, "http://a.example", ""), urlPickEndpoint(t, "http://b.example", "")))
	reg.RecordRateLimit(csm.holdoffKey("chainstack"), "http://b.example", 30*time.Second)
	*now = urlPickNow.Add(time.Minute) // B's hold-off is over
	reg.RecordRateLimit(csm.holdoffKey("chainstack"), "http://a.example", 5*time.Minute)

	require.Equal(t, "http://b.example", pickedURL(t, csm, "chainstack", nil))
}

// Every url held: nothing is refused. Two held urls escalate to the provider, which holds both
// until the longer ends, so the pick is config order.
func TestHoldoff_AllURLsHeldStillHandsOneOut(t *testing.T) {
	reg, _ := urlPickRegistry()
	csm := urlPickCSM(reg, "ETH1", "jsonrpc", urlPickProvider("chainstack", urlPickEndpoint(t, "http://a.example", ""), urlPickEndpoint(t, "http://b.example", "")))
	reg.RecordRateLimit(csm.holdoffKey("chainstack"), "http://a.example", 10*time.Minute)
	reg.RecordRateLimit(csm.holdoffKey("chainstack"), "http://b.example", 2*time.Minute)

	require.Equal(t, "http://a.example", pickedURL(t, csm, "chainstack", nil))
}

// One name on two api-interfaces (two config entries, one registry): a 429 on the jsonrpc entry's
// https url holds that entry, whose wss url cannot take a relay, and leaves the rest entry free.
func TestHoldoff_A429OnJsonrpcDoesNotHoldTheSameNameOnRest(t *testing.T) {
	reg, _ := urlPickRegistry()
	jsonrpc := urlPickCSM(reg, "ETH1", "jsonrpc", urlPickProvider("chainstack", urlPickEndpoint(t, "https://eth.example", ""), urlPickEndpoint(t, "wss://eth.example", "")))
	rest := urlPickCSM(reg, "ETH1", "rest", urlPickProvider("chainstack", urlPickEndpoint(t, "https://rest.example", "")))
	reg.RecordRateLimit(jsonrpc.holdoffKey("chainstack"), "https://eth.example", time.Minute)

	_, held := jsonrpc.providerHeldOff("chainstack", "", nil, nil)
	require.True(t, held, "the wss url has no hold-off, but it cannot take a relay, so it does not free the provider")
	_, held = rest.providerHeldOff("chainstack", "", nil, nil)
	require.False(t, held, "the rest url never answered 429")
}

// Two held urls escalate their entry, and only their entry: the same name on another
// api-interface and on another chain keeps serving (review of #618).
func TestHoldoff_EscalationStaysOnTheEntryThatWasLimited(t *testing.T) {
	reg, _ := urlPickRegistry()
	jsonrpc := urlPickCSM(reg, "ETH1", "jsonrpc", urlPickProvider("chainstack",
		urlPickEndpoint(t, "https://a.example", ""), urlPickEndpoint(t, "https://b.example", ""), urlPickEndpoint(t, "https://c.example", "")))
	rest := urlPickCSM(reg, "ETH1", "rest", urlPickProvider("chainstack", urlPickEndpoint(t, "https://rest.example", "")))
	otherChain := urlPickCSM(reg, "BASE", "jsonrpc", urlPickProvider("chainstack", urlPickEndpoint(t, "https://base.example", "")))
	reg.RecordRateLimit(jsonrpc.holdoffKey("chainstack"), "https://a.example", time.Minute)
	reg.RecordRateLimit(jsonrpc.holdoffKey("chainstack"), "https://b.example", time.Minute)

	_, held := jsonrpc.providerHeldOff("chainstack", "", nil, nil)
	require.True(t, held, "two held urls escalate the entry, so its free third url is held too")
	_, held = rest.providerHeldOff("chainstack", "", nil, nil)
	require.False(t, held, "the same name on rest is another entry")
	_, held = otherChain.providerHeldOff("chainstack", "", nil, nil)
	require.False(t, held, "the same name on another chain is another entry")
}

// A url generated for an internal path only counts for that path: /v2 held holds the provider for
// a v2 request and not for a v3 one.
func TestHoldoff_AHeldPathURLHoldsOnlyThatPath(t *testing.T) {
	reg, _ := urlPickRegistry()
	csm := urlPickCSM(reg, "TON", "rest", urlPickProvider("ton", urlPickEndpoint(t, "https://ton.example/v2", "/v2"), urlPickEndpoint(t, "https://ton.example/v3", "/v3")))
	reg.RecordRateLimit(csm.holdoffKey("ton"), "https://ton.example/v2", time.Minute)

	v2, v3 := "/v2", "/v3"
	_, held := csm.providerHeldOff("ton", "", nil, &v2)
	require.True(t, held, "the only url serving /v2 is held")
	_, held = csm.providerHeldOff("ton", "", nil, &v3)
	require.False(t, held, "the /v3 url never answered 429")
}
