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
// held before one that is.

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

func pickedURL(t *testing.T, p *ConsumerSessionsWithProvider, reg *holdoff.Registry, internalPath *string) string {
	t.Helper()
	connected, endpoints, _, err := p.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickSkipDisabled, false, "", nil, internalPath, reg)
	require.NoError(t, err)
	require.True(t, connected)
	return endpoints[0].endpoint.NetworkAddress
}

// Url A answered 429, url B never did: the provider is not held, and B serves.
func TestHoldoff_OneHeldURLLeavesTheProviderToItsSibling(t *testing.T) {
	reg, _ := urlPickRegistry()
	p := urlPickProvider("chainstack", urlPickEndpoint(t, "http://a.example", ""), urlPickEndpoint(t, "http://b.example", ""))
	csm := &ConsumerSessionManager{rateLimitHoldoff: reg, pairing: map[string]*ConsumerSessionsWithProvider{"chainstack": p}}
	reg.RecordRateLimit("chainstack", "http://a.example", time.Minute)

	_, held := csm.providerHeldOff("chainstack", "", nil, nil)
	require.False(t, held, "url B never answered 429, so the provider still has a url to send to")
	require.Equal(t, "http://b.example", pickedURL(t, p, reg, nil), "the held url A is listed first, but B is free")
	require.Equal(t, "http://a.example", pickedURL(t, p, nil, nil), "without a registry the pick is config order, as before")
}

// Url B had a 429 that has expired, url A is held now: the walk hands out B, not A.
func TestHoldoff_ExpiredSiblingIsPickedBeforeAHeldURL(t *testing.T) {
	reg, now := urlPickRegistry()
	p := urlPickProvider("chainstack", urlPickEndpoint(t, "http://a.example", ""), urlPickEndpoint(t, "http://b.example", ""))
	reg.RecordRateLimit("chainstack", "http://b.example", 30*time.Second)
	*now = urlPickNow.Add(time.Minute) // B's hold-off is over
	reg.RecordRateLimit("chainstack", "http://a.example", 5*time.Minute)

	require.Equal(t, "http://b.example", pickedURL(t, p, reg, nil))
}

// Every url held: nothing is refused. Two held urls escalate to the provider name, which holds
// both until the longer ends, so the pick is config order.
func TestHoldoff_AllURLsHeldStillHandsOneOut(t *testing.T) {
	reg, _ := urlPickRegistry()
	p := urlPickProvider("chainstack", urlPickEndpoint(t, "http://a.example", ""), urlPickEndpoint(t, "http://b.example", ""))
	reg.RecordRateLimit("chainstack", "http://a.example", 10*time.Minute)
	reg.RecordRateLimit("chainstack", "http://b.example", 2*time.Minute)

	require.Equal(t, "http://a.example", pickedURL(t, p, reg, nil))
}

// One name on two api-interfaces (two config entries, one registry): a 429 on the jsonrpc entry's
// https url holds that entry, whose wss url cannot take a relay, and leaves the rest entry free.
func TestHoldoff_A429OnJsonrpcDoesNotHoldTheSameNameOnRest(t *testing.T) {
	reg, _ := urlPickRegistry()
	jsonrpc := &ConsumerSessionManager{rateLimitHoldoff: reg, pairing: map[string]*ConsumerSessionsWithProvider{
		"chainstack": urlPickProvider("chainstack", urlPickEndpoint(t, "https://eth.example", ""), urlPickEndpoint(t, "wss://eth.example", "")),
	}}
	rest := &ConsumerSessionManager{rateLimitHoldoff: reg, pairing: map[string]*ConsumerSessionsWithProvider{
		"chainstack": urlPickProvider("chainstack", urlPickEndpoint(t, "https://rest.example", "")),
	}}
	reg.RecordRateLimit("chainstack", "https://eth.example", time.Minute)

	_, held := jsonrpc.providerHeldOff("chainstack", "", nil, nil)
	require.True(t, held, "the wss url has no hold-off, but it cannot take a relay, so it does not free the provider")
	_, held = rest.providerHeldOff("chainstack", "", nil, nil)
	require.False(t, held, "the rest url never answered 429")
}

// A url generated for an internal path only counts for that path: /v2 held holds the provider for
// a v2 request and not for a v3 one.
func TestHoldoff_AHeldPathURLHoldsOnlyThatPath(t *testing.T) {
	reg, _ := urlPickRegistry()
	csm := &ConsumerSessionManager{rateLimitHoldoff: reg, pairing: map[string]*ConsumerSessionsWithProvider{
		"ton": urlPickProvider("ton", urlPickEndpoint(t, "https://ton.example/v2", "/v2"), urlPickEndpoint(t, "https://ton.example/v3", "/v3")),
	}}
	reg.RecordRateLimit("ton", "https://ton.example/v2", time.Minute)

	v2, v3 := "/v2", "/v3"
	_, held := csm.providerHeldOff("ton", "", nil, &v2)
	require.True(t, held, "the only url serving /v2 is held")
	_, held = csm.providerHeldOff("ton", "", nil, &v3)
	require.False(t, held, "the /v3 url never answered 429")
}
