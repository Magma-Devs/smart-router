package lavasession

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-4734: the last chance lends one url of the provider it hands out. It tries the urls that can
// answer before the dead ones: a url in rotation, then a head-stalled one (the node answers, a
// block behind), then a switched-off one. Within a rank, a url held off after a rate limit goes
// after its sibling, and config order holds.

// lcTwoURLs is a provider named "primary" with urls a and b, in that config order.
func lcTwoURLs(t *testing.T) *ConsumerSessionsWithProvider {
	t.Helper()
	return urlPickProvider("primary",
		urlPickEndpoint(t, "http://127.0.0.1:1", ""),
		urlPickEndpoint(t, "http://127.0.0.1:2", ""),
	)
}

// lcServedURL is the url the only session in sessions was sent to.
func lcServedURL(t *testing.T, sessions ConsumerSessionsMap) string {
	t.Helper()
	require.Len(t, sessions, 1)
	for _, s := range sessions {
		direct, ok := s.Session.Connection.(*DirectRPCSessionConnection)
		require.True(t, ok)
		return direct.EndpointAddress
	}
	return ""
}

// lentURL is the url the last chance's walk hands out for provider p, heldOff saying which urls are
// held off after a rate limit.
func lentURL(t *testing.T, p *ConsumerSessionsWithProvider, heldOff func(url string) bool) string {
	t.Helper()
	connected, endpoints, _, err := p.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickLendDisabled, false, "", nil, nil, heldOff)
	require.NoError(t, err)
	require.True(t, connected)
	return endpoints[0].endpoint.NetworkAddress
}

// Url a failed 50 times and is switched off; url b is head-stalled, one block behind but
// answering. Before, the request's one chance went to a, listed first, and failed.
func TestLastChance_LendsAStalledURLBeforeADeadOne(t *testing.T) {
	p := lcTwoURLs(t)
	disableAt(t, p.Endpoints[0], probeBase)
	require.True(t, p.Endpoints[1].AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	sessions, err := lcGet(csm, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:2", lcServedURL(t, sessions))
	require.False(t, p.Endpoints[0].IsEnabled(), "the dead url was not touched")
}

// Url a is switched off and url b works again, but the provider is still blocked (the epoch
// switch, before the block is lifted). Another provider took the first attempt, so the retry
// reaches the blocked-list fallback. Before, it lent a and spent the request's one chance.
func TestLastChance_StaleBlockIsServedByTheWorkingSibling(t *testing.T) {
	p := lcTwoURLs(t)
	disableAt(t, p.Endpoints[0], probeBase)
	other := lcProvider(t, "other", "http://127.0.0.1:3")
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p, other}, nil)
	csm.removeAddressFromValidAddresses("primary", BlockRecord{Reason: BlockReasonAllEndpointsDisabled})
	used := NewUsedProviders(nil)

	sessions, err := lcGet(csm, used)
	require.NoError(t, err)
	require.Equal(t, "other", lcServedBy(t, sessions))
	for _, s := range sessions {
		require.NoError(t, csm.OnSessionFailure(s.Session, errLastChanceRelay))
	}

	sessions, err = lcGet(csm, used)
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:2", lcServedURL(t, sessions))
	require.True(t, used.TakeLastChance(), "a url in rotation served it: the request's one chance is still unused")
}

// Every url is switched off: config order, as before.
func TestLastChance_AllURLsSwitchedOffKeepsConfigOrder(t *testing.T) {
	p := lcTwoURLs(t)
	disableAt(t, p.Endpoints[0], probeBase)
	disableAt(t, p.Endpoints[1], probeBase)
	require.Equal(t, "http://127.0.0.1:1", lentURL(t, p, nil))
}

// Both urls switched off, a held off after a rate limit: b is lent. A url held by the rate limit
// still goes after its sibling within a rank.
func TestLastChance_ARateLimitedURLGoesAfterItsSiblingInARank(t *testing.T) {
	p := lcTwoURLs(t)
	disableAt(t, p.Endpoints[0], probeBase)
	disableAt(t, p.Endpoints[1], probeBase)
	heldOff := func(url string) bool { return url == "http://127.0.0.1:1" }
	require.Equal(t, "http://127.0.0.1:2", lentURL(t, p, heldOff))
}

// The rank beats the rate limit: a switched-off url is lent after a stalled one even when the
// stalled one is held off.
func TestLastChance_RankComesBeforeTheRateLimit(t *testing.T) {
	p := lcTwoURLs(t)
	disableAt(t, p.Endpoints[0], probeBase)
	require.True(t, p.Endpoints[1].AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	heldOff := func(url string) bool { return url == "http://127.0.0.1:2" }
	require.Equal(t, "http://127.0.0.1:2", lentURL(t, p, heldOff))
}

// Only the last chance ranks. Normal selection and the reconnect probe keep the rate-limit order,
// and the provider's own url list is never reordered.
func TestEndpointsInPickOrder_OnlyTheLastChanceRanks(t *testing.T) {
	p := lcTwoURLs(t)
	disableAt(t, p.Endpoints[0], probeBase)
	a, b := p.Endpoints[0], p.Endpoints[1]

	require.Equal(t, []*Endpoint{a, b}, p.endpointsInPickOrderLocked(endpointPickSkipDisabled, nil))
	require.Equal(t, []*Endpoint{a, b}, p.endpointsInPickOrderLocked(endpointPickReconnectDisabled, nil))
	require.Equal(t, []*Endpoint{b, a}, p.endpointsInPickOrderLocked(endpointPickLendDisabled, nil))
	require.Equal(t, []*Endpoint{a, b}, p.Endpoints, "the config order is left as it was")
}
