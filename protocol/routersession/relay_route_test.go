package routersession

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MAG-5667: a node url's `methods` list restricts it to the apis it names, and an api a url names
// goes only to the urls that name it while one of them is usable, as the chain router routes them
// (chainlib/chain_router.go).

func methodsEndpoint(t *testing.T, url, internalPath string, methods ...string) *Endpoint {
	t.Helper()
	endpoint := urlPickEndpoint(t, url, internalPath)
	if len(methods) > 0 {
		endpoint.Methods = map[string]struct{}{}
		for _, method := range methods {
			endpoint.Methods[method] = struct{}{}
		}
	}
	return endpoint
}

// switchOff takes an endpoint out of rotation the way relay failures do.
func switchOff(t *testing.T, endpoint *Endpoint) {
	t.Helper()
	for i := uint64(0); i < MaxConsecutiveConnectionAttempts; i++ {
		endpoint.MarkUnhealthy(EndpointDisableUnreachable)
	}
	require.False(t, endpoint.IsEnabled())
}

// pickedURLs is every url the walk would hand out for one request of this route, in pick order.
func pickedURLs(t *testing.T, cswp *ConsumerSessionsWithProvider, route *RelayRoute) []string {
	t.Helper()
	connected, endpoints, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickSkipDisabled, true, "", nil, route, nil)
	require.NoError(t, err)
	if !connected {
		return nil
	}
	urls := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		urls = append(urls, endpoint.endpoint.NetworkAddress)
	}
	return urls
}

func TestRelayRoute_MethodsListPartitionsTheProvidersUrls(t *testing.T) {
	const a, b, c = "https://a.example", "https://b.example", "https://c.example"
	root := ""
	method := func(name string) *RelayRoute { return newRelayRoute(&root, name) }
	provider := urlPickProvider("p",
		methodsEndpoint(t, a, ""),
		methodsEndpoint(t, b, "", "eth_call"),
		methodsEndpoint(t, c, "", "eth_call"),
	)

	require.Equal(t, []string{b, c}, pickedURLs(t, provider, method("eth_call")), "a method a url names goes only to the urls that name it")
	require.Equal(t, []string{b, c}, provider.relayURLs("", nil, method("eth_call")), "the rate-limit read counts the same urls")
	for _, other := range []string{"eth_getBalance", "eth_getCode", "eth_getTransactionCount"} {
		require.Equal(t, []string{a}, pickedURLs(t, provider, method(other)), "%s stays off the urls restricted to eth_call", other)
		require.Equal(t, []string{a}, provider.relayURLs("", nil, method(other)))
	}
	require.Equal(t, []string{a, b, c}, pickedURLs(t, provider, nil), "a probe has no method and keeps every url")
}

// Config order does not matter: MAG-5587's run 4 listed the eth_call url first.
func TestRelayRoute_MethodUrlListedFirstStillTakesOnlyItsMethods(t *testing.T) {
	const a, b = "https://a.example", "https://b.example"
	root := ""
	provider := urlPickProvider("p", methodsEndpoint(t, b, "", "eth_call"), methodsEndpoint(t, a, ""))

	require.Equal(t, []string{a}, pickedURLs(t, provider, newRelayRoute(&root, "eth_getBalance")))
	require.Equal(t, []string{b}, pickedURLs(t, provider, newRelayRoute(&root, "eth_call")))
}

// A url's list claims its methods on its own path only, as a method route is keyed by path in the
// chain router: on another path the method goes to that path's urls.
func TestRelayRoute_MethodClaimIsPerPath(t *testing.T) {
	const v2, v3, v3Call = "https://ton.example/v2", "https://ton.example/v3", "https://call.example/v3"
	provider := urlPickProvider("ton",
		methodsEndpoint(t, v2, "/v2"),
		methodsEndpoint(t, v3, "/v3"),
		methodsEndpoint(t, v3Call, "/v3", "runGetMethod"),
	)
	p2, p3 := "/v2", "/v3"

	require.Equal(t, []string{v2}, pickedURLs(t, provider, newRelayRoute(&p2, "runGetMethod")), "no /v2 url names it, so /v2's catch-all takes it")
	require.Equal(t, []string{v3Call}, pickedURLs(t, provider, newRelayRoute(&p3, "runGetMethod")))
	require.Equal(t, []string{v3}, pickedURLs(t, provider, newRelayRoute(&p3, "getAccount")))
}

// A provider whose every url names other methods has nothing for this one: it is passed over, as
// one without the request's add-on is.
func TestRelayRoute_NoUrlForTheMethodPassesTheProviderOver(t *testing.T) {
	root := ""
	provider := urlPickProvider("p", methodsEndpoint(t, "https://b.example", "", "eth_call"))

	require.Empty(t, pickedURLs(t, provider, newRelayRoute(&root, "eth_getBalance")))
	require.Empty(t, provider.relayURLs("", nil, newRelayRoute(&root, "eth_getBalance")))
}

// While every url that names a method is switched off, the catch-all takes the method rather than
// the provider being passed over for it, and the switched-off url still takes nothing else. Once
// it is back, the method returns to it.
func TestRelayRoute_SwitchedOffMethodUrlLeavesItsMethodToTheCatchAll(t *testing.T) {
	const a, b, c = "https://a.example", "https://b.example", "https://c.example"
	root := ""
	callB, callC := methodsEndpoint(t, b, "", "eth_call"), methodsEndpoint(t, c, "", "eth_call")
	provider := urlPickProvider("p", methodsEndpoint(t, a, ""), callB, callC)

	switchOff(t, callB)
	require.Equal(t, []string{c}, pickedURLs(t, provider, newRelayRoute(&root, "eth_call")), "one url naming it is still usable")
	switchOff(t, callC)
	require.Equal(t, []string{a}, pickedURLs(t, provider, newRelayRoute(&root, "eth_call")), "none is: the catch-all answers")
	require.Equal(t, []string{a}, provider.relayURLs("", nil, newRelayRoute(&root, "eth_call")))
	require.Equal(t, []string{a}, pickedURLs(t, provider, newRelayRoute(&root, "eth_getBalance")))

	require.True(t, callB.ResetHealth())
	require.Equal(t, []string{b}, pickedURLs(t, provider, newRelayRoute(&root, "eth_call")))
}

// A provider whose only url for a method is switched off, with no catch-all, is lent that url on
// the last chance like any other switched-off url.
func TestRelayRoute_LastChanceLendsTheMethodsUrl(t *testing.T) {
	const b, c = "https://b.example", "https://c.example"
	root := ""
	callB := methodsEndpoint(t, b, "", "eth_call")
	provider := urlPickProvider("p", callB, methodsEndpoint(t, c, "", "eth_getBalance"))
	switchOff(t, callB)

	connected, endpoints, _, err := provider.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickLendDisabled, false, "", nil, newRelayRoute(&root, "eth_call"), nil)
	require.NoError(t, err)
	require.True(t, connected)
	require.Equal(t, b, endpoints[0].endpoint.NetworkAddress, "the eth_getBalance url is never lent an eth_call")
}

// The rate-limit hold-off reads the urls that can take the method (MAG-4732): a held eth_call url
// holds the provider for eth_call, and leaves it free for every other method.
func TestRelayRoute_HoldoffReadsTheMethodsUrls(t *testing.T) {
	const a, b = "https://a.example", "https://b.example"
	reg, _ := urlPickRegistry()
	csm := urlPickCSM(reg, "ETH1", "jsonrpc", urlPickProvider("p", methodsEndpoint(t, a, ""), methodsEndpoint(t, b, "", "eth_call")))
	reg.RecordRateLimit(csm.holdoffKey("p"), b, time.Minute)
	root := ""

	_, held := csm.providerHeldOff("p", "", nil, newRelayRoute(&root, "eth_call"))
	require.True(t, held, "the only url for eth_call is held")
	_, held = csm.providerHeldOff("p", "", nil, newRelayRoute(&root, "eth_getBalance"))
	require.False(t, held, "eth_getBalance goes to a, which never answered 429")
}
