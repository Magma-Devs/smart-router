// Package chainlibtest builds chainlib parsers, routers and fetchers against mock nodes, and holds
// the generated chainlib mocks. Only tests import it, so none of it ships in the router binary.
//
// chainlib's own tests cannot import this package (it imports chainlib), so they keep a copy of
// CreateChainLibMocks in protocol/chainlib/common_test_utils_test.go; both serve their nodes
// through the fixture package, which does not import chainlib.
package chainlibtest

//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -destination chainlib_mock.go -package chainlibtest github.com/magma-Devs/smart-router/protocol/chainlib ChainParser,ChainMessage

import (
	"context"
	"net/http"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlibtest/fixture"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// CreateChainLibMocks generates a chain parser, a chain router and a chain fetcher for specIndex,
// served by a mock node (see fixture.StartNode). apiInterface can either be an ApiInterface
// string as in spectypes.ApiInterfaceXXX or a number for an index in the apiCollections.
func CreateChainLibMocks(
	ctx context.Context,
	specIndex string,
	apiInterface string,
	httpServerCallback http.HandlerFunc,
	wsServerCallback http.HandlerFunc,
	getToTopMostPath string,
	services []string,
) (cpar chainlib.ChainParser, crout chainlib.ChainRouter, cfetc chaintracker.ChainFetcher, closeServer func(), endpointRet *routersession.RPCProviderEndpoint, errRet error) {
	// The mocks log at debug. The level is process-wide, so it is set once for the whole binary.
	fixture.UseMockLogLevel()
	// Create a cancellable context for the connector to prevent leaks
	connectorCtx, cancelConnector := context.WithCancel(ctx)

	spec, apiInterface, err := fixture.LoadSpec(getToTopMostPath, specIndex, apiInterface)
	if err != nil {
		cancelConnector()
		return nil, nil, nil, nil, nil, err
	}
	chainParser, err := chainlib.NewChainParser(apiInterface)
	if err != nil {
		cancelConnector()
		return nil, nil, nil, nil, nil, err
	}
	chainParser.SetSpec(spec)
	addons, extensions, err := chainParser.SeparateAddonsExtensions(context.Background(), services)
	if err != nil {
		cancelConnector()
		return nil, nil, nil, nil, nil, err
	}

	nodeUrls, closeNode, err := fixture.StartNode(apiInterface, httpServerCallback, wsServerCallback, addons, extensions)
	if err != nil {
		cancelConnector()
		return nil, nil, nil, cancelConnector, nil, err
	}
	closeServer = func() {
		closeNode()
		cancelConnector()
	}
	endpoint := &routersession.RPCProviderEndpoint{
		NetworkAddress: routersession.NetworkAddressData{},
		ChainID:        specIndex,
		ApiInterface:   apiInterface,
		NodeUrls:       nodeUrls,
	}
	chainRouter, err := chainlib.GetChainRouter(connectorCtx, 1, endpoint, chainParser)
	if err != nil {
		closeServer()
		return nil, nil, nil, closeServer, nil, err
	}
	chainFetcher := chainlib.NewChainFetcher(ctx, &chainlib.ChainFetcherOptions{ChainRouter: chainRouter, ChainParser: chainParser, Endpoint: endpoint})
	return chainParser, chainRouter, chainFetcher, closeServer, endpoint, nil
}
