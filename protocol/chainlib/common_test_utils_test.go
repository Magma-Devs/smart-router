package chainlib

// chainlib's own tests mock ChainParser too, so it is generated here as well as in chainlibtest.
//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -destination chainlib_mock_test.go -package chainlib -self_package github.com/magma-Devs/smart-router/protocol/chainlib github.com/magma-Devs/smart-router/protocol/chainlib ChainParser,ChainRouter
//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -destination mock_websocket_test.go -package chainlib -self_package github.com/magma-Devs/smart-router/protocol/chainlib github.com/magma-Devs/smart-router/protocol/chainlib WebsocketConnection

import (
	"context"
	"net/http"

	"github.com/magma-Devs/smart-router/protocol/chainlibtest/fixture"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// CreateChainLibMocks is chainlibtest.CreateChainLibMocks for chainlib's own tests, which cannot
// import chainlibtest (it imports chainlib). Keep the two in step.
func CreateChainLibMocks(
	ctx context.Context,
	specIndex string,
	apiInterface string,
	httpServerCallback http.HandlerFunc,
	wsServerCallback http.HandlerFunc,
	getToTopMostPath string,
	services []string,
) (cpar ChainParser, crout ChainRouter, cfetc chaintracker.ChainFetcher, closeServer func(), endpointRet *routersession.RPCProviderEndpoint, errRet error) {
	// The mocks log at debug. The level is process-wide, so it is set once for the whole binary.
	fixture.UseMockLogLevel()
	// Create a cancellable context for the connector to prevent leaks
	connectorCtx, cancelConnector := context.WithCancel(ctx)

	spec, apiInterface, err := fixture.LoadSpec(getToTopMostPath, specIndex, apiInterface)
	if err != nil {
		cancelConnector()
		return nil, nil, nil, nil, nil, err
	}
	chainParser, err := NewChainParser(apiInterface)
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
	chainRouter, err := GetChainRouter(connectorCtx, 1, endpoint, chainParser)
	if err != nil {
		closeServer()
		return nil, nil, nil, closeServer, nil, err
	}
	chainFetcher := NewChainFetcher(ctx, &ChainFetcherOptions{ChainRouter: chainRouter, ChainParser: chainParser, Endpoint: endpoint})
	return chainParser, chainRouter, chainFetcher, closeServer, endpoint, nil
}
