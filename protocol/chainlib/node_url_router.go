package chainlib

import (
	"context"
	"errors"
	"fmt"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// NodeUrlRouterFactory dials one node url and returns a router that sends every
// message to that url, whatever extensions the message asks for. The parser is the
// one the url's checks are crafted with; a gRPC proxy binds it to its connection.
type NodeUrlRouterFactory func(ctx context.Context, url common.NodeUrl, chainParser ChainParser) (ChainRouter, error)

// NewNodeUrlRouterFactory builds each node url's own chain proxy, on demand, for
// verification. The chain router keeps one url per set of declared services, so a
// check routed through it can land on another url than the one it verifies.
func NewNodeUrlRouterFactory(nConns uint, endpoint *lavasession.RPCProviderEndpoint) (NodeUrlRouterFactory, error) {
	newProxy, err := chainProxyConstructor(endpoint.ApiInterface)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, url common.NodeUrl, chainParser ChainParser) (ChainRouter, error) {
		single := *endpoint
		single.NodeUrls = []common.NodeUrl{url}
		proxy, err := newProxy(ctx, nConns, single, chainParser)
		if err != nil {
			return nil, err
		}
		return nodeUrlRouter{proxy: proxy}, nil
	}, nil
}

// nodeUrlRouter sends every message to one node url's proxy.
type nodeUrlRouter struct {
	proxy ChainProxy
}

func (r nodeUrlRouter) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend, _ []string) (*RelayReplyWrapper, common.NodeUrl, string, error) {
	reply, err := r.proxy.SendNodeMsg(ctx, chainMessage)
	url, chainID := r.proxy.GetChainProxyInformation()
	return reply, url, chainID, err
}

func (nodeUrlRouter) ExtensionsSupported(string, []string) bool { return true }

func chainProxyConstructor(apiInterface string) (func(context.Context, uint, lavasession.RPCProviderEndpoint, ChainParser) (ChainProxy, error), error) {
	switch apiInterface {
	case spectypes.APIInterfaceJsonRPC:
		return NewJrpcChainProxy, nil
	case spectypes.APIInterfaceTendermintRPC:
		return NewtendermintRpcChainProxy, nil
	case spectypes.APIInterfaceRest:
		return NewRestChainProxy, nil
	case spectypes.APIInterfaceGrpc:
		return NewGrpcChainProxy, nil
	default:
		return nil, fmt.Errorf("chain proxy for apiInterface (%s) not found", apiInterface)
	}
}

// ProviderShapeError is why the router cannot serve from a provider's node urls
// whatever they answer: no http url (none at all, or none at the root of a spec
// whose root path is enabled), or no websocket url on a spec that subscribes while
// websocket verification is on.
// Nil when the list is servable. It dials nothing.
func ProviderShapeError(endpoint *lavasession.RPCProviderEndpoint, chainParser ChainParser) error {
	http, httpRoot, websocket := false, false, false
	for _, url := range endpoint.NodeUrls {
		if isWebSocketUrl(url.Url) {
			websocket = true
			continue
		}
		http = true
		if url.InternalPath == "" {
			httpRoot = true
		}
	}
	if !http || (!httpRoot && chainParser.IsInternalPathEnabled("", endpoint.ApiInterface, "")) {
		return errors.New("HTTP/HTTPS is mandatory: configure an http(s) node url, and a ws(s) one beside it for subscriptions")
	}
	_, collection, subscribes := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_SUBSCRIBE)
	if subscribes && collection != nil && collection.Enabled &&
		collection.GetCollectionData().ApiInterface != spectypes.APIInterfaceGrpc &&
		!websocket && !chainParser.SkipWebsocketVerification() {
		return errors.New("subscriptions are applicable for this chain, but websocket is not provided: add a ws(s) node url, or run with --skip-websocket-verification")
	}
	return nil
}
