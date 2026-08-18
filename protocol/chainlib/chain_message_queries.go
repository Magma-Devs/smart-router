package chainlib

import (
	types "github.com/magma-Devs/smart-router/types/spec"
)

func GetAddon(chainMessage ChainMessageForSend) string {
	return chainMessage.GetApiCollection().CollectionData.AddOn
}

func IsHangingApi(chainMessage ChainMessageForSend) bool {
	return chainMessage.GetApi().Category.HangingApi
}

func GetComputeUnits(chainMessage ChainMessageForSend) uint64 {
	return chainMessage.GetApi().ComputeUnits
}

func GetStateful(chainMessage ChainMessageForSend) uint32 {
	return chainMessage.GetApi().Category.Stateful
}

// IsGrpcSubscription reports whether this message targets a gRPC server-streaming
// method, according to `category.subscription` in the spec.
//
// The spec flag is what decides routing, not a live reflection lookup. Reflection is
// throttled or switched off on many public gRPC gateways (the header of
// config/smartrouter_examples/smartrouter_cosmos.yml calls this out), and a router
// that could only learn streaming-ness from reflection fell back to a unary Invoke
// whenever the lookup failed. A unary invoke on a server-streaming method reads one
// message and then errors on the second, so the caller got a truncated stream after
// waiting out the hanging_api timeout — a silent wrong answer rather than a refusal
// (MAG-2643). Reading it from the spec means the classification is available before
// any upstream is touched, and is identical whether or not reflection answers.
//
// The interface check keeps the flag scoped: the WebSocket path identifies
// subscriptions by the SUBSCRIBE function tag and must keep doing so, since a
// tendermintrpc collection can carry both.
func IsGrpcSubscription(chainMessage ChainMessageForSend) bool {
	if chainMessage == nil {
		return false
	}
	if chainMessage.GetApiCollection().GetCollectionData().ApiInterface != types.APIInterfaceGrpc {
		return false
	}
	api := chainMessage.GetApi()
	return api != nil && api.Category.Subscription
}

func GetParseDirective(api *types.Api, apiCollection *types.ApiCollection) *types.ParseDirective {
	chainMessageApiName := api.Name
	for _, parseDirective := range apiCollection.GetParseDirectives() {
		if parseDirective.ApiName == chainMessageApiName {
			return parseDirective
		}
	}
	return nil
}

func IsFunctionTagOfType(chainMessage ChainMessageForSend, functionTag types.FUNCTION_TAG) bool {
	parseDirective := chainMessage.GetParseDirective()
	if parseDirective != nil {
		return parseDirective.FunctionTag == functionTag
	}
	return false
}
