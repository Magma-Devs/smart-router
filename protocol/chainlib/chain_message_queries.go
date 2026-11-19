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
// method, from the SUBSCRIBE parse directive its API carries in the spec.
//
// The spec is what decides routing, not a live reflection lookup. Reflection is
// throttled or switched off on many public gRPC gateways (the header of
// config/smartrouter_examples/smartrouter_cosmos.yml calls this out), and a router
// that could only learn streaming-ness from reflection fell back to a unary Invoke
// whenever the lookup failed. A unary invoke on a server-streaming method reads one
// message and then errors on the second, so the caller got a truncated stream after
// waiting out the hanging_api timeout — a silent wrong answer rather than a refusal
// (MAG-2643). Reading it from the spec means the classification is available before
// any upstream is touched, and is identical whether or not reflection answers.
//
// The signal is the SUBSCRIBE function tag — the same one the WebSocket path uses,
// and the only sanctioned way to declare a subscription: `category.subscription` is
// on lava-specs' removed-fields list and its CI rejects any spec carrying it.
//
// The interface check keeps this gRPC-scoped. A tendermintrpc collection carries
// SUBSCRIBE directives too, and those belong to the WebSocket path.
func IsGrpcSubscription(chainMessage ChainMessageForSend) bool {
	if chainMessage == nil {
		return false
	}
	if chainMessage.GetApiCollection().GetCollectionData().ApiInterface != types.APIInterfaceGrpc {
		return false
	}
	return IsFunctionTagOfType(chainMessage, types.FUNCTION_TAG_SUBSCRIBE)
}

// signedBytesRedactor is a request whose interface knows the encoding its signed bytes take
// (rpcInterfaceMessages.GraphQLMessage, MAG-5122).
type signedBytesRedactor interface {
	RedactSignedBytes(text string) string
}

// SignedBytesRedactor is what takes the signed bytes out of a request or reply that a log line of
// chainMessage carries, when the message's interface knows their encoding; nil for every other
// interface, whose text is logged as is.
func SignedBytesRedactor(chainMessage ChainMessageForSend) func(text string) string {
	if chainMessage == nil {
		return nil
	}
	if redactor, ok := chainMessage.GetRPCMessage().(signedBytesRedactor); ok {
		return redactor.RedactSignedBytes
	}
	return nil
}

// RedactSignedBytesForLog is text through SignedBytesRedactor, or text as is when there is none.
func RedactSignedBytesForLog(chainMessage ChainMessageForSend, text string) string {
	if redact := SignedBytesRedactor(chainMessage); redact != nil {
		return redact(text)
	}
	return text
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
