package rpcsmartrouter

import (
	"strings"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// grpcNeverCacheMethods are the gRPC methods whose answer is never served from the cache, because it
// differs from provider to provider (MAG-5186, the gRPC section of the chain list), as
// graphQLNeverCacheRootFields is for GraphQL.
//
// The spec's deterministic flag does not decide caching (cacheExclusionReason), so a method the spec
// marks deterministic is still kept out here. The keys are full method names, without the leading
// slash, and are read for a gRPC request only.
var grpcNeverCacheMethods = map[string]string{
	// Sui's twin of GraphQL's simulateTransaction, which MAG-5123 keeps out of the cache.
	"sui.rpc.v2.TransactionExecutionService/SimulateTransaction": "runs a transaction against the answering provider's own state and server version",
}

// grpcNeverCacheReason says why a gRPC request's answer must never be served from the cache, or
// returns "" when it may be or the request is not gRPC.
func grpcNeverCacheReason(protocolMessage chainlib.ChainMessageForSend) string {
	collection := protocolMessage.GetApiCollection()
	if collection == nil || collection.CollectionData.ApiInterface != spectypes.APIInterfaceGrpc {
		return ""
	}
	api := protocolMessage.GetApi()
	if api == nil {
		return ""
	}
	method := strings.TrimPrefix(api.Name, "/")
	if reason, ok := grpcNeverCacheMethods[method]; ok {
		return method + " " + reason
	}
	return ""
}
