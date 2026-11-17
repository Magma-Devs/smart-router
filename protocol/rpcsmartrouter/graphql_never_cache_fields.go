package rpcsmartrouter

import (
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
)

// graphQLNeverCacheRootFields are the GraphQL root fields whose answer is never served from the
// cache, because it differs from provider to provider (MAG-5123, GraphQL chapter section 6).
// Providers run different server versions, with different limits, on their own state, so the
// answer one of them gives is not the answer the next caller would get from another.
//
// It is a separate list from nodeBoundMethods on purpose. That one is keyed by API name across
// every interface, and these names are not unique across interfaces: Solana's JSON-RPC has its own
// simulateTransaction, which this rule must not change. This list is only read for a GraphQL
// request, and only against the root fields the request selects as parsed, because a field the
// spec does not declare (introspection) is served through the default api, whose name is not the
// field's.
//
// The rows are the chain list's never-cache fields. __schema and __type are introspection, which
// every GraphQL server answers for the version it runs. serviceConfig and simulateTransaction are
// Sui's. A row costs only the cache hits of a read that carries the field.
var graphQLNeverCacheRootFields = map[string]string{
	"__schema":            "describes the schema of the server version the answering provider runs",
	"__type":              "describes a type of the server version the answering provider runs",
	"serviceConfig":       "reports the answering provider's own limits and the data range it serves",
	"simulateTransaction": "runs a transaction against the answering provider's own state and server version",
}

// graphQLNeverCacheReason says why a GraphQL request's answer must never be served from the cache,
// or returns "" when it may be or the request is not GraphQL. One never-cache root field keeps the
// whole request out, the way one node-bound member keeps a JSON-RPC batch out.
func graphQLNeverCacheReason(protocolMessage chainlib.ChainMessageForSend) string {
	graphQLMessage, ok := protocolMessage.GetRPCMessage().(*rpcInterfaceMessages.GraphQLMessage)
	if !ok || graphQLMessage == nil {
		return ""
	}
	for _, name := range graphQLMessage.RootFieldNames() {
		if reason, ok := graphQLNeverCacheRootFields[name]; ok {
			return name + " " + reason
		}
	}
	return ""
}

// graphQLReplyCacheExclusion says why a GraphQL reply must not be written to the cache, or returns
// "" when it may be or the request is not GraphQL (rpcInterfaceMessages.GraphQLReplyCacheExclusion).
// It reads the reply, so it is a write-gate rule only.
func graphQLReplyCacheExclusion(protocolMessage chainlib.ChainMessageForSend, replyData []byte) string {
	if _, ok := protocolMessage.GetRPCMessage().(*rpcInterfaceMessages.GraphQLMessage); !ok {
		return ""
	}
	return rpcInterfaceMessages.GraphQLReplyCacheExclusion(replyData)
}
