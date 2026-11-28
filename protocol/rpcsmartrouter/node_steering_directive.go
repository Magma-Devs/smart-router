package rpcsmartrouter

import (
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// nodePinDirective names the directive with which the caller pinned this request to one node, or
// returns "" when the caller pinned none: router-select-provider names the node, router-stickiness
// keeps a session on one. A pinned request neither reads the cache (nodeSteeringDirective) nor
// writes it (tryCacheWriteResolved).
//
// Omer ruled on MAG-4122 (2026-10-05) that a pinned request is answered by the node it is pinned
// to, never from the cache, and MAG-5249 adds that its answer is not written either (rulebook Part
// IV section 6). The caller chose that node for a reason of its own, a session's consistency or a
// node under test, and its answer is that node's view, which can be behind or ahead of the chain's;
// written, it would answer every caller that steered nothing for the entry's lifetime, and nothing
// in the reply would say which node it came from.
//
// A directive counts when selection acts on it: a non-empty pin or session id (GetSessions ignores
// an empty one).
func nodePinDirective(protocolMessage chainlib.ProtocolMessage) string {
	directives := protocolMessage.GetDirectiveHeaders()
	switch {
	case directives[common.SELECT_PROVIDER_HEADER_NAME] != "":
		return common.SELECT_PROVIDER_HEADER_NAME
	case directives[common.STICKINESS_HEADER_NAME] != "":
		return common.STICKINESS_HEADER_NAME
	}
	return ""
}

// nodeSteeringDirective names the directive with which the caller chose the node that answers this
// request, or returns "" when the caller chose none. A request that carries one is never answered
// from either cache tier (MAG-3520, MAG-3864).
//
// A cached answer came from whichever node served the request first, and its reply names no node
// (Router-Provider-Address: Cached). A caller that pinned a node with router-select-provider, kept a
// session on one with router-stickiness, or excluded nodes with router-providers-block could therefore
// be handed the answer of a node it did not choose, even the very node it excluded, with no way to
// tell. MAG-3520 holds an exclusion list to the same rule as a pin (nodePinDirective). The two differ
// at the write: a pinned request's answer is one node's view and is not written, while the node that
// answers an exclusion is one any caller may be served by, so its answer is written for requests
// that steer nothing.
//
// An exclusion list counts when it names at least one node (GetBlockedProviders).
func nodeSteeringDirective(protocolMessage chainlib.ProtocolMessage) string {
	if pin := nodePinDirective(protocolMessage); pin != "" {
		return pin
	}
	if len(protocolMessage.GetBlockedProviders()) > 0 {
		return common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME
	}
	return ""
}
