package rpcsmartrouter

import (
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// nodeSteeringDirective names the directive with which the caller chose the node that answers this
// request, or returns "" when the caller chose none. A request that carries one is never answered
// from either cache tier (MAG-3520, MAG-3864).
//
// A cached answer came from whichever node served the request first, and its reply names no node
// (Lava-Provider-Address: Cached). A caller that pinned a node with lava-select-provider, kept a
// session on one with lava-stickiness, or excluded nodes with lava-providers-block could therefore
// be handed the answer of a node it did not choose, even the very node it excluded, with no way to
// tell. Omer ruled on MAG-4122 (2026-10-05) that a pinned request is answered by the node it is
// pinned to, never from the cache, and MAG-3520 holds an exclusion list to the same rule. Only the
// read is skipped: the answer of the chosen node is still written, for requests that steer nothing.
//
// A directive counts when selection acts on it: a non-empty pin or session id (GetSessions ignores
// an empty one), and an exclusion list that names at least one node (GetBlockedProviders).
func nodeSteeringDirective(protocolMessage chainlib.ProtocolMessage) string {
	directives := protocolMessage.GetDirectiveHeaders()
	switch {
	case directives[common.SELECT_PROVIDER_HEADER_NAME] != "":
		return common.SELECT_PROVIDER_HEADER_NAME
	case directives[common.STICKINESS_HEADER_NAME] != "":
		return common.STICKINESS_HEADER_NAME
	case len(protocolMessage.GetBlockedProviders()) > 0:
		return common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME
	}
	return ""
}
