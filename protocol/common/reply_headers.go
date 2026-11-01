package common

import (
	"strings"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// UpstreamReplyHeaderAllowlist is the fixed set of upstream response headers that reach
// the reply on every chain: TransportReplyHeaders, which the client needs to decode the
// body (the upstream request asks for identity encoding, but a node that compresses
// anyway must be able to say so), plus the two the 429 path reads back out of the reply:
// httpStatusRelayError rebuilds an http.Header from Reply.Metadata, and ParseRetryAfter
// measures an HTTP-date Retry-After against the upstream's own Date so a skewed clock
// does not inflate or cancel the wait. Date is kept for that reading only; the HTTP
// response the client sees carries fasthttp's own Date.
//
// Names are matched case-insensitively, so this list may use canonical spelling.
var UpstreamReplyHeaderAllowlist = append(append([]string(nil), TransportReplyHeaders...), "Retry-After", "Date")

// HeaderPassesOnReply reports whether a spec header directive applies in the reply
// direction: pass_reply and pass_both say so outright, and pass_ignore is defined as
// "allows it to pass around but is not signed" (Aptos marks its per-node ledger timestamp
// so, and its SDK needs it on every reply). pass_send, pass_nullify and pass_override
// describe the request.
func HeaderPassesOnReply(kind spectypes.Header_HeaderType) bool {
	switch kind {
	case spectypes.Header_pass_reply, spectypes.Header_pass_both, spectypes.Header_pass_ignore:
		return true
	}
	return false
}

// FilterUpstreamReplyMetadata reduces an upstream's response headers, already converted to
// reply metadata, to what the client is entitled to: UpstreamReplyHeaderAllowlist, plus
// the headers the chain spec declares in the reply direction (HeaderPassesOnReply) in any
// of the directive lists given. Everything else is dropped (MAG-3104): the vendor's edge
// and product headers, its CORS policy, its cookies, and any name the router mints itself.
//
// Two directive lists are the normal call: the API interface's union, resolved by the
// chain parser across every collection of the interface
// (ChainParser.ReplyHeaderDirectives), and the matched collection's own. The union is
// what makes the filter right for a chain that declares its reply headers once and sends
// them on every route: Aptos lists its ledger-state headers on the GET collection only,
// its nodes send them on POST too, and the Aptos Rust SDK fails any call that comes back
// without all seven. The matched collection's list is what a sender built without a
// parser (tests) runs on.
//
// This is the one filter for every path a reply can take to the client, so the paths
// cannot drift on what a client sees. The live relay runs it in SendDirectRelay, the only
// point where an upstream's headers are still distinguishable from the router's own:
// appendHeadersToRelayResult adds the router's after this, and addHeadersAndSendBytes
// writes the combined list last-wins, so an upstream header the router sets only on some
// replies (Lava-Provider-Address outside cross-validation, the cross-validation headers
// inside it) would otherwise reach the client whenever the router did not. A primary
// cache hit runs it when served (filterCachedReplyMetadata), so an entry written by an
// older router replays the same reduced set, and a reply that crossed in from the
// secondary cache runs it inside performance.SanitizeForeignCacheReply with the same
// directive lists, so a secondary hit of an entry answers with the headers a primary hit
// of the same entry does (MAG-4278).
//
// A spec cannot authorise a router-owned name: a directive whose name carries the lava-
// prefix or matches a header the router mints is ignored, so a spec edit cannot reopen
// what this closes. Names match case-insensitively, since HTTP canonicalises and gRPC
// lowercases; the upstream's own spelling is kept. The input carries one entry per
// header name (the senders keep a multi-valued header's first value), so the first
// case-insensitive match is the only one. Output order is fixed: the allow-list in its
// order, then the directives in theirs. No lookup map is built: the wanted names are few
// and this is the CPU-bound hot path. Returns nil when nothing survives.
func FilterUpstreamReplyMetadata(metadata []pairingtypes.Metadata, directiveLists ...[]*spectypes.Header) []pairingtypes.Metadata {
	if len(metadata) == 0 {
		return nil
	}
	var kept []pairingtypes.Metadata
	keep := func(name string) {
		for _, already := range kept {
			if strings.EqualFold(already.Name, name) {
				return
			}
		}
		for _, md := range metadata {
			if strings.EqualFold(md.Name, name) {
				kept = append(kept, md)
				return
			}
		}
	}
	for _, name := range UpstreamReplyHeaderAllowlist {
		keep(name)
	}
	for _, directives := range directiveLists {
		for _, directive := range directives {
			if directive == nil || !HeaderPassesOnReply(directive.Kind) || IsRouterOwnedHeader(directive.Name) {
				continue
			}
			keep(directive.Name)
		}
	}
	return kept
}

// IsRouterOwnedHeader reports whether name is one the router mints itself and a client
// reads as the router's own word: every lava- prefixed header (both spellings), the two
// router headers outside that prefix, and the status-code trailer the gRPC listener sets.
// The check is on the name alone, so it holds for headers the router adds on every reply
// and for the ones it adds only on some.
func IsRouterOwnedHeader(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "lava-") {
		return true
	}
	switch lower {
	case strings.ToLower(PROVIDER_LATEST_BLOCK_HEADER_NAME),
		strings.ToLower(SMART_ROUTER_VERSION_HEADER_NAME),
		StatusCodeMetadataKey:
		return true
	}
	return false
}
