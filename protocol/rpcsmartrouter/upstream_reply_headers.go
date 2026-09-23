package rpcsmartrouter

import (
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// upstreamReplyHeaderAllowlist is the fixed set of upstream response headers that reach
// the reply on every chain. The first two are what the transport needs to decode the body
// (the upstream request asks for identity encoding, but a node that compresses anyway must
// be able to say so). The last two are what the 429 path reads back out of the reply:
// httpStatusRelayError rebuilds an http.Header from Reply.Metadata, and ParseRetryAfter
// measures an HTTP-date Retry-After against the upstream's own Date so a skewed clock
// does not inflate or cancel the wait.
//
// Everything else an upstream sends is dropped by upstreamReplyMetadata. Names are matched
// case-insensitively, so this list may use canonical spelling.
var upstreamReplyHeaderAllowlist = []string{
	"Content-Type",
	"Content-Encoding",
	"Retry-After",
	"Date",
}

// upstreamReplyMetadata turns an upstream's response headers (HTTP headers or gRPC
// metadata, both map[string][]string) into the reply's metadata, keeping only what the
// client is entitled to: upstreamReplyHeaderAllowlist, plus the headers the chain spec
// declares as pass_reply or pass_both for this API collection (the Cosmos and Aptos
// block-height headers, which are part of those chains' API contract). Everything else
// is dropped (MAG-3104): the vendor's edge and product headers, its CORS policy, its
// cookies, and any name the router mints itself.
//
// The filter sits here, on the relay path, rather than at the listener, because this is
// the only point where an upstream's headers are still distinguishable from the router's
// own: appendHeadersToRelayResult adds the router's after this, and addHeadersAndSendBytes
// writes the combined list last-wins, so an upstream header the router sets only on some
// replies (Lava-Provider-Address outside cross-validation, the cross-validation headers
// inside it) would otherwise reach the client whenever the router did not. The reduced
// list is also what the cache stores, so a hit replays the same set; the secondary tier
// already enforced this through performance.SanitizeForeignCacheReply, and the primary
// now matches.
//
// A spec cannot authorise a router-owned name: a directive whose name carries the lava-
// prefix or matches a header the router mints is ignored, so a spec edit cannot reopen
// what this closes. Names match case-insensitively, since HTTP canonicalises and gRPC
// lowercases; the upstream's own spelling is kept, and a multi-valued header keeps its
// first value, as before. Output order is fixed: the allow-list in its order, then the
// spec's directives in theirs. Returns nil when nothing survives.
func upstreamReplyMetadata(headers map[string][]string, apiCollection *spectypes.ApiCollection) []pairingtypes.Metadata {
	if len(headers) == 0 {
		return nil
	}
	byName := make(map[string]pairingtypes.Metadata, len(headers))
	for name, values := range headers {
		if len(values) == 0 {
			continue
		}
		byName[strings.ToLower(name)] = pairingtypes.Metadata{Name: name, Value: values[0]}
	}

	var kept []pairingtypes.Metadata
	seen := make(map[string]struct{})
	keep := func(name string) {
		key := strings.ToLower(name)
		if _, done := seen[key]; done {
			return
		}
		if md, ok := byName[key]; ok {
			seen[key] = struct{}{}
			kept = append(kept, md)
		}
	}

	for _, name := range upstreamReplyHeaderAllowlist {
		keep(name)
	}
	if apiCollection != nil {
		for _, directive := range apiCollection.Headers {
			if directive == nil {
				continue
			}
			if directive.Kind != spectypes.Header_pass_reply && directive.Kind != spectypes.Header_pass_both {
				continue
			}
			if isRouterOwnedHeader(directive.Name) {
				continue
			}
			keep(directive.Name)
		}
	}
	return kept
}

// isRouterOwnedHeader reports whether name is one the router mints itself and a client
// reads as the router's own word: every lava- prefixed header (both spellings), and the
// two router headers outside that prefix. The check is on the name alone, so it holds for
// headers the router adds on every reply and for the ones it adds only on some.
func isRouterOwnedHeader(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "lava-") {
		return true
	}
	switch lower {
	case strings.ToLower(common.PROVIDER_LATEST_BLOCK_HEADER_NAME),
		strings.ToLower(common.SMART_ROUTER_VERSION_HEADER_NAME):
		return true
	}
	return false
}
