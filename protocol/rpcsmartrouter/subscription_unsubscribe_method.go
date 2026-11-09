package rpcsmartrouter

import (
	"strings"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// resolveTeardownUnsubscribeMethod returns the method the NODE is told to drop a subscription
// that was created with subscribeMethod, for the paths where no client message names it: the
// teardown that runs when the last client disconnects, the cleanup of a failed restore, and
// the discard of a subscription a restore re-opened onto an entry that had gone.
//
// Those paths used to derive the method by string surgery (getUnsubscribeMethod), whose
// fallback is the literal "unsubscribe". That is a method only Tendermint has, so on every
// other non-EVM chain the node rejected the call, the failure was traced, and the node kept
// publishing into the pooled connection: the MAG-3366 leak shape (MAG-4278 item 1). The
// explicit-unsubscribe path never had the problem, because the client names the method.
//
// The spec gives no field linking a SUBSCRIBE directive to its UNSUBSCRIBE one — a SUBSCRIBE
// directive carries nothing but its tag and api_name — so the pairing has to be recovered from
// the names the collection declares. Two rules do it, in order, and both only ever return a
// name the collection DECLARES, so the router cannot invent a method a node does not have:
//
//  1. The subscribe name with its first "subscribe"/"Subscribe" token replaced by
//     "unsubscribe"/"Unsubscribe", when the collection declares that name. This is the usual
//     convention: eth_subscribe/eth_unsubscribe, accountSubscribe/accountUnsubscribe,
//     chain_subscribeNewHeads/chain_unsubscribeNewHeads, subscribe/unsubscribe.
//  2. Namespace pairing for the names rule 1 cannot reach, which Substrate has three of
//     (author_submitAndWatchExtrinsic/author_unwatchExtrinsic,
//     chainHead_v1_follow/chainHead_v1_unfollow,
//     transactionWatch_v1_submitAndWatch/transactionWatch_v1_unwatch). Within one namespace —
//     the name up to its last "_" or "." — an unclaimed subscribe pairs with an unclaimed
//     unsubscribe when each is the only one of its kind in that namespace. The uniqueness is
//     required in BOTH directions so an ambiguous namespace resolves to nothing rather than to
//     a guess: Enjin's archive_v1_storage and archive_v1_storageDiff share a namespace with
//     one unclaimed unsubscribe between them, and pairing either of them would be a coin flip.
//
// Measured over every spec in the lava-specs catalog (317 subscribe apis in 57 collections):
// rule 1 resolves 242, rule 2 resolves 63, and 12 resolve to nothing. Nine of those twelve are
// gRPC or REST subscriptions, which this manager never serves; the other three are the two
// Enjin archive methods above and Filecoin.ChainNotify, for which the FVM spec declares no
// unsubscribe at all. For those the honest answer is to send nothing: a call the spec says the
// node does not answer to cannot stop it publishing, and a warning naming the method is what
// gets the pair added to the spec.
//
// A collection that declares no UNSUBSCRIBE directive at all is a different case: there is
// nothing to match against, so the legacy derivation stands in. It is what the subscription
// tests drive the manager with, and for a spec that simply omits its unsubscribe directives
// a derived "<x>_unsubscribe" is still the best guess available — but its bare "unsubscribe"
// fallback is not, so that one is refused here too.
func resolveTeardownUnsubscribeMethod(apiCollection *spectypes.ApiCollection, subscribeMethod string) (string, bool) {
	if subscribeMethod == "" {
		return "", false
	}
	subscribes, unsubscribes := subscriptionDirectiveNames(apiCollection)
	if len(unsubscribes) == 0 {
		return derivedUnsubscribeMethod(subscribeMethod)
	}

	declared := make(map[string]struct{}, len(unsubscribes))
	for _, name := range unsubscribes {
		declared[name] = struct{}{}
	}

	// Rule 1, and the claims it makes, which rule 2 must not undo.
	claimed := make(map[string]struct{}, len(unsubscribes))
	resolved := make(map[string]string, len(subscribes))
	for _, name := range subscribes {
		if candidate, ok := unsubscribeTokenOf(name, declared); ok {
			resolved[name] = candidate
			claimed[candidate] = struct{}{}
		}
	}
	if candidate, ok := resolved[subscribeMethod]; ok {
		return candidate, true
	}

	// Rule 2 over what rule 1 left.
	namespace := apiNamespaceOf(subscribeMethod)
	if namespace == "" {
		return derivedUnsubscribeMethod(subscribeMethod)
	}
	var candidate string
	for _, name := range unsubscribes {
		if _, taken := claimed[name]; taken || apiNamespaceOf(name) != namespace {
			continue
		}
		if candidate != "" {
			return derivedUnsubscribeMethod(subscribeMethod) // ambiguous namespace
		}
		candidate = name
	}
	if candidate == "" {
		return derivedUnsubscribeMethod(subscribeMethod)
	}
	for _, name := range subscribes {
		if name == subscribeMethod || apiNamespaceOf(name) != namespace {
			continue
		}
		if _, byRule1 := resolved[name]; !byRule1 {
			return derivedUnsubscribeMethod(subscribeMethod) // another subscribe wants it too
		}
	}
	return candidate, true
}

// subscriptionDirectiveNames returns the api names of the collection's SUBSCRIBE and
// UNSUBSCRIBE directives, in the spec's order.
func subscriptionDirectiveNames(apiCollection *spectypes.ApiCollection) (subscribes, unsubscribes []string) {
	if apiCollection == nil {
		return nil, nil
	}
	for _, directive := range apiCollection.ParseDirectives {
		if directive == nil || directive.ApiName == "" {
			continue
		}
		switch directive.FunctionTag {
		case spectypes.FUNCTION_TAG_SUBSCRIBE:
			subscribes = append(subscribes, directive.ApiName)
		case spectypes.FUNCTION_TAG_UNSUBSCRIBE:
			unsubscribes = append(unsubscribes, directive.ApiName)
		}
	}
	return subscribes, unsubscribes
}

// unsubscribeTokenOf returns the declared unsubscribe name reached by replacing
// subscribeMethod's first subscribe token. Case matters: a name carries one of the two forms,
// and replacing both would turn chain_subscribeNewHeads into a name with two negations.
func unsubscribeTokenOf(subscribeMethod string, declared map[string]struct{}) (string, bool) {
	for _, token := range [...][2]string{{"subscribe", "unsubscribe"}, {"Subscribe", "Unsubscribe"}} {
		if !strings.Contains(subscribeMethod, token[0]) {
			continue
		}
		candidate := strings.Replace(subscribeMethod, token[0], token[1], 1)
		if _, ok := declared[candidate]; ok {
			return candidate, true
		}
	}
	return "", false
}

// apiNamespaceOf returns an api name up to and including its last separator, which is the
// part its unsubscribe counterpart shares: "author_" for author_submitAndWatchExtrinsic,
// "chainHead_v1_" for chainHead_v1_follow. A name with no separator has no namespace.
func apiNamespaceOf(apiName string) string {
	if cut := strings.LastIndexAny(apiName, "_."); cut >= 0 {
		return apiName[:cut+1]
	}
	return ""
}

// derivedUnsubscribeMethod is the legacy string derivation, minus the bare "unsubscribe"
// fallback that made the router call a method almost no chain has. Reached only for a
// collection that declares no unsubscribe to match against.
func derivedUnsubscribeMethod(subscribeMethod string) (string, bool) {
	switch {
	case subscribeMethod == "eth_subscribe":
		return "eth_unsubscribe", true
	case subscribeMethod == "subscribe":
		return "unsubscribe", true
	case strings.HasSuffix(subscribeMethod, "_subscribe"):
		return strings.TrimSuffix(subscribeMethod, "_subscribe") + "_unsubscribe", true
	default:
		return "", false
	}
}
