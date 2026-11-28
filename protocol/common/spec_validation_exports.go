package common

import (
	"sort"
	"strings"
)

// The router's lists the spec validator reads (PRD §4: "rules are read from the router's registry,
// vocabulary and tables, never a copied list"; MAG-5604). Each comes from where the router keeps
// it, so a verdict, family or route row added to the router reaches lava-specs CI and the boot check
// with no second edit.

// ChainFamilies is every chain family the registry names, in declaration order: a chain's own
// verdict is CHAIN_<FAMILY>_* or NODE_<FAMILY>_*, FAMILY being one of these upper-cased (PRD §2.2).
func ChainFamilies() []ChainFamily {
	var families []ChainFamily
	for family := ChainFamilyEVM; !strings.HasPrefix(family.String(), "ChainFamily("); family++ {
		families = append(families, family)
	}
	return families
}

// RegisteredRouterErrors is every verdict the registry holds, UNKNOWN_ERROR included, by code.
func RegisteredRouterErrors() []*RouterError {
	routerErrors := make([]*RouterError, 0, len(errorRegistry))
	for _, routerError := range errorRegistry {
		routerErrors = append(routerErrors, routerError)
	}
	sort.Slice(routerErrors, func(i, j int) bool { return routerErrors[i].Code < routerErrors[j].Code })
	return routerErrors
}

// StatusDecidedHTTPStatuses are the HTTP statuses the status alone decides, whatever the body
// (rulebook Part I §1; PRD §3.2 step 3): 401, 402 and 403 are C6, 429 is C4. A spec row may not
// match them (PRD §4.3). The classifier's step 3 decides 429 today (ClassifyPrefix); the other three
// are MAG-5248 item 1 and MAG-5254 item 3.
var StatusDecidedHTTPStatuses = []int{401, 402, 403, httpTooManyRequests}

// StatusDecidedGRPCStatuses are the gRPC statuses the status alone decides (C6; PRD §3.2 step 3,
// MAG-5132). RESOURCE_EXHAUSTED is decided too, by its own rule: only a C2 over_limit or a C4 row
// may name it (PRD §4.3).
var StatusDecidedGRPCStatuses = []string{"PERMISSION_DENIED", "UNAUTHENTICATED"}

// StatusDecidedVerdicts are the verdicts a status the status decides gives as C6, which no spec row
// may name (PRD §4.2): the status decides them.
var StatusDecidedVerdicts = []*RouterError{RouterErrorNodeUnauthorized, RouterErrorNodeAccessDenied}

// RouteRefusalPhrases are the phrases the router's REST rows read as a node refusing the route
// (genericErrorMappings), lower-cased. A spec row on rest or graphql may not match them until
// MAG-5253 removes those rows (PRD §4.3).
func RouteRefusalPhrases() []string {
	var phrases []string
	for _, mapping := range genericErrorMappings[TransportREST] {
		if mapping.RouterError != RouterErrorNodeEndpointNotFound && mapping.RouterError != RouterErrorNodeMethodNotAllowed {
			continue
		}
		if contains, ok := mapping.Matcher.(messageContainsMatcher); ok {
			phrases = append(phrases, contains.substring)
		}
	}
	return phrases
}
