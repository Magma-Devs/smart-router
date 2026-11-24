package rpcsmartrouter

import (
	"sort"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils"
)

// nodeUrlMethods is the set of apis a node url's `methods` list restricts it to, or nil when the
// url takes every api (routersession.Endpoint.Methods, MAG-5667).
func nodeUrlMethods(url common.NodeUrl) map[string]struct{} {
	if len(url.Methods) == 0 {
		return nil
	}
	methods := make(map[string]struct{}, len(url.Methods))
	for _, method := range url.Methods {
		methods[method] = struct{}{}
	}
	return methods
}

// warnMethodOnlyPaths logs each internal path on which every url of the provider that answers
// relays has a `methods` list. No other api is sent to the provider on that path. The chain router
// refused this shape ("no chainProxy supports all methods", chainlib/chain_router.go); the router
// keeps the provider for the methods it does list and says so.
func warnMethodOnlyPaths(providerName string, endpoints []*routersession.Endpoint) {
	hasCatchAll := map[string]bool{}
	for _, endpoint := range endpoints {
		if !endpoint.ServesDirectRelays() {
			continue
		}
		hasCatchAll[endpoint.InternalPath] = hasCatchAll[endpoint.InternalPath] || len(endpoint.Methods) == 0
	}
	paths := make([]string, 0, len(hasCatchAll))
	for path, covered := range hasCatchAll {
		if !covered {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return
	}
	sort.Strings(paths)
	utils.FormatWarning("every url of this provider on these internal paths has a methods list: no other method is sent to the provider there", nil,
		utils.LogAttr("provider", providerName),
		utils.LogAttr("internalPaths", paths),
		utils.LogAttr("hint", "add a node url with no methods list to serve every other method"),
	)
}
