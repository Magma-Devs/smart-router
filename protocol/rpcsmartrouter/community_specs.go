package rpcsmartrouter

// communityAllowedSpecIndices is the explicit allowlist of spec indices the
// community edition will load. Each entry MUST be a chain whose specs/*.json
// declares at least one "api_interface": "jsonrpc" collection — community
// rejects rest/grpc/tendermintrpc at the API-interface gate, so listing a
// non-jsonrpc spec here would be a config trap (ValidateSpec passes,
// ValidateAPIInterface then fails on every collection).
//
// Adding an entry is a product decision and must go through PR review.
// Spec indices correspond to the top-level "index" field in specs/*.json.
//
// LAVA is intentionally excluded: specs/lava.json declares only rest/grpc/
// tendermintrpc collections and cannot be served by community edition until
// a jsonrpc collection is added to the spec.
var communityAllowedSpecIndices = map[string]struct{}{
	"ETH1": {},
}

func isCommunityAllowedSpec(specIndex string) bool {
	_, ok := communityAllowedSpecIndices[specIndex]
	return ok
}
