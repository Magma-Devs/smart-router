package lavasession

// EndpointDisableReason names WHY one endpoint URL was taken out of rotation.
//
// This is the endpoint-level counterpart to BlockReason, and it exists because BlockReason cannot
// answer the question. A provider block reads `all-endpoints-disabled`, which is not a cause — it is
// a COUNT. It says "every URL I have is off" and nothing about why any of them went off, so an
// operator reading it learns only what the block itself already told them.
//
// The causes that matter are all one level down, and they lead to different actions:
//
//	unreachable  network, DNS, TLS, a firewall — the request never arrived
//	node-error   the node answered, and the answer was its own failure
//
// Every one of those produced the identical line before this existed:
//
//	WRN disabled unhealthy endpoint endpoint=... refusals=50
//
// The strings are operator-facing — they appear in that log line and in /debug/endpoint-state — so
// the same two rules as BlockReason apply: say what HAPPENED rather than which counter tripped, and
// prefer adding a value over redefining one, since renaming breaks dashboards and log queries.
//
// The vocabulary is deliberately the registry's own Internal/External boundary and nothing else.
// An earlier draft carried a third value, `http-server-error`, for the disable site that decides on
// an HTTP status without reading the body. It was dropped before merge because it did not name a
// KIND of fault: JSON-RPC wraps a 5xx into an HTTPStatusError that reaches the registry and lands on
// node-error, while REST returns it as a status and reached the status branch — so one upstream 5xx
// incident split into two reasons purely by which api-interface the customer had configured, and a
// dashboard grouped by reason would have shown two half-incidents. Both sites now classify through
// the registry, so the label describes the fault rather than the transport that carried it.
//
// MERGE NOTE — #340 (bench-after) adds a THIRD disable call site that this branch cannot see.
//
// #340 introduces a disable for a node error delivered inside an HTTP 200 — the freeze it exists to
// fix — at rpcsmartrouter_server.go. Merging the two branches is a COMPILE ERROR, not a silent
// mismatch ("not enough arguments in call to targetEndpoint.MarkUnhealthy"), plus four textual
// conflicts in direct_rpc_session_selection_test.go and endpoint_probe_reenable_test.go where #340
// changed the loop counter to uint64 and this branch added the reason argument — both changes are
// needed, so take theirs and keep the uint64.
//
// That third site MUST pass EndpointDisableNodeError. It is the commonest shape of a dying node and
// the whole point of FAILOVER-TASKS section 2; resolving the compile error with
// EndpointDisableUnspecified would make it build while shipping section 2's main disable path with
// no reason at all — defeating this file precisely where it matters most.

type EndpointDisableReason string

const (
	// EndpointDisableUnreachable — the request never got an answer. Timeout, connection refused,
	// DNS failure, TLS mismatch, connection reset. CategoryInternal in the error registry.
	//
	// Actionable as an infrastructure problem: the address, the network path, or the credentials
	// needed to open the connection.
	//
	// KNOWN GAP (MAG-3563): DNS, TLS and EOF faults do NOT reach this value today. The shared
	// classifier has no branch for *net.DNSError, x509 errors or io.EOF, so they fall through to
	// LavaErrorUnknown — CategoryExternal — and are recorded as node-error despite no node having
	// answered. Pinned by the KNOWN-WRONG rows in
	// rpcsmartrouter/endpoint_disable_reason_mapping_test.go, which turn red when MAG-3563 lands.
	EndpointDisableUnreachable EndpointDisableReason = "unreachable"

	// EndpointDisableNodeError — the node answered, and the answer was its own failure: an internal
	// error, a bad gateway, not-ready-yet, an HTTP 5xx. CategoryExternal and retryable, with none of
	// the not-at-fault subcategories.
	//
	// Actionable as a node problem: the process is up and reachable but cannot serve.
	EndpointDisableNodeError EndpointDisableReason = "node-error"

	// EndpointDisableUnspecified — an endpoint was disabled without naming a reason. This is a bug:
	// every call site names one. It exists so a missing reason is visibly wrong rather than an empty
	// string that reads like "no reason needed".
	//
	// Unreachable by construction today — every call site passes a named constant, and the two
	// normalisation guards that can produce it (markUnhealthyAt's empty check and
	// endpointDisableReasonFor's nil check) are both dead on current callers. They are kept as
	// invariant guards so a FUTURE call site cannot introduce a silently empty reason, and both are
	// pinned by tests rather than left to be rediscovered.
	EndpointDisableUnspecified EndpointDisableReason = "unspecified"
)

// allEndpointDisableReasons is the shared backing array — a package-level var rather than a fresh
// slice per call, matching AllBlockReasons.
var allEndpointDisableReasons = []EndpointDisableReason{
	EndpointDisableUnreachable,
	EndpointDisableNodeError,
	EndpointDisableUnspecified,
}

// AllEndpointDisableReasons lists every reason a disable can carry.
//
// It has no production caller yet. It exists for a per-reason gauge that is NOT wired in this
// change — smartrouter_csm_disabled_endpoints_by_reason, mirroring how AllBlockReasons feeds
// smartrouter_csm_blocked_providers_by_reason — so that gauge can publish a zero for the reasons not
// currently in use and stay self-correcting. Tracked in the PR's "Follow-up, not in this PR"
// section. Until then it is used only by the coverage test below.
//
// Keep in sync with the constants above — a reason missing here would, once that gauge exists, be a
// series that never returns to 0 after it has fired.
// TestEndpointDisableReasons_ListCoversEveryDeclaredConstant guards that by scanning this file.
//
// The returned slice is shared; callers must not mutate it.
func AllEndpointDisableReasons() []EndpointDisableReason { return allEndpointDisableReasons }
