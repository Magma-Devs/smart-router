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
//	unreachable        network, DNS, TLS, a firewall — the request never arrived
//	node-error         the node answered, and the answer was its own failure
//	http-server-error  the node answered 5xx
//
// Every one of those produced the identical line before this existed:
//
//	WRN disabled unhealthy endpoint endpoint=... refusals=50
//
// The strings are operator-facing — they appear in that log line and in /debug/endpoint-state — so
// the same two rules as BlockReason apply: say what HAPPENED rather than which counter tripped, and
// prefer adding a value over redefining one, since renaming breaks dashboards and log queries.
type EndpointDisableReason string

const (
	// EndpointDisableUnreachable — the request never got an answer. Timeout, connection refused,
	// DNS failure, TLS mismatch, connection reset. CategoryInternal in the error registry.
	//
	// Actionable as an infrastructure problem: the address, the network path, or the credentials
	// needed to open the connection.
	EndpointDisableUnreachable EndpointDisableReason = "unreachable"

	// EndpointDisableNodeError — the node answered, and the answer was its own failure: an internal
	// error, a bad gateway, not-ready-yet. CategoryExternal and retryable, with none of the
	// not-at-fault subcategories.
	//
	// Actionable as a node problem: the process is up and reachable but cannot serve.
	EndpointDisableNodeError EndpointDisableReason = "node-error"

	// EndpointDisableServerError — the node answered with an HTTP 5xx, decided on the status alone
	// without reading the body.
	//
	// Kept distinct from node-error even though both mean "it answered badly": this one carries no
	// registry classification at all, so it is the case where we know least. Merging them would hide
	// that.
	EndpointDisableServerError EndpointDisableReason = "http-server-error"

	// EndpointDisableUnspecified — an endpoint was disabled without naming a reason. This is a bug:
	// every call site names one. It exists so a missing reason is visibly wrong rather than an empty
	// string that reads like "no reason needed".
	EndpointDisableUnspecified EndpointDisableReason = "unspecified"
)

// allEndpointDisableReasons is the shared backing array — a package-level var rather than a fresh
// slice per call, matching AllBlockReasons.
var allEndpointDisableReasons = []EndpointDisableReason{
	EndpointDisableUnreachable,
	EndpointDisableNodeError,
	EndpointDisableServerError,
	EndpointDisableUnspecified,
}

// AllEndpointDisableReasons lists every reason a disable can carry, so a per-reason gauge can
// publish a zero for the ones not currently in use and stay self-correcting.
//
// Keep in sync with the constants above — a reason missing here is a series that never returns to 0
// once it has fired. TestEndpointDisableReasons_ListCoversEveryDeclaredConstant guards that.
//
// The returned slice is shared; callers must not mutate it.
func AllEndpointDisableReasons() []EndpointDisableReason { return allEndpointDisableReasons }
