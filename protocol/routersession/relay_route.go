package routersession

// RelayRoute is what a relay resolved to that decides which of a provider's urls may take it: the
// internal path of its api collection and the api's name. Endpoint selection matches both, as the
// chain router's route key does (chainlib/chain_router.go). A nil *RelayRoute is the "nothing to
// match on" case (probes, tests) and leaves selection unfiltered.
type RelayRoute struct {
	// InternalPath is the collection's path ("/v2", "/P", or "" for the root), or nil when the
	// relay resolved no collection. See GetSessionsOptions.InternalPath.
	InternalPath *string
	// Method is the api's name as the spec names it, or "" to match on no method. See
	// Endpoint.Methods.
	Method string
}

// newRelayRoute is the route of a relay with this path and method, or nil when it has neither.
func newRelayRoute(internalPath *string, method string) *RelayRoute {
	if internalPath == nil && method == "" {
		return nil
	}
	return &RelayRoute{InternalPath: internalPath, Method: method}
}

func (r *RelayRoute) internalPath() *string {
	if r == nil {
		return nil
	}
	return r.InternalPath
}

func (r *RelayRoute) method() string {
	if r == nil {
		return ""
	}
	return r.Method
}

// routeRestriction is how a provider narrows its endpoints for one request: to the endpoints on
// the request's path (restrictsToPathLocked), and to the endpoints that name its method
// (restrictsToMethodLocked).
type routeRestriction struct {
	path   bool
	method bool
}
