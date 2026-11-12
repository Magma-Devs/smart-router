package rpcsmartrouter

import (
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// MAG-5244 — why a url is out of rotation, as metrics.
//
// A provider is blocked when all its urls are out, so its block reason is always
// all-endpoints-disabled. The cause is on the url: unreachable, node-error or head-stalled. Until
// now that was only in log lines and /debug/endpoint-state. Once per probe cycle this step reads
// every endpoint's own record and hands it to the metrics manager, which publishes
// rpc_endpoint_url_out_of_rotation (1 while out, per reason) and
// rpc_endpoint_url_out_of_rotation_total (times it went out, per reason).
//
// It only reads. Nothing here decides whether a url is in or out.

// urlOutCountsSeen is, per endpoint, the out-of-rotation counts already handed to the metrics
// manager. Touched only by the probe loop.
type urlOutCountsSeen map[*routersession.Endpoint]map[routersession.EndpointDisableReason]uint64

// collectURLsOutOfRotation turns this cycle's endpoints into one record per (provider, url): the
// reasons the url is out for now, and how many times it newly went out since the cycle before.
// It returns the records and the counts to remember for the next cycle.
//
// One url can back several endpoints of a provider (the chart writes an archive url twice). They are
// one node and one series, so the url is out for a reason when any of its endpoints is, and an
// event that takes all of them out together counts once: the largest increase among them.
//
// seen is keyed by endpoint, not by url, because a pairing rebuild replaces the Endpoint objects and
// their counts start again at zero. An endpoint not seen before is counted from zero, and one that
// is gone is forgotten.
func collectURLsOutOfRotation(endpoints []*routersession.EndpointWithDirectConnection, seen urlOutCountsSeen) ([]metrics.URLOutOfRotation, urlOutCountsSeen) {
	type urlKey struct{ provider, url string }
	type urlAcc struct {
		reasons map[string]struct{}
		wentOut map[string]uint64
	}
	var order []urlKey
	accs := make(map[urlKey]*urlAcc)
	nextSeen := make(urlOutCountsSeen, len(endpoints))

	for _, ep := range endpoints {
		if ep == nil || ep.Endpoint == nil {
			continue
		}
		if _, dup := nextSeen[ep.Endpoint]; dup {
			continue // a provider in both pools lists the same Endpoint twice
		}
		snap := ep.Endpoint.HealthSnapshot()
		nextSeen[ep.Endpoint] = snap.OutOfRotationCounts

		key := urlKey{provider: ep.ProviderAddress, url: ep.Endpoint.NetworkAddress}
		acc := accs[key]
		if acc == nil {
			acc = &urlAcc{reasons: make(map[string]struct{}), wentOut: make(map[string]uint64)}
			accs[key] = acc
			order = append(order, key)
		}
		if !snap.Enabled {
			acc.reasons[string(snap.DisableReason)] = struct{}{}
		}
		for _, rec := range snap.StateReasons {
			acc.reasons[string(rec.Reason)] = struct{}{}
		}
		before := seen[ep.Endpoint]
		for reason, count := range snap.OutOfRotationCounts {
			if increase := count - before[reason]; count > before[reason] && increase > acc.wentOut[string(reason)] {
				acc.wentOut[string(reason)] = increase
			}
		}
	}

	out := make([]metrics.URLOutOfRotation, 0, len(order))
	for _, key := range order {
		acc := accs[key]
		rec := metrics.URLOutOfRotation{Provider: key.provider, URL: key.url}
		for reason := range acc.reasons {
			rec.Reasons = append(rec.Reasons, reason)
		}
		if len(acc.wentOut) > 0 {
			rec.WentOut = acc.wentOut
		}
		out = append(out, rec)
	}
	return out, nextSeen
}

// publishURLsOutOfRotation wires collectURLsOutOfRotation to this server's metrics. It runs at the
// end of every probe cycle, after the re-enable pass and the head-stall step, so the gauge shows
// the state those two left.
func (rpcss *RPCSmartRouterServer) publishURLsOutOfRotation(endpoints []*routersession.EndpointWithDirectConnection) {
	if rpcss.smartRouterEndpointMetrics == nil || rpcss.listenEndpoint == nil {
		return
	}
	var urls []metrics.URLOutOfRotation
	urls, rpcss.urlOutCountsSeen = collectURLsOutOfRotation(endpoints, rpcss.urlOutCountsSeen)
	rpcss.smartRouterEndpointMetrics.PublishURLsOutOfRotation(rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface, urls)
}
