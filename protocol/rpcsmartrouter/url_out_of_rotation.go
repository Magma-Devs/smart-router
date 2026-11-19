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
// rpc_endpoint_url_out_of_rotation_exits_total (times it went out, per reason).
//
// It only reads. Nothing here decides whether a url is in or out.

// urlReasonKey names one url of a provider and one reason it can be out for.
type urlReasonKey struct{ provider, url, reason string }

// endpointOutSeen is what the last probe cycle saw of one endpoint: its out-of-rotation counts, and
// the reasons it was out for.
type endpointOutSeen struct {
	counts map[routersession.EndpointDisableReason]uint64
	out    map[string]struct{}
}

// urlOutSeen is what the last probe cycle saw, kept for the next one. The zero value is a cycle that
// saw nothing. Touched only by the probe loop.
type urlOutSeen struct {
	// endpoints is keyed by endpoint, not by url, because a pairing rebuild replaces the Endpoint
	// objects and their counts start again at zero.
	endpoints map[*routersession.Endpoint]endpointOutSeen
	// urlsOut is every (provider, url, reason) that was out, whichever entry of the url held it.
	urlsOut map[urlReasonKey]struct{}
}

// collectURLsOutOfRotation turns this cycle's endpoints into one record per (provider, url): the
// reasons the url is out for now, and how many times it newly went out since the cycle before.
// It returns the records and what to remember for the next cycle.
//
// One url can back several endpoints of a provider (the chart writes an archive url twice). They are
// one node and one series, so the url is out for a reason when any of its entries is, and an exit is
// the url going out, not an entry:
//   - entries that go out in the same cycle are one exit: the largest increase among them;
//   - an entry that goes out while another entry of the url was already out for that reason at the
//     cycle before joins that outage and adds nothing. Without this, one dead node behind two
//     entries counted two exits whenever the entries crossed their failure threshold in different
//     cycles.
//
// An endpoint not seen before is counted from zero, and one that is gone is forgotten. A rebuilt
// endpoint that is out for a reason the url was already out for is the same outage, so it adds
// nothing either.
func collectURLsOutOfRotation(endpoints []*routersession.EndpointWithDirectConnection, seen urlOutSeen) ([]metrics.URLOutOfRotation, urlOutSeen) {
	type urlKey struct{ provider, url string }
	type urlAcc struct {
		reasons map[string]struct{}
		wentOut map[string]uint64
	}
	var order []urlKey
	accs := make(map[urlKey]*urlAcc)
	next := urlOutSeen{
		endpoints: make(map[*routersession.Endpoint]endpointOutSeen, len(endpoints)),
		urlsOut:   make(map[urlReasonKey]struct{}),
	}

	for _, ep := range endpoints {
		if ep == nil || ep.Endpoint == nil {
			continue
		}
		if _, dup := next.endpoints[ep.Endpoint]; dup {
			continue // a provider in both pools lists the same Endpoint twice
		}
		snap := ep.Endpoint.HealthSnapshot()
		out := make(map[string]struct{})
		if !snap.Enabled {
			out[string(snap.DisableReason)] = struct{}{}
		}
		for _, rec := range snap.StateReasons {
			out[string(rec.Reason)] = struct{}{}
		}
		next.endpoints[ep.Endpoint] = endpointOutSeen{counts: snap.OutOfRotationCounts, out: out}

		key := urlKey{provider: ep.ProviderAddress, url: ep.Endpoint.NetworkAddress}
		acc := accs[key]
		if acc == nil {
			acc = &urlAcc{reasons: make(map[string]struct{}), wentOut: make(map[string]uint64)}
			accs[key] = acc
			order = append(order, key)
		}
		for reason := range out {
			acc.reasons[reason] = struct{}{}
			next.urlsOut[urlReasonKey{key.provider, key.url, reason}] = struct{}{}
		}

		before := seen.endpoints[ep.Endpoint]
		for reason, count := range snap.OutOfRotationCounts {
			if count <= before.counts[reason] {
				continue
			}
			r := string(reason)
			if _, entryWasOut := before.out[r]; !entryWasOut {
				if _, urlWasOut := seen.urlsOut[urlReasonKey{key.provider, key.url, r}]; urlWasOut {
					continue // joined an outage of the url already under way
				}
			}
			if increase := count - before.counts[reason]; increase > acc.wentOut[r] {
				acc.wentOut[r] = increase
			}
		}
	}

	records := make([]metrics.URLOutOfRotation, 0, len(order))
	for _, key := range order {
		acc := accs[key]
		rec := metrics.URLOutOfRotation{Provider: key.provider, URL: key.url}
		for reason := range acc.reasons {
			rec.Reasons = append(rec.Reasons, reason)
		}
		if len(acc.wentOut) > 0 {
			rec.WentOut = acc.wentOut
		}
		records = append(records, rec)
	}
	return records, next
}

// publishURLsOutOfRotation wires collectURLsOutOfRotation to this server's metrics. It runs at the
// end of every probe cycle, after the re-enable pass and the head-stall step, so the gauge shows
// the state those two left.
//
// The cycle reads its endpoints at its start and publishes here at its end. cleanupStaleTrackers can
// remove a url in between and delete its per-url series; this publish would then create them again,
// and nothing deletes the counter afterwards (the gauge goes on the next cycle, which no longer lists
// the url). So each url written to is checked for its tracker after the write, as the relay counter
// does.
func (rpcss *RPCSmartRouterServer) publishURLsOutOfRotation(endpoints []*routersession.EndpointWithDirectConnection) {
	if rpcss.smartRouterEndpointMetrics == nil || rpcss.listenEndpoint == nil {
		return
	}
	var urls []metrics.URLOutOfRotation
	urls, rpcss.urlOutSeen = collectURLsOutOfRotation(endpoints, rpcss.urlOutSeen)
	rpcss.smartRouterEndpointMetrics.PublishURLsOutOfRotation(rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface, urls)
	for _, u := range urls {
		if len(u.Reasons) > 0 || len(u.WentOut) > 0 {
			rpcss.forgetURLLatestBlockIfUntracked(u.URL)
		}
	}
}
