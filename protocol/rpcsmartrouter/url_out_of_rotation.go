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
// rpc_endpoint_url_out_of_rotation (1 while out, per reason that holds) and
// rpc_endpoint_url_out_of_rotation_exits_total (times the url went from in to out, under the reason
// that took it out).
//
// It only reads. Nothing here decides whether a url is in or out.

// exitReasonOrder picks the one reason an exit carries when a url went out for several at once
// (between two cycles). The cause nearest the node comes first: no answer, then the node's own
// failure, then a block that stopped moving. A reason not listed here comes after, by name.
var exitReasonOrder = []routersession.EndpointDisableReason{
	routersession.EndpointDisableUnreachable,
	routersession.EndpointDisableNodeError,
	routersession.EndpointDisableHeadStalled,
	routersession.EndpointDisableUnspecified,
}

// exitCounterReasons are the reasons each url's exits counter is created at 0 under, before the url
// ever goes out. Prometheus reads a series' first sample as its start, not as an increase, so a
// series born at its first exit hides that exit from increase() and rate(), on every new pod.
// unspecified is left out: it should not appear, and if it does its series starts at its first
// exit.
var exitCounterReasons = []string{
	string(routersession.EndpointDisableUnreachable),
	string(routersession.EndpointDisableNodeError),
	string(routersession.EndpointDisableHeadStalled),
}

// exitReasonRank orders reasons by exitReasonOrder.
func exitReasonRank(reason string) int {
	for i, r := range exitReasonOrder {
		if string(r) == reason {
			return i
		}
	}
	return len(exitReasonOrder)
}

// urlKey names one url of a provider.
type urlKey struct{ provider, url string }

// urlOutSeen is what the last probe cycle saw, kept for the next one. The zero value is a cycle that
// saw nothing. Touched only by the probe loop.
type urlOutSeen struct {
	// counts is each endpoint's out-of-rotation counts. Keyed by endpoint, not by url, because a
	// pairing rebuild replaces the Endpoint objects and their counts start again at zero.
	counts map[*routersession.Endpoint]map[routersession.EndpointDisableReason]uint64
	// urlsOut is every url that was out, for any reason, through any of its entries.
	urlsOut map[urlKey]struct{}
}

// collectURLsOutOfRotation turns this cycle's endpoints into one record per (provider, url): the
// reasons the url is out for now, and the times it went from in to out since the cycle before.
// It returns the records and what to remember for the next cycle.
//
// An exit is the url going from fully in rotation to out. A url already out at the cycle before
// adds nothing, whatever happens to it until the next cycle: gaining a second reason is the same
// outage. So a url that came back and went out again within one cycle (5 s by default), and reads
// out at both, counts once. A url in rotation at the cycle before counts the most times any one of
// its reasons, on any one of its entries, went out since then, all under one reason: the first of
// them in exitReasonOrder.
//
// One url can back several endpoints of a provider (the chart writes an archive url twice). They are
// one node and one series: the url is out when any of its entries is, and entries that go out
// together are one exit.
//
// ws/wss endpoints are skipped: no relay is sent to one, so nothing takes it out of rotation, and
// it would only add zero series. An endpoint not seen before is counted from zero, and one that is
// gone is forgotten.
func collectURLsOutOfRotation(endpoints []*routersession.EndpointWithDirectConnection, seen urlOutSeen) ([]metrics.URLOutOfRotation, urlOutSeen) {
	type urlAcc struct {
		reasons map[string]struct{}
		wentOut map[string]uint64 // per reason, the largest increase among the url's entries
	}
	var order []urlKey
	accs := make(map[urlKey]*urlAcc)
	next := urlOutSeen{
		counts:  make(map[*routersession.Endpoint]map[routersession.EndpointDisableReason]uint64, len(endpoints)),
		urlsOut: make(map[urlKey]struct{}),
	}

	for _, ep := range endpoints {
		if ep == nil || ep.Endpoint == nil || !ep.Endpoint.ServesDirectRelays() {
			continue
		}
		if _, dup := next.counts[ep.Endpoint]; dup {
			continue // a provider in both pools lists the same Endpoint twice
		}
		snap := ep.Endpoint.HealthSnapshot()
		next.counts[ep.Endpoint] = snap.OutOfRotationCounts

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
		if len(acc.reasons) > 0 {
			next.urlsOut[key] = struct{}{}
		}

		before := seen.counts[ep.Endpoint]
		for reason, count := range snap.OutOfRotationCounts {
			if increase := count - min(count, before[reason]); increase > acc.wentOut[string(reason)] {
				acc.wentOut[string(reason)] = increase
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
		if _, wasOut := seen.urlsOut[key]; !wasOut {
			if reason, times := firstExit(acc.wentOut); times > 0 {
				rec.WentOut = map[string]uint64{reason: times}
			}
		}
		records = append(records, rec)
	}
	return records, next
}

// firstExit returns the reason an exit carries, the first in exitReasonOrder among those that went
// out, and how many times the url went out: the most times any one reason did.
func firstExit(wentOut map[string]uint64) (reason string, times uint64) {
	for r, n := range wentOut {
		if n == 0 {
			continue
		}
		if n > times {
			times = n
		}
		if reason == "" || exitReasonRank(r) < exitReasonRank(reason) ||
			(exitReasonRank(r) == exitReasonRank(reason) && r < reason) {
			reason = r
		}
	}
	return reason, times
}

// publishURLsOutOfRotation wires collectURLsOutOfRotation to this server's metrics. It runs at the
// end of every probe cycle, after the re-enable pass and the head-stall step, so the gauge shows
// the state those two left.
//
// The cycle reads its endpoints at its start and publishes here at its end. cleanupStaleTrackers can
// remove a url in between and delete its per-url series; this publish would then create them again,
// and nothing deletes the counter afterwards (the gauge goes on the next cycle, which no longer lists
// the url). So each url is checked for its tracker after the write, as the relay counter does.
func (rpcss *RPCSmartRouterServer) publishURLsOutOfRotation(endpoints []*routersession.EndpointWithDirectConnection) {
	if rpcss.smartRouterEndpointMetrics == nil || rpcss.listenEndpoint == nil {
		return
	}
	var urls []metrics.URLOutOfRotation
	urls, rpcss.urlOutSeen = collectURLsOutOfRotation(endpoints, rpcss.urlOutSeen)
	rpcss.smartRouterEndpointMetrics.PublishURLsOutOfRotation(rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface, urls, exitCounterReasons)
	for _, u := range urls {
		rpcss.forgetURLLatestBlockIfUntracked(u.URL)
	}
}
