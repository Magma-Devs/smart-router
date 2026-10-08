package rpcsmartrouter

import (
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

// MAG-3986 — head-stalled endpoints.
//
// An endpoint that keeps answering but whose block stopped moving is invisible to every error-based
// rule: each answer is a 200, so the relay path scores it a success while it serves stale data. The
// endpoint monitor counts, per url, the consecutive tracker cycles in which the endpoint answered with
// the same block (EndpointObservation.SameBlockCycles). This file is the decision half: once per probe
// cycle it reads that evidence and adds or removes the head-stalled STATE reason on the endpoint.
//
// A head-stalled endpoint is out of rotation for everything (selection skips it; a provider whose
// endpoints are all out is blocked, and its traffic moves to the next primary, then the backup tier).
// Its repeated block also stops voting on the chain tip (onTipObservation), which is what kept a wrong
// tip alive in the Plasma incident.
//
// Rules, from agent_docs/bug-reports/stuck-provider-not-detected/plan.md:
//   - add at the first cycle that sees SameBlockCycles >= N (D11), N = StallCycleThreshold (D20);
//   - remove only when the endpoint answers ABOVE the block it was stuck on (D9, H2) — never because
//     the counter restarted, which also happens when a tracker is recreated;
//   - never take out the last usable endpoint of this chain and interface, across both tiers (D13);
//     re-checked every cycle, releasing the stuck endpoint on the highest block when nothing else is
//     usable (D22, D26);
//   - the provider health gauge reads 0 while any of its endpoints holds head-stalled (D24). This step
//     is the only code that writes the gauge for head-stall: it re-asserts 0 every cycle, so an epoch
//     reset or a relay success on a sibling url that sets it back to 1 is corrected within one cycle,
//     and it sets 1 when the last stalled url of a provider comes back with every url usable.

// headStallState is the router-side state of the head-stall step. The stalled url set is read on the
// hot path (every tip observation) and written by the probe loop; kept is touched only by the probe
// loop, but shares the lock for simplicity.
type headStallState struct {
	mu      sync.RWMutex
	stalled map[string]struct{}            // urls holding head-stalled, for the tip-vote filter
	kept    map[*lavasession.Endpoint]bool // stuck endpoints never-empty is holding in rotation (log once)
}

func (s *headStallState) isStalled(url string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, held := s.stalled[url]
	return held
}

// markKept reports whether this is the first cycle never-empty holds the endpoint (so the caller logs
// once per episode, not every 5 s).
func (s *headStallState) markKept(e *lavasession.Endpoint) (first bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kept[e] {
		return false
	}
	if s.kept == nil {
		s.kept = make(map[*lavasession.Endpoint]bool)
	}
	s.kept[e] = true
	return true
}

func (s *headStallState) clearKept(e *lavasession.Endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.kept, e)
}

// sync makes the stalled url set and the kept set describe exactly the endpoints of this cycle. It is
// the only writer of the stalled set: the endpoints themselves are the source of truth, rebuilt from
// once per probe cycle. That also covers a pairing rebuild, which replaces Endpoint objects (the new
// ones hold no state reason) — the old url drops out instead of having its repeat tip votes refused
// for ever.
func (s *headStallState) sync(stalledURLs map[string]struct{}, live map[*lavasession.Endpoint]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stalled = stalledURLs
	for e := range s.kept {
		if _, ok := live[e]; !ok {
			delete(s.kept, e)
		}
	}
}

// reset forgets every stalled url and kept endpoint — the operator reset.
func (s *headStallState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stalled = nil
	s.kept = nil
}

// headStallResult is what one head-stall step did, for /debug/probe-loop.
type headStallResult struct {
	added, removed, kept, released int
}

// runHeadStallStep is the pure per-cycle body (no rpcss fields), testable with constructed endpoints
// and a fake observation getter. endpoints is the same slice the probe cycle scored: both tiers,
// possibly listing one *Endpoint twice when a provider sits in both pools.
func runHeadStallStep(
	endpoints []*lavasession.EndpointWithDirectConnection,
	getObservation func(url string) (endpointstate.EndpointObservation, bool),
	n int,
	now time.Time,
	onRecover func(provider string),
	setProviderHealth func(provider string, healthy bool),
	state *headStallState,
	logAttrs []utils.Attribute,
) (res headStallResult) {
	// One entry per *Endpoint, and each provider's endpoints for the gauge rule.
	unique := make([]*lavasession.EndpointWithDirectConnection, 0, len(endpoints))
	seen := make(map[*lavasession.Endpoint]struct{}, len(endpoints))
	byProvider := make(map[string][]*lavasession.Endpoint)
	for _, ep := range endpoints {
		if ep == nil || ep.Endpoint == nil {
			continue
		}
		if _, dup := seen[ep.Endpoint]; dup {
			continue
		}
		seen[ep.Endpoint] = struct{}{}
		byProvider[ep.ProviderAddress] = append(byProvider[ep.ProviderAddress], ep.Endpoint)
		// Only endpoints that can answer a relay take part: a ws/wss url is never selected for a
		// relay (Endpoint.ServesDirectRelays), so counting it as "usable" would let never-empty
		// keep a websocket url while taking out the provider's only https url — leaving nothing
		// that can serve. Subscriptions on a stuck wss url are out of scope (MAG-4161).
		if ep.Endpoint.ServesDirectRelays() {
			unique = append(unique, ep)
		}
	}

	// The decision is per url, not per endpoint. The chart writes an `archive` node-url twice (once
	// with the extension, once without), so one url can back several endpoints. They share one
	// observation, and they are one node: counting them apart let never-empty take out the archive
	// entry of a single-node router and keep only the plain one, leaving archive/trace requests with
	// nothing to go to (MAG-4025 shape).
	type urlGroup struct {
		url string
		eps []*lavasession.EndpointWithDirectConnection
	}
	var groups []*urlGroup
	groupOf := make(map[string]*urlGroup)
	for _, ep := range unique {
		g := groupOf[ep.Endpoint.NetworkAddress]
		if g == nil {
			g = &urlGroup{url: ep.Endpoint.NetworkAddress}
			groupOf[g.url] = g
			groups = append(groups, g)
		}
		g.eps = append(g.eps, ep)
	}
	groupUsable := func(g *urlGroup) bool {
		for _, ep := range g.eps {
			if ep.Endpoint.IsUsable() {
				return true
			}
		}
		return false
	}
	countUsableURLs := func() int {
		n := 0
		for _, g := range groups {
			if groupUsable(g) {
				n++
			}
		}
		return n
	}

	// providerAllUsable: every url of the provider can be routed to — the only state in which this
	// step sets the provider's gauge back to 1.
	providerAllUsable := func(provider string) bool {
		for _, e := range byProvider[provider] {
			if !e.IsUsable() {
				return false
			}
		}
		return true
	}

	// release takes head-stalled off every endpoint of the url and, for each that becomes usable,
	// returns its provider to the valid list (H1) and its gauge to 1 when every url of the provider
	// is usable (D24). It reports whether anything was released.
	release := func(g *urlGroup, why string) bool {
		var released []*lavasession.EndpointWithDirectConnection
		var rec lavasession.StateReasonRecord
		for _, ep := range g.eps {
			if r, held := ep.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled); held && ep.Endpoint.RemoveStateReason(lavasession.EndpointDisableHeadStalled) {
				rec = r
				released = append(released, ep)
				state.clearKept(ep.Endpoint)
			}
		}
		if len(released) == 0 {
			return false
		}
		first := released[0]
		utils.LavaFormatInfo("head-stalled "+why, append([]utils.Attribute{
			utils.LogAttr("endpoint", g.url),
			utils.LogAttr("provider", first.ProviderAddress),
			utils.LogAttr("backup", first.Backup),
			utils.LogAttr("entries", len(released)),
			utils.LogAttr("stuck_block", rec.Block),
			utils.LogAttr("stuck_for", now.Sub(rec.Since).String()),
		}, logAttrs...)...)
		for _, ep := range released {
			if !ep.Endpoint.IsUsable() {
				continue
			}
			if onRecover != nil {
				onRecover(ep.ProviderAddress)
			}
			if setProviderHealth != nil && providerAllUsable(ep.ProviderAddress) {
				setProviderHealth(ep.ProviderAddress, true)
			}
		}
		return true
	}

	// stuckRecord returns the head-stalled record of the url, if any of its endpoints holds one.
	stuckRecord := func(g *urlGroup) (lavasession.StateReasonRecord, bool) {
		for _, ep := range g.eps {
			if rec, held := ep.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled); held {
				return rec, true
			}
		}
		return lavasession.StateReasonRecord{}, false
	}

	// 1. Remove: the url answered above the block it was stuck on.
	for _, g := range groups {
		rec, held := stuckRecord(g)
		if !held {
			continue
		}
		if obs, _ := getObservation(g.url); obs.LastAnsweredBlock > rec.Block && release(g, "removed") {
			res.removed++
		}
	}

	usable := countUsableURLs()

	// 2. Add: N same-block cycles in a row, unless it is the last usable url. Every endpoint of the
	// url goes out together, or none does.
	for _, g := range groups {
		var pending []*lavasession.EndpointWithDirectConnection
		for _, ep := range g.eps {
			if _, held := ep.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled); !held {
				pending = append(pending, ep)
			}
		}
		if len(pending) == 0 {
			continue
		}
		obs, _ := getObservation(g.url)
		if obs.SameBlockCycles < n || obs.LastAnsweredBlock <= 0 {
			for _, ep := range pending {
				state.clearKept(ep.Endpoint)
			}
			continue
		}
		wasUsable := groupUsable(g)
		if wasUsable && usable <= 1 {
			firstKept := false
			for _, ep := range pending {
				if state.markKept(ep.Endpoint) {
					firstKept = true
				}
			}
			if firstKept {
				res.kept++
				utils.LavaFormatInfo("head-stalled kept by never-empty", append([]utils.Attribute{
					utils.LogAttr("endpoint", g.url),
					utils.LogAttr("provider", pending[0].ProviderAddress),
					utils.LogAttr("backup", pending[0].Backup),
					utils.LogAttr("block", obs.LastAnsweredBlock),
					utils.LogAttr("same_block_cycles", obs.SameBlockCycles),
				}, logAttrs...)...)
			}
			continue
		}
		added := false
		for _, ep := range pending {
			if ep.Endpoint.AddStateReason(lavasession.EndpointDisableHeadStalled, now, obs.LastAnsweredBlock) {
				added = true
				state.clearKept(ep.Endpoint)
			}
		}
		if !added {
			continue
		}
		if wasUsable {
			usable--
		}
		res.added++
		utils.LavaFormatInfo("head-stalled added", append([]utils.Attribute{
			utils.LogAttr("endpoint", g.url),
			utils.LogAttr("provider", pending[0].ProviderAddress),
			utils.LogAttr("backup", pending[0].Backup),
			utils.LogAttr("entries", len(pending)),
			utils.LogAttr("block", obs.LastAnsweredBlock),
			utils.LogAttr("same_block_cycles", obs.SameBlockCycles),
			utils.LogAttr("threshold", n),
		}, logAttrs...)...)
	}

	// 3. Never-empty after the fact: something else (50 node errors on the backup) may have emptied
	// the pool since this url was taken out. Stale answers beat no answers: bring back the stuck url
	// on the highest block, the most recently stuck on a tie. Only a url with an endpoint whose event
	// slot is on can actually serve, so only those are candidates.
	if countUsableURLs() == 0 {
		var pick *urlGroup
		var pickRec lavasession.StateReasonRecord
		for _, g := range groups {
			rec, held := stuckRecord(g)
			if !held {
				continue
			}
			canServe := false
			for _, ep := range g.eps {
				if ep.Endpoint.IsEnabled() {
					canServe = true
					break
				}
			}
			if !canServe {
				continue
			}
			if pick == nil || rec.Block > pickRec.Block || (rec.Block == pickRec.Block && rec.Since.After(pickRec.Since)) {
				pick, pickRec = g, rec
			}
		}
		if pick != nil && release(pick, "released by never-empty") {
			for _, ep := range pick.eps {
				state.markKept(ep.Endpoint) // it is still stuck: step 2 must keep it, not re-add it
			}
			res.released++
		}
	}

	// Publish this cycle's stalled set, and hold the gauge of every provider with a stalled url at 0.
	stalledURLs := make(map[string]struct{})
	stalledProviders := make(map[string]struct{})
	for _, ep := range unique {
		if _, held := ep.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled); held {
			stalledURLs[ep.Endpoint.NetworkAddress] = struct{}{}
			stalledProviders[ep.ProviderAddress] = struct{}{}
		}
	}
	state.sync(stalledURLs, seen)
	if setProviderHealth != nil {
		for provider := range stalledProviders {
			setProviderHealth(provider, false)
		}
	}
	return res
}

// runHeadStall wires runHeadStallStep to this server's live dependencies.
func (rpcss *RPCSmartRouterServer) runHeadStall(endpoints []*lavasession.EndpointWithDirectConnection, now time.Time) {
	if rpcss.endpointChainTrackerManager == nil {
		return
	}
	var chainID, apiInterface string
	if rpcss.listenEndpoint != nil {
		chainID, apiInterface = rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface
	}
	var setHealth func(string, bool)
	if rpcss.smartRouterEndpointMetrics != nil {
		setHealth = func(provider string, healthy bool) {
			rpcss.smartRouterEndpointMetrics.SetEndpointOverallHealth(chainID, apiInterface, provider, healthy)
		}
	}
	var onRecover func(string)
	if rpcss.sessionManager != nil {
		onRecover = rpcss.sessionManager.RestoreRecoveredProvider
	}
	res := runHeadStallStep(
		endpoints,
		rpcss.endpointChainTrackerManager.GetObservation,
		rpcss.endpointChainTrackerManager.StallCycleThreshold(),
		now, onRecover, setHealth, &rpcss.headStall,
		[]utils.Attribute{utils.LogAttr("chainID", chainID), utils.LogAttr("apiInterface", apiInterface)},
	)
	rpcss.probeStats.recordHeadStall(res)
}
