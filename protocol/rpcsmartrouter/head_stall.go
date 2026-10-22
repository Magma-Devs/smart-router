package rpcsmartrouter

import (
	"maps"
	"slices"
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
//   - add at the first cycle that sees SameBlockCycles >= N (D11), N = StallCycleThreshold (D20), and
//     only while another url of the chain has shown a higher block — a quiet chain marks nothing;
//   - remove only when the endpoint's highest block goes ABOVE the one it was stuck on (D9, H2) — never because
//     the counter restarted, which also happens when a tracker is recreated;
//   - never take out the last usable endpoint of this chain and interface, across both tiers, that
//     takes some request: the base collection or an add-on, with its extensions (D13, MAG-4591);
//     re-checked every cycle, releasing the stuck endpoint on the highest block when nothing else
//     usable takes a request it takes (D22, D26);
//   - the provider health gauge reads 0 while any of its endpoints holds head-stalled (D24). This step
//     is the only code that writes the gauge for head-stall: it re-asserts 0 every cycle, so an epoch
//     reset or a relay success on a sibling url that sets it back to 1 is corrected within one cycle,
//     and it sets 1 when the last stalled url of a provider comes back with every url usable.

// headStallState is the router-side state of the head-stall step. The stalled url set is read on the
// hot path (every tip observation) and written by the probe loop; kept is touched only by the probe
// loop, but shares the lock for simplicity.
type headStallState struct {
	mu      sync.RWMutex
	stalled map[string]struct{} // urls holding head-stalled, for the tip-vote filter
	kept    map[string]struct{} // stuck urls never-empty is holding in rotation (logged once)
}

func (s *headStallState) isStalled(url string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, held := s.stalled[url]
	return held
}

// markKept reports whether this is the first cycle never-empty holds the url (so the caller logs once
// per episode, not every 5 s).
func (s *headStallState) markKept(url string) (first bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, kept := s.kept[url]; kept {
		return false
	}
	if s.kept == nil {
		s.kept = make(map[string]struct{})
	}
	s.kept[url] = struct{}{}
	return true
}

func (s *headStallState) clearKept(url string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.kept, url)
}

// sync makes the stalled url set and the kept set describe exactly the urls of this cycle. It is the
// only writer of the stalled set: the endpoints themselves are the source of truth, rebuilt from once
// per probe cycle. That also covers a pairing rebuild, which replaces Endpoint objects (the new ones
// hold no state reason) — the old url drops out instead of having its repeat tip votes refused for
// ever. A kept url no longer in this cycle's endpoints is forgotten too.
func (s *headStallState) sync(stalledURLs map[string]struct{}, liveURLs map[string]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stalled = stalledURLs
	for url := range s.kept {
		if _, ok := liveURLs[url]; !ok {
			delete(s.kept, url)
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

// stallService is one request an endpoint takes: an add-on ("" for the base collection) with the
// extensions the request carries.
type stallService struct {
	addon      string
	extensions []string
}

// widestServices lists, for each add-on the endpoint takes ("" when it serves the base collection),
// the request carrying all of its extensions. A url that takes that request takes every narrower one
// with the same add-on, so it is the only one never-empty has to ask about. The router builds Addons
// and Extensions from the same node-url list (rpcsmartrouter.go), so this asks whether one other url
// serves at least what this one serves, add-on by add-on. A set split across two other urls is not
// enough and the url stays: the safe side, since stale answers beat no answers.
func widestServices(e *lavasession.Endpoint) []stallService {
	extensions := slices.Sorted(maps.Keys(e.Extensions))
	var services []stallService
	if e.ServesBaseCollection() {
		services = append(services, stallService{extensions: extensions})
	}
	for _, addon := range slices.Sorted(maps.Keys(e.Addons)) {
		services = append(services, stallService{addon: addon, extensions: extensions})
	}
	return services
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
	// groupAny reports whether any endpoint of the url satisfies pred.
	groupAny := func(g *urlGroup, pred func(*lavasession.Endpoint) bool) bool {
		for _, ep := range g.eps {
			if pred(ep.Endpoint) {
				return true
			}
		}
		return false
	}
	isUsable := func(e *lavasession.Endpoint) bool { return e.IsUsable() }
	isEnabled := func(e *lavasession.Endpoint) bool { return e.IsEnabled() }
	groupUsable := func(g *urlGroup) bool { return groupAny(g, isUsable) }

	// takenElsewhere reports whether a usable endpoint of another url takes the request, asked
	// through CheckSupportForServices, the predicate selection itself uses.
	takenElsewhere := func(g *urlGroup, svc stallService) bool {
		for _, other := range groups {
			if other == g {
				continue
			}
			for _, ep := range other.eps {
				if ep.Endpoint.IsUsable() && ep.Endpoint.CheckSupportForServices(svc.addon, svc.extensions) {
					return true
				}
			}
		}
		return false
	}

	// uncovered returns a request the url takes that no other usable url takes — what never-empty
	// protects (D13). It is asked per add-on, not answered by a count of usable urls: a count let a
	// url with no add-ons (a backup without debug) stand in for the only url serving debug, and
	// every debug request failed for as long as the stall held (MAG-4591). Only endpoints whose
	// event slot is on take anything, so only theirs are asked about.
	uncovered := func(g *urlGroup) (stallService, bool) {
		for _, ep := range g.eps {
			if !ep.Endpoint.IsEnabled() {
				continue
			}
			for _, svc := range widestServices(ep.Endpoint) {
				if !takenElsewhere(g, svc) {
					return svc, true
				}
			}
		}
		return stallService{}, false
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

	// stuckRecord returns the head-stalled record of the url, if any of its endpoints holds one.
	stuckRecord := func(g *urlGroup) (lavasession.StateReasonRecord, bool) {
		for _, ep := range g.eps {
			if rec, held := ep.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled); held {
				return rec, true
			}
		}
		return lavasession.StateReasonRecord{}, false
	}

	// holdsStall reports whether one endpoint holds head-stalled.
	holdsStall := func(e *lavasession.Endpoint) bool {
		_, held := e.StateReason(lavasession.EndpointDisableHeadStalled)
		return held
	}

	// release takes head-stalled off every endpoint of the url and, for each that becomes usable,
	// returns its provider to the valid list (H1) and its gauge to 1 when every url of the provider
	// is usable (D24). It reports whether anything was released.
	release := func(g *urlGroup, why string) bool {
		rec, held := stuckRecord(g)
		if !held {
			return false
		}
		var released []*lavasession.EndpointWithDirectConnection
		for _, ep := range g.eps {
			if ep.Endpoint.RemoveStateReason(lavasession.EndpointDisableHeadStalled) {
				released = append(released, ep)
			}
		}
		if len(released) == 0 {
			return false
		}
		state.clearKept(g.url)
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

	// 1. Remove: the url answered above the block it was stuck on. Its highest block, not its last
	// answer: a frozen Solana node alternates between its confirmed and finalized slot, and the
	// higher of the two is what it was stuck on.
	for _, g := range groups {
		rec, held := stuckRecord(g)
		if !held {
			continue
		}
		if obs, _ := getObservation(g.url); obs.HighestBlockSeen > rec.Block && release(g, "removed") {
			res.removed++
		}
	}

	// chainHigh is the highest block any url of this chain and interface has shown. A url is only
	// stalled when another one has gone above it: if nothing is ahead, the chain itself is quiet
	// (a testnet with no traffic, a halt), every url answers the same block, and none of them is
	// at fault. Marking them would hold every provider's health gauge at 0, and the production
	// EndpointHealthDegraded alert pages critical after 15 minutes of that.
	// Every url counts as evidence the chain moved, including ws/wss urls that never serve a relay.
	var chainHigh int64
	for e := range seen {
		if obs, _ := getObservation(e.NetworkAddress); obs.HighestBlockSeen > chainHigh {
			chainHigh = obs.HighestBlockSeen
		}
	}

	// 2. Add: N cycles in a row without moving while another url is ahead, unless it is the last
	// usable url taking some request it takes. Every endpoint of the url goes out together, or none
	// does. Urls are taken out one at a time and uncovered reads live state, so of two stuck urls
	// that cover each other the second stays.
	for _, g := range groups {
		var pending []*lavasession.EndpointWithDirectConnection
		for _, ep := range g.eps {
			if !holdsStall(ep.Endpoint) {
				pending = append(pending, ep)
			}
		}
		if len(pending) == 0 {
			continue
		}
		obs, _ := getObservation(g.url)
		if obs.SameBlockCycles < n || obs.HighestBlockSeen <= 0 || obs.HighestBlockSeen >= chainHigh {
			state.clearKept(g.url)
			continue
		}
		if groupUsable(g) {
			if svc, alone := uncovered(g); alone {
				if state.markKept(g.url) {
					res.kept++
					utils.LavaFormatInfo("head-stalled kept by never-empty", append([]utils.Attribute{
						utils.LogAttr("endpoint", g.url),
						utils.LogAttr("provider", pending[0].ProviderAddress),
						utils.LogAttr("backup", pending[0].Backup),
						utils.LogAttr("block", obs.HighestBlockSeen),
						utils.LogAttr("same_block_cycles", obs.SameBlockCycles),
						utils.LogAttr("addon", svc.addon),
						utils.LogAttr("extensions", svc.extensions),
					}, logAttrs...)...)
				}
				continue
			}
		}
		added := false
		for _, ep := range pending {
			if ep.Endpoint.AddStateReason(lavasession.EndpointDisableHeadStalled, now, obs.HighestBlockSeen) {
				added = true
			}
		}
		if !added {
			continue
		}
		state.clearKept(g.url)
		res.added++
		utils.LavaFormatInfo("head-stalled added", append([]utils.Attribute{
			utils.LogAttr("endpoint", g.url),
			utils.LogAttr("provider", pending[0].ProviderAddress),
			utils.LogAttr("backup", pending[0].Backup),
			utils.LogAttr("entries", len(pending)),
			utils.LogAttr("block", obs.HighestBlockSeen),
			utils.LogAttr("chain_high", chainHigh),
			utils.LogAttr("same_block_cycles", obs.SameBlockCycles),
			utils.LogAttr("threshold", n),
		}, logAttrs...)...)
	}

	// 3. Never-empty after the fact: something else (50 node errors on the backup) may have taken
	// out the last usable url taking some request since a stuck url that takes it went out. Stale
	// answers beat no answers: bring back, among the stuck urls taking a request nothing usable
	// takes, the one on the highest block, the most recently stuck on a tie, and ask again until
	// every such request has a url. Only a url with an endpoint whose event slot is on can actually
	// serve, so only those are candidates. Each pass releases a different url, so the loop ends.
	for {
		var pick *urlGroup
		var pickRec lavasession.StateReasonRecord
		for _, g := range groups {
			rec, held := stuckRecord(g)
			if !held {
				continue
			}
			if !groupAny(g, isEnabled) {
				continue
			}
			if _, alone := uncovered(g); !alone {
				continue
			}
			if pick == nil || rec.Block > pickRec.Block || (rec.Block == pickRec.Block && rec.Since.After(pickRec.Since)) {
				pick, pickRec = g, rec
			}
		}
		if pick == nil || !release(pick, "released by never-empty") {
			break
		}
		state.markKept(pick.url) // it is still stuck: step 2 must keep it, not re-add it
		res.released++
	}

	// Publish this cycle's stalled set, and hold the gauge of every provider with a stalled url at 0.
	stalledURLs := make(map[string]struct{})
	stalledProviders := make(map[string]struct{})
	liveURLs := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		liveURLs[g.url] = struct{}{}
		for _, ep := range g.eps {
			if holdsStall(ep.Endpoint) {
				stalledURLs[g.url] = struct{}{}
				stalledProviders[ep.ProviderAddress] = struct{}{}
			}
		}
	}
	state.sync(stalledURLs, liveURLs)
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
