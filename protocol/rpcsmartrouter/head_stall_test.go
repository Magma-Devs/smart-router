package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// MAG-3986 — the head-stall step. These tests drive runHeadStallStep with constructed endpoints and
// a fake observation table, the same way probe_loop_test.go drives runProbeCycleCore.

const stallN = 20

type stallObs map[string]endpointstate.EndpointObservation

func (o stallObs) get(url string) (endpointstate.EndpointObservation, bool) {
	v, ok := o[url]
	return v, ok
}

func stuckAt(block int64, cycles int) endpointstate.EndpointObservation {
	return endpointstate.EndpointObservation{HighestBlockSeen: block, LastAnsweredBlock: block, SameBlockCycles: cycles}
}

type stallHarness struct {
	obs       stallObs
	state     headStallState
	recovered []string
	health    map[string]bool
}

func newStallHarness() *stallHarness {
	return &stallHarness{obs: stallObs{}, health: map[string]bool{}}
}

func (h *stallHarness) step(t *testing.T, now time.Time, endpoints ...*lavasession.EndpointWithDirectConnection) headStallResult {
	t.Helper()
	return runHeadStallStep(endpoints, h.obs.get, stallN, now,
		func(p string) { h.recovered = append(h.recovered, p) },
		func(p string, healthy bool) { h.health[p] = healthy },
		&h.state, nil)
}

func backupEp(url, provider string) *lavasession.EndpointWithDirectConnection {
	e := ep(url, provider, true)
	e.Backup = true
	return e
}

func stalled(t *testing.T, e *lavasession.EndpointWithDirectConnection) bool {
	t.Helper()
	_, held := e.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled)
	return held
}

var stallNow = time.Unix(1_900_000_000, 0)

// The Polygon shape: one primary frozen, one fresh backup. The primary goes out at exactly N cycles,
// not N-1, its gauge goes to 0, and its repeat votes are now refused.
func TestHeadStall_PrimaryOutAtN_BackupTakesOver(t *testing.T) {
	h := newStallHarness()
	primary := ep("https://primary", "customer-node", true)
	backup := backupEp("https://chainstack", "chainstack")
	h.obs["https://chainstack"] = stuckAt(5000, 0)

	h.obs["https://primary"] = stuckAt(1000, stallN-1)
	require.Equal(t, headStallResult{}, h.step(t, stallNow, primary, backup))
	require.False(t, stalled(t, primary), "N-1 cycles is not stuck")

	h.obs["https://primary"] = stuckAt(1000, stallN)
	require.Equal(t, headStallResult{added: 1}, h.step(t, stallNow, primary, backup))
	require.True(t, stalled(t, primary))
	require.False(t, primary.Endpoint.IsUsable())
	require.True(t, primary.Endpoint.IsEnabled(), "the event slot is untouched")
	require.Equal(t, false, h.health["customer-node"])
	require.True(t, h.state.isStalled("https://primary"))
	rec, _ := primary.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled)
	require.Equal(t, int64(1000), rec.Block)
	require.Equal(t, stallNow, rec.Since)

	// Later cycles with the same evidence change nothing.
	require.Equal(t, headStallResult{}, h.step(t, stallNow.Add(5*time.Second), primary, backup))
}

// It comes back only when it answers ABOVE the stuck block — not when the counter restarts, which
// also happens when its tracker is recreated (H2). Coming back returns the provider to the valid
// list (H1) and its gauge to 1.
func TestHeadStall_RemovedOnlyAboveStuckBlock(t *testing.T) {
	h := newStallHarness()
	primary := ep("https://primary", "customer-node", true)
	backup := backupEp("https://chainstack", "chainstack")
	h.obs["https://chainstack"] = stuckAt(5000, 0)
	h.obs["https://primary"] = stuckAt(1000, stallN)
	h.step(t, stallNow, primary, backup)
	require.True(t, stalled(t, primary))

	h.obs["https://primary"] = stuckAt(1000, 0) // tracker recreated: counter 0, same block
	require.Equal(t, headStallResult{}, h.step(t, stallNow.Add(5*time.Second), primary, backup))
	require.True(t, stalled(t, primary), "a counter reset is not proof of movement")

	h.obs["https://primary"] = stuckAt(1001, 0)
	require.Equal(t, headStallResult{removed: 1}, h.step(t, stallNow.Add(10*time.Second), primary, backup))
	require.False(t, stalled(t, primary))
	require.True(t, primary.Endpoint.IsUsable())
	require.Equal(t, []string{"customer-node"}, h.recovered)
	require.Equal(t, true, h.health["customer-node"])
	require.False(t, h.state.isStalled("https://primary"))
}

// Never-empty counts usable endpoints across BOTH tiers (D13): with every other endpoint already out,
// the stuck one is kept and logged once, not every cycle. A single endpoint with nothing ahead of it
// is not judged at all — see TestHeadStall_QuietChainMarksNothing.
func TestHeadStall_NeverEmptyKeepsTheLastUsable(t *testing.T) {
	h := newStallHarness()
	only := ep("https://only", "solo", true)
	h.obs["https://only"] = stuckAt(1000, stallN)
	require.Equal(t, headStallResult{}, h.step(t, stallNow, only), "nothing is ahead, so there is no stall to keep")
	require.False(t, stalled(t, only))

	// Primary stuck, and the backup — which shows the chain moved — already disabled for node errors:
	// the primary is the last usable endpoint, so it stays.
	h2 := newStallHarness()
	primary := ep("https://primary", "p", true)
	deadBackup := backupEp("https://backup", "b")
	deadBackup.Endpoint.Enabled = false
	h2.obs["https://primary"] = stuckAt(1000, stallN)
	h2.obs["https://backup"] = stuckAt(5000, 0)
	require.Equal(t, headStallResult{kept: 1}, h2.step(t, stallNow, primary, deadBackup))
	require.False(t, stalled(t, primary))
	require.Equal(t, headStallResult{}, h2.step(t, stallNow.Add(5*time.Second), primary, deadBackup), "kept is logged once per episode")
}

// A quiet chain (a testnet with no traffic, a halt) makes no block for N cycles, so every url answers
// the same block. None of them is behind, so none is marked and no health gauge is touched: holding
// them at 0 would page on-call through EndpointHealthDegraded (critical after 15 minutes).
func TestHeadStall_QuietChainMarksNothing(t *testing.T) {
	h := newStallHarness()
	a := ep("https://a", "vendor-a", true)
	b := ep("https://b", "vendor-b", true)
	c := backupEp("https://c", "vendor-c")
	for _, u := range []string{"https://a", "https://b", "https://c"} {
		h.obs[u] = stuckAt(1000, stallN*3)
	}
	require.Equal(t, headStallResult{}, h.step(t, stallNow, a, b, c))
	require.False(t, stalled(t, a))
	require.False(t, stalled(t, b))
	require.False(t, stalled(t, c))
	require.Empty(t, h.health, "no gauge is written for a quiet chain")

	// The chain moves again and only b follows: a and c are now behind and go out.
	h.obs["https://b"] = stuckAt(1001, 0)
	require.Equal(t, headStallResult{added: 2}, h.step(t, stallNow.Add(5*time.Second), a, b, c))
	require.True(t, stalled(t, a))
	require.False(t, stalled(t, b))
	require.True(t, stalled(t, c))
}

// An endpoint already out for an event reason does not count as usable, so marking it stuck never
// trips never-empty — and it stays held after the event reason clears.
func TestHeadStall_EventDisabledEndpointCanAlsoStall(t *testing.T) {
	h := newStallHarness()
	primary := ep("https://primary", "p", false) // disabled for node-error
	backup := backupEp("https://backup", "b")
	h.obs["https://primary"] = stuckAt(1000, stallN)
	h.obs["https://backup"] = stuckAt(5000, 0)
	require.Equal(t, headStallResult{added: 1}, h.step(t, stallNow, primary, backup))

	primary.Endpoint.ResetHealth() // the event reason clears (relay success / epoch)
	require.True(t, primary.Endpoint.IsEnabled())
	require.False(t, primary.Endpoint.IsUsable(), "head-stalled still holds")
}

// Never-empty after the fact (D22): something else emptied the pool after the stall was added. The
// step brings back the stuck endpoint on the HIGHEST block, the most recent on a tie (D26), and then
// keeps it rather than re-adding the reason.
func TestHeadStall_ReleasesHighestStuckBlockWhenPoolEmpties(t *testing.T) {
	h := newStallHarness()
	a := ep("https://a", "pa", true)
	b := ep("https://b", "pb", true)
	c := backupEp("https://c", "pc")
	h.obs["https://a"] = stuckAt(1000, stallN)
	h.obs["https://b"] = stuckAt(1200, stallN)
	h.obs["https://c"] = stuckAt(9000, 0)
	require.Equal(t, headStallResult{added: 2}, h.step(t, stallNow, a, b, c))

	c.Endpoint.Enabled = false // the backup gets disabled for node errors
	res := h.step(t, stallNow.Add(5*time.Second), a, b, c)
	require.Equal(t, headStallResult{released: 1}, res)
	require.False(t, stalled(t, b), "b was stuck on the higher block, so it serves the least stale data")
	require.True(t, b.Endpoint.IsUsable())
	require.True(t, stalled(t, a))

	// Next cycle: b is still stuck but is the only usable endpoint — kept, not re-added.
	require.Equal(t, headStallResult{}, h.step(t, stallNow.Add(10*time.Second), a, b, c))
	require.True(t, b.Endpoint.IsUsable())

	// The backup recovers: b is no longer the last usable endpoint, so it goes back out.
	c.Endpoint.ResetHealth()
	require.Equal(t, headStallResult{added: 1}, h.step(t, stallNow.Add(15*time.Second), a, b, c))
	require.True(t, stalled(t, b))
}

func TestHeadStall_ReleaseTieGoesToMostRecent(t *testing.T) {
	h := newStallHarness()
	a := ep("https://a", "pa", true)
	b := ep("https://b", "pb", true)
	keep := ep("https://keep", "pk", true)
	h.obs["https://a"] = stuckAt(1000, stallN)
	h.obs["https://keep"] = stuckAt(9000, 0)
	h.step(t, stallNow, a, b, keep)
	h.obs["https://b"] = stuckAt(1000, stallN)
	h.step(t, stallNow.Add(5*time.Second), a, b, keep)
	require.True(t, stalled(t, a) && stalled(t, b))

	keep.Endpoint.Enabled = false
	h.step(t, stallNow.Add(10*time.Second), a, b, keep)
	require.True(t, stalled(t, a))
	require.False(t, stalled(t, b), "equal blocks: the most recently stuck comes back")
}

// A provider in both pools is listed twice by GetAllDirectRPCEndpoints. It must count as one usable
// endpoint, or never-empty would let the last real endpoint go.
func TestHeadStall_DuplicatePointerCountedOnce(t *testing.T) {
	h := newStallHarness()
	both := ep("https://both", "dual", true)
	dupe := &lavasession.EndpointWithDirectConnection{Endpoint: both.Endpoint, ProviderAddress: "dual", Backup: true}
	dead := ep("https://ahead", "other", false) // shows the chain moved, but cannot serve
	h.obs["https://both"] = stuckAt(1000, stallN)
	h.obs["https://ahead"] = stuckAt(5000, 0)
	require.Equal(t, headStallResult{kept: 1}, h.step(t, stallNow, both, dupe, dead))
	require.False(t, stalled(t, both))
}

// The chart writes an `archive` node-url twice (archive entry + plain entry): two *Endpoints, one
// url, one node. Never-empty counts urls, so a single-node archive router keeps BOTH entries; counting
// endpoints took the archive entry out and left archive/trace requests with nowhere to go.
func TestHeadStall_ArchiveURLListedTwiceIsOneNode(t *testing.T) {
	h := newStallHarness()
	archive := ep("https://node", "solo", true)
	plain := ep("https://node", "solo", true)
	dead := backupEp("https://backup", "backup") // shows the chain moved, but is disabled
	dead.Endpoint.Enabled = false
	h.obs["https://node"] = stuckAt(1000, stallN)
	h.obs["https://backup"] = stuckAt(5000, 0)

	require.Equal(t, headStallResult{kept: 1}, h.step(t, stallNow, archive, plain, dead))
	require.False(t, stalled(t, archive), "the archive entry of the only usable node must stay in rotation")
	require.False(t, stalled(t, plain))
	require.Equal(t, headStallResult{}, h.step(t, stallNow.Add(5*time.Second), archive, plain, dead), "kept is logged once per episode")
}

// With a second node, the stuck url goes out whole — both of its entries — and comes back whole.
func TestHeadStall_ArchiveURLListedTwiceGoesOutAndBackTogether(t *testing.T) {
	h := newStallHarness()
	stuckArchive := ep("https://a", "vendor", true)
	stuckPlain := ep("https://a", "vendor", true)
	freshArchive := ep("https://b", "vendor", true)
	freshPlain := ep("https://b", "vendor", true)
	h.obs["https://a"] = stuckAt(1000, stallN)
	h.obs["https://b"] = stuckAt(5000, 0)
	all := []*lavasession.EndpointWithDirectConnection{stuckArchive, stuckPlain, freshArchive, freshPlain}

	require.Equal(t, headStallResult{added: 1}, h.step(t, stallNow, all...))
	require.True(t, stalled(t, stuckArchive))
	require.True(t, stalled(t, stuckPlain))
	require.False(t, stalled(t, freshArchive))
	require.False(t, stalled(t, freshPlain))

	// b freezes too, but nothing is ahead of it, so it is not judged: both of its entries stay.
	h.obs["https://b"] = stuckAt(5000, stallN)
	require.Equal(t, headStallResult{}, h.step(t, stallNow.Add(5*time.Second), all...))
	require.False(t, stalled(t, freshArchive), "the archive entry of the last usable url must stay")
	require.False(t, stalled(t, freshPlain))

	h.obs["https://a"] = stuckAt(5001, 0)
	require.Equal(t, headStallResult{removed: 1, added: 1}, h.step(t, stallNow.Add(10*time.Second), all...))
	require.False(t, stalled(t, stuckArchive))
	require.False(t, stalled(t, stuckPlain))
	require.True(t, stalled(t, freshArchive), "once a moves again, the frozen b goes out whole")
	require.True(t, stalled(t, freshPlain))
}

// When a stalled url comes back, the provider's gauge returns to 1 only if every one of its urls is
// usable: here a sibling url is still disabled for node errors, so the gauge stays at 0.
func TestHeadStall_GaugeStaysDownWhileASiblingIsOut(t *testing.T) {
	h := newStallHarness()
	stuck := ep("https://vendor/rpc", "vendor", true)
	sibling := ep("https://vendor/rpc2", "vendor", true)
	sibling.Endpoint.Enabled = false // sibling out for node-error
	other := ep("https://other", "other", true)
	h.obs["https://vendor/rpc"] = stuckAt(1000, stallN)
	h.obs["https://other"] = stuckAt(9000, 0)
	h.step(t, stallNow, stuck, sibling, other)
	require.Equal(t, false, h.health["vendor"])

	h.obs["https://vendor/rpc"] = stuckAt(1001, 0)
	h.step(t, stallNow.Add(5*time.Second), stuck, sibling, other)
	require.False(t, stalled(t, stuck))
	_, set := h.health["vendor"]
	require.True(t, set)
	require.Equal(t, false, h.health["vendor"], "the sibling still has a reason, so the gauge stays 0")
}

// A head-stalled endpoint's REPEAT vote no longer refreshes the chain tip, while the same repeat
// from an endpoint that is not stalled still does, and a higher block from the stalled one still
// counts.
func TestOnTipObservation_DropsRepeatFromStalledEndpoint(t *testing.T) {
	clk := &manualClock{t: time.Unix(1_758_784_000, 0)}
	newServer := func() *RPCSmartRouterServer {
		return &RPCSmartRouterServer{chainState: chainstate.NewWithClock("ETH1", chainstate.Config{
			BucketWidth: 5, OutlierThreshold: 512, StalenessWindow: 10 * time.Second, TTL: 10 * time.Second,
		}, clk.now)}
	}
	stalledSrv, healthySrv := newServer(), newServer()
	stalledSrv.headStall.sync(map[string]struct{}{"https://node": {}}, nil)
	for _, s := range []*RPCSmartRouterServer{stalledSrv, healthySrv} {
		s.onTipObservation("https://node", 1000, false)
	}
	for i := 0; i < 15; i++ { // 15 s of repeats: past the 10 s TTL
		clk.t = clk.t.Add(time.Second)
		stalledSrv.onTipObservation("https://node", 1000, true)
		healthySrv.onTipObservation("https://node", 1000, true)
	}
	_, fresh := healthySrv.chainState.GetLatestBlock()
	require.True(t, fresh, "an ordinary repeat keeps the tip fresh")
	_, fresh = stalledSrv.chainState.GetLatestBlock()
	require.False(t, fresh, "a stalled endpoint's repeat no longer does")

	stalledSrv.onTipObservation("https://node", 1001, false)
	tip, fresh := stalledSrv.chainState.GetLatestBlock()
	require.True(t, fresh)
	require.Equal(t, int64(1001), tip, "a higher block from the stalled endpoint still counts")
}

// A provider listing an https url and a wss url (the shape of smartrouter_eth.yml): the wss url
// can never answer a relay, so it must not count as "usable" for never-empty. With no other
// relay-capable url the stuck https url is KEPT (stale answers beat no answers), not taken out
// behind a websocket url that cannot serve.
func TestHeadStall_WebsocketURLDoesNotSatisfyNeverEmpty(t *testing.T) {
	ctx := context.Background()
	conn := func(url string) lavasession.DirectRPCConnection {
		c, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "")
		require.NoError(t, err)
		return c
	}
	https := &lavasession.EndpointWithDirectConnection{
		Endpoint:        &lavasession.Endpoint{NetworkAddress: "https://node", Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn("https://node")}},
		ProviderAddress: "vendor",
	}
	wss := &lavasession.EndpointWithDirectConnection{
		Endpoint:        &lavasession.Endpoint{NetworkAddress: "wss://node", Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn("wss://node")}},
		ProviderAddress: "vendor",
	}
	require.False(t, wss.Endpoint.ServesDirectRelays())

	h := newStallHarness()
	h.obs["https://node"] = stuckAt(1000, stallN)
	h.obs["wss://node"] = stuckAt(1005, 0) // a moving wss url still shows the chain moved
	require.Equal(t, headStallResult{kept: 1}, h.step(t, stallNow, https, wss))
	require.True(t, https.Endpoint.IsUsable(), "the only relay-capable url stays in")
	require.False(t, stalled(t, wss), "a wss url takes no part in the head-stall step")
}

// Stalled urls do not vote in the consensus baseline: with two frozen urls out of three, the frozen
// pair would otherwise be the majority and pin the tip.
func TestConsensusObservations_ExcludesStalledURLs(t *testing.T) {
	at := time.Unix(1_758_784_000, 0)
	snap := map[string]endpointstate.EndpointObservation{
		"https://frozen-a": {LatestBlock: 1000, ObservedAt: at},
		"https://frozen-b": {LatestBlock: 1000, ObservedAt: at},
		"https://healthy":  {LatestBlock: 1660, ObservedAt: at},
		"https://unknown":  {LatestBlock: 0, ObservedAt: at},
	}
	stalled := map[string]bool{"https://frozen-a": true, "https://frozen-b": true}
	got := consensusObservations(snap, func(u string) bool { return stalled[u] })
	require.Len(t, got, 1)
	require.Equal(t, "https://healthy", got[0].URL)
	require.Len(t, consensusObservations(snap, nil), 3, "with nothing stalled every known block votes")
}

// The Plasma lock, replayed against a real ChainState (dfns PLASMAT, 2026-09-25). Tatum froze at N
// and kept repeating it; Chainstack climbed but its blocks were more than OutlierThreshold above the
// fresh tip, so they were rejected as lies while Tatum's repeats kept the tip fresh — for 70 minutes.
// Once Tatum is head-stalled its repeats stop voting, the tip goes stale after TTL, and the stale tip
// adopts Chainstack's real block.
func TestOnTipObservation_StalledRepeatsNoLongerPinTheTip(t *testing.T) {
	const n = int64(34489455)
	clk := &manualClock{t: time.Unix(1_758_784_000, 0)}
	cs := chainstate.NewWithClock("PLASMAT", chainstate.Config{
		BucketWidth:      5,
		OutlierThreshold: 512,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	}, clk.now)
	rpcss := &RPCSmartRouterServer{chainState: cs}
	const tatum, chainstack = "https://tatum", "https://chainstack"

	rpcss.onTipObservation(tatum, n, false)
	chainstackHead := n + 718
	tick := func() {
		clk.t = clk.t.Add(time.Second)
		chainstackHead++
		rpcss.onTipObservation(tatum, n, true) // Tatum repeats N
		rpcss.onTipObservation(chainstack, chainstackHead, false)
	}

	// Before: the lock. 30 s of ticks and the tip never leaves N.
	for i := 0; i < 30; i++ {
		tick()
	}
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, n, tip, "the lock: Chainstack's real head is rejected, Tatum keeps N fresh")

	// After: Tatum is head-stalled. Within one TTL the tip goes stale and adopts Chainstack's head.
	rpcss.headStall.sync(map[string]struct{}{tatum: {}}, nil)
	for i := 0; i < 12; i++ {
		tick()
	}
	tip, ok = cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, chainstackHead, tip, "with Tatum's repeat votes dropped the tip follows Chainstack")
}

// The operator reset is the one route that clears state reasons (D8), and it zeroes the stall
// counters with them so the next probe cycle does not re-add head-stalled from the evidence the
// operator just discarded (H11).
func TestResetEndpointHealthAndGauge_ClearsHeadStalled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A node frozen at block 1000, so real polls build real stall evidence.
	frozen := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		result := `"0x3e8"`
		if req.Method != "eth_blockNumber" {
			result = `{"number":"0x3e8","hash":"0x00000000000000000000000000000000000000000000000000000000000003e8"}`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, result)
	}))
	defer frozen.Close()

	f := newReconcileFixture(t, ctx)
	f.admit(t, ctx, frozen.URL)
	go f.server.initializeChainTrackers(ctx)
	require.Eventually(t, func() bool { return f.hasTracker(frozen.URL) }, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		_, _, _ = f.monitor.PollNow(ctx, frozen.URL)
		obs, _ := f.monitor.GetObservation(frozen.URL)
		return obs.SameBlockCycles >= 2
	}, 10*time.Second, 50*time.Millisecond, "polls of the frozen node must build stall evidence")

	eps := f.csm.GetAllDirectRPCEndpoints()
	require.Len(t, eps, 1)
	stuck := eps[0].Endpoint
	require.True(t, stuck.AddStateReason(lavasession.EndpointDisableHeadStalled, stallNow, 1000))
	f.server.headStall.sync(map[string]struct{}{stuck.NetworkAddress: {}}, nil)

	router := &RPCSmartRouter{
		sessionManagers: map[string]*lavasession.ConsumerSessionManager{"k": f.csm},
		rpcServers:      map[string]*RPCSmartRouterServer{"k": f.server},
	}
	resetEndpointHealthAndGauge(debugMuxDeps{router: router})

	require.True(t, stuck.IsUsable())
	require.False(t, f.server.headStall.isStalled(stuck.NetworkAddress))
	// The evidence goes with the reason (H11), or the next probe cycle re-adds head-stalled from
	// the very cycles the operator just discarded.
	obs, _ := f.monitor.GetObservation(frozen.URL)
	require.Zero(t, obs.SameBlockCycles, "the operator reset must zero the stall counter")
	require.Equal(t, int64(1000), obs.HighestBlockSeen, "the blocks seen are kept")
}

// The probe step is the only writer of the head-stall gauge: an outside write back to 1 (the epoch
// reset, or a relay success on a sibling url) is corrected on the next cycle while a url is stalled.
func TestHeadStall_GaugeReassertedEveryCycle(t *testing.T) {
	h := newStallHarness()
	stuck := ep("https://primary", "p", true)
	other := backupEp("https://backup", "b")
	h.obs["https://primary"] = stuckAt(1000, stallN)
	h.obs["https://backup"] = stuckAt(9000, 0)
	h.step(t, stallNow, stuck, other)
	require.Equal(t, false, h.health["p"])

	h.health["p"] = true // the epoch reset writes 1
	h.step(t, stallNow.Add(5*time.Second), stuck, other)
	require.Equal(t, false, h.health["p"], "the next cycle puts it back to 0")
	_, touched := h.health["b"]
	require.False(t, touched, "a provider with nothing stalled is never written by this step")
}

// A pairing rebuild replaces Endpoint objects; the new ones hold no state reason. The stalled url
// set must follow the endpoints, or the rebuilt url's repeat tip votes would be refused for ever.
func TestHeadStall_StalledSetFollowsRebuiltEndpoints(t *testing.T) {
	h := newStallHarness()
	old := ep("https://primary", "p", true)
	other := ep("https://other", "o", true)
	h.obs["https://primary"] = stuckAt(1000, stallN)
	h.obs["https://other"] = stuckAt(9000, 0)
	h.step(t, stallNow, old, other)
	require.True(t, h.state.isStalled("https://primary"))

	rebuilt := ep("https://primary", "p", true) // fresh object, no reason
	h.obs["https://primary"] = stuckAt(1000, 0) // and its counter happens to be low
	h.step(t, stallNow.Add(5*time.Second), rebuilt, other)
	require.False(t, h.state.isStalled("https://primary"), "the set follows the live endpoints")
}
