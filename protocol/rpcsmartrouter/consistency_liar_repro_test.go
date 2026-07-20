package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// This file is the Go-level regression gate for Topic C F14 — the CONFIRMED production bug where
// one provider reporting a fake-high block captured all traffic (multi-provider pods) or drove a
// pod to "No pairings" until manual reset (single-provider pods).
//
// It is the in-repo stand-in for the two Python simulator tests named in the action plan
// (test_check_timestamp_and_liar_score_isolation.py, test_lying_provider_tip_selfheal.py), which
// live outside this repository. Those remain the end-to-end gate; these lock the decision point
// where the bug actually bit — filterEndpointsByConsistency — so the regression cannot return
// without a Go test going red.
//
// The mechanism being locked, in one line: the consistency reference must be the ANTI-LIE-GUARDED
// chain tip, never a value a single provider's reply can set unilaterally.
//
// The write-side half of the bug (F15 — a served reply latching into a per-user seenBlock) is no
// longer expressible at all: the per-user store was deleted with the read path, so there is no
// per-request state left for a reply to poison. These tests therefore cover the read side, which
// is where a future regression could still land.

const (
	liarChainID      = "ETH1"
	liarAPIInterface = "jsonrpc"
	honestBlock      = int64(1000)
	// The liar's claim, ~20M blocks above the real head — the shape of the production incident
	// (armed at Provider-Latest-Block: 20100000 against a real head three orders of magnitude lower).
	liarClaim = int64(20100000)
)

func liarTestServer(t *testing.T, cs *chainstate.ChainState) *RPCSmartRouterServer {
	t.Helper()
	return &RPCSmartRouterServer{
		listenEndpoint:    &lavasession.RPCEndpoint{ChainID: liarChainID, ApiInterface: liarAPIInterface},
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		chainState:        cs,
	}
}

func liarSession(t *testing.T, addr string, tip int64) *lavasession.SessionInfo {
	t.Helper()
	seedEndpointTip(liarChainID, liarAPIInterface, addr, tip)
	return &lavasession.SessionInfo{Session: &lavasession.SingleConsumerSession{
		Connection: &lavasession.DirectRPCSessionConnection{
			Endpoint: &lavasession.Endpoint{NetworkAddress: addr},
		},
	}}
}

// liarUserData is the user the poisoned per-user seenBlock was keyed on, shared by the protocol
// message and the legacy-store writes that emulate the retired write path.
var liarUserData = common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"}

func liarProtocolMessage() *MockProtocolMessage {
	return &MockProtocolMessage{
		api:            &spectypes.Api{Name: "eth_getBalance"},
		requestedBlock: spectypes.LATEST_BLOCK,
		userData:       liarUserData,
	}
}

// TestLiar_ChainStateRejectsTheLie is the precondition everything else rests on: the tip layer
// itself refuses an implausible jump, so the liar's block never becomes the reference. This is
// what was ALREADY true in production and why the bug was so confusing — the tip was protected
// while the per-user seenBlock, which consistency actually read, was not.
func TestLiar_ChainStateRejectsTheLie(t *testing.T) {
	cs := chainstate.New(liarChainID, chainstate.DefaultConfig(12*time.Second))

	// Establish an honest tip, then let two honest endpoints form a consensus baseline so the
	// anti-lie guard has a reference to reject against.
	cs.SetLatestBlock(honestBlock)
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "honest-a", Block: honestBlock, ObservedAt: now},
		{URL: "honest-b", Block: honestBlock, ObservedAt: now},
	})

	cs.SetLatestBlock(liarClaim)

	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, honestBlock, tip,
		"the anti-lie guard must reject a block ~20M above consensus — this is the property consistency now depends on")
}

// TestLiar_MultiProviderHonestEndpointsKeepServing is the 3-provider takeover case
// (test_rejected_liar_data_never_moves_other_providers_sync_scores). Under the old per-user
// seenBlock the liar's 20100000 latched into the reference, both honest endpoints measured as
// ~20M behind, and the filter dropped them — handing the liar 100% of traffic with HTTP 200 and
// zero retries, which is what made it silent.
func TestLiar_MultiProviderHonestEndpointsKeepServing(t *testing.T) {
	ctx := context.Background()
	endpointtip.Default().Reset()

	cs := chainstate.New(liarChainID, chainstate.DefaultConfig(12*time.Second))
	cs.SetLatestBlock(honestBlock)
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "http://honest-a:8545", Block: honestBlock, ObservedAt: now},
		{URL: "http://honest-b:8545", Block: honestBlock, ObservedAt: now},
	})
	// The liar reports its fake block; the tip rejects it (locked above).
	cs.SetLatestBlock(liarClaim)

	rpcss := liarTestServer(t, cs)
	sessions := lavasession.ConsumerSessionsMap{
		"http://honest-a:8545": liarSession(t, "http://honest-a:8545", honestBlock),
		"http://honest-b:8545": liarSession(t, "http://honest-b:8545", honestBlock),
		"http://liar:8545":     liarSession(t, "http://liar:8545", liarClaim),
	}

	valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, liarProtocolMessage())
	require.NoError(t, err)

	_, honestAKept := valid["http://honest-a:8545"]
	_, honestBKept := valid["http://honest-b:8545"]
	require.True(t, honestAKept, "F14: an honest endpoint at the real head must not be filtered out by a liar's claim")
	require.True(t, honestBKept, "F14: an honest endpoint at the real head must not be filtered out by a liar's claim")
	require.Len(t, failed, 0, "no honest endpoint may be rejected — the takeover was built entirely on these rejections")

	// The liar is NOT evicted here: it reads as "ahead", which consistency does not police. That
	// is the documented residual (F19, deliberately out of scope) — C-G stops the takeover, it
	// does not de-rank the liar, which keeps its ordinary selection share.
	_, liarKept := valid["http://liar:8545"]
	require.True(t, liarKept, "F19 (known limitation): consistency only catches too-LOW endpoints, so the liar still participates")
}

// TestLiar_SingleProviderNoPairingsOutage is the 1-provider case
// (test_lying_provider_tip_selfheal.py): the sole endpoint must never reject ITSELF. Under the old
// mechanism a single processed-commitment reply latched seenBlock above the endpoint's own tracked
// tip; the endpoint then measured as behind its own past, failed the filter, and the all-failed
// path raised ConsistencyError — the "No pairings available" the customer saw, persisting until a
// manual /debug/reset-all because seenBlock was monotonic-max and traffic kept its TTL alive.
//
// C-G kills this by construction: on a 1-endpoint pod the tip is fed by that endpoint's own
// observations, so tip == endpointLatest and the lag is 0 no matter what any reply claimed.
func TestLiar_SingleProviderNoPairingsOutage(t *testing.T) {
	ctx := context.Background()
	endpointtip.Default().Reset()

	cs := chainstate.New(liarChainID, chainstate.DefaultConfig(12*time.Second))
	cs.SetLatestBlock(honestBlock)

	rpcss := liarTestServer(t, cs)
	sessions := lavasession.ConsumerSessionsMap{
		"http://solo:8545": liarSession(t, "http://solo:8545", honestBlock),
	}

	valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, liarProtocolMessage())
	require.NoError(t, err, "the sole endpoint must never self-reject into ConsistencyError (the No-pairings precursor)")
	require.Len(t, valid, 1, "a 1-endpoint pod always has its endpoint available")
	require.Len(t, failed, 0)
}

// TestLiar_SingleProviderSelfHealsWithoutManualReset is the persistence half of the bug: recovery
// must be automatic. A poisoned tip self-heals — the outlier guard bounds it, Recompute snaps it
// back to consensus, and TTL expiry allows downward re-adoption — whereas the retired per-user
// seenBlock had no downward path at all short of an operator hitting /debug/reset-all.
//
// Here the tip is poisoned at cold start (the one moment SetLatestBlock's guard cannot fire, since
// no reference exists yet), then healed by consensus, and the pod must serve again on its own.
func TestLiar_SingleProviderSelfHealsWithoutManualReset(t *testing.T) {
	ctx := context.Background()
	endpointtip.Default().Reset()

	cs := chainstate.New(liarChainID, chainstate.DefaultConfig(12*time.Second))

	// Cold-start poisoning: the very first observation is the lie, so it is accepted.
	cs.SetLatestBlock(liarClaim)
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, liarClaim, tip, "a cold-start lie is accepted — there is no reference to reject it against yet")

	rpcss := liarTestServer(t, cs)
	sessions := lavasession.ConsumerSessionsMap{
		"http://solo:8545": liarSession(t, "http://solo:8545", honestBlock),
	}

	// While poisoned, the honest endpoint really does measure ~20M behind and is filtered. This is
	// the outage state — the point of the fix is that it must not be TERMINAL.
	_, _, err := rpcss.filterEndpointsByConsistency(ctx, sessions, liarProtocolMessage())
	require.Error(t, err, "while the tip is poisoned the pod is degraded — expected, and bounded")

	// Consensus arrives and snaps the tip back down. No operator action, no /debug/reset-all.
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "http://solo:8545", Block: honestBlock, ObservedAt: now},
		{URL: "http://peer:8545", Block: honestBlock, ObservedAt: now},
	})
	tip, ok = cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, honestBlock, tip, "Recompute's edge correction snaps a poisoned tip back to consensus")

	valid, failed, err := rpcss.filterEndpointsByConsistency(ctx, sessions, liarProtocolMessage())
	require.NoError(t, err, "the pod must serve again once its tip self-heals — no manual reset")
	require.Len(t, valid, 1)
	require.Len(t, failed, 0)
}
