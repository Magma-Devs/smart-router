package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

func newHarvestMonitor(t *testing.T) *endpointstate.EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

func ethTipServer(t *testing.T, chainID string) *RPCSmartRouterServer {
	t.Helper()
	return &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: chainID, ApiInterface: "jsonrpc"}}
}

// ---------------------------------------------------------------------------
// extractSolanaContextSlot (MAG-2159 finding 2): chain-aware, strict slot parsing.
// ---------------------------------------------------------------------------

func TestExtractSolanaContextSlot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		wantSlot int64
		wantOK   bool
	}{
		{"getBalance context.slot", `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":12345},"value":42}}`, 12345, true},
		{"getAccountInfo context.slot", `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":999},"value":{"lamports":1}}}`, 999, true},
		{"getLatestBlockhash context.slot", `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":777},"value":{"blockhash":"h"}}}`, 777, true},
		{"leading whitespace", "  \n{\"result\":{\"context\":{\"slot\":5}}}", 5, true},
		{"error response ignored", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"x"},"result":{"context":{"slot":12345}}}`, 0, false},
		{"explicit null error is fine", `{"jsonrpc":"2.0","id":1,"error":null,"result":{"context":{"slot":8}}}`, 8, true},
		{"null result", `{"jsonrpc":"2.0","id":1,"result":null}`, 0, false},
		{"no context (bare getSlot-style result)", `{"jsonrpc":"2.0","id":1,"result":12345}`, 0, false},
		{"coincidental top-level slot not under context", `{"jsonrpc":"2.0","id":1,"slot":12345,"result":{"value":1}}`, 0, false},
		{"batch array not harvested", `[{"result":{"context":{"slot":1}}},{"result":{"context":{"slot":2}}}]`, 0, false},
		{"malformed json", `{not json`, 0, false},
		{"empty", ``, 0, false},
		{"non-positive slot", `{"result":{"context":{"slot":0}}}`, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slot, ok := extractSolanaContextSlot([]byte(tc.body))
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantSlot, slot)
		})
	}
}

// ---------------------------------------------------------------------------
// tipBlockFromRelay (MAG-2159 findings 1 & 2): tip-observation eligibility.
// ---------------------------------------------------------------------------

// EVM: only a latest-requesting method yields a tip; historical responses (which still
// carry a block in Reply.LatestBlock) must NOT be harvested.
func TestTipBlockFromRelay_EVM_OnlyLatestRequestIsTip(t *testing.T) {
	rpcss := ethTipServer(t, "ETH1")

	// Valid tip: the request asked for the latest block.
	cm := &mockChainMessage{requestedBlock: spectypes.LATEST_BLOCK}
	block, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 20_000_000})
	require.True(t, ok, "a latest-block request yields a current-tip observation")
	require.Equal(t, int64(20_000_000), block)

	// Historical responses: a block is present but it is NOT the tip. Each is represented
	// by its RequestedBlock (never LATEST_BLOCK).
	for _, tc := range []struct {
		name           string
		requestedBlock int64
		replyBlock     int64
	}{
		{"eth_getBlockByNumber(N)", 17_500_000, 17_500_000},
		{"eth_getBlockByHash", spectypes.NOT_APPLICABLE, 17_500_000},
		{"eth_getTransactionReceipt", spectypes.NOT_APPLICABLE, 16_000_000},
		{"eth_getLogs (historical)", 15_000_000, 15_000_500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm := &mockChainMessage{requestedBlock: tc.requestedBlock}
			_, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: tc.replyBlock})
			require.False(t, ok, "a historical response must not produce a tip observation")
		})
	}
}

func TestTipBlockFromRelay_EVM_EdgeCases(t *testing.T) {
	rpcss := ethTipServer(t, "ETH1")

	// nil reply.
	_, ok := rpcss.tipBlockFromRelay(&mockChainMessage{requestedBlock: spectypes.LATEST_BLOCK}, nil)
	require.False(t, ok)

	// latest request but no block parsed.
	_, ok = rpcss.tipBlockFromRelay(&mockChainMessage{requestedBlock: spectypes.LATEST_BLOCK}, &pairingtypes.RelayReply{LatestBlock: 0})
	require.False(t, ok)
}

// Solana: the tip comes from result.context.slot regardless of RequestedBlock; the EVM
// Reply.LatestBlock path is not used.
func TestTipBlockFromRelay_Solana_UsesContextSlot(t *testing.T) {
	rpcss := ethTipServer(t, "SOLANA")

	// getBalance carries result.context.slot but is not a "latest block" request.
	cm := &mockChainMessage{requestedBlock: spectypes.NOT_APPLICABLE}
	reply := &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":250000000},"value":42}}`)}
	block, ok := rpcss.tipBlockFromRelay(cm, reply)
	require.True(t, ok, "Solana context.slot is a current-tip observation")
	require.Equal(t, int64(250000000), block)

	// A Solana error response yields no tip.
	errReply := &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"x"}}`)}
	_, ok = rpcss.tipBlockFromRelay(cm, errReply)
	require.False(t, ok)
}

// A non-Solana chain must NOT interpret a coincidental "slot" field, and a non-latest
// request is never a tip even if the body happens to contain slot-like data.
func TestTipBlockFromRelay_NonSolana_IgnoresCoincidentalSlot(t *testing.T) {
	rpcss := ethTipServer(t, "ETH1")
	cm := &mockChainMessage{requestedBlock: spectypes.NOT_APPLICABLE}
	reply := &pairingtypes.RelayReply{
		Data:        []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":250000000}}}`),
		LatestBlock: 0,
	}
	_, ok := rpcss.tipBlockFromRelay(cm, reply)
	require.False(t, ok, "non-Solana chains must not interpret context.slot as a tip")
}

// ---------------------------------------------------------------------------
// recordRelayBlockObservation guards + generation-safe pass-through.
// ---------------------------------------------------------------------------

func TestRecordRelayBlockObservation_NoOps(t *testing.T) {
	// No monitor wired: must not panic.
	(&RPCSmartRouterServer{}).recordRelayBlockObservation(&lavasession.Endpoint{NetworkAddress: "http://ep:8545"}, 1, 100)

	m := newHarvestMonitor(t)
	rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: m}
	ep := &lavasession.Endpoint{NetworkAddress: "http://ep:8545"}

	rpcss.recordRelayBlockObservation(ep, 1, 0) // non-positive block records nothing
	_, ok := m.GetObservation(ep.NetworkAddress)
	require.False(t, ok)

	rpcss.recordRelayBlockObservation(nil, 1, 100) // nil endpoint must not panic
}

// Integration: with a live generation (from a real tracker registration), the harvest
// pass-through records into the store with Source=Relay; a stale generation is rejected.
func TestRecordRelayBlockObservation_GenerationPassThrough(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parser := newRealChainParserForHarvest(t, "ETH1")
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	url := "http://ep:8545"
	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	// GetOrCreateTracker registers the generation synchronously (a nil connection just
	// makes the background poll fail gracefully — we only need the generation here).
	_, err := m.GetOrCreateTracker(ep, nil)
	require.NoError(t, err)

	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)

	rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: m}

	// Valid generation → recorded.
	rpcss.recordRelayBlockObservation(ep, gen, 12345)
	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(12345), o.LatestBlock)
	require.Equal(t, endpointstate.ObservationSourceRelay, o.Source)
	require.True(t, o.LastPollAttempt.IsZero(), "relay harvest must not touch poll-health")

	// Stale generation → rejected (does not move the record).
	rpcss.recordRelayBlockObservation(ep, gen+1, 99999)
	o, _ = m.GetObservation(url)
	require.Equal(t, int64(12345), o.LatestBlock, "a stale-generation relay must not overwrite the record")
}

// ---------------------------------------------------------------------------
// REAL-PARSER end-to-end (MAG-2159 finding 1): the tip gate hinges on the claim
// that a latest-requesting EVM method parses to RequestedBlock() == LATEST_BLOCK,
// while a concrete-block method does not. The other tests fabricate RequestedBlock
// via mockChainMessage; this one proves the assumption against the real ChainParser
// so the gate cannot silently reject the most important EVM tip relay.
// ---------------------------------------------------------------------------

func TestTipBlockFromRelay_RealParser_EVMEligibility(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}

	// Lightweight real parser (no CreateChainLibMocks): its SetGlobalLoggingLevel write races
	// with concurrent background-poll logging from other tests' trackers under -race.
	chainParser := newRealChainParserForHarvest(t, "ETH1")

	rpcss := ethTipServer(t, "ETH1")
	parse := func(body string) chainlib.ChainMessage {
		cm, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		return cm
	}

	t.Run("eth_blockNumber is a tip (RequestedBlock==LATEST_BLOCK)", func(t *testing.T) {
		cm := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
		requested, _ := cm.RequestedBlock()
		require.Equal(t, spectypes.LATEST_BLOCK, requested,
			"eth_blockNumber must parse to LATEST_BLOCK or the harvest gate drops the canonical tip relay")
		block, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 20_000_000})
		require.True(t, ok)
		require.Equal(t, int64(20_000_000), block)
	})

	t.Run("eth_getBlockByNumber(latest) is a tip", func(t *testing.T) {
		cm := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`)
		requested, _ := cm.RequestedBlock()
		require.Equal(t, spectypes.LATEST_BLOCK, requested)
		_, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 20_000_000})
		require.True(t, ok)
	})

	t.Run("eth_getBlockByNumber(0x10a7eb0) is historical, not a tip", func(t *testing.T) {
		cm := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x10a7eb0",false]}`)
		requested, _ := cm.RequestedBlock()
		require.NotEqual(t, spectypes.LATEST_BLOCK, requested,
			"a concrete block number must not parse to LATEST_BLOCK")
		_, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 17_460_400})
		require.False(t, ok, "a historical eth_getBlockByNumber must not be harvested as a tip")
	})
}

// ---------------------------------------------------------------------------
// F4: tip-state writes must be gated on tip-eligibility. harvestAndUpdateTipFromRelay is
// the production code the relay response path runs; this drives it with REAL parsed
// chainMessages (a historical eth_getBlockByNumber(N) vs a latest eth_blockNumber) and a
// real endpoint + estimator, and asserts that a historical response — which carries a
// positive Reply.LatestBlock — moves NONE of the tip state, while a tip-eligible one moves
// all of it. This is the integration the reviewer asked for: not the tipBlockFromRelay
// helper in isolation, but the actual side-effecting writes it gates.
// ---------------------------------------------------------------------------

func TestHarvestAndUpdateTipFromRelay_HistoricalDoesNotPoisonTip(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chainParser := newRealChainParserForHarvest(t, "ETH1")
	parse := func(body string) chainlib.ChainMessage {
		cm, perr := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, perr)
		return cm
	}

	// A monitor so the observation-store write (Source=Relay) is exercised too.
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      newRealChainParserForHarvest(t, "ETH1"),
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	const url = "http://ep:8545"
	const addr = "lava@provider1"
	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	_, err := m.GetOrCreateTracker(ep, nil)
	require.NoError(t, err)
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
	}

	// A historical response: eth_getBlockByNumber(0x10a7eb0) carries Reply.LatestBlock = that
	// concrete block, but it is NOT the tip.
	histMsg := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x10a7eb0",false]}`)
	rpcss.harvestAndUpdateTipFromRelay(ep, histMsg, &pairingtypes.RelayReply{LatestBlock: 17_460_400}, gen, addr)

	require.Equal(t, int64(0), ep.LatestBlock.Load(), "a historical response must NOT move the endpoint tip")
	require.True(t, ep.LastBlockUpdate.IsZero(), "a historical response must NOT stamp LastBlockUpdate")
	require.Equal(t, uint64(0), rpcss.latestBlockHeight.Load(), "a historical response must NOT move the bootstrap atomic")
	// The store may hold a failed-poll record from the nil-connection background poll; what must
	// NOT exist is a Relay-sourced write of the historical block.
	if o, exists := m.GetObservation(url); exists {
		require.NotEqual(t, endpointstate.ObservationSourceRelay, o.Source, "a historical response must NOT write a Relay observation")
		require.NotEqual(t, int64(17_460_400), o.LatestBlock, "the historical block must NOT reach the observation store")
	}

	// A tip-eligible response: eth_blockNumber → the current tip.
	tipMsg := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	rpcss.harvestAndUpdateTipFromRelay(ep, tipMsg, &pairingtypes.RelayReply{LatestBlock: 20_000_000}, gen, addr)

	require.Equal(t, int64(20_000_000), ep.LatestBlock.Load(), "a tip-eligible response moves the endpoint tip")
	require.False(t, ep.LastBlockUpdate.IsZero(), "a tip-eligible response stamps LastBlockUpdate")
	require.Equal(t, uint64(20_000_000), rpcss.latestBlockHeight.Load(), "a tip-eligible response moves the bootstrap atomic")
	o, obsExists := m.GetObservation(url)
	require.True(t, obsExists)
	require.Equal(t, int64(20_000_000), o.LatestBlock)
	require.Equal(t, endpointstate.ObservationSourceRelay, o.Source)
}

// ---------------------------------------------------------------------------
// F3: capturing the generation BEFORE dispatch makes the harvest safe against a same-URL
// tracker replacement mid-relay. This models the race with REAL monitor incarnations (real
// RemoveTracker + GetOrCreateTracker, real generations) — not a fabricated generation value:
// the generation captured for incarnation A is rejected once the URL is replaced by
// incarnation B, while B's live generation is accepted.
//
// The production call site (sendRelayToDirectEndpoints) captures the generation at
// rpcsmartrouter_server.go right before relayInnerDirect and threads that captured value to
// harvestAndUpdateTipFromRelay — so a relay that began against incarnation A carries A's
// generation when it completes, which is exactly genA here. A full end-to-end test that drives
// the live goroutine while blocking the relay is not included: there is no successful-relay
// harness for sendRelayToDirectEndpoints (the existing driver exercises the consistency-filter
// failure path), and the capture-before-dispatch ordering is a straight-line property of the
// goroutine verified by reading the call site.
// ---------------------------------------------------------------------------

func TestHarvest_GenerationCapturedBeforeDispatch_RejectsAfterReplacement(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      newRealChainParserForHarvest(t, "ETH1"),
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	const url = "http://ep:8545"
	const addr = "lava@provider1"
	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
	}

	// Incarnation A: tracker created, generation captured "before dispatch" of relay A.
	_, err := m.GetOrCreateTracker(ep, nil)
	require.NoError(t, err)
	genA := rpcss.endpointObservationGeneration(url)
	require.NotZero(t, genA)

	// While relay A is "in flight", the endpoint's tracker is removed and recreated for the
	// same URL (incarnation B) — a new generation.
	m.RemoveTracker(url)
	_, err = m.GetOrCreateTracker(ep, nil)
	require.NoError(t, err)
	genB := rpcss.endpointObservationGeneration(url)
	require.NotEqual(t, genA, genB, "a recreated same-URL tracker must get a new generation")

	// Relay A now completes and harvests with the generation it captured BEFORE dispatch (genA).
	// The gate must reject it — it belongs to the dead incarnation, not B.
	tipMsg, perr := newRealChainParserForHarvest(t, "ETH1").ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, perr)
	rpcss.harvestAndUpdateTipFromRelay(ep, tipMsg, &pairingtypes.RelayReply{LatestBlock: 20_000_000}, genA, addr)

	// The store may carry a failed-poll record from incarnation B's nil-connection poll; what
	// must NOT exist is a Relay write of 20M attributed to B from relay A's stale generation.
	if o, exists := m.GetObservation(url); exists {
		require.NotEqual(t, int64(20_000_000), o.LatestBlock,
			"a relay that captured the OLD generation must be rejected after same-URL replacement")
	}

	// Sanity: harvesting with the live generation (genB) IS accepted — proving the rejection
	// above was the generation gate, not a broken store.
	rpcss.harvestAndUpdateTipFromRelay(ep, tipMsg, &pairingtypes.RelayReply{LatestBlock: 20_000_000}, genB, addr)
	o, obsExists := m.GetObservation(url)
	require.True(t, obsExists)
	require.Equal(t, int64(20_000_000), o.LatestBlock)
	require.Equal(t, endpointstate.ObservationSourceRelay, o.Source)
}

// newRealChainParserForHarvest builds a real ChainParser from the on-disk spec (the
// lightweight subset of CreateChainLibMocks, no server/connector), so GetOrCreateTracker
// can register a generation without a nil-parser panic in the background poll.
func newRealChainParserForHarvest(t *testing.T, specIndex string) chainlib.ChainParser {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, specIndex)
	require.NoError(t, err)
	cp, err := chainlib.NewChainParser("jsonrpc")
	require.NoError(t, err)
	cp.SetSpec(spec)
	return cp
}
