package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
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
	// Attach a REAL parser: the tip gate resolves GET_BLOCKNUM / GET_BLOCK_BY_NUM via
	// chainParser.GetParsingByTag, so a nil parser makes isMethodTagged return false for
	// everything (every tip would be dropped). The Solana path short-circuits before touching
	// the parser, but a real SOLANA parser still constructs cleanly so we keep this uniform.
	return &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: chainID, ApiInterface: "jsonrpc"},
		chainParser:    newRealChainParserForHarvest(t, chainID),
	}
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

	// Valid tip #1: eth_blockNumber — the GET_BLOCKNUM method whose result IS the node's tip.
	// (Default api name on mockChainMessage is "eth_blockNumber"; spell it out for clarity.)
	cm := &mockChainMessage{api: &spectypes.Api{Name: "eth_blockNumber"}, requestedBlock: spectypes.LATEST_BLOCK}
	block, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 20_000_000})
	require.True(t, ok, "eth_blockNumber (GET_BLOCKNUM) yields a current-tip observation")
	require.Equal(t, int64(20_000_000), block)

	// Valid tip #2: eth_getBlockByNumber(latest) — the GET_BLOCK_BY_NUM method, requesting LATEST.
	// This is the only mock case that exercises the GET_BLOCK_BY_NUM+LATEST branch of the gate.
	cm = &mockChainMessage{api: &spectypes.Api{Name: "eth_getBlockByNumber"}, requestedBlock: spectypes.LATEST_BLOCK}
	block, ok = rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 20_000_001})
	require.True(t, ok, "eth_getBlockByNumber(latest) yields a current-tip observation")
	require.Equal(t, int64(20_000_001), block)

	// Historical responses: a block is present in Reply.LatestBlock but it is NOT the tip.
	// Each carries an explicit api name so the gate's method discriminator is exercised — the
	// NOT_APPLICABLE cases REQUIRE this, since the default "eth_blockNumber" name would otherwise
	// match GET_BLOCKNUM and wrongly report a tip regardless of RequestedBlock.
	for _, tc := range []struct {
		name           string
		apiName        string
		requestedBlock int64
		replyBlock     int64
	}{
		{"eth_getBlockByNumber(N)", "eth_getBlockByNumber", 17_500_000, 17_500_000},
		{"eth_getBlockByHash", "eth_getBlockByHash", spectypes.NOT_APPLICABLE, 17_500_000},
		{"eth_getTransactionReceipt", "eth_getTransactionReceipt", spectypes.NOT_APPLICABLE, 16_000_000},
		{"eth_getLogs (historical)", "eth_getLogs", 15_000_000, 15_000_500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm := &mockChainMessage{api: &spectypes.Api{Name: tc.apiName}, requestedBlock: tc.requestedBlock}
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
		// eth_blockNumber has no block param, so it DEFAULTS to LATEST — same as a receipt. It is
		// tip-eligible via the GET_BLOCKNUM spec tag, not via the explicit-latest rule. (We do NOT
		// assert GetUsedDefaultValue here: it is non-deterministic for eth_blockNumber across runs,
		// which is exactly why eligibility is gated on the spec tag rather than on that flag.)
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

	// P1-#2: methods with no parseable block param (receipt / by-hash) fall back to the DEFAULT
	// parser, which reports LATEST_BLOCK — but Reply.LatestBlock is the HISTORICAL block of the
	// tx/block. They are neither GET_BLOCKNUM nor GET_BLOCK_BY_NUM, so the tag gate drops them.
	// (RequestedBlock==LATEST_BLOCK here proves RequestedBlock alone cannot be the discriminator.)
	t.Run("eth_getTransactionReceipt is historical (defaulted LATEST, not a tip)", func(t *testing.T) {
		cm := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0x1111111111111111111111111111111111111111111111111111111111111111"]}`)
		requested, _ := cm.RequestedBlock()
		require.Equal(t, spectypes.LATEST_BLOCK, requested, "receipt has no block param → parser defaults to LATEST_BLOCK")
		_, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 17_460_400})
		require.False(t, ok, "a historical transaction-receipt response must not be harvested as a tip")
	})

	t.Run("eth_getBlockByHash is historical (defaulted LATEST, not a tip)", func(t *testing.T) {
		cm := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["0x2222222222222222222222222222222222222222222222222222222222222222",false]}`)
		requested, _ := cm.RequestedBlock()
		require.Equal(t, spectypes.LATEST_BLOCK, requested, "by-hash has no block-number param → parser defaults to LATEST_BLOCK")
		_, ok := rpcss.tipBlockFromRelay(cm, &pairingtypes.RelayReply{LatestBlock: 17_460_400})
		require.False(t, ok, "a historical block-by-hash response must not be harvested as a tip")
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
		// The tip gate resolves GET_BLOCKNUM via the parser; without one every relay (even the
		// tip-eligible eth_blockNumber below) is rejected and the tip-eligible assertions fail.
		chainParser: newRealChainParserForHarvest(t, "ETH1"),
	}

	// The endpoint tip now lives in the shared endpointtip store; reset so a leftover entry
	// can't satisfy the "historical did NOT move the tip" assertion below.
	endpointtip.Default().Reset()

	// A historical response: eth_getBlockByNumber(0x10a7eb0) carries Reply.LatestBlock = that
	// concrete block, but it is NOT the tip.
	histMsg := parse(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x10a7eb0",false]}`)
	rpcss.harvestAndUpdateTipFromRelay(ep, histMsg, &pairingtypes.RelayReply{LatestBlock: 17_460_400}, gen, addr)

	require.Equal(t, int64(0), endpointtip.Default().Block(endpointtip.Key("ETH1", "jsonrpc", url)),
		"a historical response must NOT move the endpoint tip")
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

	require.Equal(t, int64(20_000_000), endpointtip.Default().Block(endpointtip.Key("ETH1", "jsonrpc", url)),
		"a tip-eligible response moves the endpoint tip")
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
		// Needed so the tip gate admits the eth_blockNumber harvest below; the focus here is the
		// generation gate, but a nil parser would reject the relay before generation is even checked.
		chainParser: newRealChainParserForHarvest(t, "ETH1"),
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
	// The generation gate must also block the downstream ungated tip-state writes: a stale relay
	// dropped from the store must NOT still bump the router bootstrap atomic (the poisoning the
	// harvest gate exists to prevent).
	require.Equal(t, uint64(0), rpcss.latestBlockHeight.Load(),
		"a stale-generation relay must not move the bootstrap atomic after the store rejects it")

	// Sanity: harvesting with the live generation (genB) IS accepted — proving the rejection
	// above was the generation gate, not a broken store.
	rpcss.harvestAndUpdateTipFromRelay(ep, tipMsg, &pairingtypes.RelayReply{LatestBlock: 20_000_000}, genB, addr)
	o, obsExists := m.GetObservation(url)
	require.True(t, obsExists)
	require.Equal(t, int64(20_000_000), o.LatestBlock)
	require.Equal(t, endpointstate.ObservationSourceRelay, o.Source)
}

// ---------------------------------------------------------------------------
// F4: ensureEndpointChainTracker must register the tracker and allocate its observation
// generation SYNCHRONOUSLY, so the relay dispatched immediately after captures a real (nonzero)
// generation — not 0 from a tracker that an async goroutine had not yet created. Before the fix
// the whole GetOrCreateTracker ran in `go func(){...}`, so endpointObservationGeneration could
// read 0 and the first relay's harvested tip was recorded against a generation the real tracker
// would never match (silently dropped). The blocking poll loop stays async, so this does not
// block dispatch on the network even when the endpoint URL is dead (as here).
// ---------------------------------------------------------------------------

func TestEnsureEndpointChainTracker_GenerationAvailableSynchronously(t *testing.T) {
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

	// A real connection to a dead URL: ensureEndpointChainTracker requires a non-nil connection,
	// and GetOrCreateTracker does no network I/O (the poll that would hit this URL is async and
	// fails gracefully), so registration is synchronous and deterministic.
	const url = "http://127.0.0.1:0"
	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })

	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
		chainParser:                 newRealChainParserForHarvest(t, "ETH1"),
	}

	// SYNCHRONOUS contract: immediately after ensureEndpointChainTracker returns — with NO sleep,
	// poll, or Eventually — the generation must already exist and be nonzero. This is exactly the
	// capture order the relay path uses (ensure → endpointObservationGeneration → dispatch).
	rpcss.ensureEndpointChainTracker(ctx, ep, directConn)
	gen := rpcss.endpointObservationGeneration(url)
	require.NotZero(t, gen, "the generation must be allocated synchronously, before the relay captures it")

	// The early relay's captured generation is valid: a tip-eligible harvest with it is recorded.
	tipMsg := newRealChainParserForHarvest(t, "ETH1")
	cm, perr := tipMsg.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, perr)
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 21_000_000}, gen, "lava@provider1")
	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(21_000_000), o.LatestBlock, "the early relay's tip is recorded under the synchronous generation")
	require.Equal(t, endpointstate.ObservationSourceRelay, o.Source)

	// Generation safety is preserved: after the tracker is removed, a harvest carrying the old
	// generation is rejected (a removed URL has no live generation to match).
	m.RemoveTracker(url)
	require.Zero(t, rpcss.endpointObservationGeneration(url), "a removed tracker has no live generation")
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 22_000_000}, gen, "lava@provider1")
	if o, exists := m.GetObservation(url); exists {
		require.NotEqual(t, int64(22_000_000), o.LatestBlock, "a harvest with a removed tracker's generation must be rejected")
	}
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
