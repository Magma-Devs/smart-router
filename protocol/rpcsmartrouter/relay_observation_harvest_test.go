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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	require.NoError(t, err)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}

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
