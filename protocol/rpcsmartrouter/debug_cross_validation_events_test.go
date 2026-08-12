package rpcsmartrouter

// Tests for the cross-validation dissent record and GET /debug/cross-validation-events (MAG-2772).
//
// Three layers, because the endpoint is only worth what the recording behind it is worth:
//   - the ring itself (bounded, filterable, off by default),
//   - the HTTP contract the automation codes against — above all that "recorder off" is a NON-2xx
//     and never an empty 200, the same rule /debug/provider-scores established (MAG-2707),
//   - the production glue: that the real reply-time and straggler paths actually record, with the
//     provider, group, request id and mismatch-admission decision the counter cannot carry.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// useCrossValidationEventRing installs a fresh ring for one test and removes it afterwards, so the
// package-level recorder cannot leak state between tests (or record during the many other tests in
// this package that exercise the cross-validation paths).
func useCrossValidationEventRing(t *testing.T, capacity int) {
	t.Helper()
	enableCrossValidationEventRing(capacity)
	t.Cleanup(func() { crossValidationEventRecorder.Store(nil) })
}

// newEventsMux builds a debug mux with nothing else wired: the events endpoint reads the
// package-level recorder, not deps.
func newEventsMux() http.Handler {
	var offsetNano atomic.Int64
	return buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})
}

func decodeEventRows(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(body, &rows))
	return rows
}

func TestCrossValidationEventRing_OffByDefault(t *testing.T) {
	// No enable call: this is the production configuration, and recording must be inert.
	require.False(t, crossValidationEventRecordingEnabled())
	recordCrossValidationEvent(crossValidationEvent{RequestID: "1", Outcome: "disagreed"})
	events, _, enabled := readCrossValidationEvents(crossValidationEventFilter{})
	require.False(t, enabled, "a read must report that nothing was being recorded")
	require.Empty(t, events)
}

// TestCrossValidationEventRing_BoundedAndReportsDrops pins that the ring keeps the NEWEST events and
// says how many it threw away — a silent truncation would read as "that dissent never happened".
func TestCrossValidationEventRing_BoundedAndReportsDrops(t *testing.T) {
	useCrossValidationEventRing(t, 3)
	for i := 1; i <= 5; i++ {
		recordCrossValidationEvent(crossValidationEvent{RequestID: strconv.Itoa(i), Outcome: "disagreed"})
	}
	events, dropped, enabled := readCrossValidationEvents(crossValidationEventFilter{})
	require.True(t, enabled)
	require.Len(t, events, 3)
	require.Equal(t, []string{"3", "4", "5"}, []string{events[0].RequestID, events[1].RequestID, events[2].RequestID},
		"the ring keeps the newest events, oldest-first")
	require.Equal(t, uint64(2), dropped)
	require.Equal(t, uint64(3), events[0].Seq, "Seq counts every event, including the evicted ones")
}

func TestCrossValidationEventRing_Filters(t *testing.T) {
	useCrossValidationEventRing(t, 16)
	recordCrossValidationEvent(crossValidationEvent{RequestID: "100", ChainID: "ETH1", Outcome: "disagreed"})
	recordCrossValidationEvent(crossValidationEvent{RequestID: "100", ChainID: "ETH1", Outcome: "agreed"})
	recordCrossValidationEvent(crossValidationEvent{RequestID: "200", ChainID: "ETH1", Outcome: "disagreed"})
	recordCrossValidationEvent(crossValidationEvent{RequestID: "300", ChainID: "SOLANA", Outcome: "disagreed"})

	byRequest, _, _ := readCrossValidationEvents(crossValidationEventFilter{RequestID: "100"})
	require.Len(t, byRequest, 2, "the correlation key every test holds")

	byChain, _, _ := readCrossValidationEvents(crossValidationEventFilter{ChainID: "SOLANA"})
	require.Len(t, byChain, 1)

	byOutcome, _, _ := readCrossValidationEvents(crossValidationEventFilter{Outcome: "disagreed"})
	require.Len(t, byOutcome, 3)

	anded, _, _ := readCrossValidationEvents(crossValidationEventFilter{RequestID: "100", Outcome: "agreed"})
	require.Len(t, anded, 1)

	// limit keeps the most recent matches, like /debug/logs.
	tail, _, _ := readCrossValidationEvents(crossValidationEventFilter{Limit: 2})
	require.Len(t, tail, 2)
	require.Equal(t, "200", tail[0].RequestID)
	require.Equal(t, "300", tail[1].RequestID)
}

func TestCrossValidationEventRing_Clear(t *testing.T) {
	useCrossValidationEventRing(t, 2)
	for i := 1; i <= 4; i++ {
		recordCrossValidationEvent(crossValidationEvent{RequestID: strconv.Itoa(i)})
	}
	cleared, enabled := clearCrossValidationEvents()
	require.True(t, enabled)
	require.Equal(t, 2, cleared, "the ring held its capacity")

	events, dropped, _ := readCrossValidationEvents(crossValidationEventFilter{})
	require.Empty(t, events)
	require.Zero(t, dropped, "the eviction count restarts with the cleared ring")

	recordCrossValidationEvent(crossValidationEvent{RequestID: "5"})
	events, _, _ = readCrossValidationEvents(crossValidationEventFilter{})
	require.Len(t, events, 1)
	require.Equal(t, uint64(5), events[0].Seq, "Seq keeps counting so a clear cannot reissue an id a caller already holds")
}

func TestDebugCrossValidationEvents_MethodNotAllowed(t *testing.T) {
	useCrossValidationEventRing(t, 4)
	mux := newEventsMux()
	require.Equal(t, http.StatusMethodNotAllowed, postDebugRouter(mux, "/debug/cross-validation-events").Code)
	require.Equal(t, http.StatusMethodNotAllowed, getDebugRouter(mux, "/debug/cross-validation-events/clear").Code)
}

// TestDebugCrossValidationEvents_RecorderOffIsNot200 is the MAG-2707 rule applied here: an empty 200
// from a router that was never recording is a test passing having measured nothing.
func TestDebugCrossValidationEvents_RecorderOffIsNot200(t *testing.T) {
	crossValidationEventRecorder.Store(nil)
	mux := newEventsMux()

	rr := getDebugRouter(mux, "/debug/cross-validation-events")
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.NotEmpty(t, rr.Body.String(), "the failure says why")

	rr = postDebugRouter(mux, "/debug/cross-validation-events/clear")
	require.Equal(t, http.StatusServiceUnavailable, rr.Code, "a clear that cleared nothing is not a success")
}

// TestDebugCrossValidationEvents_EnabledAndEmptyIs200 is the other half of that rule: with the
// recorder live, no dissent is a real answer and must be a 200 with an empty array — never null.
func TestDebugCrossValidationEvents_EnabledAndEmptyIs200(t *testing.T) {
	useCrossValidationEventRing(t, 4)
	rr := getDebugRouter(newEventsMux(), "/debug/cross-validation-events")
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	require.Equal(t, "[]", strings.TrimSpace(rr.Body.String()), "an empty result marshals as [], never null")
	require.Empty(t, decodeEventRows(t, rr.Body.Bytes()))
	require.Equal(t, "0", rr.Header().Get("X-Cross-Validation-Events-Dropped"))
}

// TestDebugCrossValidationEvents_RowShape pins the row contract the automation reads.
func TestDebugCrossValidationEvents_RowShape(t *testing.T) {
	useCrossValidationEventRing(t, 8)
	consensus := sha256.Sum256([]byte("consensus"))
	outlier := sha256.Sum256([]byte("outlier"))
	recordCrossValidationEvent(crossValidationEvent{
		Source:          crossValidationEventSourceReplyTime,
		ChainID:         "ETH1",
		ApiInterface:    "jsonrpc",
		RequestID:       "424242",
		Method:          "eth_getBalance",
		ProviderAddress: "lava@provider3",
		ProviderGroup:   "tier-1",
		Outcome:         common.CrossValidationStragglerOutcomeDisagreed,
		Finality:        "finalized",
		ConsensusHash:   consensus,
		OutlierHash:     outlier,
		MismatchCounted: true,
	})
	// A straggler that never answered: no content was hashed, and the row must not imply one.
	recordCrossValidationEvent(crossValidationEvent{
		Source:          crossValidationEventSourceStraggler,
		ChainID:         "ETH1",
		ApiInterface:    "jsonrpc",
		RequestID:       "424242",
		Method:          "eth_getBalance",
		ProviderAddress: "lava@provider4",
		ProviderGroup:   common.DefaultProviderGroup,
		Outcome:         common.CrossValidationStragglerOutcomeNotReceived,
		Finality:        "finalized",
		ConsensusHash:   consensus,
		DelayMs:         1500,
	})

	rr := getDebugRouter(newEventsMux(), "/debug/cross-validation-events")
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	rows := decodeEventRows(t, rr.Body.Bytes())
	require.Len(t, rows, 2)

	first := rows[0]
	require.Equal(t, "reply-time", first["Source"], "which recording path saw this dissent")
	require.Equal(t, "ETH1", first["ChainID"], "rows are self-describing on a multi-chain router")
	require.Equal(t, "jsonrpc", first["ApiInterface"])
	require.Equal(t, "424242", first["RequestID"])
	require.Equal(t, "eth_getBalance", first["Method"])
	require.Equal(t, "lava@provider3", first["ProviderAddress"])
	require.Equal(t, "tier-1", first["ProviderGroup"])
	require.Equal(t, "disagreed", first["Outcome"])
	require.Equal(t, "finalized", first["Finality"])
	require.Equal(t, hex.EncodeToString(consensus[:]), first["ConsensusHash"])
	require.Equal(t, hex.EncodeToString(outlier[:]), first["OutlierHash"])
	require.Equal(t, true, first["MismatchCounted"])
	require.Equal(t, float64(0), first["DelayMs"], "a reply-time dissent resolved before the reply")
	require.NotEmpty(t, first["RecordedAt"])
	require.Equal(t, float64(1), first["Seq"])

	second := rows[1]
	require.Equal(t, "straggler", second["Source"])
	require.Equal(t, "not-received", second["Outcome"])
	require.Equal(t, "", second["OutlierHash"], "nothing was hashed, so no hash is reported — not a zero one")
	require.Equal(t, false, second["MismatchCounted"])
	require.Equal(t, float64(1500), second["DelayMs"])
	require.Equal(t, float64(2), second["Seq"], "rows are returned oldest-first, in recording order")
}

func TestDebugCrossValidationEvents_FiltersAndClearOverHTTP(t *testing.T) {
	useCrossValidationEventRing(t, 8)
	recordCrossValidationEvent(crossValidationEvent{RequestID: "100", ChainID: "ETH1", Outcome: "disagreed"})
	recordCrossValidationEvent(crossValidationEvent{RequestID: "200", ChainID: "ETH1", Outcome: "agreed"})
	recordCrossValidationEvent(crossValidationEvent{RequestID: "200", ChainID: "SOLANA", Outcome: "disagreed"})
	mux := newEventsMux()

	rows := decodeEventRows(t, getDebugRouter(mux, "/debug/cross-validation-events?request_id=200").Body.Bytes())
	require.Len(t, rows, 2)

	rows = decodeEventRows(t, getDebugRouter(mux, "/debug/cross-validation-events?request_id=200&outcome=agreed").Body.Bytes())
	require.Len(t, rows, 1)
	require.Equal(t, "ETH1", rows[0]["ChainID"])

	rows = decodeEventRows(t, getDebugRouter(mux, "/debug/cross-validation-events?chain_id=SOLANA").Body.Bytes())
	require.Len(t, rows, 1)

	rows = decodeEventRows(t, getDebugRouter(mux, "/debug/cross-validation-events?limit=1").Body.Bytes())
	require.Len(t, rows, 1)
	require.Equal(t, "SOLANA", rows[0]["ChainID"], "limit keeps the most recent")

	clear := postDebugRouter(mux, "/debug/cross-validation-events/clear")
	require.Equal(t, http.StatusOK, clear.Code)
	require.JSONEq(t, `{"cleared":true,"events_cleared":3}`, clear.Body.String())
	require.Empty(t, decodeEventRows(t, getDebugRouter(mux, "/debug/cross-validation-events").Body.Bytes()))
}

func TestDebugCrossValidationEvents_DroppedHeader(t *testing.T) {
	useCrossValidationEventRing(t, 2)
	for i := 1; i <= 5; i++ {
		recordCrossValidationEvent(crossValidationEvent{RequestID: strconv.Itoa(i)})
	}
	rr := getDebugRouter(newEventsMux(), "/debug/cross-validation-events")
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "3", rr.Header().Get("X-Cross-Validation-Events-Dropped"),
		"a truncated ring must say so rather than let the missing events read as absence")
}

// newCrossValidationEventTestServer builds a server wired the way the mismatch-metric tests wire one:
// a real chain parser and a ChainState with a known head, so finality resolves to a real label rather
// than "unknown".
func newCrossValidationEventTestServer(t *testing.T) (*RPCSmartRouterServer, chainlib.ChainParser) {
	t.Helper()
	// Empty NetworkAddress => metrics registered on the default registry, no HTTP server started.
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	cs := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	cs.SetLatestBlock(1_000_000)
	return &RPCSmartRouterServer{
		smartRouterEndpointMetrics: mm,
		listenEndpoint:             &routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainParser:                chainParser,
		chainState:                 cs,
	}, chainParser
}

// TestCrossValidationEvents_ReplyTimePathRecords is the production glue for the first recording path:
// the same deterministic successful outlier that increments the mismatch counter must also leave a
// row naming the provider, its group and the request — the per-request facts the counter's labels
// cannot carry, and the whole reason this surface is event-shaped.
func TestCrossValidationEvents_ReplyTimePathRecords(t *testing.T) {
	useCrossValidationEventRing(t, 32)
	srv, _ := newCrossValidationEventTestServer(t)
	ctx := utils.WithUniqueIdentifier(context.Background(), 987654321)

	hashA := sha256.Sum256([]byte("A"))
	hashB := sha256.Sum256([]byte("B"))
	deterministicAPI := &spectypes.Api{Name: "test", Category: spectypes.SpecCategory{Deterministic: true}}
	method := "cv_events_reply_time"

	rp := &MockRelayProcessorForHeaders{
		crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 4},
		selection:                       relaycore.CrossValidation,
		crossValidationQueriedProviders: []string{"p1", "p2", "p3", "p4"},
		successResults: []common.RelayResult{
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1", ProviderGroup: "tier-1"}, ResponseHash: hashA},
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2", ProviderGroup: "tier-1"}, ResponseHash: hashA},
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p3", ProviderGroup: "external"}, ResponseHash: hashB}, // outlier
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p4", ProviderGroup: "external"}, ResponseHash: hashB}, // same-group outlier
		},
	}
	relayResult := &common.RelayResult{
		ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
		CrossValidation: 2,
		ResponseHash:    hashA,
		Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
	}
	srv.appendHeadersToRelayResult(ctx, relayResult, 0, rp, &MockProtocolMessage{api: deterministicAPI, requestedBlock: 100}, method, nil, true)

	events, _, enabled := readCrossValidationEvents(crossValidationEventFilter{RequestID: "987654321"})
	require.True(t, enabled)
	require.Len(t, events, 2, "one row per dissenting provider — the agreeing providers are not dissent")

	require.Equal(t, crossValidationEventSourceReplyTime, events[0].Source)
	require.Equal(t, "ETH1", events[0].ChainID)
	require.Equal(t, "jsonrpc", events[0].ApiInterface)
	require.Equal(t, method, events[0].Method)
	require.Equal(t, "p3", events[0].ProviderAddress)
	require.Equal(t, "external", events[0].ProviderGroup, "the chart's group label reaches the row")
	require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, events[0].Outcome)
	require.Equal(t, "finalized", events[0].Finality)
	require.Equal(t, hashA, events[0].ConsensusHash)
	require.Equal(t, hashB, events[0].OutlierHash)
	require.True(t, events[0].MismatchCounted, "the first outlier of a group carries the group's single increment")

	require.Equal(t, "p4", events[1].ProviderAddress)
	require.False(t, events[1].MismatchCounted,
		"the counter's once-per-distinct-group rule is visible on the row, not hidden behind it")
}

// TestCrossValidationEvents_ReplyTimeAgreementRecordsNothing guards the surface against becoming a
// log of every cross-validated relay: with no outlier there is nothing to record. A test asserting
// "no dissent happened" reads an empty result here and confirms the request ran from its own
// response headers (lava-cross-validation-status / -agreeing-providers).
func TestCrossValidationEvents_ReplyTimeAgreementRecordsNothing(t *testing.T) {
	useCrossValidationEventRing(t, 32)
	srv, _ := newCrossValidationEventTestServer(t)
	ctx := utils.WithUniqueIdentifier(context.Background(), 111222333)

	hashA := sha256.Sum256([]byte("A"))
	rp := &MockRelayProcessorForHeaders{
		crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 2},
		selection:                       relaycore.CrossValidation,
		crossValidationQueriedProviders: []string{"p1", "p2"},
		successResults: []common.RelayResult{
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1", ProviderGroup: "tier-1"}, ResponseHash: hashA},
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2", ProviderGroup: "external"}, ResponseHash: hashA},
		},
	}
	relayResult := &common.RelayResult{
		ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
		CrossValidation: 2,
		ResponseHash:    hashA,
		Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
	}
	srv.appendHeadersToRelayResult(ctx, relayResult, 0, rp, &MockProtocolMessage{api: &spectypes.Api{Name: "test", Category: spectypes.SpecCategory{Deterministic: true}}, requestedBlock: 100}, "cv_events_agreement", nil, true)

	events, _, enabled := readCrossValidationEvents(crossValidationEventFilter{RequestID: "111222333"})
	require.True(t, enabled, "the recorder was live — an empty result here means no dissent, not no measurement")
	require.Empty(t, events)
}

// TestCrossValidationEvents_StragglerPathRecords is the production glue for the second recording
// path. It covers both the late dissent (which reaches the mismatch surface the reply-time path
// could never see for a provider that lost the race to quorum) and the late AGREEMENT — the positive
// control, which records a row without touching the mismatch surface.
func TestCrossValidationEvents_StragglerPathRecords(t *testing.T) {
	useCrossValidationEventRing(t, 32)
	srv, chainParser := newCrossValidationEventTestServer(t)
	baseCtx := context.Background()

	consensusBody := []byte(`{"jsonrpc":"2.0","id":1,"result":"0xAAAA"}`)
	dissentBody := []byte(`{"jsonrpc":"2.0","id":1,"result":"0xBBBB"}`)
	pushSuccess := func(rp *relaycore.RelayProcessor, provider, group string, body []byte) {
		rp.SetResponse(&relaycore.RelayResponse{
			RelayResult: common.RelayResult{
				Reply:        &pairingtypes.RelayReply{Data: body},
				ProviderInfo: common.ProviderInfo{ProviderAddress: provider, ProviderGroup: group},
				StatusCode:   200,
			},
		})
	}
	// A real cross-validation processor holding the two agreeing responses, with p3 queried and still
	// in flight — the reply-time state the straggler watcher is launched from.
	newCVProcessorWithConsensus := func(t *testing.T) (*relaycore.RelayProcessor, [32]byte) {
		t.Helper()
		cm, perr := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xDEAD","latest"],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, perr)
		pm := chainlib.NewProtocolMessage(cm, map[string]string{
			common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
			common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
		}, nil, "dapp", "1.2.3.4")
		sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(baseCtx, routersession.NewUsedProviders(nil), &SmartRouterRelaySenderMock{retValue: nil}, pm, nil, false, nil, "ETH1", "jsonrpc")
		require.NoError(t, smErr)
		rp := relaycore.NewRelayProcessor(baseCtx, sm.GetCrossValidationParams(), relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, sm)
		rp.SetCrossValidationQueriedProviders([]string{"p1", "p2", "p3"})
		pushSuccess(rp, "p1", "tier-1", consensusBody)
		pushSuccess(rp, "p2", "external", consensusBody)
		require.Len(t, rp.NodeResults(), 2) // drains the received responses into the results manager
		successResults, _, _ := rp.GetResultsData()
		require.Len(t, successResults, 2)
		return rp, successResults[0].ResponseHash
	}
	mkRelayResult := func(consensusHash [32]byte) *common.RelayResult {
		return &common.RelayResult{
			ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
			CrossValidation: 2,
			ResponseHash:    consensusHash,
			Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}
	}
	deterministicAPI := func(method string) *spectypes.Api {
		return &spectypes.Api{Name: method, Category: spectypes.SpecCategory{Deterministic: true}}
	}
	// The watcher runs on its own goroutine, so poll the ring rather than assuming it has recorded.
	awaitEvents := func(t *testing.T, requestID string, want int) []crossValidationEvent {
		t.Helper()
		var events []crossValidationEvent
		require.Eventually(t, func() bool {
			events, _, _ = readCrossValidationEvents(crossValidationEventFilter{RequestID: requestID})
			return len(events) == want
		}, 5*time.Second, 10*time.Millisecond, "the straggler watcher must record %d event(s)", want)
		return events
	}

	t.Run("late dissent", func(t *testing.T) {
		ctx := utils.WithUniqueIdentifier(baseCtx, 555000111)
		method := "cv_events_straggler_disagreed"
		rp, consensusHash := newCVProcessorWithConsensus(t)
		srv.watchCrossValidationStragglers(ctx, rp, mkRelayResult(consensusHash), &MockProtocolMessage{api: deterministicAPI(method), requestedBlock: 100}, method, []string{"p3"})
		pushSuccess(rp, "p3", "external", dissentBody)

		events := awaitEvents(t, "555000111", 1)
		event := events[0]
		require.Equal(t, crossValidationEventSourceStraggler, event.Source,
			"the row says which path saw it: this provider was in pending-providers, not disagreeing-providers")
		require.Equal(t, "ETH1", event.ChainID)
		require.Equal(t, "jsonrpc", event.ApiInterface)
		require.Equal(t, method, event.Method)
		require.Equal(t, "p3", event.ProviderAddress)
		require.Equal(t, "external", event.ProviderGroup)
		require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, event.Outcome)
		require.Equal(t, "finalized", event.Finality)
		require.Equal(t, consensusHash, event.ConsensusHash)
		// The late answer's OWN hash — the canonicalized content hash the comparison was made on
		// (responseContentHash, not a raw sha256 of the body), so assert what it must be: a real
		// hash, and a different one from the consensus.
		require.NotEqual(t, [32]byte{}, event.OutlierHash, "the dissenting content was hashed, not left as the no-content sentinel")
		require.NotEqual(t, event.ConsensusHash, event.OutlierHash)
		require.True(t, event.MismatchCounted, "a late confirmed dissent reaches the mismatch alerting surface")
	})

	t.Run("late agreement is the positive control", func(t *testing.T) {
		ctx := utils.WithUniqueIdentifier(baseCtx, 555000222)
		method := "cv_events_straggler_agreed"
		rp, consensusHash := newCVProcessorWithConsensus(t)
		srv.watchCrossValidationStragglers(ctx, rp, mkRelayResult(consensusHash), &MockProtocolMessage{api: deterministicAPI(method), requestedBlock: 100}, method, []string{"p3"})
		pushSuccess(rp, "p3", "external", consensusBody)

		event := awaitEvents(t, "555000222", 1)[0]
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, event.Outcome,
			"a straggler that agreed is recorded too, so 'no dissent' has something to anchor on")
		require.Equal(t, consensusHash, event.OutlierHash, "an agreeing straggler's hash IS the consensus hash")
		require.False(t, event.MismatchCounted, "agreement must not reach the mismatch alerting surface")
	})

	t.Run("two same-group late dissents are both recorded but counted once", func(t *testing.T) {
		ctx := utils.WithUniqueIdentifier(baseCtx, 555000333)
		method := "cv_events_straggler_dedup"
		rp, consensusHash := newCVProcessorWithConsensus(t)
		srv.watchCrossValidationStragglers(ctx, rp, mkRelayResult(consensusHash), &MockProtocolMessage{api: deterministicAPI(method), requestedBlock: 100}, method, []string{"p3", "p4"})
		pushSuccess(rp, "p3", "external", dissentBody)
		pushSuccess(rp, "p4", "external", dissentBody)

		events := awaitEvents(t, "555000333", 2)
		counted := 0
		for _, event := range events {
			require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, event.Outcome)
			if event.MismatchCounted {
				counted++
			}
		}
		require.Equal(t, 2, len(events), "every dissenting provider is named, which a counter cannot do")
		require.Equal(t, 1, counted, "while the counter still moves once for the group")
	})
}
