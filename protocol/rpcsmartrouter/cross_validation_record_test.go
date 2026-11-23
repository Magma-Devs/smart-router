package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// observeCrossValidationRecords installs the test hook for the duration of the test and returns the
// records observed so far, in order, each time it is called. Restores the hook via t.Cleanup since it
// is process-global.
func observeCrossValidationRecords(t *testing.T) func() []*crossValidationRecord {
	t.Helper()
	var mu sync.Mutex
	var observed []*crossValidationRecord
	crossValidationRecordObserveForTest = func(rec *crossValidationRecord) {
		mu.Lock()
		defer mu.Unlock()
		observed = append(observed, rec)
	}
	t.Cleanup(func() { crossValidationRecordObserveForTest = nil })
	return func() []*crossValidationRecord {
		mu.Lock()
		defer mu.Unlock()
		out := make([]*crossValidationRecord, len(observed))
		copy(out, observed)
		return out
	}
}

func TestBuildCrossValidationRecord_DigestNeverAmbiguousAlone(t *testing.T) {
	// A success result with an empty-but-valid reply (zero hash, never actually stored on
	// common.RelayResult here — the test only needs ResponseHash left at its zero value, same as a
	// genuinely empty payload would leave it) and a node-error result both render Digest "". MAG-2787's
	// fix-first requirement is that a reader can still tell them apart — via Outcome, never via Digest
	// alone.
	rec := buildCrossValidationRecord(crossValidationRecordInputs{
		CVSuccess: false,
		SuccessResults: []common.RelayResult{
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}}, // zero ResponseHash: an empty, valid reply
		},
		NodeErrorResults: []common.RelayResult{
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2"}}, // zero ResponseHash: never hashed at all
		},
		AllProviders: []string{"p1", "p2"},
	})
	require.Len(t, rec.Endpoints, 2)
	byProvider := map[string]crossValidationEndpointOutcome{}
	for _, e := range rec.Endpoints {
		byProvider[e.ProviderAddress] = e
	}
	require.Equal(t, "", byProvider["p1"].Digest)
	require.Equal(t, "", byProvider["p2"].Digest, "digest is just as empty for a node error — Outcome is what disambiguates")
	require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, byProvider["p1"].Outcome, "an empty but valid reply still answered — it's a disagreement with a failed quorum, not a non-answer")
	require.Equal(t, common.CrossValidationStragglerOutcomeNodeError, byProvider["p2"].Outcome)
}

func TestBuildCrossValidationRecord_NotReceivedForUnaccountedProviders(t *testing.T) {
	rec := buildCrossValidationRecord(crossValidationRecordInputs{
		CVSuccess:     false,
		FailureReason: common.CrossValidationReasonInsufficientResponses,
		AllProviders:  []string{"p1", "p2", "p3"},
	})
	require.Len(t, rec.Endpoints, 3)
	for _, e := range rec.Endpoints {
		require.Equal(t, common.CrossValidationStragglerOutcomeNotReceived, e.Outcome)
		require.Equal(t, "", e.Digest)
	}
	require.Equal(t, "failed", rec.Status)
	require.Equal(t, common.CrossValidationReasonInsufficientResponses, rec.FailureReason)
	require.Equal(t, "", rec.ServedDigest, "no winner on failure — never a stand-in zero hash")
}

func TestBuildCrossValidationRecord_ServedDigestOnlyOnSuccess(t *testing.T) {
	hash := [32]byte{0xAA, 0xBB}
	success := buildCrossValidationRecord(crossValidationRecordInputs{
		CVSuccess:      true,
		ConsensusHash:  hash,
		SuccessResults: []common.RelayResult{{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}, ResponseHash: hash}},
	})
	require.Equal(t, "success", success.Status)
	require.Equal(t, crossValidationHashHex(hash), success.ServedDigest)
	require.NotEmpty(t, success.ServedDigest)

	failure := buildCrossValidationRecord(crossValidationRecordInputs{
		CVSuccess:      false,
		FailureReason:  common.CrossValidationReasonNoAgreement,
		SuccessResults: []common.RelayResult{{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}, ResponseHash: hash}},
	})
	require.Equal(t, "failed", failure.Status)
	require.Equal(t, "", failure.ServedDigest)
}

func TestBuildCrossValidationRecord_PluralityOnFailure(t *testing.T) {
	hashA := [32]byte{0xAA}
	hashB := [32]byte{0xBB}
	rec := buildCrossValidationRecord(crossValidationRecordInputs{
		CVSuccess:     false,
		FailureReason: common.CrossValidationReasonNoAgreement,
		SuccessResults: []common.RelayResult{
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}, ResponseHash: hashA},
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2"}, ResponseHash: hashA},
			{ProviderInfo: common.ProviderInfo{ProviderAddress: "p3"}, ResponseHash: hashB},
		},
		AllProviders: []string{"p1", "p2", "p3"},
	})
	require.Equal(t, 2, rec.PluralitySize)
	require.Equal(t, []string{"p1", "p2"}, rec.PluralityProviders)
}

func TestBuildCrossValidationRecord_PolicyAsResolved(t *testing.T) {
	rec := buildCrossValidationRecord(crossValidationRecordInputs{
		CVParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 3},
	})
	require.Equal(t, 2, rec.AgreementThreshold)
	require.Equal(t, 5, rec.MaxParticipants)
	require.Equal(t, 3, rec.MinGroups)
}

func TestCrossValidationRecordSeq_Monotonic(t *testing.T) {
	rec1 := buildCrossValidationRecord(crossValidationRecordInputs{})
	rec2 := buildCrossValidationRecord(crossValidationRecordInputs{})
	require.Equal(t, rec1.Seq+1, rec2.Seq, "Seq is assigned at build time and must strictly increase, so a dropped record leaves a visible gap")
}

func TestCrossValidationRecordTenant(t *testing.T) {
	longValue := strings.Repeat("a", crossValidationRecordTenantMaxLength+1)
	invalidUTF8 := string([]byte{0xff, 0xfe})
	tests := []struct {
		name      string
		analytics *metrics.RelayMetrics
		want      string
	}{
		{"nil analytics", nil, metrics.CustomerNone},
		{"empty project hash", &metrics.RelayMetrics{}, metrics.CustomerNone},
		{"normal value passes through", &metrics.RelayMetrics{ProjectHash: "dapp-123"}, "dapp-123"},
		{"too long collapses to other", &metrics.RelayMetrics{ProjectHash: longValue}, metrics.CustomerOther},
		{"invalid utf8 collapses to other", &metrics.RelayMetrics{ProjectHash: invalidUTF8}, metrics.CustomerOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, crossValidationRecordTenant(tt.analytics))
		})
	}
}

func TestEmitCrossValidationRecord_DropsAndCountsWhenQueueFull(t *testing.T) {
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	srv := &RPCSmartRouterServer{smartRouterEndpointMetrics: mm}

	droppedBefore := gatherCounter(t, "smartrouter_cross_validation_record_dropped_total", map[string]string{"spec": "DROPTESTCHAIN"})

	// An unbuffered channel with no reader: select-default on it always takes the drop path,
	// deterministically, without racing the real background writer.
	original := crossValidationRecordChan
	crossValidationRecordChan = make(chan *crossValidationRecord)
	t.Cleanup(func() { crossValidationRecordChan = original })

	done := make(chan struct{})
	go func() {
		srv.emitCrossValidationRecord(&crossValidationRecord{ChainID: "DROPTESTCHAIN", ApiInterface: "jsonrpc"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("emitCrossValidationRecord must never block, even when the queue is full")
	}

	droppedAfter := gatherCounter(t, "smartrouter_cross_validation_record_dropped_total", map[string]string{"spec": "DROPTESTCHAIN"})
	require.Equal(t, droppedBefore+1, droppedAfter)
}

// gatherCounter sums a CounterVec's value across every series whose labels are a superset of want.
func gatherCounter(t *testing.T, name string, want map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			matches := true
			for k, v := range want {
				if labels[k] != v {
					matches = false
					break
				}
			}
			if matches {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

// TestCrossValidationRecord_EndToEnd exercises the actual request-handling entry points
// (appendHeadersToRelayResult, and watchCrossValidationStragglers for the deferred path), proving the
// acceptance criteria that matter end-to-end rather than only at the record-builder level.
func TestCrossValidationRecord_EndToEnd(t *testing.T) {
	hashA := [32]byte{0xAA}
	hashB := [32]byte{0xBB}
	responded := func(provider string, hash [32]byte) common.RelayResult {
		return common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: provider, ProviderGroup: "g"}, ResponseHash: hash}
	}

	t.Run("success with a real dissenter: exactly one record, right dissenter, right digests", func(t *testing.T) {
		getRecords := observeCrossValidationRecords(t)
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 3},
			selection:                       relaycore.CrossValidation,
			crossValidationQueriedProviders: []string{"p1", "p2", "p3"},
			successResults:                  []common.RelayResult{responded("p1", hashA), responded("p2", hashA), responded("p3", hashB)},
			nodeErrors:                      []common.RelayResult{},
			usedProviders:                   dispatchedTo("p1", "p2", "p3"),
		}
		relayResult := &common.RelayResult{
			ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
			CrossValidation: 2,
			ResponseHash:    hashA,
			Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}
		mockProtocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api", Category: spectypes.SpecCategory{Deterministic: true}}, requestedBlock: 42}

		(&RPCSmartRouterServer{}).appendHeadersToRelayResult(context.Background(), relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		records := getRecords()
		require.Len(t, records, 1, "every cross-validated request produces exactly one record")
		rec := records[0]
		require.Equal(t, "success", rec.Status)
		require.Equal(t, crossValidationHashHex(hashA), rec.ServedDigest)
		require.Equal(t, int64(42), rec.BlockHeight)
		require.Len(t, rec.Endpoints, 3)
		byProvider := map[string]crossValidationEndpointOutcome{}
		for _, e := range rec.Endpoints {
			byProvider[e.ProviderAddress] = e
		}
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, byProvider["p1"].Outcome)
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, byProvider["p2"].Outcome)
		require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, byProvider["p3"].Outcome, "p3 is the dissenter")
		require.Equal(t, crossValidationHashHex(hashA), byProvider["p1"].Digest)
		require.Equal(t, crossValidationHashHex(hashB), byProvider["p3"].Digest)
		require.NotEqual(t, byProvider["p1"].Digest, byProvider["p3"].Digest)
	})

	t.Run("failure that never reached quorum: exactly one record naming the plurality", func(t *testing.T) {
		getRecords := observeCrossValidationRecords(t)
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 3, MaxParticipants: 3},
			selection:                       relaycore.CrossValidation,
			crossValidationQueriedProviders: []string{"p1", "p2", "p3"},
			successResults:                  []common.RelayResult{responded("p1", hashA), responded("p2", hashA), responded("p3", hashB)},
			nodeErrors:                      []common.RelayResult{},
			usedProviders:                   dispatchedTo("p1", "p2", "p3"),
		}
		relayResult := &common.RelayResult{
			StatusCode:                   500,
			CrossValidationFailureReason: common.CrossValidationReasonNoAgreement,
			Reply:                        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}
		mockProtocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api"}}

		(&RPCSmartRouterServer{}).appendHeadersToRelayResult(context.Background(), relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, false)

		records := getRecords()
		require.Len(t, records, 1)
		rec := records[0]
		require.Equal(t, "failed", rec.Status)
		require.Equal(t, common.CrossValidationReasonNoAgreement, rec.FailureReason)
		require.Equal(t, "", rec.ServedDigest)
		require.Equal(t, 2, rec.PluralitySize)
		require.Equal(t, []string{"p1", "p2"}, rec.PluralityProviders)
	})

	t.Run("non-cross-validation request produces no record", func(t *testing.T) {
		getRecords := observeCrossValidationRecords(t)
		relayProcessor := &MockRelayProcessorForHeaders{selection: relaycore.Stateless}
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}
		mockProtocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api"}}

		(&RPCSmartRouterServer{}).appendHeadersToRelayResult(context.Background(), relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		require.Empty(t, getRecords(), "a request that was never cross-validated must produce no record")
	})

	t.Run("no request or response body ever appears in the record", func(t *testing.T) {
		getRecords := observeCrossValidationRecords(t)
		const secretPayloadMarker = "TOTALLY-SECRET-CLIENT-ORDER-PAYLOAD-0xDEADBEEF"
		relayProcessor := &MockRelayProcessorForHeaders{
			crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 2},
			selection:                       relaycore.CrossValidation,
			crossValidationQueriedProviders: []string{"p1", "p2"},
			successResults:                  []common.RelayResult{responded("p1", hashA), responded("p2", hashA)},
			nodeErrors:                      []common.RelayResult{},
			usedProviders:                   dispatchedTo("p1", "p2"),
		}
		relayResult := &common.RelayResult{
			ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
			CrossValidation: 2,
			ResponseHash:    hashA,
			Reply: &pairingtypes.RelayReply{
				Data:     []byte(secretPayloadMarker), // the actual response body — must never reach the record
				Metadata: []pairingtypes.Metadata{},
			},
		}
		mockProtocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api"}}

		(&RPCSmartRouterServer{}).appendHeadersToRelayResult(context.Background(), relayResult, 0, relayProcessor, mockProtocolMessage, "test-api", nil, true)

		records := getRecords()
		require.Len(t, records, 1)
		payload, err := json.Marshal(records[0])
		require.NoError(t, err)
		require.NotContains(t, string(payload), secretPayloadMarker)
	})
}

// TestCrossValidationRecord_DeferredToStragglerWatcher covers the path where a consensus was reached
// but a provider was still in flight: appendHeadersToRelayResult must emit NOTHING for this request
// (the straggler watcher owns it), and the watcher must emit exactly one record, carrying the
// straggler's real resolution, once it is done — the full MAG-2187 scenario this ticket's "late
// responses... and how they resolved" bullet describes. Run with and without a metrics manager:
// --metrics-listen-address defaults to disabled, which leaves the server with none, and the record
// must carry the resolution either way.
func TestCrossValidationRecord_DeferredToStragglerWatcher(t *testing.T) {
	t.Run("metrics enabled", func(t *testing.T) { testCrossValidationRecordDeferred(t, true) })
	t.Run("metrics disabled", func(t *testing.T) { testCrossValidationRecordDeferred(t, false) })
}

func testCrossValidationRecordDeferred(t *testing.T, withMetrics bool) {
	ctx := context.Background()
	var mm *metrics.SmartRouterMetricsManager
	if withMetrics {
		mm = metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
		require.NotNil(t, mm)
	}
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	cs.SetLatestBlock(1_000_000)
	srv := &RPCSmartRouterServer{
		smartRouterEndpointMetrics: mm,
		listenEndpoint:             &routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainParser:                chainParser,
		chainState:                 cs,
	}

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

	cm, perr := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xDEAD","latest"],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, perr)
	pm := chainlib.NewProtocolMessage(cm, map[string]string{
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}, nil, "dapp", "1.2.3.4")
	sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, routersession.NewUsedProviders(nil), &SmartRouterRelaySenderMock{retValue: nil}, pm, nil, false, nil, "ETH1", "jsonrpc")
	require.NoError(t, smErr)
	require.Equal(t, relaycore.CrossValidation, sm.GetSelection())
	rp := relaycore.NewRelayProcessor(ctx, sm.GetCrossValidationParams(), relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, sm)
	rp.SetCrossValidationQueriedProviders([]string{"p1", "p2", "p3"})
	pushSuccess(rp, "p1", "g1", consensusBody)
	pushSuccess(rp, "p2", "g2", consensusBody)
	require.Len(t, rp.NodeResults(), 2)
	successResults, _, _ := rp.GetResultsData()
	require.Len(t, successResults, 2)
	consensusHash := successResults[0].ResponseHash

	relayResult := &common.RelayResult{
		ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
		CrossValidation: 2,
		ResponseHash:    consensusHash,
		Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
	}
	protocolMessage := &MockProtocolMessage{api: &spectypes.Api{Name: "test-api", Category: spectypes.SpecCategory{Deterministic: true}}, requestedBlock: 100}

	getRecords := observeCrossValidationRecords(t)
	pending := srv.appendHeadersToRelayResult(ctx, relayResult, 0, rp, protocolMessage, "test-api", nil, true)
	require.Equal(t, []string{"p3"}, pending)
	require.Empty(t, getRecords(), "appendHeadersToRelayResult must defer to the straggler watcher, not emit its own record, while p3 is still pending")

	srv.watchCrossValidationStragglers(ctx, rp, relayResult, protocolMessage, "test-api", pending, nil)
	pushSuccess(rp, "p3", "g3", dissentBody)

	require.Eventually(t, func() bool {
		return len(getRecords()) == 1
	}, 5*time.Second, 10*time.Millisecond, "the straggler watcher must emit exactly one record once p3 resolves")

	rec := getRecords()[0]
	require.Equal(t, "success", rec.Status)
	require.Len(t, rec.LateResolutions, 1)
	require.Equal(t, "p3", rec.LateResolutions[0].ProviderAddress)
	require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, rec.LateResolutions[0].Outcome)
	byProvider := map[string]crossValidationEndpointOutcome{}
	for _, e := range rec.Endpoints {
		byProvider[e.ProviderAddress] = e
	}
	require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, byProvider["p3"].Outcome, "the final Endpoints state reflects the resolved outcome, not the provisional one")
}

// TestPatchCrossValidationRecordStraggler_NotReceivedKeepsNoGroup: a provider that never answered
// has no group the record could know (a group is learned only from a response), at reply time and
// after the straggler deadline alike. The patch used to file the deadline case under the default
// group.
func TestPatchCrossValidationRecordStraggler_NotReceivedKeepsNoGroup(t *testing.T) {
	replyTime := buildCrossValidationRecord(crossValidationRecordInputs{AllProviders: []string{"p3"}})
	deferred := buildCrossValidationRecord(crossValidationRecordInputs{CVSuccess: true, AllProviders: []string{"p3"}})
	patchCrossValidationRecordStraggler(deferred, relaycore.CrossValidationStragglerResult{ProviderAddress: "p3", Outcome: common.CrossValidationStragglerOutcomeNotReceived, Delay: time.Second})
	require.Equal(t, "", replyTime.Endpoints[0].ProviderGroup)
	require.Equal(t, "", deferred.Endpoints[0].ProviderGroup)
	require.Equal(t, "", deferred.LateResolutions[0].ProviderGroup)

	// A straggler that answered keeps the group it answered from; an unlabelled one is the default.
	patchCrossValidationRecordStraggler(deferred, relaycore.CrossValidationStragglerResult{ProviderAddress: "p3", Outcome: common.CrossValidationStragglerOutcomeNodeError})
	require.Equal(t, common.DefaultProviderGroup, deferred.Endpoints[0].ProviderGroup)
}

// TestCrossValidationRecord_LineFollowsLogFormat writes a record in a fresh process, in the order a
// router runs: package init, then RunE applies --log-format, then the first record. A logger chosen
// at package init missed the flag and wrote every record as console text beside JSON lines.
// MAG-2788: the record's method reaches Loki as the router wrote it, so a reader can filter on it.
// The fleet's collector rewrites a value that starts with "/" as /REDACTED, so a REST path is written
// without its slash. An unlisted REST path is the caller's own and may carry a credential, so it is
// withheld, as relay_end withholds it.
func TestCrossValidationRecordMethod_SurvivesTheCollector(t *testing.T) {
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	restParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "COSMOSHUB", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	rest := func(path string) chainlib.ProtocolMessage {
		chainMsg, err := restParser.ParseMsg(path, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{})
		require.NoError(t, err, path)
		return chainlib.NewProtocolMessage(chainMsg, nil, nil, "test-dapp", "127.0.0.1")
	}
	jsonRPC := ethProtocolMessage(t, ethJsonRPCParser(t), `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`, 100)

	for _, tc := range []struct {
		name         string
		msg          chainlib.ProtocolMessage
		apiInterface string
		want         string
	}{
		{"a listed REST path keeps its spec name", rest("/cosmos/bank/v1beta1/balances/cosmos1qqqsyqcyq5rqwzqf"), spectypes.APIInterfaceRest, "cosmos/bank/v1beta1/balances/{address}"},
		{"an unlisted REST path is withheld", rest("/v1/AbC123SecretKey/status?apikey=AbC123SecretKey"), spectypes.APIInterfaceRest, "Default-REDACTED"},
		{"a JSON-RPC method is unchanged", jsonRPC, spectypes.APIInterfaceJsonRPC, "eth_getBalance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := buildCrossValidationRecord(crossValidationRecordInputs{ApiInterface: tc.apiInterface, Method: crossValidationRecordMethod(tc.msg, tc.apiInterface)})
			require.Equal(t, tc.want, rec.Method)
			line, err := json.Marshal(rec)
			require.NoError(t, err)
			require.Equal(t, string(line), collectorBarePathRedaction.ReplaceAllString(string(line), "${1}REDACTED"), "the collector leaves the record as it is")
			require.NotContains(t, string(line), "AbC123SecretKey")
		})
	}
}

func TestCrossValidationRecord_LineFollowsLogFormat(t *testing.T) {
	if format := os.Getenv("CV_RECORD_LOG_FORMAT_CHILD"); format != "" {
		utils.JsonFormat = format == "json" // what RunE does with --log-format
		writeCrossValidationRecord(&crossValidationRecord{Seq: 7, RequestID: "123"})
		os.Exit(0)
	}
	for _, format := range []string{"json", "text"} {
		t.Run(format, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrossValidationRecord_LineFollowsLogFormat$")
			cmd.Env = append(os.Environ(), "CV_RECORD_LOG_FORMAT_CHILD="+format)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			require.NoError(t, cmd.Run())
			var line string
			for _, l := range strings.Split(stderr.String(), "\n") {
				if strings.Contains(l, "cross-validation record") {
					line = l
					break
				}
			}
			require.NotEmpty(t, line, "the child wrote no record line: %q", stderr.String())
			var parsed struct {
				Message string                `json:"message"`
				Record  crossValidationRecord `json:"crossValidationRecord"`
			}
			jsonErr := json.Unmarshal([]byte(line), &parsed)
			if format == "json" {
				require.NoError(t, jsonErr, "with --log-format json the record line must be JSON: %q", line)
				require.Equal(t, "cross-validation record", parsed.Message)
				require.Equal(t, uint64(7), parsed.Record.Seq)
				return
			}
			require.Error(t, jsonErr, "with --log-format text the record line is console text, like every other line: %q", line)
		})
	}
}

// TestCrossValidationRecord_FailFastBeforeDispatch sends a request the capacity guard refuses before
// any provider is asked (max-participants 5 against three paired providers) through SendParsedRelay.
// crossValidationFailFast counts it as a failed cross-validated request, but the path returns before
// appendHeadersToRelayResult, so it produced no record, and its reply had no Lava-Guid to name one.
func TestCrossValidationRecord_FailFastBeforeDispatch(t *testing.T) {
	rand.InitRandomSeed()
	ctx := utils.WithUniqueIdentifier(context.Background(), 771200001)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)
	hits, server := newPinOverrideServer(t, chainParser, logs, false)

	body := []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`)
	chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceJsonRPC,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, map[string]string{
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "5",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}, relayData, "dapp", "1.2.3.4")

	getRecords := observeCrossValidationRecords(t)
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, sendErr := server.SendParsedRelay(callCtx, nil, protocolMessage)
	require.Error(t, sendErr)
	require.NotNil(t, result)
	require.Equal(t, common.CrossValidationReasonInsufficientCapacity, result.CrossValidationFailureReason)
	require.Equal(t, []int64{0, 0, 0}, loadPinOverrideHits(hits), "setup: the guard refuses before any provider is asked")

	records := getRecords()
	require.Len(t, records, 1, "a cross-validated request refused at request time still gets its one record")
	rec := records[0]
	require.Equal(t, "failed", rec.Status)
	require.Equal(t, common.CrossValidationReasonInsufficientCapacity, rec.FailureReason)
	require.Equal(t, 5, rec.MaxParticipants, "the policy as resolved for the request")
	require.Equal(t, 2, rec.AgreementThreshold)
	require.Empty(t, rec.Endpoints, "no provider was asked")
	require.Equal(t, "", rec.ServedDigest)
	require.Equal(t, "771200001", rec.RequestID)
	require.Equal(t, rec.RequestID, pinOverrideReplyHeader(result, common.GUID_HEADER_NAME), "the reply names the record in Lava-Guid")
}
