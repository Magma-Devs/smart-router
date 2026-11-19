package relaycore

// Tests for the debug record of the answers each request received (MAG-4358): the order they
// reached the request, the status each came back with, and the router's verdict on it.

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlibtest"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// useRelayAnswerRing installs a fresh ring for one test and removes it afterwards, so the
// process-wide recorder records nothing for the other tests of the package.
func useRelayAnswerRing(t *testing.T, capacity int) {
	t.Helper()
	EnableRelayAnswerRecording(capacity, func(protocolMessage chainlib.ProtocolMessage, _ string) string {
		return protocolMessage.GetApi().Name
	})
	t.Cleanup(DisableRelayAnswerRecording)
}

// newRecordedWriteProcessor builds the relay processor of a REST write broadcast to the given
// providers, on a request whose guid is guid.
func newRecordedWriteProcessor(t *testing.T, guid uint64, providers ...string) *RelayProcessor {
	t.Helper()
	ctx := utils.WithUniqueIdentifier(context.Background(), guid)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlibtest.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/tx/v1beta1/txs", []byte("data"), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "", "")
	usedProviders := routersession.NewUsedProviders(nil)
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateful))

	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	require.Nil(t, usedProviders.TryLockSelection(lockCtx))
	sessions := routersession.ConsumerSessionsMap{}
	for _, provider := range providers {
		sessions[provider] = &routersession.SessionInfo{}
	}
	usedProviders.AddUsed(sessions, nil)
	return relayProcessor
}

func requireAnswer(t *testing.T, answer RelayAnswer, provider string, status int, outcome string) {
	t.Helper()
	require.Equal(t, provider, answer.ProviderAddress)
	require.Equal(t, status, answer.StatusCode, "status of %s", provider)
	require.Equal(t, outcome, answer.Outcome, "outcome of %s", provider)
}

func TestRelayAnswers_OffByDefault(t *testing.T) {
	// No enable call: the production configuration, where recording must be inert.
	relayProcessor := newRecordedWriteProcessor(t, 7, "node@test")
	sendRestReply(relayProcessor, "node@test", 0, http.StatusOK, "ok")
	answers, _, enabled := ReadRelayAnswers("", 0)
	require.False(t, enabled, "a read must say that nothing was being recorded")
	require.Empty(t, answers)
}

// TestRelayAnswers_AWriteShowsTheRefusalBeforeTheAcceptance is the ticket's case: one provider
// refuses a write at once and another accepts it later, so the router must hold the refusal and
// wait. The record shows the refusal first, then the acceptance, then the third provider's answer,
// which arrived after the router had what it replied with and so was never read.
func TestRelayAnswers_AWriteShowsTheRefusalBeforeTheAcceptance(t *testing.T) {
	useRelayAnswerRing(t, 16)
	const guid = 4358
	relayProcessor := newRecordedWriteProcessor(t, guid, "refuser@test", "acceptor@test", "late@test")

	go sendRestReply(relayProcessor, "late@test", 250*time.Millisecond, http.StatusOK, `{"tx_response":{"code":0,"txhash":"LATE"}}`)
	go sendRestReply(relayProcessor, "acceptor@test", 80*time.Millisecond, http.StatusOK, `{"tx_response":{"code":0,"txhash":"ABC"}}`)
	go sendRestReply(relayProcessor, "refuser@test", 0, http.StatusBadRequest, `{"code":3,"message":"tx already in mempool"}`)

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(waitCtx))
	result, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, "acceptor@test", result.ProviderInfo.ProviderAddress, "setup: the acceptance is the reply")

	var answers []RelayAnswer
	require.Eventually(t, func() bool {
		answers, _, _ = ReadRelayAnswers(strconv.FormatUint(guid, 10), 0)
		return len(answers) == 3
	}, 2*time.Second, 10*time.Millisecond, "every provider's answer is on the record, the one that came after the reply too")

	requireAnswer(t, answers[0], "refuser@test", http.StatusBadRequest, RelayAnswerNodeError)
	requireAnswer(t, answers[1], "acceptor@test", http.StatusOK, RelayAnswerSuccess)
	requireAnswer(t, answers[2], "late@test", http.StatusOK, RelayAnswerUnread)
	require.Less(t, answers[0].Seq, answers[1].Seq)
	require.Less(t, answers[1].Seq, answers[2].Seq)
	require.GreaterOrEqual(t, answers[1].Elapsed, 80*time.Millisecond, "the acceptance was held back 80 ms")
	require.Less(t, answers[0].Elapsed, answers[1].Elapsed)
	for _, answer := range answers {
		require.Equal(t, strconv.FormatUint(guid, 10), answer.RequestID)
		require.Equal(t, "testId", answer.ChainID)
		require.Equal(t, "testInterface", answer.ApiInterface)
		require.NotEmpty(t, answer.Method)
	}
}

// TestRelayAnswers_AFailedAttemptKeepsItsStatus pins where a failed attempt's status comes from: a
// 5xx or a 429 fails the attempt, and relayInnerDirect keeps the status off its result and on the
// HTTPStatusError it fails with. Read from the result alone, a gateway's 502 would show as 0, the
// value of an attempt that got no answer at all.
func TestRelayAnswers_AFailedAttemptKeepsItsStatus(t *testing.T) {
	useRelayAnswerRing(t, 16)
	const guid = 502
	relayProcessor := newRecordedWriteProcessor(t, guid, "gateway@test", "silent@test")

	relayProcessor.SetResponse(&RelayResponse{
		RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "gateway@test"}},
		Err:         &routersession.HTTPStatusError{StatusCode: http.StatusBadGateway},
	})
	relayProcessor.SetResponse(&RelayResponse{
		RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "silent@test"}},
		Err:         errors.New("context deadline exceeded"),
	})
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(waitCtx))

	answers, _, _ := ReadRelayAnswers(strconv.FormatUint(guid, 10), 0)
	require.Len(t, answers, 2)
	requireAnswer(t, answers[0], "gateway@test", http.StatusBadGateway, RelayAnswerProtocolError)
	requireAnswer(t, answers[1], "silent@test", 0, RelayAnswerProtocolError)
}

// TestRelayAnswers_OnlyAProvidersAnswerToAClient pins the two answers the record leaves out: a
// cache hit, which no provider answered, and the router's own health check, which is not a
// client's request.
func TestRelayAnswers_OnlyAProvidersAnswerToAClient(t *testing.T) {
	useRelayAnswerRing(t, 16)

	cached := newRecordedWriteProcessor(t, 1, "node@test")
	cached.SetResponse(&RelayResponse{RelayResult: common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte("ok")}, StatusCode: http.StatusOK}})

	healthCheck := newRecordedWriteProcessor(t, 2, "node@test")
	healthCheck.GetUsedProviders().MarkRouterHealthCheck()
	sendRestReply(healthCheck, "node@test", 0, http.StatusOK, "ok")

	answers, _, enabled := ReadRelayAnswers("", 0)
	require.True(t, enabled)
	require.Empty(t, answers)
}

// TestRelayAnswers_BoundedAndReportsDrops pins that the ring keeps the newest answers, says how
// many it threw away, and that a verdict for an evicted answer lands on no other answer.
func TestRelayAnswers_BoundedAndReportsDrops(t *testing.T) {
	useRelayAnswerRing(t, 2)
	ring := relayAnswerRecorder.Load()
	var seqs []uint64
	for i := 1; i <= 3; i++ {
		seqs = append(seqs, ring.record(RelayAnswer{RequestID: strconv.Itoa(i)}))
	}
	ring.setOutcome(seqs[0], RelayAnswerSuccess)

	answers, dropped, _ := ReadRelayAnswers("", 0)
	require.Equal(t, uint64(1), dropped)
	require.Len(t, answers, 2)
	require.Equal(t, []string{"2", "3"}, []string{answers[0].RequestID, answers[1].RequestID})
	require.Equal(t, RelayAnswerUnread, answers[0].Outcome, "the evicted answer's verdict was written onto another")
	require.Equal(t, RelayAnswerUnread, answers[1].Outcome)

	answers, _, _ = ReadRelayAnswers("", 1)
	require.Len(t, answers, 1)
	require.Equal(t, "3", answers[0].RequestID, "a limit keeps the most recent answers")
}

// TestRelayAnswers_AVerdictLandsOnTheRingThatRecordedTheAnswer covers a write's delivery that
// outlives its test: its answer was recorded on one ring, and a later test installed another. The
// verdict must not land on the new ring's entry that happens to share the Seq.
func TestRelayAnswers_AVerdictLandsOnTheRingThatRecordedTheAnswer(t *testing.T) {
	useRelayAnswerRing(t, 16)
	relayProcessor := newRecordedWriteProcessor(t, 9, "node@test")
	response := &RelayResponse{
		RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "node@test"}},
		Err:         errors.New("refused"),
	}
	relayProcessor.recordAnswer(response)

	useRelayAnswerRing(t, 16)
	relayAnswerRecorder.Load().record(RelayAnswer{RequestID: "other"})
	recordAnswerOutcome(response, nil)

	answers, _, _ := ReadRelayAnswers("", 0)
	require.Len(t, answers, 1)
	require.Equal(t, RelayAnswerUnread, answers[0].Outcome)
}
