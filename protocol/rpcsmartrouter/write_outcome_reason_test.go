package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	relaycoretest "github.com/magma-Devs/smart-router/protocol/relaycoretest"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// What an unclear write tells the client, and why (MAG-4380). The verdict "unclear" is right in
// every row below; the reason it gives must follow the cause. Before, every unclear write said
// "timeout reached before any node responded", including one where every node answered within
// milliseconds and the answers were lost on the way back (Fuel Sepolia, 2026-10-15: both nodes
// executed the transfer, both answers became a 502 from a proxy, the client was told in 0.3 s that
// it had timed out).
const (
	unclearWritePrefix = "transaction status unclear: "
	unclearWriteSuffix = "; the transaction may already have been submitted — verify on-chain before resubmitting"
)

// restWriteUpstream is one upstream of a REST write broadcast and how it answers the write.
type restWriteUpstream struct {
	name string
	// hang: never answer; the attempt is held until the client has its reply (see
	// heldCancelledRESTConnection), so the relay ends on the budget with this upstream silent.
	hang   bool
	status int
	body   string
}

type restWriteOutcome struct {
	status      int
	clientError string           // the Error field of the router's error envelope
	relayEnd    map[string]any   // the relay's relay_end event, nil when none was logged
	records     []map[string]any // every record logged while the write was served
}

// sendRESTWriteThroughListener broadcasts one Cosmos tx submit through the real REST listener and
// SendParsedRelay to the given upstreams, and returns what the client got and what was logged.
func sendRESTWriteThroughListener(t *testing.T, upstreams []restWriteUpstream) restWriteOutcome {
	t.Helper()
	rand.InitRandomSeed()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	anyHang := false
	for _, upstream := range upstreams {
		anyHang = anyHang || upstream.hang
	}
	if anyHang {
		originalTimeout := common.DefaultTimeout
		common.DefaultTimeout = 250 * time.Millisecond // stateful processing budget: 6 x 250 ms
		t.Cleanup(func() { common.DefaultTimeout = originalTimeout })
	}

	const path = "/cosmos/tx/v1beta1/txs"
	data := []byte(`{"tx_bytes":"dHg=","mode":"BROADCAST_MODE_SYNC"}`)
	handlerFor := func(upstream restWriteUpstream) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			if upstream.hang {
				<-r.Context().Done()
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(upstream.status)
			_, _ = w.Write([]byte(upstream.body))
		}
	}

	parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, handlerFor(upstreams[0]), nil, "../../", nil)
	require.NoError(t, err)
	t.Cleanup(closeServer)

	releases := []func(){}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	for index, upstream := range upstreams {
		nodeURL := endpoint.NodeUrls[0]
		if index > 0 {
			server := httptest.NewServer(handlerFor(upstream))
			t.Cleanup(server.Close)
			nodeURL = common.NodeUrl{Url: server.URL}
		}
		if upstream.hang {
			// The attempt's own deadline is later than the request budget, so the budget ends the
			// relay with this upstream silent.
			nodeURL.Timeout = 2 * time.Second
		}
		conn, err := routersession.NewDirectRPCConnection(ctx, nodeURL, 5, spectypes.APIInterfaceRest)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		if upstream.hang {
			release := make(chan struct{})
			releases = append(releases, sync.OnceFunc(func() { close(release) }))
			doer, ok := conn.(routersession.HTTPDirectRPCDoer)
			require.True(t, ok)
			conn = &heldCancelledRESTConnection{HTTPDirectRPCDoer: doer, cancelled: make(chan struct{}), release: release}
		}
		providerEndpoint := &routersession.Endpoint{
			NetworkAddress: nodeURL.Url, Enabled: true,
			DirectConnections: []routersession.DirectRPCConnection{conn},
		}
		provider := routersession.NewConsumerSessionWithProvider(upstream.name, []*routersession.Endpoint{providerEndpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(index)] = provider
	}

	sessionManager, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	require.NoError(t, sessionManager.UpdateAllProviders(1, providers, nil))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs: logs,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
	}
	listener := chainlib.NewRestChainListener(ctx, rpcEndpoint, server, nil, logs)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		assert.NoError(t, listener.Shutdown(shutdownCtx))
		select {
		case <-listenerDone:
		case <-shutdownCtx.Done():
			t.Error("REST listener did not stop")
		}
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)

	utils.EnableDebugLogBuffer(100000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	utils.ClearDebugLogBuffer()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+listener.GetListeningAddress()+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if anyHang {
		req.Header.Set(common.RELAY_TIMEOUT_HEADER_NAME, "200ms")
	}
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	outcome := restWriteOutcome{status: response.StatusCode}
	// REST keeps the GUID envelope as a JSON string inside its error field.
	var restError struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &restError) == nil && restError.Error != "" {
		var envelope struct {
			Error string `json:"Error"`
		}
		require.NoError(t, json.Unmarshal([]byte(restError.Error), &envelope), "body=%s", body)
		outcome.clientError = envelope.Error
	}

	for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 100000) {
		record := map[string]any{}
		if json.Unmarshal(line, &record) != nil {
			continue
		}
		outcome.records = append(outcome.records, record)
		if record["event"] == relayEndEvent {
			outcome.relayEnd = record
		}
	}
	return outcome
}

// TestUnclearWriteSaysWhy pins the reason an unclear write gives for each cause, end to end through
// the REST listener: the reason travels to the client verbatim inside the router's error envelope.
func TestUnclearWriteSaysWhy(t *testing.T) {
	badGateway := `<html><body>502 Bad Gateway</body></html>`
	gatewayTimeout := `<html><body>504 Gateway Time-out</body></html>`
	for _, tc := range []struct {
		name       string
		upstreams  []restWriteUpstream
		wantReason string
	}{
		{
			// Already right before MAG-4380, and unchanged: nobody answered, the budget ran out.
			name: "every node silent until the budget",
			upstreams: []restWriteUpstream{
				{name: "silent-a", hang: true},
				{name: "silent-b", hang: true},
			},
			wantReason: "timeout reached before any node responded",
		},
		{
			// The U-lost shape: both nodes answered at once, both answers were lost on the way back.
			// Nothing timed out.
			name: "every answer lost after the transaction was sent",
			upstreams: []restWriteUpstream{
				{name: "lost-a", status: http.StatusBadGateway, body: badGateway},
				{name: "lost-b", status: http.StatusBadGateway, body: badGateway},
			},
			wantReason: "a node's answer was lost after the transaction was sent",
		},
		{
			name: "a single node's answer lost",
			upstreams: []restWriteUpstream{
				{name: "lost", status: http.StatusBadGateway, body: badGateway},
			},
			wantReason: "a node's answer was lost after the transaction was sent",
		},
		{
			// Both causes at once. Silence wins: the request spent its budget waiting on the node
			// that never answered, so that is what the client lived through, and either way the
			// advice is the same.
			name: "one node silent, the other's answer lost",
			upstreams: []restWriteUpstream{
				{name: "silent", hang: true},
				{name: "lost", status: http.StatusBadGateway, body: badGateway},
			},
			wantReason: "timeout reached before every node responded",
		},
		{
			// The U-01 shape: one node refused the transaction, the other took it and went silent.
			name: "one node rejected the transaction, the other silent",
			upstreams: []restWriteUpstream{
				{name: "rejects", status: http.StatusBadRequest, body: `{"code":3,"message":"invalid transaction"}`},
				{name: "silent", hang: true},
			},
			wantReason: "timeout reached before every node responded",
		},
		{
			// A 504 is the gateway saying the node behind it did not answer in time: a timeout one
			// hop further out, not an answer lost on the way back.
			name: "every node behind a gateway that timed out",
			upstreams: []restWriteUpstream{
				{name: "gateway-a", status: http.StatusGatewayTimeout, body: gatewayTimeout},
				{name: "gateway-b", status: http.StatusGatewayTimeout, body: gatewayTimeout},
			},
			wantReason: "timeout reached before any node responded",
		},
		{
			// A timeout beside a lost answer: the timeout wins, as silence does.
			name: "one gateway timed out, the other's answer lost",
			upstreams: []restWriteUpstream{
				{name: "gateway", status: http.StatusGatewayTimeout, body: gatewayTimeout},
				{name: "lost", status: http.StatusBadGateway, body: badGateway},
			},
			wantReason: "timeout reached before every node responded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := sendRESTWriteThroughListener(t, tc.upstreams)

			require.Equal(t, http.StatusInternalServerError, outcome.status, "an unclear write is a 500")
			require.True(t, strings.HasPrefix(outcome.clientError, unclearWritePrefix),
				"the verdict the client reads first stays the same: %q", outcome.clientError)
			require.True(t, strings.HasSuffix(outcome.clientError, unclearWriteSuffix),
				"the advice stays the same: %q", outcome.clientError)
			assert.Equal(t, unclearWritePrefix+tc.wantReason+unclearWriteSuffix, outcome.clientError,
				"the reason must follow the cause")

			// relay_end logs what the client got: the 500 and the message with its reason. U-01's
			// old "relay finished" line said status=200 for a reply the client got as a 500.
			require.NotNil(t, outcome.relayEnd, "the unclear write must end with a relay_end event")
			assert.Equal(t, relayOutcomeWriteUnknown, outcome.relayEnd["outcome"], "relay_end=%v", outcome.relayEnd)
			assert.Equal(t, "500", fmt.Sprint(outcome.relayEnd["status"]), "relay_end=%v", outcome.relayEnd)
			assert.Equal(t, unclearWritePrefix+tc.wantReason+unclearWriteSuffix, outcome.relayEnd["error"],
				"relay_end logs the message the client read, reason included: relay_end=%v", outcome.relayEnd)
		})
	}
}

// The reason is read off the live processor: which transport failures came back, and how each was
// classified. One row per failure through the real classifier, and each row checks the code its
// failure was given, so it tests the classification it names.
func TestUnclearWriteOutcomeFor_ReasonFollowsTheFailure(t *testing.T) {
	const (
		noNodeResponded = "timeout reached before any node responded"
		notEveryNode    = "timeout reached before every node responded"
		answerLost      = "a node's answer was lost after the transaction was sent"
		noAnswer        = "a request to a node ended without an answer"
	)
	dialTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	reset := errors.New("read tcp 127.0.0.1:443: connection reset by peer")
	unclassified := errors.New("zork")
	type failure struct {
		err  error
		code *common.RouterError
	}
	for _, tc := range []struct {
		name       string
		stop       string
		failures   []failure
		wantReason string
	}{
		// A dial that timed out never sent the transaction, and it reads exactly like a response
		// that timed out, so neither may be called an answer lost after sending.
		{name: "a dial that timed out", failures: []failure{{dialTimeout, common.RouterErrorConnectionTimeout}}, wantReason: noNodeResponded},
		{name: "the attempt's own deadline", failures: []failure{{context.DeadlineExceeded, common.RouterErrorContextDeadline}}, wantReason: noNodeResponded},
		{
			name: "cancelled because the budget ran out", stop: relaycore.StopReasonProcessingTimeout,
			failures: []failure{{context.Canceled, common.RouterErrorContextCanceled}}, wantReason: noNodeResponded,
		},
		{name: "a connection reset", failures: []failure{{reset, common.RouterErrorConnectionReset}}, wantReason: answerLost},
		{name: "an EOF", failures: []failure{{io.EOF, common.RouterErrorConnectionClosed}}, wantReason: answerLost},
		{
			name:       "a reset beside a timeout: the timeout wins",
			failures:   []failure{{reset, common.RouterErrorConnectionReset}, {dialTimeout, common.RouterErrorConnectionTimeout}},
			wantReason: notEveryNode,
		},
		// Neither a timeout nor a broken connection: the reason claims neither.
		{name: "cancelled with budget left", failures: []failure{{context.Canceled, common.RouterErrorContextCanceled}}, wantReason: noAnswer},
		{name: "an error nothing classifies", failures: []failure{{unclassified, common.RouterErrorUnknown}}, wantReason: noAnswer},
		{
			name:       "an unclassified error beside a reset: the lost answer wins",
			failures:   []failure{{unclassified, common.RouterErrorUnknown}, {reset, common.RouterErrorConnectionReset}},
			wantReason: answerLost,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
			processor := newWriteOutcomeProcessor(t, msg, len(tc.failures))
			if tc.stop != "" {
				processor.SetStopReason(tc.stop)
			}
			provider := func(i int) string { return "provider@" + string(rune('a'+i)) }
			for i, f := range tc.failures {
				relaycoretest.SendProtocolError(processor, provider(i), 0, f.err)
			}
			drain(t, processor)

			_, _, protocolErrors := processor.GetResultsData()
			require.Len(t, protocolErrors, len(tc.failures))
			classified := map[string]*common.RouterError{}
			for _, protocolError := range protocolErrors {
				classified[protocolError.ProviderInfo.ProviderAddress] = protocolError.RouterError
			}
			for i, f := range tc.failures {
				require.Same(t, f.code, classified[provider(i)], "%v must be classified as %s", f.err, f.code.Name)
			}

			unclear := unclearWriteOutcomeFor(msg, processor)
			require.NotNil(t, unclear, "every row is a write whose outcome is unknown")
			assert.Equal(t, unclearWritePrefix+tc.wantReason+unclearWriteSuffix, unclear.Error())
		})
	}
}

// TestUnclearWriteReason pins which reason each unknown outcome gives. The verdict itself is
// unknownWriteOutcome's, unchanged; the reason is only asked for once that verdict is "unknown".
func TestUnclearWriteReason(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		answered, dispatched int
		cutOffs              writeCutOffs
		want                 *unclearWriteError
	}{
		{name: "nothing came back before the budget", answered: 0, dispatched: 2, want: errUnknownWriteOutcome},
		{name: "one failed, one silent", answered: 1, dispatched: 2, want: errUnclearWriteNodeSilent},
		{name: "one cut off, one silent: silence wins", answered: 1, dispatched: 2, cutOffs: writeCutOffs{lost: 1}, want: errUnclearWriteNodeSilent},
		{name: "one timed out, one silent", answered: 1, dispatched: 2, cutOffs: writeCutOffs{timedOut: 1}, want: errUnknownWriteOutcome},
		{name: "every endpoint came back with a timeout", answered: 2, dispatched: 2, cutOffs: writeCutOffs{timedOut: 2}, want: errUnknownWriteOutcome},
		{name: "one timed out, one cut off: the timeout wins", answered: 2, dispatched: 2, cutOffs: writeCutOffs{timedOut: 1, lost: 1}, want: errUnclearWriteNodeSilent},
		{name: "every endpoint came back, one connection broke", answered: 2, dispatched: 2, cutOffs: writeCutOffs{lost: 1}, want: errUnclearWriteAnswerLost},
		{name: "every endpoint came back, nothing timed out or broke", answered: 2, dispatched: 2, want: errUnclearWriteNoAnswer},
		// The counts must count the same thing, and if they ever do not, the reason stays in range.
		{name: "more timeouts than endpoints", answered: 3, dispatched: 2, cutOffs: writeCutOffs{timedOut: 3}, want: errUnknownWriteOutcome},
		{name: "more answers than endpoints", answered: 3, dispatched: 2, want: errUnclearWriteNoAnswer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Same(t, tc.want, unclearWriteReason(tc.answered, tc.dispatched, tc.cutOffs))
		})
	}
}

// TestWriteCutOffs_Classification pins which failures an unclear reason reads as a timeout and which
// as a broken connection. The timeouts are relay_end's own (protocolErrorOutcome), so the attempts
// relay_end logs and the reason the client gets cannot disagree; a gateway's timeout is the one
// addition.
func TestWriteCutOffs_Classification(t *testing.T) {
	plain := errors.New("upstream failure")
	for _, tc := range []struct {
		name         string
		routerError  *common.RouterError
		err          error
		ranOutOfRoad bool
		relayEnd     string
		want         writeCutOffs
	}{
		{name: "connection timeout", routerError: common.RouterErrorConnectionTimeout, err: plain, relayEnd: attemptOutcomeTimeout, want: writeCutOffs{timedOut: 1}},
		{name: "context deadline", routerError: common.RouterErrorContextDeadline, err: plain, relayEnd: attemptOutcomeTimeout, want: writeCutOffs{timedOut: 1}},
		{name: "a raw deadline", routerError: common.RouterErrorUnknown, err: context.DeadlineExceeded, relayEnd: attemptOutcomeTimeout, want: writeCutOffs{timedOut: 1}},
		{name: "no response before the race cutoff", routerError: common.RouterErrorNoResponseTimeout, err: plain, relayEnd: attemptOutcomeTimeout, want: writeCutOffs{timedOut: 1}},
		{name: "cancelled by the budget", routerError: common.RouterErrorContextCanceled, err: context.Canceled, ranOutOfRoad: true, relayEnd: attemptOutcomeTimeout, want: writeCutOffs{timedOut: 1}},
		{name: "cancelled with budget left", routerError: common.RouterErrorContextCanceled, err: context.Canceled, relayEnd: attemptOutcomeCancelled},
		{name: "gateway timeout", routerError: common.RouterErrorNodeGatewayTimeout, err: plain, relayEnd: attemptOutcomeTransportError, want: writeCutOffs{timedOut: 1}},
		{name: "connection reset", routerError: common.RouterErrorConnectionReset, err: plain, relayEnd: attemptOutcomeTransportError, want: writeCutOffs{lost: 1}},
		{name: "EOF", routerError: common.RouterErrorConnectionClosed, err: plain, relayEnd: attemptOutcomeTransportError, want: writeCutOffs{lost: 1}},
		{name: "bad gateway", routerError: common.RouterErrorNodeBadGateway, err: plain, relayEnd: attemptOutcomeTransportError, want: writeCutOffs{lost: 1}},
		{name: "unclassified", routerError: common.RouterErrorUnknown, err: plain, relayEnd: attemptOutcomeTransportError},
		{name: "connection refused", routerError: common.RouterErrorConnectionRefused, err: plain, relayEnd: attemptOutcomeTransportError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			protocolError := relaycore.RelayError{Err: tc.err, RouterError: tc.routerError}
			require.Equal(t, tc.relayEnd, protocolErrorOutcome(protocolError, tc.ranOutOfRoad), "what relay_end logs for the attempt")
			var got writeCutOffs
			got.add(protocolError, tc.ranOutOfRoad)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestUnclearWriteErrorsReachTheClientVerbatim is TestUnknownWriteOutcomeReachesTheClientVerbatim
// for every reason: each message reaches the client exactly as written, redaction included, reads
// the same around its reason, and never says the write failed.
func TestUnclearWriteErrorsReachTheClientVerbatim(t *testing.T) {
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	for _, unclear := range []*unclearWriteError{errUnknownWriteOutcome, errUnclearWriteNodeSilent, errUnclearWriteAnswerLost, errUnclearWriteNoAnswer} {
		t.Run(unclear.reason, func(t *testing.T) {
			var body struct {
				Error string `json:"Error"`
			}
			require.NoError(t, json.Unmarshal([]byte(logger.GetUniqueGuidResponseForError(unclear, "1234567890")), &body))
			require.Equal(t, unclear.Error(), body.Error)
			require.Equal(t, unclearWritePrefix+unclear.reason+unclearWriteSuffix, body.Error)
			require.NotContains(t, strings.ToLower(body.Error), "failed",
				"a write we cannot prove undelivered must not be reported as failed")
		})
	}
}
