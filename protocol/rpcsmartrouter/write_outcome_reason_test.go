package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
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
	providers := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
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
		conn, err := lavasession.NewDirectRPCConnection(ctx, nodeURL, 5, spectypes.APIInterfaceRest)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		if upstream.hang {
			release := make(chan struct{})
			releases = append(releases, sync.OnceFunc(func() { close(release) }))
			doer, ok := conn.(lavasession.HTTPDirectRPCDoer)
			require.True(t, ok)
			conn = &heldCancelledRESTConnection{HTTPDirectRPCDoer: doer, cancelled: make(chan struct{}), release: release}
		}
		providerEndpoint := &lavasession.Endpoint{
			NetworkAddress: nodeURL.Url, Enabled: true,
			DirectConnections: []lavasession.DirectRPCConnection{conn},
		}
		provider := lavasession.NewConsumerSessionWithProvider(upstream.name, []*lavasession.Endpoint{providerEndpoint}, 100000, 1, 1)
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

// TestUnclearWriteOutcomeReason pins which reason each unknown outcome gives, and that the reason
// and the yes/no verdict never disagree.
func TestUnclearWriteOutcomeReason(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		successes, answered, dispatched int
		ranOutOfRoad, cutOff            bool
		want                            *unclearWriteError
	}{
		{name: "nothing dispatched", dispatched: 0, ranOutOfRoad: true, want: nil},
		{name: "a success", successes: 1, answered: 2, dispatched: 2, cutOff: true, want: nil},
		{name: "nothing recorded, budget spent", answered: 0, dispatched: 2, ranOutOfRoad: true, want: errUnknownWriteOutcome},
		{name: "nothing recorded, budget not spent", answered: 0, dispatched: 2, want: nil},
		{name: "one failed, one silent", answered: 1, dispatched: 2, ranOutOfRoad: true, want: errUnclearWriteNodeSilent},
		{name: "one cut off, one silent: silence wins", answered: 1, dispatched: 2, ranOutOfRoad: true, cutOff: true, want: errUnclearWriteNodeSilent},
		{name: "everyone came back, one cut off", answered: 2, dispatched: 2, cutOff: true, want: errUnclearWriteAnswerLost},
		{name: "everyone came back with a real reply", answered: 2, dispatched: 2, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := unclearWriteOutcome(tc.successes, tc.answered, tc.dispatched, tc.ranOutOfRoad, tc.cutOff)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.want != nil, unknownWriteOutcome(tc.successes, tc.answered, tc.dispatched, tc.ranOutOfRoad, tc.cutOff),
				"the verdict and its reason must agree")
		})
	}
}

// TestUnclearWriteErrorsReachTheClientVerbatim is TestUnknownWriteOutcomeReachesTheClientVerbatim
// for every reason: each message reaches the client exactly as written, redaction included, and
// reads the same around its reason.
func TestUnclearWriteErrorsReachTheClientVerbatim(t *testing.T) {
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	for _, unclear := range []*unclearWriteError{errUnknownWriteOutcome, errUnclearWriteNodeSilent, errUnclearWriteAnswerLost} {
		t.Run(unclear.reason, func(t *testing.T) {
			var body struct {
				Error string `json:"Error"`
			}
			require.NoError(t, json.Unmarshal([]byte(logger.GetUniqueGuidResponseForError(unclear, "1234567890")), &body))
			require.Equal(t, unclear.Error(), body.Error)
			require.Equal(t, unclearWritePrefix+unclear.reason+unclearWriteSuffix, body.Error)
		})
	}
}
