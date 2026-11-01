package rpcsmartrouter

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// relayEndCaller is what a caller sends that names it: the project-id header, and the tracing
// headers a listener stamps on the context. An empty one is a header the caller did not send.
type relayEndCaller struct {
	projectID, requestID, taskID, txID string
}

func (c relayEndCaller) context(ctx context.Context) context.Context {
	if c.requestID != "" {
		ctx = utils.WithRequestId(ctx, c.requestID)
	}
	if c.taskID != "" {
		ctx = utils.WithTaskId(ctx, c.taskID)
	}
	if c.txID != "" {
		ctx = utils.WithTxId(ctx, c.txID)
	}
	return ctx
}

// A request's event names its customer and the caller's tracing ids, each only when the caller sent
// it, on a request that did not parse too. A value is written as valid UTF-8, at most 128 bytes.
func TestRelayEnd_CustomerAndCallerIDs(t *testing.T) {
	rand.InitRandomSeed()
	eth := ethJsonRPCParser(t)
	longest := "demo-acme-" + strings.Repeat("€", 50) // 160 bytes; the 40th € straddles byte 128

	for _, tc := range []struct {
		name   string
		caller relayEndCaller
		body   string
	}{
		{"caller_ids", relayEndCaller{"demo-acme", "req-7f3a9c", "task-42", "tx-0009"}, relayEndRead},
		{"caller_ids_request_id_only", relayEndCaller{relayEndNoProjectID, "req-7f3a9c", "", ""}, relayEndRead},
		{"caller_ids_parse_failure", relayEndCaller{"demo-globex", "req-7f3a9c", "task-42", "tx-0009"}, `{"jsonrpc":"2.0","method":`},
		{"caller_ids_invalid_utf8", relayEndCaller{"demo-globex\xff\xfe-team", "", "task-\xc3", ""}, relayEndRead},
		{"caller_ids_too_long", relayEndCaller{longest, longest, "", ""}, relayEndRead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := captureRelayEnds(t)
			server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth, relayEndNode{"node-a", relayEndReply(200, relayEndBalance, 0)})
			guid := relayEndCaseGUID(tc.name)
			ctx, cancel := context.WithTimeout(tc.caller.context(utils.WithUniqueIdentifier(context.Background(), guid)), relayEndShortWait)
			defer cancel()
			_, _ = server.SendRelay(ctx, "", tc.body, http.MethodPost, tc.caller.projectID, "", nil, nil)
			requireRelayEndGolden(t, tc.name, normalizeRelayEnd(t, onlyRelayEnd(t, read(), guid), false))
		})
	}
}

func TestRelayEnd_CallerIDCleaning(t *testing.T) {
	require.Empty(t, relayEndCustomer(metrics.CustomerNone), "the listeners' default names no customer")
	require.Empty(t, relayEndCustomer(""))
	require.Equal(t, "demo-acme", relayEndCustomer("demo-acme"))

	require.Equal(t, "demo-globex�-team", relayEndID("demo-globex\xff\xfe-team"), "a run of invalid bytes is one U+FFFD")
	exact := strings.Repeat("a", relayEndIDLimit)
	require.Equal(t, exact, relayEndID(exact))
	require.Equal(t, exact, relayEndID(exact+"b"))
	for _, char := range []string{"é", "€", "𝄞"} {
		capped := relayEndID(strings.Repeat(char, relayEndIDLimit))
		require.True(t, utf8.ValidString(capped), char)
		require.LessOrEqual(t, len(capped), relayEndIDLimit, char)
		require.Greater(t, len(capped), relayEndIDLimit-len(char), "cut at the last rune boundary, %s", char)
	}
}

// The listeners put the headers where the event reads them: an HTTP request's project-id and tracing
// headers, and a WebSocket connection's project-id, which a frame that does not parse is reported
// with too.
func TestRelayEnd_CallerIDsFromTheListener(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	read := captureRelayEnds(t)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t), relayEndNode{"node-a", relayEndReply(200, relayEndBalance, 0)})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC, HealthCheckPath: common.DEFAULT_HEALTH_PATH}
	listener := chainlib.NewJrpcChainListener(ctx, endpoint, server, nil, logs, nil, NewNoOpWSSubscriptionManager("ETH1", spectypes.APIInterfaceJsonRPC))
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)
	address := listener.GetListeningAddress()

	t.Run("HTTP", func(t *testing.T) {
		seen := len(read())
		request, err := http.NewRequest(http.MethodPost, "http://"+address+"/", bytes.NewBufferString(relayEndRead))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(chainlib.ProjectIDHeader, "demo-acme")
		request.Header.Set("X-Request-Id", "req-7f3a9c")
		request.Header.Set("X-Task-Id", "task-42")
		request.Header.Set("X-Tx-Id", "tx-0009")
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		_ = response.Body.Close()

		events := newRelayEndsSince(read, seen)
		require.Len(t, events, 1)
		for field, want := range map[string]string{"customer": "demo-acme", "request_id": "req-7f3a9c", "task_id": "task-42", "tx_id": "tx-0009"} {
			require.Equal(t, want, relayEndField(t, events[0], field), field)
		}
	})

	t.Run("WebSocket", func(t *testing.T) {
		client, _, err := websocket.DefaultDialer.Dial("ws://"+address+"/ws", http.Header{chainlib.ProjectIDHeader: []string{"demo-globex"}})
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })

		seen := len(read())
		for _, frame := range []string{relayEndRead, `{"jsonrpc":"2.0","method":`} {
			require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte(frame)))
			_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, _, err := client.ReadMessage()
			require.NoError(t, err)
		}
		events := newRelayEndsSince(read, seen)
		require.Len(t, events, 2)
		require.Equal(t, relayOutcomeRejectedBeforeDispatch, relayEndField(t, events[1], "outcome"), "the case under test: a frame that does not parse")
		for i := range events {
			require.Equal(t, "demo-globex", relayEndField(t, events[i], "customer"), "frame %d", i)
		}
	})
}

// A subscription request is a relay of its own, and names its caller as any relay does.
func TestRelayEnd_SubscriptionCallerIDs(t *testing.T) {
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t))
	subscribe, err := server.ParseRelay(context.Background(), "", `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`, http.MethodPost, "demo-acme", "", nil)
	require.NoError(t, err)
	caller := relayEndCaller{requestID: "req-7f3a9c", taskID: "task-42", txID: "tx-0009"}
	ctx := chainlib.WithSubscriptionUpstream(caller.context(utils.WithUniqueIdentifier(context.Background(), 1)))
	chainlib.SubscriptionUpstreamFrom(ctx).Joined("node-ws")

	end := server.buildSubscriptionRelayEnd(ctx, subscribe, time.Now(), nil)
	require.Equal(t, []string{"demo-acme", "req-7f3a9c", "task-42", "tx-0009"}, []string{end.customer, end.requestID, end.taskID, end.txID})
}
