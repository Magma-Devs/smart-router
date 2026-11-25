package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// countedError is one increment of smartrouter_errors_total, as the callback sees it.
type countedError struct {
	name, chainID, apiInterface string
	source                      common.ErrorSource
}

// captureCountedErrors records every increment of smartrouter_errors_total until the test ends.
func captureCountedErrors(t *testing.T) func() []countedError {
	t.Helper()
	var mu sync.Mutex
	var counted []countedError
	common.SetErrorMetricsCallback(func(_ uint32, name, _ string, _ bool, chainID, apiInterface string, source common.ErrorSource) {
		mu.Lock()
		defer mu.Unlock()
		counted = append(counted, countedError{name: name, chainID: chainID, apiInterface: apiInterface, source: source})
	})
	t.Cleanup(func() { common.SetErrorMetricsCallback(nil) })
	return func() []countedError {
		mu.Lock()
		defer mu.Unlock()
		return append([]countedError(nil), counted...)
	}
}

type reportedFrame struct {
	method, upstream string
	nodeCode         int
	errorName        string
}

// PRD §5.7: an error frame the node sends on an open subscription builds a record, takes its verdict
// through the order, and is counted with source="subscription" and in relay_end. The subscription
// ends as it always did (TestWebSocketSubscriptionTheNodeEndsKeepsTheSocket).
func TestSubscriptionErrorFrameIsCountedAsTheSubscriptions(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = time.Hour
	t.Cleanup(srv.Close)
	manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", "ETH1", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil)
	t.Cleanup(manager.Close)
	manager.SetEndpointNames(map[string]string{srv.URL(): "node-ws"})
	var mu sync.Mutex
	var reported []reportedFrame
	manager.SetErrorFrameReporter(func(method, upstream string, nodeErr *rpcclient.JsonError, routerError *common.RouterError) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, reportedFrame{method: method, upstream: upstream, nodeCode: nodeErr.Code, errorName: routerError.Name})
	})
	h := &releaseHarness{t: t, srv: srv, manager: manager}
	client := dialIngress(t, manager)
	counted := captureCountedErrors(t)

	subscribeOverSocket(t, client, 1, "newHeads")
	require.Empty(t, counted(), "a subscribe the node acknowledged counts nothing")
	srv.writeToRouter(t, func(write func([]byte)) {
		frame, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params":  map[string]any{"subscription": srv.subscriptionID, "result": nil},
			"error":   map[string]any{"code": -32000, "message": "subscription ended by the node"},
		})
		require.NoError(t, err)
		write(frame)
	})
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the node ended the subscription")

	require.Eventually(t, func() bool { return len(counted()) == 1 }, 2*time.Second, 5*time.Millisecond)
	got := counted()[0]
	require.Equal(t, common.ErrorSourceSubscription, got.source)
	require.Equal(t, spectypes.APIInterfaceJsonRPC, got.apiInterface)
	require.Equal(t, "ETH1", got.chainID)
	want := subscriptionNodeErrorVerdict("ETH1", spectypes.APIInterfaceJsonRPC, "eth_subscribe", false,
		&rpcclient.JsonError{Code: -32000, Message: "subscription ended by the node"})
	require.Equal(t, want.Name, got.name, "the verdict the order gives the frame's record")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []reportedFrame{{method: "eth_subscribe", upstream: "node-ws", nodeCode: -32000, errorName: got.name}}, reported,
		"relay_end writes the frame once, under the same name the counter recorded")
}

// PRD §5.7: a subscribe the node refuses is counted with source="subscription", once, and the url's
// health counts the refusal as before (noteSubscribeFailure, unchanged).
func TestRefusedSubscribeIsCountedAsTheSubscriptions(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.rejectSubscribes.Store(true)
	t.Cleanup(srv.Close)
	manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", "ETH1", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil)
	t.Cleanup(manager.Close)
	client := dialIngress(t, manager)
	counted := captureCountedErrors(t)

	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "eth_subscribe", "params": []any{"newHeads"}, "id": 1})
	require.NoError(t, err)
	require.NoError(t, client.WriteMessage(websocket.TextMessage, request))
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, reply, err := client.ReadMessage()
	require.NoError(t, err)
	require.Contains(t, string(reply), "upstream rejected subscribe", "the client is refused")

	require.Eventually(t, func() bool { return len(counted()) == 1 }, 2*time.Second, 5*time.Millisecond)
	got := counted()[0]
	want := subscriptionNodeErrorVerdict("ETH1", spectypes.APIInterfaceJsonRPC, "eth_subscribe", false,
		&rpcclient.JsonError{Code: -32601, Message: "the method eth_subscribe does not exist"})
	require.Equal(t, countedError{name: want.Name, chainID: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, source: common.ErrorSourceSubscription}, got)
}

// The relay_end of an error frame: no request is in flight, so it names no GUID; the provider that
// ended the subscription is its one attempt, a node error under the counted name.
func TestRelayEnd_SubscriptionErrorFrame(t *testing.T) {
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t))
	nodeErr := &rpcclient.JsonError{Code: -32000, Message: "subscription ended by the node"}
	routerError := subscriptionNodeErrorVerdict("ETH1", spectypes.APIInterfaceJsonRPC, "eth_subscribe", false, nodeErr)

	end := server.buildSubscriptionErrorFrameRelayEnd("eth_subscribe", "node-ws", nodeErr, routerError)
	require.Empty(t, end.guid)
	require.Equal(t, relayEndSelectionSubscription, end.selection)
	require.Equal(t, relayOutcomeNodeError, end.outcome)
	require.Equal(t, "node-ws", end.servedBy)
	require.Equal(t, routerError.Name, end.errorCode)
	require.Len(t, end.attempts, 1)
	require.Equal(t, attemptOutcomeNodeError, end.attempts[0].outcome)
	require.Equal(t, -32000, end.attempts[0].nodeCode)
	require.Equal(t, routerError.Name, end.attempts[0].errorCode)
}

// The boot checks' errors are counted, as they were, but with source="boot", so the SLA's
// source=~"client|" leaves them out; a client's request after boot is a client's. Measured on main
// before MAG-5598: a url that refuses connections adds PROTOCOL_CONNECTION_REFUSED at boot with no
// client request at all.
func TestBootChecksCountTheirErrorsAsBoot(t *testing.T) {
	counted := captureCountedErrors(t)
	up := newFakeUpstreams(t)
	server := bootETH1Endpoint(t, []*routersession.RPCStaticProviderEndpoint{
		{Name: "healthy", ChainID: "ETH1", ApiInterface: "jsonrpc", NodeUrls: []common.NodeUrl{{Url: up.httpURL("/e/" + upArchive)}}},
		{Name: "refused", ChainID: "ETH1", ApiInterface: "jsonrpc", NodeUrls: []common.NodeUrl{{Url: "http://127.0.0.1:1/"}}},
	}, nil, nil)

	atBoot := counted()
	require.NotEmpty(t, atBoot, "the refused url's boot checks are counted")
	for _, c := range atBoot {
		require.Equal(t, common.ErrorSourceBoot, c.source, "%+v", c)
		require.Equal(t, spectypes.APIInterfaceJsonRPC, c.apiInterface, "%+v", c)
		require.Equal(t, "ETH1", c.chainID, "%+v", c)
	}

	relayGetBalance(t, server)
	require.Eventually(t, func() bool { return len(counted()) > len(atBoot) }, 2*time.Second, 5*time.Millisecond)
	for _, c := range counted()[len(atBoot):] {
		require.Equal(t, common.ErrorSourceClient, c.source, "the client's request: %+v", c)
		require.Equal(t, spectypes.APIInterfaceJsonRPC, c.apiInterface, "%+v", c)
	}
}

// relayGetBalance sends one eth_getBalance through the listener, as a client would. The fake
// upstreams answer it with -32601, so it is counted.
func relayGetBalance(t *testing.T, server *RPCSmartRouterServer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body := `{"jsonrpc":"2.0","id":7,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","latest"]}`
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+server.GetListeningAddress()+"/", strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, response.Body)
	require.NoError(t, response.Body.Close())
}
