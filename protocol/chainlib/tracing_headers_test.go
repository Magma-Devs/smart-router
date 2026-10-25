package chainlib

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// MAG-3798: a caller's X-Request-Id, X-Task-Id and X-Tx-Id must be on the context the
// relay runs under on every interface, and on a log line a router at --log-level info
// writes. JSON-RPC and REST did both from the start; the ...ReachTheRelay tests cover the
// two interfaces that did not, Tendermint RPC and gRPC, and each carries a control request
// without the headers, so a test cannot pass by comparing two blanks.

// tracingIds is what a relay sender sees of the caller's tracing headers.
type tracingIds struct {
	requestId, taskId, txId string
}

func tracingIdsFromContext(ctx context.Context) tracingIds {
	var ids tracingIds
	ids.requestId, _ = utils.GetRequestId(ctx)
	ids.taskId, _ = utils.GetTaskId(ctx)
	ids.txId, _ = utils.GetTxId(ctx)
	return ids
}

var callerTracingIds = tracingIds{requestId: "req-42", taskId: "task-7", txId: "tx-9"}

// httpTracingEntryPoint is one HTTP route that reads the caller's tracing headers.
type httpTracingEntryPoint struct {
	name, apiInterface, method, path, body string
	newListener                            func(context.Context, *lavasession.RPCEndpoint, RelaySender, *metrics.RPCConsumerLogs) ChainListener
}

// httpTracingEntryPoints lists every HTTP route that reads the tracing headers: JSON-RPC's and
// REST's from the start, Tendermint RPC's two since MAG-3798.
func httpTracingEntryPoints() []httpTracingEntryPoint {
	newJsonRPC := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewJrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	}
	newRest := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewRestChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	}
	newTendermint := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewTendermintRpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	}
	return []httpTracingEntryPoint{
		{"tendermintrpc POST", spectypes.APIInterfaceTendermintRPC, http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"status","params":{}}`, newTendermint},
		{"tendermintrpc GET", spectypes.APIInterfaceTendermintRPC, http.MethodGet, "/status?height=7", "", newTendermint},
		{"jsonrpc POST", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`, newJsonRPC},
		{"rest POST", spectypes.APIInterfaceRest, http.MethodPost, "/cosmos/tx/v1beta1/simulate", "{}", newRest},
		{"rest GET", spectypes.APIInterfaceRest, http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/latest", "", newRest},
	}
}

// serve starts the entry point's listener on an ephemeral port in front of relay and returns
// its address. The listener stops with ctx.
func (ep httpTracingEntryPoint) serve(t *testing.T, ctx context.Context, relay RelaySender) string {
	t.Helper()
	// GenerateUniqueIdentifier uses the custom rand package, which the package-level
	// TestMain does not seed. InitRandomSeed is idempotent.
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener := ep.newListener(ctx, &lavasession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "LAV1",
		ApiInterface:    ep.apiInterface,
		HealthCheckPath: common.DEFAULT_HEALTH_PATH,
	}, relay, logger)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	addr := ""
	for deadline := time.Now().Add(3 * time.Second); addr == "" && time.Now().Before(deadline); {
		if addr = listener.GetListeningAddress(); addr == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")
	return addr
}

// newRequest builds a request to the entry point's route at addr, carrying ids as the
// caller's tracing headers.
func (ep httpTracingEntryPoint) newRequest(t *testing.T, ctx context.Context, addr string, ids tracingIds) *http.Request {
	t.Helper()
	var body io.Reader
	if ep.body != "" {
		body = strings.NewReader(ep.body)
	}
	req, err := http.NewRequestWithContext(ctx, ep.method, "http://"+addr+ep.path, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", ids.requestId)
	req.Header.Set("X-Task-Id", ids.taskId)
	req.Header.Set("X-Tx-Id", ids.txId)
	return req
}

func TestTendermintRpcChainListener_TracingHeadersReachTheRelay(t *testing.T) {
	serveCtx, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	stub := &tendermintGetRelayStub{}
	_, addr := startTestTendermintListenerWithOptions(t, serveCtx, common.DEFAULT_HEALTH_PATH, stub)
	httpClient := &http.Client{Timeout: 2 * time.Second}

	send := func(t *testing.T, req *http.Request, withIds bool) tracingIds {
		t.Helper()
		if withIds {
			req.Header.Set("X-Request-Id", callerTracingIds.requestId)
			req.Header.Set("X-Task-Id", callerTracingIds.taskId)
			req.Header.Set("X-Tx-Id", callerTracingIds.txId)
		}
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		seen, ok := stub.lastTracing()
		require.True(t, ok, "the request must have reached the relay path")
		return seen
	}
	post := func(t *testing.T) *http.Request {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"status","params":{}}`))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		return req
	}
	get := func(t *testing.T) *http.Request {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/status?height=7", nil)
		require.NoError(t, err)
		return req
	}

	t.Run("POST carries the ids", func(t *testing.T) {
		require.Equal(t, callerTracingIds, send(t, post(t), true))
	})
	t.Run("URI-style GET carries the ids", func(t *testing.T) {
		require.Equal(t, callerTracingIds, send(t, get(t), true))
	})
	t.Run("a request without the headers leaves the context bare", func(t *testing.T) {
		require.Equal(t, tracingIds{}, send(t, post(t), false))
		require.Equal(t, tracingIds{}, send(t, get(t), false))
	})
}

// startTestGrpcListener serves a gRPC listener on an ephemeral port through the same h2c
// proxy production uses, so the ids travel the wire as gRPC metadata rather than being
// planted on a context by hand.
func startTestGrpcListener(t *testing.T, sender *stubRelaySender) string {
	t.Helper()
	// GenerateUniqueIdentifier uses the custom rand package, which the package-level
	// TestMain does not seed. InitRandomSeed is idempotent.
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	endpoint := &lavasession.RPCEndpoint{
		NetworkAddress: "127.0.0.1:0",
		ChainID:        "SUIT",
		ApiInterface:   spectypes.APIInterfaceGrpc,
	}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener := NewGrpcChainListener(context.Background(), endpoint, sender, alwaysHealthyReporter{}, logger, sender.parser)
	go listener.Serve(context.Background(), common.ConsumerCmdFlags{HeadersFlag: "*", OriginFlag: "*", MethodsFlag: "GET,POST,OPTIONS", CDNCacheDuration: "86400"})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = listener.Shutdown(shutdownCtx)
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := listener.GetListeningAddress(); addr != "" {
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("grpc listener never reported a listening address")
	return ""
}

// The unary path is exercised over the wire because that is where the casing changes:
// the client attaches X-Request-Id and the listener reads x-request-id, as gRPC lowers
// metadata keys in transit.
func TestGrpcChainListener_TracingHeadersReachTheRelay(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription(), reply: []byte("checkpoint")}
	addr := startTestGrpcListener(t, sender)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	call := func(t *testing.T, ctx context.Context) tracingIds {
		t.Helper()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var reply []byte
		err := conn.Invoke(ctx, "/"+unaryApiName, []byte("{}"), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true))
		require.NoError(t, err)
		require.Equal(t, []byte("checkpoint"), reply, "the relay's reply must reach the caller")
		seen, ok := sender.lastTracing()
		require.True(t, ok, "the call must have reached the relay sender")
		return seen
	}

	t.Run("ids sent as metadata carry to the relay", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(),
			"X-Request-Id", callerTracingIds.requestId,
			"X-Task-Id", callerTracingIds.taskId,
			"X-Tx-Id", callerTracingIds.txId,
		)
		require.Equal(t, callerTracingIds, call(t, ctx))
	})
	t.Run("a call without them leaves the context bare", func(t *testing.T) {
		require.Equal(t, tracingIds{}, call(t, context.Background()))
	})
}

// The streaming entry point reads its metadata before the unary callback ever runs, so it
// needs its own stamp: the ids must be on the context ParseRelay classifies under and on
// the one the subscription is started with.
func TestStreamRelayCallback_TracingHeadersReachTheRelay(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription()}
	listener := newStreamingListener(t, sender)
	manager := newStubGRPCSubscriptionManager()

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-request-id", callerTracingIds.requestId,
		"x-task-id", callerTracingIds.taskId,
		"x-tx-id", callerTracingIds.txId,
	))
	response, err := listener.makeStreamRelayCallback(manager)(ctx, streamingApiName, []byte("{}"))
	require.NoError(t, err)
	require.NotNil(t, response)

	seen, ok := sender.lastTracing()
	require.True(t, ok)
	require.Equal(t, callerTracingIds, seen, "ParseRelay must run under the stamped context")
	require.Equal(t, callerTracingIds, manager.tracing, "StartSubscription must run under the stamped context")
}

// Every tenant runs --log-level info, so ids that reach only a debug line cannot be found: the
// MAG-3798 caller sends their id, gets an answer, and cannot find the request in our logs.
// JSON-RPC and REST always logged the ids on an info line, while Tendermint RPC's and gRPC's
// ingress lines were debug. Every entry point must put a successful request's ids on an info
// line.
func TestListeners_TracingIdsReachAnInfoLine(t *testing.T) {
	// The ring records every level, so a record's own level says whether a router at
	// --log-level info would have written it.
	utils.EnableDebugLogBuffer(1000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	utils.ClearDebugLogBuffer()

	idsFor := func(entryPoint string) tracingIds {
		entryPoint = strings.ReplaceAll(entryPoint, " ", "-")
		return tracingIds{requestId: "req-" + entryPoint, taskId: "task-" + entryPoint, txId: "tx-" + entryPoint}
	}
	requireInfoLine := func(t *testing.T, ids tracingIds) {
		t.Helper()
		lines := utils.ReadDebugLogBuffer(ids.requestId, time.Time{}, time.Time{}, 0)
		require.NotEmpty(t, lines, "no line at any level carries request_id %q", ids.requestId)
		for _, line := range lines {
			record := map[string]any{}
			require.NoError(t, json.Unmarshal(line, &record))
			if record["level"] == "info" && record[utils.KEY_TASK_ID] == ids.taskId && record[utils.KEY_TRANSACTION_ID] == ids.txId {
				return
			}
		}
		t.Fatalf("request_id %q is logged, but on no info line with its task and tx ids: a router at --log-level info writes none of it", ids.requestId)
	}

	for _, ep := range httpTracingEntryPoints() {
		t.Run(ep.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			addr := ep.serve(t, ctx, &contextKeepingRelay{})
			ids := idsFor(ep.name)
			resp, err := http.DefaultClient.Do(ep.newRequest(t, ctx, addr, ids))
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			requireInfoLine(t, ids)
		})
	}
	t.Run("grpc unary", func(t *testing.T) {
		addr := startTestGrpcListener(t, &stubRelaySender{parser: grpcParserWithSubscription(), reply: []byte("checkpoint")})
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()
		ids := idsFor("grpc unary")
		ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(),
			"X-Request-Id", ids.requestId,
			"X-Task-Id", ids.taskId,
			"X-Tx-Id", ids.txId,
		), 5*time.Second)
		defer cancel()
		var reply []byte
		require.NoError(t, conn.Invoke(ctx, "/"+unaryApiName, []byte("{}"), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true)))
		requireInfoLine(t, ids)
	})
	t.Run("grpc stream subscribe", func(t *testing.T) {
		listener := newStreamingListener(t, &stubRelaySender{parser: grpcParserWithSubscription()})
		ids := idsFor("grpc stream subscribe")
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			"x-request-id", ids.requestId,
			"x-task-id", ids.taskId,
			"x-tx-id", ids.txId,
		))
		response, err := listener.makeStreamRelayCallback(newStubGRPCSubscriptionManager())(ctx, streamingApiName, []byte("{}"))
		require.NoError(t, err)
		require.NotNil(t, response)
		requireInfoLine(t, ids)
	})
}
