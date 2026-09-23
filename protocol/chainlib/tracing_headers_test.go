package chainlib

import (
	"context"
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
// relay runs under on every interface. JSON-RPC and REST read them from the start; these
// tests cover the two interfaces that did not, Tendermint RPC and gRPC, and each carries a
// control request without the headers, so a test cannot pass by comparing two blanks.

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
