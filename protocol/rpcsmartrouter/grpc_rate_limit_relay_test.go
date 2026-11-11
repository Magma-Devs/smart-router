package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// grpcStatusRelayWithMetadata is grpcStatusRelay for a status the node sent with response
// metadata: the error is built the way handleGRPCError builds it, so a rate limit only the
// metadata shows is typed on it.
func grpcStatusRelayWithMetadata(t *testing.T, code codes.Code, nodeMessage string, md map[string][]string) *common.RelayResult {
	t.Helper()
	body, err := json.Marshal(routersession.GRPCNodeErrorResponse{ErrorMessage: nodeMessage, ErrorCode: uint32(code)})
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: &fakeDirectConn{
			resp: &routersession.DirectRPCResponse{Data: body, StatusCode: int(code), Metadata: md},
			err:  routersession.NewGRPCStatusError(uint32(code), nodeMessage, nil, md),
		},
		endpointName: "grpc-endpoint",
		chainFamily:  common.ChainFamily(-1),
	}
	chainMessage := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: "cosmos.bank.v1beta1.Query/Balance"},
	}}
	result, relayErr := sender.sendGRPCRelay(context.Background(), chainMessage, time.Second)
	require.NoError(t, relayErr)
	require.NotNil(t, result)
	return result
}

// The gRPC rate-limit shapes the transport already typed as rate limits now reach the relay
// result as rate limits too: held off, not scored against the endpoint (MAG-4278 item 5). The wait
// the node asked for reaches it as well, since this relay returns no error to carry it (MAG-4914).
func TestSendGRPCRelay_RateLimitShapesAreHeldOffNotScored(t *testing.T) {
	cases := []struct {
		name           string
		code           codes.Code
		message        string
		md             map[string][]string
		wantRetryAfter time.Duration
	}{
		{"unavailable carrying an HTTP edge's 429", codes.Unavailable, "unexpected HTTP status code received from server: 429 (Too Many Requests)", nil, 0},
		{"resource exhausted saying too many requests", codes.ResourceExhausted, "too many requests", nil, 0},
		{"resource exhausted with only a retry delay", codes.ResourceExhausted, "quota", map[string][]string{"grpc-retry-pushback-ms": {"1500"}}, 1500 * time.Millisecond},
		{"too many requests with a retry-after", codes.ResourceExhausted, "too many requests", map[string][]string{"retry-after": {"2"}}, 2 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := grpcStatusRelayWithMetadata(t, tc.code, tc.message, tc.md)
			assert.True(t, result.IsNodeError)
			assert.True(t, result.IsRateLimited, "healthy but busy")
			assert.False(t, result.IsNodeAtFault, "not scored against the endpoint")
			assert.False(t, result.IsNonRetryable)
			assert.Equal(t, tc.wantRetryAfter, result.RetryAfter, "the wait the node asked for")
		})
	}
}

// RESOURCE_EXHAUSTED with neither a rate-limit text nor a retry delay stays unclassified: alone,
// grpc-go also mints the code for an oversized message.
func TestSendGRPCRelay_BareResourceExhaustedIsNotARateLimit(t *testing.T) {
	result := grpcStatusRelayWithMetadata(t, codes.ResourceExhausted, "grpc: received message larger than max", nil)
	assert.False(t, result.IsRateLimited)
	assert.Zero(t, result.RetryAfter)
}

// grpcRateLimitTestMethod is the one method the rate-limit router's spec serves. The relay sends it a
// binary body, which goes to the node unchanged (invokeRaw), so the node needs no schema.
const grpcRateLimitTestMethod = "test.v1.Node/Call"

// startRateLimitingGRPCNode answers every call with RESOURCE_EXHAUSTED and message, the way a gRPC
// vendor refuses a client, sending header as the reply's header metadata and trailer as its trailer
// metadata. Reflection is refused, so a background descriptor lookup learns nothing.
func startRateLimitingGRPCNode(t *testing.T, message string, header, trailer metadata.MD) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		if method, _ := grpc.MethodFromServerStream(stream); strings.Contains(method, "ServerReflection") {
			return status.Error(codes.Unimplemented, "no reflection")
		}
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		// Header metadata set before the status goes out in a header block of its own. With none,
		// the reply is trailers-only: a status-only refusal reaches the router with no header block.
		if err := stream.SetHeader(header); err != nil {
			return err
		}
		stream.SetTrailer(trailer)
		return status.Error(codes.ResourceExhausted, message)
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// newGRPCRateLimitRouter builds a router whose one provider is the gRPC node at address, and returns
// it with the keys the relay path records that provider's hold-off under.
func newGRPCRateLimitRouter(t *testing.T, address string) (server *RPCSmartRouterServer, provider, url string) {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	const chainID = "GRPCRL"
	raw := `{
	  "index": "` + chainID + `", "name": "gRPC rate limit", "enabled": true, "average_block_time": 1000,
	  "api_collections": [{
	    "enabled": true,
	    "collection_data": {"api_interface": "grpc", "internal_path": "", "type": "", "add_on": ""},
	    "apis": [{"name": "` + grpcRateLimitTestMethod + `", "enabled": true, "compute_units": 10,
	              "category": {"deterministic": false, "stateful": 0}}]
	  }]
	}`
	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(raw), &spec))
	parser, err := chainlib.NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	nodeURL := common.NodeUrl{Url: "grpc://" + address, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	connection, err := routersession.NewDirectRPCConnection(context.Background(), nodeURL, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	provider = "grpc-rate-limited"
	endpoint := &routersession.Endpoint{NetworkAddress: nodeURL.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
	providerSessions := routersession.NewConsumerSessionWithProvider(provider, []*routersession.Endpoint{endpoint}, 100000, 1, 1)
	providerSessions.StaticProvider = true

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: chainID, ApiInterface: spectypes.APIInterfaceGrpc}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, 1, nil, chainID)
	sessionManager := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	require.NoError(t, sessionManager.UpdateAllProviders(1, map[uint64]*routersession.ConsumerSessionsWithProvider{0: providerSessions}, nil))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	return &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        parser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     sessionManager,
		rpcSmartRouterLogs: logs,
	}, provider, nodeURL.Url
}

// relayGRPCCall sends one call through the router's whole relay path, as its gRPC listener does. The
// outcome is not the point: what the attempt left in the hold-off registry is.
func relayGRPCCall(t *testing.T, server *RPCSmartRouterServer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	protocolMessage, err := server.ParseRelay(ctx, grpcRateLimitTestMethod, string([]byte{0x08, 0x01}), "", "dapp", "1.2.3.4", nil)
	require.NoError(t, err)
	_, _ = server.SendParsedRelay(ctx, nil, protocolMessage)
}

// A gRPC upstream that rate-limits the router and says how long to wait is held off for that long,
// as an HTTP one is: "Retry-After is the floor" (docs/RATE-LIMIT-HOLDOFF.md). Its refusal completes
// the relay as a node error, so the wait used to die with the GRPCStatusError that carried it, and
// the endpoint was held off for the default first hold-off whatever it asked for (MAG-4914). The wait
// counts in either block of the reply: a status-only reply has no header block, and grpc-go's own
// retry reads grpc-retry-pushback-ms from the trailer, where the request path never looked
// (MAG-4915). Driven through the whole relay path: the hold-off is recorded where the attempt's
// session is released.
func TestRelay_GRPCRateLimitIsHeldOffForTheWaitItAskedFor(t *testing.T) {
	// Twice the default first hold-off, so a hold-off of the asked length cannot be the default.
	asked := 2 * holdoff.InitialHoldoff
	pushback := strconv.FormatInt(asked.Milliseconds(), 10)
	retryAfter := strconv.FormatInt(int64(asked/time.Second), 10)
	cases := []struct {
		name    string
		message string
		header  metadata.MD
		trailer metadata.MD
		// want is the hold-off the relay leaves; 0 is none at all.
		want time.Duration
	}{
		{"grpc-retry-pushback-ms in the header", "RESOURCE_EXHAUSTED", metadata.Pairs("grpc-retry-pushback-ms", pushback), nil, asked},
		{"retry-after in the header", "RESOURCE_EXHAUSTED", metadata.Pairs("retry-after", retryAfter), nil, asked},
		{"grpc-retry-pushback-ms in the trailer", "RESOURCE_EXHAUSTED", nil, metadata.Pairs("grpc-retry-pushback-ms", pushback), asked},
		{"retry-after in the trailer", "RESOURCE_EXHAUSTED", nil, metadata.Pairs("retry-after", retryAfter), asked},
		// The automation test's two controls: a rate limit that asks for nothing gets the default,
		// and a bare RESOURCE_EXHAUSTED is no rate limit at all (grpc-go mints it for an oversized
		// message too).
		{"rate-limit text with no wait", "Too many requests", nil, nil, holdoff.InitialHoldoff},
		{"bare RESOURCE_EXHAUSTED", "RESOURCE_EXHAUSTED", nil, nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A frozen clock and no jitter make the applied hold-off exact.
			now := time.Now()
			registry := holdoff.NewRegistryWithClock(func() time.Time { return now })
			restore := relayHoldoff
			relayHoldoff = registry
			t.Cleanup(func() { relayHoldoff = restore })

			server, provider, url := newGRPCRateLimitRouter(t, startRateLimitingGRPCNode(t, tc.message, tc.header, tc.trailer))
			relayGRPCCall(t, server)

			if tc.want == 0 {
				require.False(t, registry.HeldOff(provider, url), "a refusal that is no rate limit holds nothing off")
				return
			}
			require.True(t, registry.HeldOff(provider, url), "the rate limit holds the endpoint off")
			readyAt := registry.ReadyAt(provider, url)
			require.Equal(t, now.Add(tc.want), readyAt, "held off for %s, want %s", readyAt.Sub(now), tc.want)
		})
	}
}
