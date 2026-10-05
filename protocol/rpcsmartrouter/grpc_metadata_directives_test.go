package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	grpcDirectivesTestMethod = "sui.rpc.v2.LedgerService/GetServiceInfo"
	// grpcDirectivesPassthroughHeader is an ordinary header the test spec forwards to the node.
	grpcDirectivesPassthroughHeader = "x-passthrough"
)

// parsingRelaySender stands in for the router behind a real gRPC listener: SendRelay
// runs the router's own ParseRelay on exactly what the listener handed over, keeps the
// protocol message, and answers with an empty body instead of relaying upstream.
type parsingRelaySender struct {
	rpcss *RPCSmartRouterServer

	mu     sync.Mutex
	parsed []chainlib.ProtocolMessage
}

func (s *parsingRelaySender) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	protocolMessage, err := s.rpcss.ParseRelay(ctx, url, req, connectionType, dappID, consumerIp, metadataValues)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.parsed = append(s.parsed, protocolMessage)
	s.mu.Unlock()
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte{}}}, nil
}

func (s *parsingRelaySender) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadataValues []pairingtypes.Metadata) (chainlib.ProtocolMessage, error) {
	return s.rpcss.ParseRelay(ctx, url, req, connectionType, dappID, consumerIp, metadataValues)
}

func (s *parsingRelaySender) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage chainlib.ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (s *parsingRelaySender) CancelSubscriptionContext(subscriptionKey string) {}

func (s *parsingRelaySender) only(t *testing.T) chainlib.ProtocolMessage {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Len(t, s.parsed, 1, "the call must reach the router exactly once")
	return s.parsed[0]
}

type healthyReporter struct{}

func (healthyReporter) IsHealthy() bool { return true }

// dialGrpcDirectivesListener starts a real gRPC listener in front of a router that
// parses with a one-method gRPC spec, and returns a client connection to it.
func dialGrpcDirectivesListener(t *testing.T) (*grpc.ClientConn, *parsingRelaySender) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	raw := `{
      "index": "SUIT", "name": "Sui Testnet", "enabled": true, "average_block_time": 222,
      "api_collections": [{
        "enabled": true,
        "collection_data": {"api_interface": "grpc", "internal_path": "", "type": "", "add_on": ""},
        "apis": [{"name": "` + grpcDirectivesTestMethod + `", "enabled": true, "compute_units": 10,
                  "category": {"deterministic": false, "stateful": 0}}]
      }]
    }`
	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(raw), &spec))
	// The spec passes one ordinary header and every directive's name through to the
	// node. ParseRelay takes the directives out before the spec's header rules run, so
	// none may reach the node all the same. Without these declarations the rules would
	// drop an undeclared directive anyway, and the "not forwarded" asserts could not
	// fail; the ordinary header shows the forwarding path is live.
	collection := spec.ApiCollections[0]
	collection.Headers = append(collection.Headers, &spectypes.Header{Name: grpcDirectivesPassthroughHeader, Kind: spectypes.Header_pass_send})
	for name := range common.SPECIAL_LAVA_DIRECTIVE_HEADERS {
		collection.Headers = append(collection.Headers, &spectypes.Header{Name: name, Kind: spectypes.Header_pass_send})
	}
	parser, err := chainlib.NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "SUIT", ApiInterface: spectypes.APIInterfaceGrpc}
	sender := &parsingRelaySender{rpcss: &RPCSmartRouterServer{chainParser: parser, listenEndpoint: endpoint}}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	listener := chainlib.NewGrpcChainListener(ctx, endpoint, sender, healthyReporter{}, logger, parser)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	// Serve does not watch ctx; only Shutdown stops it. Registered before the client
	// connection, so the client is closed first.
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		assert.NoError(t, listener.Shutdown(shutdownCtx))
		select {
		case <-listenerDone:
		case <-shutdownCtx.Done():
			t.Error("gRPC listener did not stop")
		}
	})

	var addr string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && addr == "" {
		addr = listener.GetListeningAddress()
		time.Sleep(20 * time.Millisecond)
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn, sender
}

func invokeWithMetadata(t *testing.T, conn *grpc.ClientConn, kv ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, kv...)
	require.NoError(t, conn.Invoke(ctx, "/"+grpcDirectivesTestMethod, &emptypb.Empty{}, &emptypb.Empty{}))
}

// forwardedHeaders returns the metadata the router would send the node for this call:
// sendGRPCRelay forwards the RPC message's headers.
func forwardedHeaders(protocolMessage chainlib.ProtocolMessage) map[string]string {
	forwarded := map[string]string{}
	for _, header := range protocolMessage.GetRPCMessage().GetHeaders() {
		forwarded[strings.ToLower(header.Name)] = header.Value
	}
	return forwarded
}

// A gRPC client pins a unary call the same way an HTTP client does: lava-select-provider
// travels as call metadata, the gRPC listener hands its metadata to the same
// directive parser as an HTTP header, the pin is kept out of the metadata the node
// receives, and it reaches resolvePinDirectives. What the session manager does with a
// pin is covered in lavasession; this guards the listener-to-directive seam no other
// test crosses for gRPC.
//
// Unary calls only. A server-streaming subscription parses the pin into the same
// directive map, but DirectGRPCSubscriptionManager picks its upstream without reading
// it (selectEndpoint: the stream's sticky claim, then the optimizer), so a pinned
// subscription is not pinned.
func TestGrpcMetadataPinReachesPinResolution(t *testing.T) {
	conn, sender := dialGrpcDirectivesListener(t)

	invokeWithMetadata(t, conn, common.SELECT_PROVIDER_HEADER_NAME, "upstream-b", grpcDirectivesPassthroughHeader, "kept")

	parsed := sender.only(t)
	directives := parsed.GetDirectiveHeaders()
	require.Equal(t, "upstream-b", directives[common.SELECT_PROVIDER_HEADER_NAME],
		"a pin sent as gRPC metadata must land in the directive map")
	forwarded := forwardedHeaders(parsed)
	require.Equal(t, "kept", forwarded[grpcDirectivesPassthroughHeader], "a header the spec passes must reach the node")
	require.NotContains(t, forwarded, common.SELECT_PROVIDER_HEADER_NAME, "the pin is for the router and must not reach the node")
	selected, _ := resolvePinDirectives(context.Background(), directives, true)
	require.Equal(t, "upstream-b", selected, "the first attempt must be pinned to the named upstream")
}

func TestGrpcMetadataWithoutPinLeavesSelectionToTheRouter(t *testing.T) {
	conn, sender := dialGrpcDirectivesListener(t)

	invokeWithMetadata(t, conn, "x-unrelated-header", "value")

	directives := sender.only(t).GetDirectiveHeaders()
	require.NotContains(t, directives, common.SELECT_PROVIDER_HEADER_NAME)
	selected, _ := resolvePinDirectives(context.Background(), directives, true)
	require.Empty(t, selected, "an unpinned call must leave the choice to the router")
}

// Every registered directive is read off gRPC metadata, not just the pin — the
// public Directives page tells gRPC clients to send them all that way — and none is
// forwarded to the node. Ranging over the registry means a directive added later is
// covered without touching this test.
func TestGrpcMetadataCarriesEveryDirective(t *testing.T) {
	values := map[string]string{
		common.RELAY_TIMEOUT_HEADER_NAME: "12s",
		// An extension the spec doesn't declare; the directive is still read.
		common.EXTENSION_OVERRIDE_HEADER_NAME: "archive",
	}
	for name := range common.SPECIAL_LAVA_DIRECTIVE_HEADERS {
		t.Run(name, func(t *testing.T) {
			conn, sender := dialGrpcDirectivesListener(t)
			value, ok := values[name]
			if !ok {
				value = "directive-value"
			}

			invokeWithMetadata(t, conn, name, value)

			parsed := sender.only(t)
			require.Equal(t, value, parsed.GetDirectiveHeaders()[name],
				"a directive sent as gRPC metadata must land in the directive map")
			require.NotContains(t, forwardedHeaders(parsed), name, "a directive is for the router and must not reach the node")
		})
	}
}
