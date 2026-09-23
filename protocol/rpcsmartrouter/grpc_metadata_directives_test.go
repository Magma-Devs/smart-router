package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

const grpcDirectivesTestMethod = "sui.rpc.v2.LedgerService/GetServiceInfo"

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
	parser, err := chainlib.NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "SUIT", ApiInterface: spectypes.APIInterfaceGrpc}
	sender := &parsingRelaySender{rpcss: &RPCSmartRouterServer{chainParser: parser, listenEndpoint: endpoint}}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	listener := chainlib.NewGrpcChainListener(ctx, endpoint, sender, healthyReporter{}, logger, parser)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})

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

// A gRPC client pins a call the same way an HTTP client does: lava-select-provider
// travels as call metadata, the gRPC listener hands its metadata to the same
// directive parser as an HTTP header, and the pin reaches resolvePinDirectives. What
// the session manager does with a pin is covered in lavasession; this guards the
// listener-to-directive seam no other test crosses for gRPC.
func TestGrpcMetadataPinReachesPinResolution(t *testing.T) {
	conn, sender := dialGrpcDirectivesListener(t)

	invokeWithMetadata(t, conn, common.SELECT_PROVIDER_HEADER_NAME, "upstream-b")

	directives := sender.only(t).GetDirectiveHeaders()
	require.Equal(t, "upstream-b", directives[common.SELECT_PROVIDER_HEADER_NAME],
		"a pin sent as gRPC metadata must land in the directive map")
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
// public Directives page tells gRPC clients to send them all that way. Ranging over
// the registry means a directive added later is covered without touching this test.
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

			require.Equal(t, value, sender.only(t).GetDirectiveHeaders()[name],
				"a directive sent as gRPC metadata must land in the directive map")
		})
	}
}
