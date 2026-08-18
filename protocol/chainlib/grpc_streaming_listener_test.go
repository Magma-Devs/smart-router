package chainlib

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// --- test doubles -----------------------------------------------------------

// stubGRPCSubscriptionManager records what the listener asked of it and lets the
// test drive the reply channel.
type stubGRPCSubscriptionManager struct {
	replies       chan *pairingtypes.RelayReply
	startErr      error
	firstReply    *pairingtypes.RelayReply
	startCalls    int
	startedWith   string // clientKey passed to StartSubscription, via ClientKey
	unsubscribed  chan string
	lastChainName string
}

func newStubGRPCSubscriptionManager() *stubGRPCSubscriptionManager {
	return &stubGRPCSubscriptionManager{
		replies:      make(chan *pairingtypes.RelayReply, 8),
		unsubscribed: make(chan string, 4),
		firstReply: &pairingtypes.RelayReply{
			Data:     []byte(`{"subscription_id":"router-1","status":"STREAMING"}`),
			Metadata: []pairingtypes.Metadata{{Name: "x-lava-grpc-sub-id", Value: "router-1"}},
		},
	}
}

func (s *stubGRPCSubscriptionManager) StartSubscription(
	ctx context.Context,
	chainMessage ChainMessage,
	dappID string,
	consumerIp string,
	connectionUniqueId string,
	metricsData *metrics.RelayMetrics,
) (*pairingtypes.RelayReply, <-chan *pairingtypes.RelayReply, error) {
	s.startCalls++
	s.startedWith = s.ClientKey(dappID, consumerIp, connectionUniqueId)
	s.lastChainName = chainMessage.GetApi().Name
	if s.startErr != nil {
		return nil, nil, s.startErr
	}
	return s.firstReply, s.replies, nil
}

func (s *stubGRPCSubscriptionManager) UnsubscribeAll(ctx context.Context, clientKey string) error {
	s.unsubscribed <- clientKey
	return nil
}

func (s *stubGRPCSubscriptionManager) ClientKey(dappID, consumerIp, connectionUniqueId string) string {
	return dappID + ":" + consumerIp + ":" + connectionUniqueId
}

// stubRelaySender implements the slice of RelaySender the streaming callback uses.
type stubRelaySender struct {
	parser     *GrpcChainParser
	parseErr   error
	parseCalls int
}

func (s *stubRelaySender) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (s *stubRelaySender) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	s.parseCalls++
	if s.parseErr != nil {
		return nil, s.parseErr
	}
	chainMessage, err := s.parser.ParseMsg(url, []byte(req), connectionType, metadata, extensionslib.ExtensionInfo{LatestBlock: 0})
	if err != nil {
		return nil, err
	}
	return NewProtocolMessage(chainMessage, nil, nil, dappID, consumerIp), nil
}

func (s *stubRelaySender) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (s *stubRelaySender) CancelSubscriptionContext(subscriptionKey string) {}

// --- fixtures ---------------------------------------------------------------

const (
	streamingApiName = "sui.rpc.v2.SubscriptionService/SubscribeCheckpoints"
	unaryApiName     = "sui.rpc.v2.LedgerService/GetCheckpoint"
)

// grpcParserWithSubscription builds a parser whose collection holds one API marked
// `"subscription": true` and one that is not, so the classification split is real
// rather than mocked.
func grpcParserWithSubscription() *GrpcChainParser {
	collectionKey := CollectionKey{ConnectionType: ""}
	collection := &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGrpc},
	}
	return &GrpcChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{
				{Name: streamingApiName, ConnectionType: ""}: {
					api:           &spectypes.Api{Name: streamingApiName, Enabled: true, Category: spectypes.SpecCategory{Subscription: true, HangingApi: true}},
					collectionKey: collectionKey,
				},
				{Name: unaryApiName, ConnectionType: ""}: {
					api:           &spectypes.Api{Name: unaryApiName, Enabled: true},
					collectionKey: collectionKey,
				},
			},
			apiCollections: map[CollectionKey]*spectypes.ApiCollection{collectionKey: collection},
		},
	}
}

func newStreamingListener(t *testing.T, sender *stubRelaySender) *GrpcChainListener {
	t.Helper()
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	return &GrpcChainListener{
		endpoint:    &lavasession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGrpc},
		relaySender: sender,
		logger:      logger,
		chainParser: sender.parser,
	}
}

// --- tests ------------------------------------------------------------------

// TestIsGrpcSubscription pins the classification the whole change hangs on: it comes
// from the spec, so it is available whether or not upstream reflection answers.
func TestIsGrpcSubscription(t *testing.T) {
	parser := grpcParserWithSubscription()

	streaming, err := parser.ParseMsg(streamingApiName, []byte("{}"), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	require.True(t, IsGrpcSubscription(streaming), "an API marked subscription:true must classify as streaming")

	unary, err := parser.ParseMsg(unaryApiName, []byte("{}"), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	require.False(t, IsGrpcSubscription(unary), "an ordinary query must not classify as streaming")

	require.False(t, IsGrpcSubscription(nil), "a nil message must not classify as streaming")
}

// TestIsGrpcSubscription_OnlyGrpcInterface keeps the flag scoped to gRPC. WebSocket
// subscriptions are identified by the SUBSCRIBE function tag and must stay that way.
func TestIsGrpcSubscription_OnlyGrpcInterface(t *testing.T) {
	collectionKey := CollectionKey{ConnectionType: ""}
	parser := &JsonRPCChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{
				{Name: "eth_subscribe", ConnectionType: ""}: {
					api:           &spectypes.Api{Name: "eth_subscribe", Enabled: true, Category: spectypes.SpecCategory{Subscription: true}},
					collectionKey: collectionKey,
				},
			},
			apiCollections: map[CollectionKey]*spectypes.ApiCollection{collectionKey: {
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC},
			}},
		},
	}
	message, err := parser.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	require.False(t, IsGrpcSubscription(message), "the flag is gRPC-scoped; jsonrpc keeps using the SUBSCRIBE tag")
}

// TestHasSubscriptionApis decides whether the listener installs the streaming callback
// at all. Getting it wrong in one direction costs a wasted parse on every request; in
// the other it silently disables streaming for a chain that needs it.
func TestHasSubscriptionApis(t *testing.T) {
	require.True(t, grpcParserWithSubscription().HasSubscriptionApis(),
		"a spec declaring a subscription must enable the streaming callback")

	collectionKey := CollectionKey{ConnectionType: ""}
	unaryOnly := &GrpcChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{
				{Name: unaryApiName, ConnectionType: ""}: {
					api:           &spectypes.Api{Name: unaryApiName, Enabled: true},
					collectionKey: collectionKey,
				},
			},
			apiCollections: map[CollectionKey]*spectypes.ApiCollection{collectionKey: {
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGrpc},
			}},
		},
	}
	require.False(t, unaryOnly.HasSubscriptionApis(),
		"a spec with no subscriptions must not pay the per-request parse")
}

// TestStreamRelayCallback_StreamsSubscription is the wiring MAG-2643 was opened for:
// a spec-declared streaming method reaches StartSubscription and its per-client channel
// comes back as the payload channel grpcproxy pumps.
func TestStreamRelayCallback_StreamsSubscription(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription()}
	listener := newStreamingListener(t, sender)
	manager := newStubGRPCSubscriptionManager()

	callback := listener.makeStreamRelayCallback(manager)
	response, err := callback(context.Background(), streamingApiName, []byte("{}"))
	require.NoError(t, err)
	require.NotNil(t, response, "a spec-declared subscription must be served as a stream")
	require.Equal(t, 1, manager.startCalls)
	require.Equal(t, streamingApiName, manager.lastChainName)

	// The subscription id rides in the headers. The acknowledgement payload does not
	// go on the wire — it is JSON and would not decode as the method's output type.
	require.Equal(t, []string{"router-1"}, response.Metadata.Get("x-lava-grpc-sub-id"))

	manager.replies <- &pairingtypes.RelayReply{Data: []byte("checkpoint-1")}
	manager.replies <- &pairingtypes.RelayReply{Data: []byte("checkpoint-2")}
	require.Equal(t, []byte("checkpoint-1"), receiveWithin(t, response.Replies))
	require.Equal(t, []byte("checkpoint-2"), receiveWithin(t, response.Replies))

	// Upstream end closes the forwarded channel, which is how grpcproxy learns to
	// close the client stream with OK.
	close(manager.replies)
	_, open := <-response.Replies
	require.False(t, open, "closing the manager's channel must close the forwarded channel")
}

// TestStreamRelayCallback_UnaryFallsThrough proves ordinary gRPC queries are untouched:
// no subscription is started and the proxy is told to use the unary path.
func TestStreamRelayCallback_UnaryFallsThrough(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription()}
	listener := newStreamingListener(t, sender)
	manager := newStubGRPCSubscriptionManager()

	response, err := listener.makeStreamRelayCallback(manager)(context.Background(), unaryApiName, []byte("{}"))
	require.NoError(t, err)
	require.Nil(t, response, "a unary method must fall through to the unary callback")
	require.Zero(t, manager.startCalls, "no upstream subscription may be opened for a unary method")
}

// TestStreamRelayCallback_UnparseableFallsThrough keeps error reporting where it was:
// an unknown method must produce the unary path's existing error, not a new one.
func TestStreamRelayCallback_UnparseableFallsThrough(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription(), parseErr: errors.New("api not supported")}
	listener := newStreamingListener(t, sender)
	manager := newStubGRPCSubscriptionManager()

	response, err := listener.makeStreamRelayCallback(manager)(context.Background(), "unknown.Service/Method", []byte("{}"))
	require.NoError(t, err, "the streaming callback must not claim an error it does not own")
	require.Nil(t, response)
	require.Zero(t, manager.startCalls)
}

// TestStreamRelayCallback_ReleasesClientOnClose covers the disconnect half: the Close
// grpcproxy runs when the client stream ends must drop this client upstream.
func TestStreamRelayCallback_ReleasesClientOnClose(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription()}
	listener := newStreamingListener(t, sender)
	manager := newStubGRPCSubscriptionManager()

	response, err := listener.makeStreamRelayCallback(manager)(context.Background(), streamingApiName, []byte("{}"))
	require.NoError(t, err)
	require.NotNil(t, response.Close)

	response.Close()
	select {
	case released := <-manager.unsubscribed:
		require.Equal(t, manager.startedWith, released,
			"the key released on disconnect must be the key the subscription was started under")
	case <-time.After(time.Second):
		t.Fatal("Close did not release the client upstream")
	}
}

// TestStreamRelayCallback_StartFailurePropagates: a failed subscribe (no endpoints,
// rate limit, reflection down) must surface to the client instead of an empty stream.
func TestStreamRelayCallback_StartFailurePropagates(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription()}
	listener := newStreamingListener(t, sender)
	manager := newStubGRPCSubscriptionManager()
	manager.startErr = errors.New("no gRPC endpoints available")

	response, err := listener.makeStreamRelayCallback(manager)(context.Background(), streamingApiName, []byte("{}"))
	require.Error(t, err)
	require.Nil(t, response)
	require.Contains(t, err.Error(), "no gRPC endpoints available")
}

// TestForwardSubscriptionReplies_StopsWhenClientGone guards the forwarder against
// leaking: once the client stream's context is done nobody reads the payload channel,
// so an unconditional send would park this goroutine for the process lifetime.
func TestForwardSubscriptionReplies_StopsWhenClientGone(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription()}
	listener := newStreamingListener(t, sender)

	replies := make(chan *pairingtypes.RelayReply, 4)
	replies <- &pairingtypes.RelayReply{Data: []byte("one")}
	replies <- &pairingtypes.RelayReply{Data: []byte("two")}

	ctx, cancel := context.WithCancel(context.Background())
	payloads := listener.forwardSubscriptionReplies(ctx, replies, *metrics.NewRelayAnalytics("dapp", "SUI", spectypes.APIInterfaceGrpc), nil)

	require.Equal(t, []byte("one"), receiveWithin(t, payloads))
	cancel() // client disconnects with "two" still queued and nobody reading

	select {
	case _, open := <-payloads:
		// Either the queued message lands first or the channel is already closed;
		// both are fine. What matters is that the goroutine finished.
		if open {
			_, open = <-payloads
			require.False(t, open, "forwarder must close its channel once the client is gone")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("forwarder is still parked on a send nobody will read")
	}
}

func receiveWithin(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case payload := <-ch:
		return payload
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a forwarded stream message")
		return nil
	}
}
