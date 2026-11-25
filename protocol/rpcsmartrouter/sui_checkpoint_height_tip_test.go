package rpcsmartrouter

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	rand "github.com/magma-Devs/smart-router/utils/rand"
)

// MAG-5141 part 2: a Sui node stamps its latest checkpoint on every gRPC reply, in the
// x-sui-checkpoint-height header, and the router takes it as that provider's latest block on any
// method, as it takes Solana's context slot.

const (
	suiGetServiceInfo = "sui.rpc.v2.LedgerService/GetServiceInfo"
	suiGetObject      = "sui.rpc.v2.LedgerService/GetObject"
)

func TestSuiCheckpointHeightFromMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		md     map[string][]string
		height int64
		ok     bool
	}{
		{"stamped", map[string][]string{"x-sui-checkpoint-height": {"346725123"}}, 346725123, true},
		{"the first value counts", map[string][]string{"x-sui-checkpoint-height": {"7", "9"}}, 7, true},
		{"no header", map[string][]string{"x-sui-epoch": {"1315"}}, 0, false},
		{"no metadata", nil, 0, false},
		{"no value", map[string][]string{"x-sui-checkpoint-height": {}}, 0, false},
		{"empty value", map[string][]string{"x-sui-checkpoint-height": {""}}, 0, false},
		{"zero", map[string][]string{"x-sui-checkpoint-height": {"0"}}, 0, false},
		{"negative", map[string][]string{"x-sui-checkpoint-height": {"-5"}}, 0, false},
		{"not a number", map[string][]string{"x-sui-checkpoint-height": {"latest"}}, 0, false},
		{"an exponent", map[string][]string{"x-sui-checkpoint-height": {"3e8"}}, 0, false},
		{"out of range", map[string][]string{"x-sui-checkpoint-height": {"99999999999999999999"}}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			height, ok := suiCheckpointHeightFromMetadata(tc.md)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.height, height)
		})
	}
}

// Only a Sui node's stamp is read: another chain's node that happened to send the same name is
// not taken for its tip.
func TestGRPCNodeStampedLatestBlockIsReadOnSuiOnly(t *testing.T) {
	md := map[string][]string{"x-sui-checkpoint-height": {"346725123"}}
	require.Equal(t, int64(346725123), (&DirectRPCRelaySender{chainFamily: common.ChainFamilySui}).grpcNodeStampedLatestBlock(md))
	for _, family := range []common.ChainFamily{common.ChainFamily(-1), common.ChainFamilyCosmosSDK, common.ChainFamilyAptos, common.ChainFamilySolana} {
		require.Zero(t, (&DirectRPCRelaySender{chainFamily: family}).grpcNodeStampedLatestBlock(md), "family %s", family)
	}
}

// startSuiStampingNode serves every method like a Sui node: each reply carries md as its header
// block, sent ahead of the reply or the status. answer gives the reply's bytes or a status error.
// Reflection never answers, so a test reads what a reply without a method definition gives.
func startSuiStampingNode(t *testing.T, md metadata.MD, answer func(method string) ([]byte, error)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if strings.Contains(method, "ServerReflection") {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		if err := stream.SendHeader(md); err != nil {
			return err
		}
		reply, err := answer(method)
		if err != nil {
			return err
		}
		return stream.SendMsg(reply)
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func suiGRPCSenderTo(t *testing.T, addr string) *DirectRPCRelaySender {
	t.Helper()
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	conn, err := routersession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &DirectRPCRelaySender{directConnection: conn, endpointName: "sui-node", chainFamily: common.ChainFamilySui}
}

func suiBinaryRead(method string) *grpcStatusChainMessage {
	return &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		api:          &spectypes.Api{Name: method},
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: []byte{0x0a, 0x01, 0x61}},
	}}
}

// The sender reads the stamp from the node's own headers, before the reply-header filter, on a
// success and on a node error that sent its headers. The client-facing metadata is the filter's
// business: with no directive listing the name, the stamp is still read but not passed on.
func TestSendGRPCRelay_ReadsTheSuiStampBeforeTheReplyHeaderFilter(t *testing.T) {
	stamp := metadata.Pairs("x-sui-checkpoint-height", "346725123", "x-sui-epoch", "1315")

	t.Run("success", func(t *testing.T) {
		node := startSuiStampingNode(t, stamp, func(string) ([]byte, error) { return []byte{0x08, 0x01}, nil })
		result, err := suiGRPCSenderTo(t, node).SendDirectRelay(context.Background(), suiBinaryRead(suiGetObject), 5*time.Second)
		require.NoError(t, err)
		require.False(t, result.IsNodeError)
		require.Equal(t, int64(346725123), result.NodeLatestBlock)
		for _, md := range result.Reply.Metadata {
			require.NotEqual(t, "x-sui-checkpoint-height", md.Name, "no directive lists it, so the client does not get it")
		}
	})

	t.Run("node error", func(t *testing.T) {
		node := startSuiStampingNode(t, stamp, func(string) ([]byte, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid object_id")
		})
		result, err := suiGRPCSenderTo(t, node).SendDirectRelay(context.Background(), suiBinaryRead(suiGetObject), 5*time.Second)
		require.NoError(t, err)
		require.True(t, result.IsNodeError)
		require.Equal(t, int64(346725123), result.NodeLatestBlock)
	})

	t.Run("no stamp", func(t *testing.T) {
		node := startSuiStampingNode(t, metadata.Pairs("x-sui-epoch", "1315"), func(string) ([]byte, error) { return []byte{0x08, 0x01}, nil })
		result, err := suiGRPCSenderTo(t, node).SendDirectRelay(context.Background(), suiBinaryRead(suiGetObject), 5*time.Second)
		require.NoError(t, err)
		require.Zero(t, result.NodeLatestBlock)
	})
}

// suiTipHarness is a router for SUI gRPC with a real endpoint monitor, one tracked url, and a
// parser whose spec tags GetServiceInfo GET_BLOCKNUM, as lava-specs sui.json does.
type suiTipHarness struct {
	rpcss *RPCSmartRouterServer
	m     *endpointstate.EndpointMonitor
	ep    *routersession.Endpoint
	gen   uint64
}

const suiTipURL = "grpcs://sui-node:443"

func newSuiTipHarness(t *testing.T) *suiTipHarness {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed() // the tracker GetOrCreateTracker starts needs one
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	parser := newSuiGRPCTipParser(t)
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          "SUI",
		ApiInterface:     spectypes.APIInterfaceGrpc,
		AverageBlockTime: 222 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)
	ep := &routersession.Endpoint{NetworkAddress: suiTipURL, Enabled: true}
	_, err := m.GetOrCreateTracker("", ep, nil)
	require.NoError(t, err)
	gen, ok := m.ObservationGeneration(suiTipURL)
	require.True(t, ok)
	endpointtip.Default().Reset()
	t.Cleanup(endpointtip.Default().Reset)
	return &suiTipHarness{
		rpcss: &RPCSmartRouterServer{
			listenEndpoint:              &routersession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGrpc},
			endpointChainTrackerManager: m,
			chainParser:                 parser,
		},
		m:   m,
		ep:  ep,
		gen: gen,
	}
}

func (h *suiTipHarness) tip() int64 {
	return endpointtip.Default().Block(endpointtip.Key("SUI", spectypes.APIInterfaceGrpc, suiTipURL))
}

func newSuiGRPCTipParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	parser, err := chainlib.NewChainParser(spectypes.APIInterfaceGrpc)
	require.NoError(t, err)
	parser.SetSpec(spectypes.Spec{
		Index:                         "SUI",
		Name:                          "sui mainnet",
		Enabled:                       true,
		AverageBlockTime:              222,
		AllowedBlockLagForQosSync:     46,
		BlockDistanceForFinalizedData: 1,
		BlocksInFinalizationProof:     1,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGrpc},
			Apis: []*spectypes.Api{
				{Name: suiGetServiceInfo, Enabled: true, ComputeUnits: 10, BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT}},
				{Name: suiGetObject, Enabled: true, ComputeUnits: 10, BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT}},
			},
			ParseDirectives: []*spectypes.ParseDirective{{
				FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM,
				ApiName:     suiGetServiceInfo,
				ResultParsing: spectypes.BlockParser{
					ParserArg:  []string{"0", "checkpointHeight"},
					ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL,
				},
			}},
		}},
	})
	return parser
}

// Ticket tests 3 to 5, on the harvest: a read that is not a tip method moves the provider's
// latest block to its stamp, a lower stamp does not move it back, and a reply without a stamp is
// read as before.
func TestHarvest_SuiStampIsTheProvidersLatestBlockOnAnyMethod(t *testing.T) {
	read := &mockChainMessage{apiInterface: "grpc", api: &spectypes.Api{Name: suiGetObject}, requestedBlock: spectypes.LATEST_BLOCK}

	t.Run("a read that is not a tip method moves it to the stamp", func(t *testing.T) {
		h := newSuiTipHarness(t)
		// Without the stamp, as before this change: an object read says nothing about the tip.
		h.rpcss.harvestAndUpdateTipFromRelay(h.ep, read, &pairingtypes.RelayReply{}, 0, h.gen, "sui-provider")
		require.Zero(t, h.tip())

		h.rpcss.harvestAndUpdateTipFromRelay(h.ep, read, &pairingtypes.RelayReply{}, 346_725_123, h.gen, "sui-provider")
		require.Equal(t, int64(346_725_123), h.tip())
		o, ok := h.m.GetObservation(suiTipURL)
		require.True(t, ok)
		require.Equal(t, int64(346_725_123), o.LatestBlock)
		require.Equal(t, endpointstate.ObservationSourceRelay, o.Source)
	})

	t.Run("a lower stamp does not move it back", func(t *testing.T) {
		h := newSuiTipHarness(t)
		h.rpcss.harvestAndUpdateTipFromRelay(h.ep, read, &pairingtypes.RelayReply{}, 346_725_123, h.gen, "sui-provider")
		h.rpcss.harvestAndUpdateTipFromRelay(h.ep, read, &pairingtypes.RelayReply{}, 346_725_100, h.gen, "sui-provider")
		require.Equal(t, int64(346_725_123), h.tip())
	})

	t.Run("a reply without a stamp is read as before", func(t *testing.T) {
		h := newSuiTipHarness(t)
		// The tip method still gives its reply's block.
		serviceInfo := &mockChainMessage{apiInterface: "grpc", api: &spectypes.Api{Name: suiGetServiceInfo}, requestedBlock: spectypes.LATEST_BLOCK}
		h.rpcss.harvestAndUpdateTipFromRelay(h.ep, serviceInfo, &pairingtypes.RelayReply{LatestBlock: 346_725_000}, 0, h.gen, "sui-provider")
		require.Equal(t, int64(346_725_000), h.tip())
		// Any other read's own block is not a tip: an old checkpoint's block stays out.
		h.rpcss.harvestAndUpdateTipFromRelay(h.ep, read, &pairingtypes.RelayReply{LatestBlock: 346_725_500}, 0, h.gen, "sui-provider")
		require.Equal(t, int64(346_725_000), h.tip())
	})

	t.Run("a stamp from a replaced provider is dropped", func(t *testing.T) {
		h := newSuiTipHarness(t)
		h.m.RemoveTracker(suiTipURL)
		_, err := h.m.GetOrCreateTracker("", h.ep, nil)
		require.NoError(t, err)
		h.rpcss.harvestAndUpdateTipFromRelay(h.ep, read, &pairingtypes.RelayReply{}, 346_725_123, h.gen, "sui-provider")
		require.Zero(t, h.tip(), "the stamp was harvested under the replaced tracker's generation")
	})
}

// The same through the relay path: relayInnerDirect sends a binary read the router has no method
// definition for to a node that stamps its reply, and the result it hands the harvest carries the
// stamp.
func TestRelayInnerDirect_SuiStampReachesTheHarvest(t *testing.T) {
	node := startSuiStampingNode(t, metadata.Pairs("x-sui-checkpoint-height", "346725123"), func(string) ([]byte, error) {
		return []byte{0x08, 0x01}, nil
	})
	h := newSuiTipHarness(t)
	nodeUrl := common.NodeUrl{Url: "grpc://" + node, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	directConn, err := routersession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })
	h.ep.DirectConnections = []routersession.DirectRPCConnection{directConn}
	session := &routersession.SingleConsumerSession{
		Parent:     &routersession.ConsumerSessionsWithProvider{PublicAddress: "sui-provider", PairingEpoch: 100, Endpoints: []*routersession.Endpoint{h.ep}},
		Connection: &routersession.DirectRPCSessionConnection{DirectConnection: directConn, EndpointAddress: suiTipURL, Endpoint: h.ep},
	}

	msg := &mockChainMessage{
		apiInterface: "grpc",
		api:          &spectypes.Api{Name: suiGetObject},
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: suiGetObject, Msg: []byte{0x0a, 0x01, 0x61}},
	}
	relayResult := &common.RelayResult{}
	_, err = h.rpcss.relayInnerDirect(context.Background(), session, relayResult, 5*time.Second, 30*time.Second,
		msg, msg.requestData, func() bool { return false }, nil, false)
	require.NoError(t, err)
	require.Equal(t, int64(346725123), relayResult.NodeLatestBlock, "relayInnerDirect dropped the stamp on its way to the harvest")

	// What sendRelayToDirectEndpoints does with a served relay.
	h.rpcss.harvestAndUpdateTipFromRelay(h.ep, msg, relayResult.Reply, relayResult.NodeLatestBlock, h.gen, "sui-provider")
	require.Equal(t, int64(346725123), h.tip())
}

// The same through the router's own relay path, end to end: an object read comes in through the
// gRPC listener and goes to a node that stamps its reply, and the harvest in
// sendRelayToDirectEndpoints takes the stamp. The tests above call the harvest themselves, so
// without this one the call that hands it the stamp could pass nothing and every test would pass.
func TestSuiStampReachesTheProvidersLatestBlockThroughTheListener(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	endpointtip.Default().Reset()
	t.Cleanup(endpointtip.Default().Reset)
	node := startSuiStampingNode(t, metadata.Pairs("x-sui-checkpoint-height", "346725123"), func(string) ([]byte, error) {
		return []byte{0x08, 0x01}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	parser := newSuiGRPCTipParser(t)
	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          "SUI",
		ApiInterface:     spectypes.APIInterfaceGrpc,
		AverageBlockTime: 222 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(monitor.Stop)

	nodeUrl := common.NodeUrl{Url: "grpc://" + node, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
	connection, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGrpc}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "SUI")
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	provider := routersession.NewConsumerSessionWithProvider("sui-provider", []*routersession.Endpoint{{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}}, 100000, 1, 1)
	provider.StaticProvider = true
	require.NoError(t, csm.UpdateAllProviders(1, map[uint64]*routersession.ConsumerSessionsWithProvider{0: provider}, map[uint64]*routersession.ConsumerSessionsWithProvider{}))
	csm.SetConfiguredProviderNames([]string{"sui-provider"})
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		listenEndpoint:              rpcEndpoint,
		chainParser:                 parser,
		consistencyConfig:           relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:              csm,
		rpcSmartRouterLogs:          logs,
		endpointChainTrackerManager: monitor,
	}

	listenEndpoint := &routersession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGrpc}
	listener := chainlib.NewGrpcChainListener(ctx, listenEndpoint, server, healthyReporter{}, logs, parser)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
		<-listenerDone
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, 20*time.Millisecond)
	client, err := grpc.NewClient(listener.GetListeningAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer callCancel()
	var reply []byte
	require.NoError(t, client.Invoke(callCtx, "/"+suiGetObject, []byte{0x0a, 0x01, 0x61}, &reply, grpc.ForceCodec(stalledReflectionCodec{})))
	require.Equal(t, []byte{0x08, 0x01}, reply)

	tipKey := endpointtip.Key("SUI", spectypes.APIInterfaceGrpc, nodeUrl.Url)
	require.Eventually(t, func() bool { return endpointtip.Default().Block(tipKey) == 346725123 }, 5*time.Second, 20*time.Millisecond,
		"an object read served through the router moves the provider's latest block to the stamp")
}
