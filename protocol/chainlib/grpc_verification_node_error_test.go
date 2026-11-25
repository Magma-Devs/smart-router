package chainlib

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
)

// methodRefusingBlockService refuses every GetLatestBlock the way a public gateway refuses a method
// it does not serve: status Unknown, and the reason in the message.
type methodRefusingBlockService struct{}

const methodRefusedMessage = "Method /routertest.v1.BlockService/GetLatestBlock is not allowed"

func (methodRefusingBlockService) GetLatestBlock(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
	return nil, status.Error(codes.Unknown, methodRefusedMessage)
}

// A node that refuses the method a verification or the head read calls is reported with the status
// and the message it answered with. The gRPC chain proxy hands that answer back as a JSON reply, and
// the chain fetcher used to decode the JSON as the method's protobuf reply, so the check failed as
// "proto: bad wiretype": it read as a router that cannot parse the chain's replies (MAG-5002).
func TestGrpcVerificationReportsTheNodesRefusal(t *testing.T) {
	UseMockLogLevel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "GRPCTEST")
	require.NoError(t, err)
	chainParser, err := NewChainParser(spectypes.APIInterfaceGrpc)
	require.NoError(t, err)
	chainParser.SetSpec(spec)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	srv.RegisterService(&mockGRPCServiceDesc, methodRefusingBlockService{})
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	endpoint := &routersession.RPCProviderEndpoint{
		ChainID:      "GRPCTEST",
		ApiInterface: spectypes.APIInterfaceGrpc,
		NodeUrls:     []common.NodeUrl{{Url: lis.Addr().String()}},
	}
	chainRouter, err := GetChainRouter(ctx, 1, endpoint, chainParser)
	require.NoError(t, err)
	chainFetcher := NewChainFetcher(ctx, &ChainFetcherOptions{ChainRouter: chainRouter, ChainParser: chainParser, Endpoint: endpoint})

	parsing, apiCollection, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	verificationDirective := *parsing
	verificationDirective.FunctionTag = spectypes.FUNCTION_TAG_VERIFICATION
	verification := VerificationContainer{
		ConnectionType: apiCollection.CollectionData.Type,
		Name:           "chain-id",
		ParseDirective: verificationDirective,
		Value:          "expected-chain",
	}

	assertNodesRefusal := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.Contains(t, err.Error(), "the node answered with an error")
		require.Contains(t, err.Error(), "gRPC status Unknown")
		require.Contains(t, err.Error(), "is not allowed", "the node's own message is kept")
		require.NotContains(t, err.Error(), "wiretype", "the node's error body is not decoded as the method's reply")
	}

	t.Run("verification", func(t *testing.T) {
		assertNodesRefusal(t, chainFetcher.verifyVia(ctx, chainRouter, chainParser, verification, 0))
	})
	t.Run("latest block", func(t *testing.T) {
		_, err := chainFetcher.FetchLatestBlockNum(ctx)
		assertNodesRefusal(t, err)
	})
	t.Run("block hash", func(t *testing.T) {
		// GRPCTEST has no block-by-number directive; the head's method stands in for one. The
		// request parser ignores the unknown field the template fills.
		blockByNum := *parsing
		blockByNum.FunctionTag = spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM
		blockByNum.FunctionTemplate = `{"height":"%d"}`
		_, _, err := chainFetcher.fetchSingleBlockHashByNum(ctx, 5, &blockByNum, apiCollection.CollectionData, spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM.String())
		assertNodesRefusal(t, err)
	})
}

func TestGrpcNodeErrorReply(t *testing.T) {
	grpcMessage := &rpcInterfaceMessages.GrpcMessage{Path: "routertest.v1.BlockService/GetLatestBlock"}
	nodeErrorBody, err := json.Marshal(GrpcNodeErrorResponse{ErrorCode: uint32(codes.Unknown), ErrorMessage: methodRefusedMessage})
	require.NoError(t, err)

	tests := []struct {
		name        string
		rpcMessage  rpcInterfaceMessages.GenericMessage
		statusCode  int
		body        []byte
		wantErrText string // empty: not a node error
	}{
		{
			name:       "a served reply",
			rpcMessage: grpcMessage,
			statusCode: http.StatusOK,
			body:       []byte{0x08, 0x2a},
		},
		{
			name:        "a node error",
			rpcMessage:  grpcMessage,
			statusCode:  int(codes.Unknown),
			body:        nodeErrorBody,
			wantErrText: "gRPC status Unknown: " + methodRefusedMessage,
		},
		{
			name:        "a node error whose body does not decode keeps its status",
			rpcMessage:  grpcMessage,
			statusCode:  int(codes.PermissionDenied),
			body:        []byte{0x08, 0x2a},
			wantErrText: "gRPC status PermissionDenied: ",
		},
		{
			// Only the gRPC chain proxy puts a gRPC code in StatusCode. A REST reply's status is
			// HTTP, and its error body is read by the parser as it is today.
			name:       "not a gRPC reply",
			rpcMessage: &rpcInterfaceMessages.RestMessage{},
			statusCode: http.StatusNotFound,
			body:       []byte(`{"code":5}`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reply := &RelayReplyWrapper{StatusCode: tc.statusCode, RelayReply: &pairingtypes.RelayReply{Data: tc.body}}
			err := grpcNodeErrorReply(reply, rpcMessageOnly{rpcMessage: tc.rpcMessage})
			if tc.wantErrText == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErrText)
		})
	}
}

// rpcMessageOnly is a chain message that answers GetRPCMessage only, which is all grpcNodeErrorReply reads.
type rpcMessageOnly struct {
	ChainMessageForSend
	rpcMessage rpcInterfaceMessages.GenericMessage
}

func (m rpcMessageOnly) GetRPCMessage() rpcInterfaceMessages.GenericMessage {
	return m.rpcMessage
}
