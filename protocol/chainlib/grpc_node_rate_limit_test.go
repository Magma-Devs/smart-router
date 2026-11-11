package chainlib

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
)

// rateLimitingBlockService refuses every GetLatestBlock with RESOURCE_EXHAUSTED, sending header and
// trailer as the reply's header and trailer metadata.
type rateLimitingBlockService struct {
	header, trailer metadata.MD
}

func (s rateLimitingBlockService) GetLatestBlock(ctx context.Context, _ *dynamicpb.Message) (*dynamicpb.Message, error) {
	if err := grpc.SetHeader(ctx, s.header); err != nil {
		return nil, err
	}
	if err := grpc.SetTrailer(ctx, s.trailer); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.ResourceExhausted, "RESOURCE_EXHAUSTED")
}

// The gRPC chain proxy types a node's rate limit whichever block of the reply carries the wait, as
// the direct connection does: a status-only reply has no header block, and grpc-go's own retry reads
// grpc-retry-pushback-ms from the trailer (MAG-4915).
func TestGrpcChainProxy_RateLimitWaitIsReadFromHeaderOrTrailer(t *testing.T) {
	const pushbackKey = "grpc-retry-pushback-ms"
	asked := time.Minute // the wait the MAG-4915 reproduction asked for
	pushback := metadata.Pairs(pushbackKey, strconv.FormatInt(asked.Milliseconds(), 10))
	cases := []struct {
		name            string
		header, trailer metadata.MD
	}{
		{"header", pushback, nil},
		{"trailer", nil, pushback},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			srv.RegisterService(&mockGRPCServiceDesc, rateLimitingBlockService{header: tc.header, trailer: tc.trailer})
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

			parsing, apiCollection, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
			require.True(t, ok)
			chainMessage, err := chainParser.ParseMsg(parsing.ApiName, []byte{}, apiCollection.CollectionData.Type, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)

			_, _, _, err = chainRouter.SendNodeMsg(ctx, chainMessage, nil)
			require.ErrorIs(t, err, common.StatusCodeError429, "RESOURCE_EXHAUSTED with a wait is a rate limit")
			wait, ok := common.RetryAfterFrom(err)
			require.True(t, ok, "the wait reaches the caller's hold-off")
			require.Equal(t, asked, wait)
		})
	}
}
