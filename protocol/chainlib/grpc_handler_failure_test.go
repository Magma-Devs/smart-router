package chainlib

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// failingRelaySender stops while handling a request whose body is "fail", as a handler does when
// something under it panics, and answers any other request.
type failingRelaySender struct{ *stubRelaySender }

func (failingRelaySender) SendRelay(_ context.Context, _, req, _, _, _ string, _ *metrics.RelayMetrics, _ []pairingtypes.Metadata) (*common.RelayResult, error) {
	if req == "fail" {
		panic("the handler failed")
	}
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte("ok")}}, nil
}

// A request whose handling stops is answered INTERNAL under the request id its own request line
// carries, and logged at ERROR with that id; the listener keeps serving.
func TestGrpcListener_AHandlerFailureIsAnsweredUnderTheRequestsID(t *testing.T) {
	utils.EnableDebugLogBuffer(1000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	sender := failingRelaySender{&stubRelaySender{parser: grpcParserWithSubscription()}}
	newGrpc := func(ctx context.Context, endpoint *routersession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewGrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, sender.parser)
	}
	addr := serveWithFlags(t, newGrpc, spectypes.APIInterfaceGrpc, sender, common.ConsumerCmdFlags{})
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var reply []byte
	var trailer metadata.MD
	err = conn.Invoke(ctx, "/"+unaryApiName, []byte("fail"), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true), grpc.Trailer(&trailer))
	require.Equal(t, codes.Internal, status.Code(err), "%v", err)
	require.Equal(t, grpcproxy.HandlerFailedMessage, status.Convert(err).Message())
	guid := trailer.Get(common.GUID_HEADER_NAME)
	require.Len(t, guid, 1)
	lines := requireLinesOfGUID(t, guid[0])
	require.Contains(t, lines, "in <<< GRPC Relay ", "the request line carries the id the caller got")
	require.Contains(t, lines, "gRPC handler failed")
	require.Equal(t, "error", lines["gRPC handler failed"]["level"])

	require.NoError(t, conn.Invoke(ctx, "/"+unaryApiName, []byte("echo"), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{})), "the listener keeps serving")
	require.Equal(t, []byte("ok"), reply)
}
