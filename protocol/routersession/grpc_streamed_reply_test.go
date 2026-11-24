package routersession

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpc-go ends a single-reply call whose node sent a second reply with its own INTERNAL, and
// without the node's address. It is the router's refusal of a call type it does not serve
// (MAG-5133), not a node's answer and not an undelivered call, however far the call got.
func TestHandleGRPCError_StreamedReplyIsTheRoutersRefusal(t *testing.T) {
	for name, streamed := range map[string]error{
		"a second reply": status.Error(codes.Internal, "cardinality violation: expected <EOF> for non server-streaming RPCs, but received another message"),
		// A server stream with no item: grpc-go's other cardinality text, on a call that did reach
		// the node. Read as the node's INTERNAL, it was retried on every provider and counted
		// toward --bench-after.
		"no reply": status.Error(codes.Internal, "cardinality violation: received no response message from non-server-streaming RPC"),
	} {
		for _, reach := range []grpcCallReach{grpcCallUndelivered, grpcCallUnanswered, grpcCallAnswered} {
			g := &GRPCDirectRPCConnection{}
			resp, err := g.handleGRPCError(context.Background(), streamed, nil, nil, reach)
			require.Nil(t, resp, "%s: it is not the node's answer", name)
			require.ErrorIs(t, err, common.UnservedGRPCCallError, name)
			var undelivered *common.GRPCCallUndeliveredError
			require.False(t, errors.As(err, &undelivered), "%s: it is not a call that never reached the node", name)
		}
	}
}

// A node's own INTERNAL, with any other text, stays the node's answer.
func TestHandleGRPCError_NodeInternalIsNotTheRefusal(t *testing.T) {
	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(context.Background(), status.Error(codes.Internal, "panic in handler"), nil, nil, grpcCallAnswered)
	require.NotNil(t, resp)
	require.Equal(t, int(codes.Internal), resp.StatusCode)
	require.False(t, errors.Is(err, common.UnservedGRPCCallError))
}
