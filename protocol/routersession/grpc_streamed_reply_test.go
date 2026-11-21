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
// (MAG-5133), not a node's answer and not an undelivered call, whatever nodeReached says.
func TestHandleGRPCError_StreamedReplyIsTheRoutersRefusal(t *testing.T) {
	streamed := status.Error(codes.Internal, "cardinality violation: expected <EOF> for non server-streaming RPCs, but received another message")
	for _, nodeReached := range []bool{false, true} {
		g := &GRPCDirectRPCConnection{}
		resp, err := g.handleGRPCError(context.Background(), streamed, nil, nil, nodeReached)
		require.Nil(t, resp, "it is not the node's answer")
		require.ErrorIs(t, err, common.UnservedGRPCCallError)
		var undelivered *common.GRPCCallUndeliveredError
		require.False(t, errors.As(err, &undelivered), "it is not a call that never reached the node")
	}
}

// A node's own INTERNAL, with any other text, stays the node's answer.
func TestHandleGRPCError_NodeInternalIsNotTheRefusal(t *testing.T) {
	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(context.Background(), status.Error(codes.Internal, "panic in handler"), nil, nil, true)
	require.NotNil(t, resp)
	require.Equal(t, int(codes.Internal), resp.StatusCode)
	require.False(t, errors.Is(err, common.UnservedGRPCCallError))
}
