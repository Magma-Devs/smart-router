package chainlib

import (
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSpecificErrorFromUser classifies a provider answering "Not Implemented" for a Default path, a
// shape reported by a user. It is the node saying it does not implement the method, which since
// MAG-2771 is a claim about that node: NODE_UNIMPLEMENTED, retried on another provider, and not a
// router-terminal unsupported method.
func TestSpecificErrorFromUser(t *testing.T) {
	errorMsg := "rpc error: code = Unknown desc = unsupported method 'Default-/cosmos/base/tendermint/v1beta1/blocks1/2': Not Implemented"

	t.Run("Error object is the node's NODE_UNIMPLEMENTED, not a router-terminal unsupported method", func(t *testing.T) {
		err := errors.New(errorMsg)
		require.Equal(t, common.LavaErrorNodeUnimplemented, ClassifyNodeError(err, -1, common.TransportGRPC))
		require.False(t, IsUnsupportedMethodError(err))
	})

	t.Run("gRPC Unknown status is the node's NODE_UNIMPLEMENTED too", func(t *testing.T) {
		grpcErr := status.Error(codes.Unknown, "unsupported method 'Default-/cosmos/base/tendermint/v1beta1/blocks1/2': Not Implemented")
		require.Equal(t, common.LavaErrorNodeUnimplemented, ClassifyNodeError(grpcErr, -1, common.TransportGRPC))
		require.False(t, IsUnsupportedMethodError(grpcErr))
	})

	// This is a gRPC error ("rpc error: code = Unknown desc = ..."), so the gRPC transport must be
	// specified for the registry to detect "Not Implemented".
	t.Run("ShouldRetryError retries it on another provider", func(t *testing.T) {
		err := errors.New(errorMsg)
		require.True(t, ShouldRetryErrorWithContext(err, -1, common.TransportGRPC),
			"another provider may implement the method (MAG-2771)")
	})
}
