package rpcsmartrouter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// A JSON gRPC request no provider of the chain has a definition for cannot be converted on this
// provider. That is not the provider's failure: it is NODE_UNIMPLEMENTED, retried on a provider
// that may hold the definition, and the endpoint is neither marked unhealthy nor held off
// (MAG-5134). As an unrecognised error it marked the endpoint unhealthy, so a client calling a
// method no provider knows could take healthy endpoints out.
func TestSendGRPCRelay_NoMethodDefinitionIsNotTheEndpointsFault(t *testing.T) {
	sender := &DirectRPCRelaySender{
		directConnection: &fakeDirectConn{err: fmt.Errorf("failed to get method descriptor: %w", routersession.ErrNoMethodDefinition)},
		endpointName:     "grpc-endpoint",
		chainFamily:      common.ChainFamily(-1),
	}
	chainMessage := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: "unknown.v1.Service/Call", Msg: []byte(`{}`)},
	}}

	_, err := sender.sendGRPCRelay(context.Background(), chainMessage, time.Second)
	require.ErrorIs(t, err, routersession.ErrNoMethodDefinition)

	verdict := healthVerdict(err, common.ChainFamily(-1), common.TransportGRPC)
	require.Equal(t, common.RouterErrorNodeUnimplemented, verdict)
	require.True(t, verdict.Retryable, "another provider may hold the definition")
	unhealthy, backoff := classifyEndpointHealth(verdict, false)
	require.False(t, unhealthy, "the endpoint is not marked unhealthy")
	require.False(t, backoff, "nor held off")
}
