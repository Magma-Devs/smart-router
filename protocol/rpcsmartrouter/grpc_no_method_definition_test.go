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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// relayGRPCBody sends one call with body through the router's whole relay path, as its gRPC
// listener does, and returns how the relay ended.
func relayGRPCBody(t *testing.T, server *RPCSmartRouterServer, body string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	protocolMessage, err := server.ParseRelay(ctx, grpcRateLimitTestMethod, body, "", "dapp", "1.2.3.4", nil)
	require.NoError(t, err)
	_, err = server.SendParsedRelay(ctx, nil, protocolMessage)
	return err
}

// providerAvailability is the availability the router's optimizer holds for provider.
func providerAvailability(server *RPCSmartRouterServer, provider string) float64 {
	report, _ := server.sessionManager.GetProviderOptimizer().GetReputationReportForProvider(provider)
	return report.GetAvailability()
}

// A JSON request the router could not convert, because no provider of the chain has given it a
// definition of the method, never reached the node, so it says nothing about the node's
// availability: the session is released without a score or a consecutive error (MAG-5134). It used
// to land on the failure arm with every other relay error: an availability sample of 0 for a
// request the node never saw. Driven through the whole relay path to a node that offers no
// reflection. The control is a node error that is scored, seen through the same harness, so the
// wait below is long enough for a score to land.
func TestRelay_NoMethodDefinitionIsNotScoredAgainstTheProvider(t *testing.T) {
	const relays = 5

	t.Run("a node error is scored (control)", func(t *testing.T) {
		server, provider, _ := newSingleGRPCNodeRouter(t, startAnsweringGRPCNode(t, status.New(codes.Unavailable, "node down"), nil, nil))
		start := providerAvailability(server, provider)
		for i := 0; i < relays; i++ {
			_ = relayGRPCBody(t, server, string([]byte{0x08, 0x01}))
		}
		require.Eventually(t, func() bool { return providerAvailability(server, provider) < start },
			5*time.Second, 20*time.Millisecond, "a scored node error lowers the availability")
	})

	t.Run("a request the router could not build is not", func(t *testing.T) {
		server, provider, _ := newSingleGRPCNodeRouter(t, startAnsweringGRPCNode(t, status.New(codes.OK, ""), nil, nil))
		require.NoError(t, relayGRPCBody(t, server, string([]byte{0x08, 0x01})), "a binary request needs no definition and is served")
		start := providerAvailability(server, provider)
		for i := 0; i < relays; i++ {
			require.ErrorContains(t, relayGRPCBody(t, server, `{}`), routersession.ErrNoMethodDefinition.Error())
		}
		require.Never(t, func() bool { return providerAvailability(server, provider) < start },
			2*time.Second, 20*time.Millisecond, "a request the router never sent lowered the provider's availability")
	})
}
