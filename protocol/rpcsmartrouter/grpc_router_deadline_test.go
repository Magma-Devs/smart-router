package rpcsmartrouter

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// startGRPCNode serves every method with handle, through the router's real gRPC
// connection, so these tests read what grpc-go and handleGRPCError actually produce.
func startGRPCNode(t *testing.T, handle func(stream grpc.ServerStream) error) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		return handle(stream)
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func sendGRPCRelayTo(t *testing.T, addr string, attemptBudget time.Duration) (*common.RelayResult, error) {
	t.Helper()
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	conn, err := lavasession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	sender := &DirectRPCRelaySender{directConnection: conn, endpointName: "grpc-node", chainFamily: common.ChainFamily(-1)}
	chainMessage := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		// A binary body goes to the node as is, so the test node needs no reflection.
		rpcMessage: &rpcInterfaceMessages.GrpcMessage{Path: "cosmos.bank.v1beta1.Query/Balance", Msg: []byte{0x0a, 0x01, 0x61}},
	}}
	return sender.sendGRPCRelay(context.Background(), chainMessage, attemptBudget)
}

// MAG-4625: a node that never answers runs out the router's attempt budget. That is the
// router's timeout, as on every HTTP transport: an error, classified PROTOCOL_CONTEXT_DEADLINE,
// blaming the endpoint. It used to come back as (result, nil) with the router's own
// "context deadline exceeded" as the node's reply, and so counted as served.
func TestSendGRPCRelay_RouterDeadlineIsATimeout(t *testing.T) {
	silent := startGRPCNode(t, func(stream grpc.ServerStream) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	result, err := sendGRPCRelayTo(t, silent, 200*time.Millisecond)

	require.Nil(t, result, "the router's own timeout is not an answer from the node")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, common.LavaErrorContextDeadline, common.AsLavaError(err))
	require.False(t, common.IsClientCancellation(err, context.Background()), "a slow endpoint is not excused")
	unhealthy, _ := classifyEndpointHealth(common.AsLavaError(err), false)
	require.True(t, unhealthy, "the endpoint is still blamed for the timeout")

	attempt := relayEndProtocolErrorAttempt(relaycore.RelayError{Err: err, LavaError: common.AsLavaError(err)}, false)
	require.Equal(t, attemptOutcomeTimeout, attempt.outcome, "relay_end reads it as a timeout")
}

// The guard on the other side: a node that answers DEADLINE_EXCEEDED itself, while the
// router's budget is still open, did answer. That stays a node error.
func TestSendGRPCRelay_NodeSentDeadlineIsANodeError(t *testing.T) {
	node := startGRPCNode(t, func(grpc.ServerStream) error {
		return status.Error(codes.DeadlineExceeded, "query timed out on the node")
	})

	result, err := sendGRPCRelayTo(t, node, 5*time.Second)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.IsNodeError)
	require.Equal(t, int(codes.DeadlineExceeded), result.StatusCode)
	require.Contains(t, string(result.Reply.Data), "query timed out on the node")
}

// sendGRPCRelayOnItsOwnConnection is sendGRPCRelayTo for a goroutine: it returns its errors
// instead of failing the test, and closes its connection itself.
func sendGRPCRelayOnItsOwnConnection(addr string, attemptBudget time.Duration) (*common.RelayResult, error) {
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	conn, err := lavasession.NewDirectRPCConnection(context.Background(), nodeUrl, 1, "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	sender := &DirectRPCRelaySender{directConnection: conn, endpointName: "grpc-node", chainFamily: common.ChainFamily(-1)}
	chainMessage := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: "cosmos.bank.v1beta1.Query/Balance", Msg: []byte{0x0a, 0x01, 0x61}},
	}}
	return sender.sendGRPCRelay(context.Background(), chainMessage, attemptBudget)
}

// MAG-4925: the node's gRPC server enforces the router's deadline too (it travels in the
// grpc-timeout header) and reports it back as a DEADLINE_EXCEEDED of its own, so a call that runs
// out the router's deadline is ended from two sides at the same instant. grpc-go hands back
// whichever arrives first, and a deadline context reports no error until the runtime has run its
// timer. A node status read in that gap used to be taken for the node's answer: a node error,
// which nothing counts against the endpoint, with DEADLINE_EXCEEDED served to the caller as the
// node's reply. Under a burst of 55 such requests one of three quiet providers was left on, short
// of the disable threshold.
//
// The node here does what the provider simulator's server does: it waits for the deadline the
// router sent and reports it. Every attempt must end as the router's own timeout, like
// TestSendGRPCRelay_RouterDeadlineIsATimeout. The attempts run at once because the gap is a
// scheduling race: before the fix, 200 attempts at once took it a handful of times in most runs.
func TestSendGRPCRelay_DeadlineEchoedByTheNodeIsATimeout(t *testing.T) {
	node := startGRPCNode(t, func(stream grpc.ServerStream) error {
		<-stream.Context().Done()
		return status.Error(codes.DeadlineExceeded, "Deadline Exceeded")
	})

	const attempts = 200
	type outcome struct {
		result *common.RelayResult
		err    error
	}
	outcomes := make([]outcome, attempts)
	var wg sync.WaitGroup
	for i := range outcomes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outcomes[i].result, outcomes[i].err = sendGRPCRelayOnItsOwnConnection(node, time.Second)
		}()
	}
	wg.Wait()

	nodeAnswers := 0
	for _, got := range outcomes {
		if got.err == nil {
			nodeAnswers++
			continue
		}
		require.Nil(t, got.result, "the router's own timeout is not an answer from the node")
		require.ErrorIs(t, got.err, context.DeadlineExceeded)
		require.Equal(t, common.LavaErrorContextDeadline, common.AsLavaError(got.err))
	}
	require.Zero(t, nodeAnswers,
		"%d of %d attempts took the node's DEADLINE_EXCEEDED for its answer; each is an attempt the endpoint is not blamed for",
		nodeAnswers, attempts)
}
