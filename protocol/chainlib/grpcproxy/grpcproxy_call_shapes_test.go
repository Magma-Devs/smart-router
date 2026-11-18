package grpcproxy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/testproto"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// countingEcho is unaryEcho that counts the calls relayed.
func countingEcho(t *testing.T, relayed *atomic.Int32) ProxyCallBack {
	echo := unaryEcho(t)
	return func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error) {
		relayed.Add(1)
		return echo(ctx, method, reqBody)
	}
}

func unaryCallServed(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := testproto.NewTestClient(conn).Test(ctx, &testproto.TestRequest{Request: "after"})
	require.NoError(t, err, "a single-reply call on the same connection is served")
	require.Equal(t, "after-unary", resp.Response)
}

// A call the gate refuses is answered at once, before the proxy reads any of it, so a two-way
// client that waits for a reply before sending gets the refusal and not a hang. The connection
// keeps serving.
func TestGRPCProxy_CallGateRefusesBeforeReading(t *testing.T) {
	const refused = "/grpc.testing.TestService/FullDuplexCall"
	var relayed atomic.Int32
	gate := func(_ context.Context, method string) error {
		if "/"+method == refused {
			return status.Error(codes.Unimplemented, "not served")
		}
		return nil
	}
	_, httpServer, err := NewGRPCProxyWithReflection(countingEcho(t, &relayed), "", testCmdFlags(), nil, nil, nil, WithCallGate(gate))
	require.NoError(t, err)
	_, conn := serveProxyOverHTTP(t, httpServer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, refused)
	require.NoError(t, err)
	// Nothing is sent: the client waits for the server.
	err = stream.RecvMsg(new(testproto.TestResponse))
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
	require.Zero(t, relayed.Load(), "a refused call is never relayed")

	unaryCallServed(t, conn)
	require.Equal(t, int32(1), relayed.Load())
}

// Without a definition, a client stream is caught by what it does: a second request message.
// The caller gets the router's refusal, not the reply to its first message alone.
func TestGRPCProxy_SecondRequestMessageIsRefused(t *testing.T) {
	var relayed atomic.Int32
	_, httpServer, err := NewGRPCProxyWithReflection(countingEcho(t, &relayed), "", testCmdFlags(), nil, nil, nil)
	require.NoError(t, err)
	_, conn := serveProxyOverHTTP(t, httpServer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, testMethodPath)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&testproto.TestRequest{Request: "one"}))
	require.NoError(t, stream.SendMsg(&testproto.TestRequest{Request: "two"}))
	require.NoError(t, stream.CloseSend())
	err = stream.RecvMsg(new(testproto.TestResponse))
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
	require.Equal(t, common.UnservedGRPCCallError.Error(), status.Convert(err).Message())

	unaryCallServed(t, conn)
}

// The limit of judging a call by what it does: a two-way client that sends one message and then
// waits, without closing its side, looks like a single-reply call until it sends again. With no
// definition to say otherwise it is relayed as one, as before.
func TestGRPCProxy_WaitingTwoWayClientWithoutADefinitionIsRelayedAsSingleReply(t *testing.T) {
	var relayed atomic.Int32
	_, httpServer, err := NewGRPCProxyWithReflection(countingEcho(t, &relayed), "", testCmdFlags(), nil, nil, nil)
	require.NoError(t, err)
	_, conn := serveProxyOverHTTP(t, httpServer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, testMethodPath)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&testproto.TestRequest{Request: "one"}))
	resp := new(testproto.TestResponse)
	require.NoError(t, stream.RecvMsg(resp))
	require.Equal(t, "one-unary", resp.Response)
	require.Equal(t, int32(1), relayed.Load())
}
