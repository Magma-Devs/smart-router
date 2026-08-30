package grpcproxy

import (
	"context"
	"errors"
	"io"
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

const testMethodPath = "/lavanet.testproto.Test/Test"

func testCmdFlags() common.ConsumerCmdFlags {
	return common.ConsumerCmdFlags{HeadersFlag: "*", OriginFlag: "*", MethodsFlag: "GET,POST,OPTIONS", CDNCacheDuration: "86400"}
}

// unaryEcho is the pre-streaming behaviour, used to prove it is untouched.
func unaryEcho(t *testing.T) ProxyCallBack {
	return func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error) {
		req := new(testproto.TestRequest)
		require.NoError(t, req.Unmarshal(reqBody))
		respBytes, err := (&testproto.TestResponse{Response: req.Request + "-unary"}).Marshal()
		require.NoError(t, err)
		return respBytes, nil, nil
	}
}

func mustMarshalResponse(t *testing.T, value string) []byte {
	t.Helper()
	payload, err := (&testproto.TestResponse{Response: value}).Marshal()
	require.NoError(t, err)
	return payload
}

// openStream starts a server-streaming call against the proxy. The proxy installs an
// UnknownServiceHandler, so any method path reaches it.
func openStream(ctx context.Context, t *testing.T, conn *grpc.ClientConn) grpc.ClientStream {
	t.Helper()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, testMethodPath)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&testproto.TestRequest{Request: "subscribe"}))
	require.NoError(t, stream.CloseSend())
	return stream
}

func recvResponse(t *testing.T, stream grpc.ClientStream) (string, error) {
	t.Helper()
	resp := new(testproto.TestResponse)
	if err := stream.RecvMsg(resp); err != nil {
		return "", err
	}
	return resp.Response, nil
}

// TestGRPCProxy_ServerStreaming_ForwardsUntilUpstreamEnds is the core of the listener
// half MAG-2643 added: multiple messages reach the client on one call, and closing the
// producer channel ends the stream cleanly rather than erroring.
func TestGRPCProxy_ServerStreaming_ForwardsUntilUpstreamEnds(t *testing.T) {
	replies := make(chan []byte, 3)
	replies <- mustMarshalResponse(t, "first")
	replies <- mustMarshalResponse(t, "second")
	replies <- mustMarshalResponse(t, "third")
	close(replies)

	proxyGRPCSrv, _, err := NewGRPCProxyWithReflection(unaryEcho(t), "", testCmdFlags(), nil, nil,
		func(ctx context.Context, method string, reqBody []byte) (*StreamResponse, error) {
			return &StreamResponse{
				Replies:  replies,
				Metadata: metadata.MD{"X-Lava-Grpc-Sub-Id": []string{"router-sub-1"}},
			}, nil
		})
	require.NoError(t, err)

	conn := testproto.InMemoryClientConn(t, proxyGRPCSrv)
	stream := openStream(context.Background(), t, conn)

	header, err := stream.Header()
	require.NoError(t, err)
	require.Equal(t, []string{"router-sub-1"}, header.Get("x-lava-grpc-sub-id"),
		"the subscription id must ride in the headers: a stream message has to decode as the method's output type")

	for _, expected := range []string{"first", "second", "third"} {
		got, err := recvResponse(t, stream)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	}

	_, err = recvResponse(t, stream)
	require.ErrorIs(t, err, io.EOF, "a closed producer channel must end the stream with OK, not an error")
}

// TestGRPCProxy_ServerStreaming_ClosesOnClientDisconnect proves the teardown side:
// without it a client that walks away leaves the upstream subscription running unread.
func TestGRPCProxy_ServerStreaming_ClosesOnClientDisconnect(t *testing.T) {
	replies := make(chan []byte, 1)
	replies <- mustMarshalResponse(t, "only")
	closed := make(chan struct{})

	proxyGRPCSrv, _, err := NewGRPCProxyWithReflection(unaryEcho(t), "", testCmdFlags(), nil, nil,
		func(ctx context.Context, method string, reqBody []byte) (*StreamResponse, error) {
			return &StreamResponse{
				Replies: replies,
				Close:   func() { close(closed) },
			}, nil
		})
	require.NoError(t, err)

	conn := testproto.InMemoryClientConn(t, proxyGRPCSrv)
	ctx, cancel := context.WithCancel(context.Background())
	stream := openStream(ctx, t, conn)

	got, err := recvResponse(t, stream)
	require.NoError(t, err)
	require.Equal(t, "only", got)

	// The producer channel stays open, so only the client going away can end this.
	cancel()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close was never called: a disconnected client would leave its upstream subscription running")
	}
}

// TestGRPCProxy_ServerStreaming_UnaryFallthrough pins the contract that keeps this
// change off the unary path: (nil, nil) from the stream callback means "not streaming".
func TestGRPCProxy_ServerStreaming_UnaryFallthrough(t *testing.T) {
	var streamCallbackCalls int

	proxyGRPCSrv, _, err := NewGRPCProxyWithReflection(unaryEcho(t), "", testCmdFlags(), nil, nil,
		func(ctx context.Context, method string, reqBody []byte) (*StreamResponse, error) {
			streamCallbackCalls++
			return nil, nil //nolint:nilnil // a test double: only the call count is asserted
		})
	require.NoError(t, err)

	client := testproto.NewTestClient(testproto.InMemoryClientConn(t, proxyGRPCSrv))
	resp, err := client.Test(context.Background(), &testproto.TestRequest{Request: "echo"})
	require.NoError(t, err)
	require.Equal(t, "echo-unary", resp.Response)
	require.Equal(t, 1, streamCallbackCalls, "every request is offered to the stream callback first")
}

// TestGRPCProxy_ServerStreaming_CallbackErrorPropagates covers a failed subscribe (no
// endpoint, rate limit): the client must see the failure rather than an empty stream.
func TestGRPCProxy_ServerStreaming_CallbackErrorPropagates(t *testing.T) {
	proxyGRPCSrv, _, err := NewGRPCProxyWithReflection(unaryEcho(t), "", testCmdFlags(), nil, nil,
		func(ctx context.Context, method string, reqBody []byte) (*StreamResponse, error) {
			return nil, errors.New("no gRPC endpoints available")
		})
	require.NoError(t, err)

	conn := testproto.InMemoryClientConn(t, proxyGRPCSrv)
	stream := openStream(context.Background(), t, conn)

	_, err = recvResponse(t, stream)
	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "no gRPC endpoints available")
}

// TestGRPCProxy_ServerStreaming_ClientCancelStatus checks the client's own cancellation
// is reported as Canceled, not as a server-side failure that would look like a bad
// endpoint to anything reading these statuses.
func TestGRPCProxy_ServerStreaming_ClientCancelStatus(t *testing.T) {
	replies := make(chan []byte) // never written to, never closed

	proxyGRPCSrv, _, err := NewGRPCProxyWithReflection(unaryEcho(t), "", testCmdFlags(), nil, nil,
		func(ctx context.Context, method string, reqBody []byte) (*StreamResponse, error) {
			return &StreamResponse{Replies: replies}, nil
		})
	require.NoError(t, err)

	conn := testproto.InMemoryClientConn(t, proxyGRPCSrv)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	stream := openStream(ctx, t, conn)

	_, err = recvResponse(t, stream)
	require.Error(t, err)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
}
