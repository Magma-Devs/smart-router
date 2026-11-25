package routersession

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// addressInText finds an IP address or a port in an error's text.
var addressInText = regexp.MustCompile(`\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-fA-F:]*:[0-9a-fA-F:]*\]|:\d{2,5}\b`)

// grpcResetTextWithAddresses is grpc-go's text for a reset connection as a router logged it in
// production, with both ends of the socket.
const grpcResetTextWithAddresses = "error reading from server: read tcp 10.42.0.7:51234->34.160.0.9:443: read: connection reset by peer"

// MAG-5130: a status grpc-go writes because the connection or the stream ended, on a call whose
// stream to the node was open and whose node sent no status, is "no reply": no response, a
// connection fault, the request possibly delivered, and no address in the text.
func TestHandleGRPCError_ConnectionCutBeforeTheNodesStatusIsNoReply(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
		msg  string
		want *common.RouterError
	}{
		{"reset by the node", codes.Unavailable, "error reading from server: read tcp 10.42.7.9:51234->34.160.1.1:443: read: connection reset by peer", common.RouterErrorConnectionReset},
		{"reset, IPv6", codes.Unavailable, "error reading from server: read tcp [fd00::7]:51234->[2600:1901::1]:443: read: connection reset by peer", common.RouterErrorConnectionReset},
		{"closed by the node", codes.Unavailable, "error reading from server: EOF", common.RouterErrorConnectionClosed},
		{"transport closing", codes.Unavailable, "transport is closing", common.RouterErrorConnectionClosed},
		{"keepalive failed", codes.Unavailable, "keepalive ping failed to receive ACK within timeout", common.RouterErrorConnectionClosed},
		{"GOAWAY", codes.Unavailable, "closing transport due to: connection error: desc = \"error reading from server: EOF\", received prior goaway: code: NO_ERROR", common.RouterErrorConnectionReset},
		{"stream reset", codes.Internal, "stream terminated by RST_STREAM with error code: INTERNAL_ERROR", common.RouterErrorConnectionReset},
		{"stream closed without a status", codes.Internal, "server closed the stream without sending trailers", common.RouterErrorConnectionClosed},
		// A text naming a connection that never opened cannot fit a stream that was open; it
		// must not turn the write into "not sent".
		{"text of a refused dial", codes.Unavailable, "connection error: desc = \"transport: Error while dialing: dial tcp 10.0.0.1:9090: connect: connection refused\"", common.RouterErrorConnectionClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &GRPCDirectRPCConnection{}
			resp, err := g.handleGRPCError(context.Background(), status.Error(tc.code, tc.msg), nil, nil, grpcCallUnanswered)

			require.Nil(t, resp, "the node sent no status, so there is no node answer to carry")
			var lost *common.GRPCReplyLostError
			require.ErrorAs(t, err, &lost)
			var statusErr *GRPCStatusError
			require.False(t, errors.As(err, &statusErr), "not the node-error arm")

			routerErr := common.DetectConnectionError(err)
			require.Equal(t, tc.want, routerErr)
			require.True(t, routerErr.Retryable, "another provider is asked")
			require.True(t, routerErr.MayHaveReachedNode, "the request went out on an open stream: a write may have run")
			require.Equal(t, common.CategoryInternal, routerErr.Category)

			require.Equal(t, tc.code, status.Code(err), "the status grpc-go wrote is kept for logs")
			require.NotRegexp(t, addressInText, err.Error(), "no address of the router or the node survives")
		})
	}
}

// The other side of the line: a status that is not one grpc-go writes for a broken call stays the
// node's reply, as on main, even with no status frame read.
func TestHandleGRPCError_UnansweredCallKeepsRepliesTheClientRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
		msg  string
	}{
		{"a gateway's HTTP 503", codes.Unavailable, "unexpected HTTP status code received from server: 503 (Service Unavailable); transport: received unexpected content-type \"text/html\""},
		{"a gateway's HTTP 502", codes.Unavailable, "unexpected HTTP status code received from server: 502 (Bad Gateway)"},
		{"no content-type", codes.Internal, "malformed header: missing HTTP content-type"},
		{"slow down, by a stream reset", codes.ResourceExhausted, "stream terminated by RST_STREAM with error code: ENHANCE_YOUR_CALM"},
		{"slow down, by a GOAWAY before the close", codes.Unavailable, grpcSlowDownGoAwayText},
		{"reply too large", codes.ResourceExhausted, "grpc: received message larger than max (600000000 vs. 536870912)"},
		{"two replies to a one-reply call", codes.Internal, "cardinality violation: expected <EOF> for non client-streaming RPCs, but received another message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &GRPCDirectRPCConnection{}
			resp, err := g.handleGRPCError(context.Background(), status.Error(tc.code, tc.msg), nil, nil, grpcCallUnanswered)
			require.NotNil(t, resp, "kept on the node-error arm, as before")
			var lost *common.GRPCReplyLostError
			require.False(t, errors.As(err, &lost))
		})
	}
}

// grpcSlowDownGoAwayText is what grpc-go writes for a call still in flight when the server sent a
// GOAWAY with ENHANCE_YOUR_CALM and closed the connection.
const grpcSlowDownGoAwayText = `closing transport due to: connection error: desc = "error reading from server: EOF", received prior goaway: code: ENHANCE_YOUR_CALM, debug data: "too_many_pings"`

// A server asking the router to slow down is a rate limit, as before MAG-5130, in both forms: a
// stream reset with ENHANCE_YOUR_CALM, and a GOAWAY with it before the server closed the
// connection. Neither is a cut, so the endpoint is held off rather than counted as unreachable.
func TestHandleGRPCError_SlowDownStaysARateLimit(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
		msg  string
	}{
		{"a stream reset", codes.ResourceExhausted, "stream terminated by RST_STREAM with error code: ENHANCE_YOUR_CALM"},
		{"a GOAWAY before the close", codes.Unavailable, grpcSlowDownGoAwayText},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &GRPCDirectRPCConnection{}
			resp, err := g.handleGRPCError(context.Background(), status.Error(tc.code, tc.msg), nil, nil, grpcCallUnanswered)
			require.NotNil(t, resp, "the node-error arm, where the rate-limit rows read it")
			require.ErrorIs(t, err, common.StatusCodeError429)
			var lost *common.GRPCReplyLostError
			require.False(t, errors.As(err, &lost), "not a cut")
		})
	}
}

// A node's own UNAVAILABLE, its status frame read, is the node's answer whatever its text says.
func TestHandleGRPCError_AnsweredUnavailableIsTheNodesEvenWithALibraryText(t *testing.T) {
	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(context.Background(), status.Error(codes.Unavailable, "error reading from server: EOF"), nil, nil, grpcCallAnswered)
	require.NotNil(t, resp)
	var lost *common.GRPCReplyLostError
	require.False(t, errors.As(err, &lost))
}

// The answered case needs a status frame read off the wire, so the end-to-end tests below cover it.
func TestGRPCCallReachOf(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	_, frame := common.WithGRPCStatusFrame(context.Background())
	require.Equal(t, grpcCallUndelivered, grpcCallReachOf(&peer.Peer{}, frame), "no stream was opened")
	require.Equal(t, grpcCallUnanswered, grpcCallReachOf(&peer.Peer{Addr: addr}, frame), "a stream, and no status frame")
	require.Equal(t, grpcCallUnanswered, grpcCallReachOf(&peer.Peer{Addr: addr}, nil), "a call made without a recorder")
}

// cutPoint is where a cutting node breaks the connection during a call.
type cutPoint int

const (
	cutBeforeAnyReplyByte cutPoint = iota
	cutAfterReplyHeaders
)

// connTracker is a listener that remembers its connections, so a handler can cut its own.
type connTracker struct {
	net.Listener
	mu    sync.Mutex
	conns map[string]net.Conn
}

func (l *connTracker) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns[conn.RemoteAddr().String()] = conn
		l.mu.Unlock()
	}
	return conn, err
}

// cut closes the connection to remote: with an RST when reset is set, else with a FIN.
func (l *connTracker) cut(remote net.Addr, reset bool) {
	l.mu.Lock()
	conn := l.conns[remote.String()]
	l.mu.Unlock()
	if conn == nil {
		return
	}
	if tcp, ok := conn.(*net.TCPConn); ok && reset {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

// startNodeAnswering starts a gRPC node whose every call is handled by handle, with the node's
// listener to cut connections on.
func startNodeAnswering(t *testing.T, handle func(stream grpc.ServerStream, lis *connTracker) error) string {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	lis := &connTracker{Listener: raw, conns: map[string]net.Conn{}}
	srv := grpc.NewServer(grpc.ForceServerCodec(rawProtoCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		return handle(stream, lis)
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return raw.Addr().String()
}

// startCuttingNode cuts the connection of every call at the point given, after reading the request.
func startCuttingNode(t *testing.T, at cutPoint, reset bool) string {
	t.Helper()
	return startNodeAnswering(t, func(stream grpc.ServerStream, lis *connTracker) error {
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		if at == cutAfterReplyHeaders {
			if err := stream.SendHeader(metadata.Pairs("x-node", "1")); err != nil {
				return err
			}
			time.Sleep(100 * time.Millisecond) // the header frame is on the wire
		}
		p, _ := peer.FromContext(stream.Context())
		lis.cut(p.Addr, reset)
		<-stream.Context().Done()
		return stream.Context().Err()
	})
}

func directGRPCConnectionTo(t *testing.T, addr string) *GRPCDirectRPCConnection {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}, 1, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	grpcConn, ok := conn.(*GRPCDirectRPCConnection)
	require.True(t, ok)
	return grpcConn
}

// End to end over a real connection: the stats handler is what tells a cut from a node's status.
// The cut comes at both points the ticket names, as a reset and as a close.
func TestSendRequest_ConnectionCutDuringTheCallIsNoReply(t *testing.T) {
	for _, tc := range []struct {
		name  string
		at    cutPoint
		reset bool
		want  *common.RouterError
	}{
		{"reset before any reply byte", cutBeforeAnyReplyByte, true, common.RouterErrorConnectionReset},
		{"reset after the reply headers", cutAfterReplyHeaders, true, common.RouterErrorConnectionReset},
		{"closed before any reply byte", cutBeforeAnyReplyByte, false, common.RouterErrorConnectionClosed},
		{"closed after the reply headers", cutAfterReplyHeaders, false, common.RouterErrorConnectionClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startCuttingNode(t, tc.at, tc.reset)

			// What grpc-go itself says. On some platforms (macOS) a reset names both ends of the
			// socket, "read tcp <router>-><node>: read: connection reset by peer"; on others (Linux)
			// it does not. The search must find them when they are there, and always in the wording
			// production logged.
			if tc.reset {
				client, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				require.NoError(t, err)
				var reply []byte
				libErr := client.Invoke(context.Background(), "/test.v1.Node/Call", []byte{0x08, 0x01}, &reply, grpc.ForceCodec(rawProtoCodec{}))
				_ = client.Close()
				require.Equal(t, codes.Unavailable, status.Code(libErr))
				if libText := status.Convert(libErr).Message(); strings.Contains(libText, "read tcp") {
					require.Regexp(t, addressInText, libText, "the search finds the addresses in grpc-go's own text")
				}
				require.Regexp(t, addressInText, grpcResetTextWithAddresses, "the search finds the addresses in the wording production logged")
			}

			g := directGRPCConnectionTo(t, addr)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := g.SendRequest(ctx, []byte{0x08, 0x01}, map[string]string{GRPCMethodHeader: "test.v1.Node/Call"})

			require.Nil(t, resp, "no node answer")
			var lost *common.GRPCReplyLostError
			require.ErrorAs(t, err, &lost, "%v", err)
			require.Equal(t, tc.want, common.DetectConnectionError(err))
			require.NotRegexp(t, addressInText, err.Error())
		})
	}
}

// The control: the same node sending UNAVAILABLE itself is read as its answer.
func TestSendRequest_NodeSentUnavailableIsAnswered(t *testing.T) {
	addr := startNodeAnswering(t, func(stream grpc.ServerStream, _ *connTracker) error {
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "node is overloaded")
	})
	g := directGRPCConnectionTo(t, addr)
	resp, err := g.SendRequest(context.Background(), []byte{0x08, 0x01}, map[string]string{GRPCMethodHeader: "test.v1.Node/Call"})

	require.NotNil(t, resp)
	require.Equal(t, int(codes.Unavailable), resp.StatusCode)
	var statusErr *GRPCStatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, "node is overloaded", statusErr.Message)
}

// A call whose connection to the node could not be set up at all (a node that is down when the
// first request arrives) opened no stream, so no node saw it. It is undelivered: another provider
// is asked, the client gets the router's no-answer status, and a write it carried was not sent.
// It used to be an error no row names, and the client got its text.
func TestSendRequest_ConnectionThatCannotBeSetUpIsUndelivered(t *testing.T) {
	newConnTo := func(t *testing.T, nodeURL string, setUpErr error) *GRPCDirectRPCConnection {
		t.Helper()
		conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: nodeURL, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}, 5, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		grpcConn, ok := conn.(*GRPCDirectRPCConnection)
		require.True(t, ok)
		grpcConn.newConnector = func(context.Context, context.Context, uint, common.NodeUrl) (grpcConnectorInterface, error) {
			return nil, setUpErr
		}
		return grpcConn
	}
	newConn := func(t *testing.T, setUpErr error) *GRPCDirectRPCConnection {
		t.Helper()
		return newConnTo(t, "grpc://10.0.0.7:9090", setUpErr)
	}
	send := func(g *GRPCDirectRPCConnection) (*DirectRPCResponse, error) {
		return g.SendRequest(context.Background(), []byte{0x08, 0x01}, map[string]string{GRPCMethodHeader: "test.v1.Node/Call"})
	}

	t.Run("a node that cannot be reached", func(t *testing.T) {
		resp, err := send(newConn(t, errors.New("grpc failed to create the first connection ErrMsg: Reached maximum number of parallel connections attempts {address:10.0.0.7:9090}")))
		require.Nil(t, resp)
		var undelivered *common.GRPCCallUndeliveredError
		require.ErrorAs(t, err, &undelivered)
		routerErr := common.DetectConnectionError(err)
		require.Equal(t, common.RouterErrorProviderUnavailable, routerErr)
		require.True(t, routerErr.Retryable, "another provider is asked")
		require.False(t, routerErr.MayHaveReachedNode, "no stream was opened, so a write it carried was not sent")
		require.NotRegexp(t, addressInText, err.Error(), "relay_end and the logs get the node's address as [address]")
	})

	t.Run("a node configured by name", func(t *testing.T) {
		g := newConnTo(t, "grpc://node.example.com:9090/node-key", errors.New("grpc failed to create the first connection ErrMsg: dial tcp: lookup node.example.com: no such host"))
		_, err := send(g)
		var undelivered *common.GRPCCallUndeliveredError
		require.ErrorAs(t, err, &undelivered)
		require.Equal(t, common.RouterErrorDNSFailure, common.DetectConnectionError(err), "the text still names the fault: %v", err)
		for _, name := range []string{"node.example.com", ":9090", "node-key"} {
			require.NotContains(t, err.Error(), name)
		}
	})

	t.Run("the request's own deadline still reads as a timeout", func(t *testing.T) {
		_, err := send(newConn(t, fmt.Errorf("grpc failed to create the first connection: %w", context.DeadlineExceeded)))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, common.RouterErrorContextDeadline, common.DetectConnectionError(err))
	})

	t.Run("a connection the router closed keeps its own error", func(t *testing.T) {
		g := newConn(t, errors.New("not reached"))
		require.NoError(t, g.Close())
		_, err := send(g)
		require.ErrorIs(t, err, ErrGRPCConnectionClosed)
		var undelivered *common.GRPCCallUndeliveredError
		require.False(t, errors.As(err, &undelivered))
	})
}
