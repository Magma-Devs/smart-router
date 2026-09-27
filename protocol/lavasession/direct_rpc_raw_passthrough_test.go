package lavasession

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// rawNode is an in-process gRPC node that serves no reflection and knows no
// schema: it records each request exactly as it arrived on the wire and answers
// with fixed bytes.
type rawNode struct {
	mu          sync.Mutex
	gotBody     []byte
	gotMetadata metadata.MD
	reply       []byte
}

func (n *rawNode) handle(_ any, stream grpc.ServerStream) error {
	var body []byte
	if err := stream.RecvMsg(&body); err != nil {
		return err
	}
	md, _ := metadata.FromIncomingContext(stream.Context())
	method, _ := grpc.MethodFromServerStream(stream)

	n.mu.Lock()
	n.gotBody, n.gotMetadata = body, md
	reply := n.reply
	n.mu.Unlock()

	if method == "/test.v1.Node/Fail" {
		return status.Error(codes.NotFound, "no such checkpoint")
	}
	if err := stream.SetHeader(metadata.Pairs("x-node", "raw")); err != nil {
		return err
	}
	return stream.SendMsg(reply)
}

// startRawNode serves n over bufconn and returns a direct connection to it.
func startRawNode(t *testing.T, n *rawNode) *GRPCDirectRPCConnection {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.ForceServerCodec(rawProtoCodec{}), grpc.UnknownServiceHandler(n.handle))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return directConnOver(t, lis)
}

func directConnOver(t *testing.T, lis *bufconn.Listener) *GRPCDirectRPCConnection {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	fake := newFakeGRPCConnector()
	fake.conn = conn
	g := newInitializedGRPCConn(fake)
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// TestBinaryRequestPassesThroughWithoutADescriptor is the regression for
// MAG-3886: a binary call used to need the method's descriptor from the node's
// reflection, so a node without reflection failed every call, and a slow one
// delayed it. The bytes here are valid protobuf that a decode and re-encode would
// reorder (field 2 before field 1), so byte identity also proves nothing re-encoded them.
func TestBinaryRequestPassesThroughWithoutADescriptor(t *testing.T) {
	node := &rawNode{reply: []byte{0x18, 0x2a, 0x0a, 0x02, 'o', 'k'}}
	g := startRawNode(t, node)
	request := []byte{0x10, 0x05, 0x0a, 0x03, 'a', 'b', 'c'}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := g.SendRequest(ctx, request, map[string]string{
		GRPCMethodHeader: "test.v1.Node/Call",
		"x-api-key":      "k",
	})
	require.NoError(t, err)

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, request, node.gotBody, "the node receives the caller's bytes unchanged")
	require.Equal(t, node.reply, resp.Data, "the caller receives the node's bytes unchanged")
	require.Equal(t, []string{"k"}, node.gotMetadata.Get("x-api-key"), "caller metadata reaches the node")
	require.Equal(t, []string{"application/grpc+proto"}, node.gotMetadata.Get("content-type"))
	require.Empty(t, node.gotMetadata.Get(GRPCMethodHeader), "internal headers stay internal")
	require.Equal(t, []string{"raw"}, resp.Metadata["x-node"], "response headers come back")
	require.Equal(t, 200, resp.StatusCode)
}

func TestEmptyBinaryRequestPassesThrough(t *testing.T) {
	node := &rawNode{reply: []byte{0x08, 0x01}}
	g := startRawNode(t, node)

	resp, err := g.SendRequest(context.Background(), nil, map[string]string{GRPCMethodHeader: "test.v1.Node/Call"})
	require.NoError(t, err)

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Empty(t, node.gotBody, "an empty request is an empty message")
	require.Equal(t, node.reply, resp.Data)
}

// TestBinaryRequestKeepsTheNodesErrorStatus pins that a raw call reports a node's
// gRPC error the same way the decoded path did: a typed status error alongside a
// response carrying the code.
func TestBinaryRequestKeepsTheNodesErrorStatus(t *testing.T) {
	g := startRawNode(t, &rawNode{})

	resp, err := g.SendRequest(context.Background(), []byte{0x08, 0x01}, map[string]string{GRPCMethodHeader: "test.v1.Node/Fail"})
	require.Error(t, err)
	var statusErr *GRPCStatusError
	require.True(t, errors.As(err, &statusErr))
	require.Equal(t, uint32(codes.NotFound), statusErr.Code)
	require.Equal(t, "no such checkpoint", statusErr.Message)
	require.NotNil(t, resp)
	require.Equal(t, int(codes.NotFound), resp.StatusCode)
}

// TestJSONRequestIsStillConvertedThroughTheDescriptor pins the other half: a JSON
// body has to become protobuf, so it still resolves the method's descriptor.
func TestJSONRequestIsStillConvertedThroughTheDescriptor(t *testing.T) {
	var mu sync.Mutex
	var gotService string
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if r, ok := req.(*healthpb.HealthCheckRequest); ok {
				mu.Lock()
				gotService = r.GetService()
				mu.Unlock()
			}
			return handler(ctx, req)
		}))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("x", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	g := directConnOver(t, lis)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := g.SendRequest(ctx, []byte(`{"service":"x"}`), map[string]string{GRPCMethodHeader: "grpc.health.v1.Health/Check"})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "x", gotService, "the JSON body reaches the node as the matching protobuf")
	require.NotEmpty(t, resp.Data)
	require.NotNil(t, g.GetCachedMethodDescriptor("grpc.health.v1.Health/Check"), "the JSON path resolved the descriptor")
}

func TestIsJSONRequest(t *testing.T) {
	require.True(t, isJSONRequest([]byte(`{}`)))
	require.True(t, isJSONRequest([]byte(`[1]`)))
	require.False(t, isJSONRequest(nil))
	require.False(t, isJSONRequest([]byte{0x0a, 0x01, 'x'}))
}
