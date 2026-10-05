package rpcsmartrouter

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
)

// restoreUpstream serves the streaming method to any number of subscriptions, told apart by
// the request's host field. Each stream gets one message carrying its host and is then held
// open, the shape of a quiet subscription. It counts the streams opened per host, and
// failFirst(host) ends the first stream of that host with an error.
type restoreUpstream struct {
	addr string

	mu        sync.Mutex
	opened    map[string]int
	failFirst map[string]chan struct{}
}

func startRestoreUpstream(t *testing.T, failingHosts ...string) *restoreUpstream {
	t.Helper()
	upstream := &restoreUpstream{opened: map[string]int{}, failFirst: map[string]chan struct{}{}}
	for _, host := range failingHosts {
		upstream.failFirst[host] = make(chan struct{})
	}

	handler := func(_ any, stream grpc.ServerStream) error {
		var request []byte
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		decoded := &grpc_reflection_v1.ServerReflectionRequest{}
		if err := proto.Unmarshal(request, decoded); err != nil {
			return err
		}
		host := decoded.GetHost()

		upstream.mu.Lock()
		upstream.opened[host]++
		var fail chan struct{}
		if upstream.opened[host] == 1 {
			fail = upstream.failFirst[host]
		}
		upstream.mu.Unlock()

		payload, err := proto.Marshal(&grpc_reflection_v1.ServerReflectionResponse{ValidHost: host})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(payload); err != nil {
			return err
		}
		select {
		case <-fail: // a nil channel never fires
			return status.Error(codes.Internal, "the upstream ended this stream")
		case <-stream.Context().Done():
			return nil
		}
	}

	server := grpc.NewServer(grpc.UnknownServiceHandler(handler), grpc.ForceServerCodec(grpcproxy.RawBytesCodec{}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	upstream.addr = listener.Addr().String()
	return upstream
}

func (u *restoreUpstream) endFirstStream(host string) {
	close(u.failFirst[host])
}

func (u *restoreUpstream) streamsOpened(host string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.opened[host]
}

// subscribeHost opens a subscription whose request names host, and waits for its first message.
func subscribeHost(t *testing.T, manager *DirectGRPCSubscriptionManager, host string) <-chan *pairingtypes.RelayReply {
	t.Helper()
	message := newGrpcSubscriptionMessageWithRequest(t, []byte(`{"host":"`+host+`"}`))
	_, replies, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", "conn-"+host, nil)
	require.NoError(t, err)
	require.Equal(t, host, awaitStreamPayload(t, replies))
	return replies
}

// TestGRPCSubscriptionRestore_EveryStreamOnADeadConnectionComesBack covers what keepalive, a
// GOAWAY or a middlebox reset does: the pooled connection dies and every stream on it fails at
// once. Each of their restores reaches ReconnectWithBackoff together, and all but the first
// used to be refused there, which tore those subscriptions down and closed their clients.
func TestGRPCSubscriptionRestore_EveryStreamOnADeadConnectionComesBack(t *testing.T) {
	upstream := startRestoreUpstream(t)
	proxy := startBlackholeProxy(t, upstream.addr)
	manager := newManagerAgainstUpstream(t, proxy.addr())
	defer manager.Stop()

	aReplies := subscribeHost(t, manager, "a")
	bReplies := subscribeHost(t, manager, "b")

	proxy.dropFlows()

	// A restored stream answers with its host again; a torn-down one closes the channel.
	require.Equal(t, "a", awaitStreamPayload(t, aReplies), "subscription a must be restored")
	require.Equal(t, "b", awaitStreamPayload(t, bReplies), "subscription b must be restored")
	require.Equal(t, int64(2), manager.GetActiveSubscriptionCount())
}

// TestGRPCSubscriptionRestore_OneFailedStreamIsReopenedOnce covers a stream that fails alone
// while its pooled connection keeps serving another. Its restore moves it to a new connection.
// The new connection used to be counted only after the old one was released, and the release's
// scale-down closed it as unused: the restored stream failed at once and went round again,
// with a new connection each time, for as long as the other stream held the old one.
func TestGRPCSubscriptionRestore_OneFailedStreamIsReopenedOnce(t *testing.T) {
	upstream := startRestoreUpstream(t, "y")
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	xReplies := subscribeHost(t, manager, "x")
	yReplies := subscribeHost(t, manager, "y")

	upstream.endFirstStream("y")

	require.Equal(t, "y", awaitStreamPayload(t, yReplies), "subscription y must be restored")
	// A restore that keeps going round shows up as more streams within this second.
	time.Sleep(time.Second)
	require.Equal(t, 2, upstream.streamsOpened("y"), "y must be reopened exactly once")
	require.Equal(t, 1, upstream.streamsOpened("x"), "x must be left alone")
	select {
	case reply, open := <-xReplies:
		t.Fatalf("x must neither be closed nor get a second message (open=%v, reply=%v)", open, reply)
	default:
	}
}
