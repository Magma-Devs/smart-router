package rpcsmartrouter

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// poolIdleTestTimeout is the PoolIdleTimeout the idle tests run with.
const poolIdleTestTimeout = 250 * time.Millisecond

// blackholeProxy is a loopback TCP proxy standing in for a NAT or load balancer. freeze()
// stops delivering bytes both ways on every flow open at that moment and passes on no close,
// which is how a forgotten flow looks to HTTP/2. It still acknowledges at the TCP layer, so
// kernel retransmission timeouts never fire here. Flows accepted after freeze() pass normally.
type blackholeProxy struct {
	listener net.Listener
	target   string

	lock   sync.Mutex
	flows  []*proxyFlow
	closed bool
}

type proxyFlow struct {
	client   net.Conn
	upstream net.Conn
	frozen   atomic.Bool
}

func startBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxy := &blackholeProxy{listener: listener, target: target}
	go proxy.acceptLoop()
	t.Cleanup(proxy.close)
	return proxy
}

func (p *blackholeProxy) acceptLoop() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return // listener closed
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		flow := &proxyFlow{client: client, upstream: upstream}

		p.lock.Lock()
		if p.closed {
			p.lock.Unlock()
			_ = client.Close()
			_ = upstream.Close()
			return
		}
		p.flows = append(p.flows, flow)
		p.lock.Unlock()

		go flow.pipe(upstream, client)
		go flow.pipe(client, upstream)
	}
}

// pipe forwards src to dst until src fails. Once the flow is frozen it keeps reading and
// drops what it reads, and a close on one side no longer reaches the other.
func (f *proxyFlow) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !f.frozen.Load() {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			if !f.frozen.Load() {
				_ = dst.Close()
			}
			return
		}
	}
}

func (p *blackholeProxy) addr() string {
	return p.listener.Addr().String()
}

func (p *blackholeProxy) freeze() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, flow := range p.flows {
		flow.frozen.Store(true)
	}
}

// flowCount reports how many flows the proxy has carried, frozen or not.
func (p *blackholeProxy) flowCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.flows)
}

func (p *blackholeProxy) close() {
	_ = p.listener.Close()
	p.lock.Lock()
	defer p.lock.Unlock()
	p.closed = true
	for _, flow := range p.flows {
		_ = flow.client.Close()
		_ = flow.upstream.Close()
	}
}

// holdingStreamService answers StreamingOutputCall with one message and then holds the
// stream open until the client leaves, the shape of a subscription on a quiet chain.
type holdingStreamService struct {
	testgrpc.UnimplementedTestServiceServer
}

func (holdingStreamService) StreamingOutputCall(_ *testgrpc.StreamingOutputCallRequest, stream grpc.ServerStreamingServer[testgrpc.StreamingOutputCallResponse]) error {
	if err := stream.Send(&testgrpc.StreamingOutputCallResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func startHoldingStreamServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	testgrpc.RegisterTestServiceServer(server, holdingStreamService{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// startPoolBehindProxy builds a pool from config whose upstream is a holding stream server
// reached through a blackholeProxy.
func startPoolBehindProxy(t *testing.T, config *GRPCStreamingConfig) (*blackholeProxy, *UpstreamGRPCPool) {
	t.Helper()
	proxy := startBlackholeProxy(t, startHoldingStreamServer(t))
	pool := NewUpstreamGRPCPoolWithConfig(&common.NodeUrl{
		Url:        "grpc://" + proxy.addr(),
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	}, config)
	t.Cleanup(func() { _ = pool.Close() })
	return proxy, pool
}

// idleOnlyConfig is the default streaming config with the given idle timeout and keepalive off.
func idleOnlyConfig(idleTimeout time.Duration) *GRPCStreamingConfig {
	config := DefaultGRPCStreamingConfig()
	config.ConnectionTimeout = 5 * time.Second
	config.PoolIdleTimeout = idleTimeout
	config.PoolKeepaliveTime = 0
	config.PoolKeepaliveTimeout = 0
	return config
}

// openPooledStream opens StreamingOutputCall on a connection from pool and returns once
// the first message has arrived.
func openPooledStream(ctx context.Context, pool *UpstreamGRPCPool) (grpc.ServerStreamingClient[testgrpc.StreamingOutputCallResponse], error) {
	conn, err := pool.GetConnectionForStream(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := testgrpc.NewTestServiceClient(conn.GetConn()).StreamingOutputCall(ctx, &testgrpc.StreamingOutputCallRequest{})
	if err != nil {
		return nil, err
	}
	if _, err := stream.Recv(); err != nil {
		return nil, err
	}
	return stream, nil
}

// streamAfterDroppedIdleFlow runs one stream to its end, leaves the pool unused well past
// poolIdleTestTimeout, black-holes every flow the proxy carries, and then opens another
// stream, allowing it 2s to deliver its first message.
func streamAfterDroppedIdleFlow(t *testing.T, proxy *blackholeProxy, pool *UpstreamGRPCPool) error {
	t.Helper()
	firstCtx, endFirst := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := openPooledStream(firstCtx, pool)
	endFirst()
	require.NoError(t, err)

	time.Sleep(4 * poolIdleTestTimeout)
	proxy.freeze()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = openPooledStream(ctx, pool)
	return err
}

// TestUpstreamGRPCPool_IdleTimeoutRedialsInsteadOfReusingADroppedFlow is the regression
// test for MAG-3887, with keepalive off to isolate the idle timeout. A stream opened over a
// flow a middlebox dropped while the channel sat idle waits on it until TCP gives up; the
// idle timeout closes that transport first, so the stream dials a new flow instead.
func TestUpstreamGRPCPool_IdleTimeoutRedialsInsteadOfReusingADroppedFlow(t *testing.T) {
	proxy, pool := startPoolBehindProxy(t, idleOnlyConfig(poolIdleTestTimeout))

	err := streamAfterDroppedIdleFlow(t, proxy, pool)

	require.NoError(t, err, "the stream must not wait on the dropped flow")
	require.Equal(t, 2, proxy.flowCount(), "the idle channel must dial a new flow for the next stream")
	require.Equal(t, 1, pool.ConnectionCount(), "an idle channel reconnects in place; the pool keeps its connection")
}

// TestUpstreamGRPCPool_WithoutIdleTimeoutAStreamWaitsOnADroppedFlow is the control for the
// test above, with PoolIdleTimeout 0: the channel keeps its transport through the idle
// spell and the stream waits on the dropped flow until its deadline. It shows the proxy
// reproduces the failure, so the test above cannot pass vacuously.
func TestUpstreamGRPCPool_WithoutIdleTimeoutAStreamWaitsOnADroppedFlow(t *testing.T) {
	proxy, pool := startPoolBehindProxy(t, idleOnlyConfig(0))

	err := streamAfterDroppedIdleFlow(t, proxy, pool)

	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "want the stream stuck on the dropped flow, got %v", err)
	require.Equal(t, 1, proxy.flowCount(), "no new flow is dialed while the old transport looks healthy")
}

// TestUpstreamGRPCPool_OpenStreamHoldsTheChannelOutOfIdle pins that the idle timeout only
// reaps unused channels: a quiet subscription is an RPC in flight, so its transport stays
// up however long the upstream has nothing to send.
func TestUpstreamGRPCPool_OpenStreamHoldsTheChannelOutOfIdle(t *testing.T) {
	proxy, pool := startPoolBehindProxy(t, idleOnlyConfig(poolIdleTestTimeout))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := openPooledStream(ctx, pool)
	require.NoError(t, err)

	time.Sleep(4 * poolIdleTestTimeout)

	conn, err := pool.GetConnectionForStream(ctx)
	require.NoError(t, err)
	require.Equal(t, connectivity.Ready, conn.GetConn().GetState(), "a channel with an open stream must not go idle")
	require.Equal(t, 1, proxy.flowCount())
}

// TestUpstreamGRPCPool_NegativeIdleTimeoutDisablesIdleness pins that a negative
// PoolIdleTimeout disables idleness the way 0 does. grpc-go fires a negative idle timer at
// once, which would idle the channel before connect() ever saw it READY.
func TestUpstreamGRPCPool_NegativeIdleTimeoutDisablesIdleness(t *testing.T) {
	_, pool := startPoolBehindProxy(t, idleOnlyConfig(-time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := openPooledStream(ctx, pool)

	require.NoError(t, err)
}

// TestUpstreamGRPCPool_KeepaliveEndsAStreamOnADroppedFlow covers a flow dropped under a
// live stream: the unacknowledged ping closes the transport and the stream fails with
// Unavailable, which is what sends a subscription to restore. grpc-go pings at most every
// 10s, so this takes about 11s.
func TestUpstreamGRPCPool_KeepaliveEndsAStreamOnADroppedFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("keepalive cannot ping sooner than grpc-go's 10s minimum")
	}
	config := DefaultGRPCStreamingConfig()
	config.ConnectionTimeout = 5 * time.Second
	config.PoolIdleTimeout = 0
	config.PoolKeepaliveTime = 10 * time.Second
	config.PoolKeepaliveTimeout = time.Second
	proxy, pool := startPoolBehindProxy(t, config)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream, err := openPooledStream(ctx, pool)
	require.NoError(t, err)

	proxy.freeze()
	_, err = stream.Recv()

	require.Equal(t, codes.Unavailable, status.Code(err), "want keepalive to close the transport, got %v", err)
}

// TestDefaultPoolKeepaliveRespectsGRPCServerMinTime pins the default ping interval at or
// above a grpc-go server's default keepalive MinTime (5m). The Cosmos SDK's gRPC server, which
// serves dYdX's order book stream, keeps that default. A client that pings more often on a
// connection whose streams are quiet gets GOAWAY too_many_pings after its third such ping, and
// the server closes the connection with every stream on it.
func TestDefaultPoolKeepaliveRespectsGRPCServerMinTime(t *testing.T) {
	const grpcServerDefaultMinTime = 5 * time.Minute
	require.GreaterOrEqual(t, DefaultGRPCStreamingConfig().PoolKeepaliveTime, grpcServerDefaultMinTime)
}

// TestGRPCChannelLiveness_Keepalive pins the keepalive a pooled channel dials with, and
// that 0 in either keepalive value leaves the option out altogether: grpc-go would raise
// a zero ping interval to its 10s minimum and ping anyway.
func TestGRPCChannelLiveness_Keepalive(t *testing.T) {
	liveness := newGRPCChannelLiveness(DefaultGRPCStreamingConfig())
	params, ok := liveness.keepaliveParams()
	require.True(t, ok)
	require.Equal(t, keepalive.ClientParameters{
		Time:                5 * time.Minute,
		Timeout:             20 * time.Second,
		PermitWithoutStream: false,
	}, params)
	require.Len(t, liveness.dialOptions(), 2, "idle timeout and keepalive")

	tests := []struct {
		name    string
		disable func(*GRPCStreamingConfig)
	}{
		{name: "keepalive time 0", disable: func(c *GRPCStreamingConfig) { c.PoolKeepaliveTime = 0 }},
		{name: "keepalive timeout 0", disable: func(c *GRPCStreamingConfig) { c.PoolKeepaliveTimeout = 0 }},
		{name: "keepalive time negative", disable: func(c *GRPCStreamingConfig) { c.PoolKeepaliveTime = -time.Second }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultGRPCStreamingConfig()
			tt.disable(config)
			liveness := newGRPCChannelLiveness(config)

			_, ok := liveness.keepaliveParams()
			require.False(t, ok)
			require.Len(t, liveness.dialOptions(), 1, "only the idle timeout")
		})
	}
}
