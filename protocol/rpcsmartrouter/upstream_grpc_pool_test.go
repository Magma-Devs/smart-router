package rpcsmartrouter

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUpstreamGRPCPool(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}

	pool := NewUpstreamGRPCPool(nodeUrl)

	require.NotNil(t, pool)
	assert.Equal(t, nodeUrl, pool.nodeUrl)
	// sanitizeEndpointURL extracts just the host:port portion
	assert.Equal(t, "localhost:9090", pool.sanitizedURL)
	assert.Equal(t, 1, pool.minConnections)
	assert.Equal(t, 5, pool.maxConnections)
	assert.Equal(t, 100, pool.streamsPerConn)
	assert.Equal(t, 30*time.Second, pool.connectTimeout)
	assert.Equal(t, grpcChannelLiveness{
		idleTimeout:      2 * time.Minute,
		keepaliveTime:    5 * time.Minute,
		keepaliveTimeout: 20 * time.Second,
	}, pool.liveness)
	assert.NotNil(t, pool.backoff)
	assert.Empty(t, pool.connections)
}

func TestNewUpstreamGRPCPoolWithConfig(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpcs://example.com:443",
	}

	config := &GRPCStreamingConfig{
		PoolMinConnections:   2,
		PoolMaxConnections:   10,
		StreamsPerConnection: 50,
		ConnectionTimeout:    60 * time.Second,
		PoolIdleTimeout:      3 * time.Minute,
		PoolKeepaliveTime:    45 * time.Second,
		PoolKeepaliveTimeout: 10 * time.Second,
	}

	pool := NewUpstreamGRPCPoolWithConfig(nodeUrl, config)

	require.NotNil(t, pool)
	assert.Equal(t, 2, pool.minConnections)
	assert.Equal(t, 10, pool.maxConnections)
	assert.Equal(t, 50, pool.streamsPerConn)
	assert.Equal(t, 60*time.Second, pool.connectTimeout)
	assert.Equal(t, grpcChannelLiveness{
		idleTimeout:      3 * time.Minute,
		keepaliveTime:    45 * time.Second,
		keepaliveTimeout: 10 * time.Second,
	}, pool.liveness)
}

func TestNewUpstreamGRPCPoolWithConfig_NilConfig(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}

	pool := NewUpstreamGRPCPoolWithConfig(nodeUrl, nil)

	require.NotNil(t, pool)
	// Should use defaults
	assert.Equal(t, 1, pool.minConnections)
	assert.Equal(t, 5, pool.maxConnections)
	assert.Equal(t, newGRPCChannelLiveness(DefaultGRPCStreamingConfig()), pool.liveness)
}

func TestUpstreamGRPCPool_SetReconnectCallback(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	callbackCalled := false
	pool.SetReconnectCallback(func() {
		callbackCalled = true
	})

	// Verify callback is stored
	pool.lock.RLock()
	callback := pool.onReconnect
	pool.lock.RUnlock()

	assert.NotNil(t, callback)

	// Call it to verify it works
	callback()
	assert.True(t, callbackCalled)
}

func TestUpstreamGRPCPool_GetEndpoint(t *testing.T) {
	// Test URL sanitization - the sanitizeEndpointURL function extracts host:port only
	tests := []struct {
		name        string
		url         string
		expectedURL string
	}{
		{
			name:        "simple URL",
			url:         "grpc://localhost:9090",
			expectedURL: "localhost:9090", // sanitizeEndpointURL extracts just host:port
		},
		{
			name:        "URL with auth",
			url:         "grpc://user:pass@localhost:9090",
			expectedURL: "localhost:9090", // Auth info is stripped
		},
		{
			name:        "secure gRPC",
			url:         "grpcs://example.com:443",
			expectedURL: "example.com:443", // Same - just host:port
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeUrl := &common.NodeUrl{Url: tt.url}
			pool := NewUpstreamGRPCPool(nodeUrl)
			assert.Equal(t, tt.expectedURL, pool.GetEndpoint())
		})
	}
}

func TestUpstreamGRPCPool_ConnectionCount(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	// Initially should be 0
	assert.Equal(t, 0, pool.ConnectionCount())
}

func TestUpstreamGRPCPool_TotalStreamCount(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	// Initially should be 0
	assert.Equal(t, int32(0), pool.TotalStreamCount())
}

func TestUpstreamGRPCPool_Close_Empty(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	// Close empty pool should not panic
	err := pool.Close()
	assert.NoError(t, err)

	// Pool should be marked as closed
	assert.True(t, pool.closed.Load())
}

func TestUpstreamGRPCPool_Close_Idempotent(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	// First close
	err1 := pool.Close()
	assert.NoError(t, err1)

	// Second close should also be fine
	err2 := pool.Close()
	assert.NoError(t, err2)
}

func TestUpstreamGRPCPool_GetConnectionForStream_ClosedPool(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	pool.Close()

	ctx := context.Background()
	conn, err := pool.GetConnectionForStream(ctx)

	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "pool is closed")
}

func TestUpstreamGRPCStreamConnection_StreamCount(t *testing.T) {
	// Create a mock connection (without actual gRPC)
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://localhost:9090",
		sanitizedURL: "grpc://localhost:9090",
		createdAt:    time.Now(),
	}
	conn.healthy.Store(true)

	// Initial count should be 0
	assert.Equal(t, int32(0), conn.StreamCount())

	// Increment
	count := conn.IncrementStreams()
	assert.Equal(t, int32(1), count)
	assert.Equal(t, int32(1), conn.StreamCount())

	// Increment again
	count = conn.IncrementStreams()
	assert.Equal(t, int32(2), count)

	// Decrement
	count = conn.DecrementStreams()
	assert.Equal(t, int32(1), count)

	// Decrement again
	count = conn.DecrementStreams()
	assert.Equal(t, int32(0), count)
}

func TestUpstreamGRPCStreamConnection_IsHealthy(t *testing.T) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://localhost:9090",
		sanitizedURL: "grpc://localhost:9090",
		createdAt:    time.Now(),
	}

	// Initially not healthy (default is false)
	assert.False(t, conn.IsHealthy())

	// Mark as healthy
	conn.healthy.Store(true)
	assert.True(t, conn.IsHealthy())

	// Mark unhealthy
	conn.MarkUnhealthy(nil)
	assert.False(t, conn.IsHealthy())
}

func TestUpstreamGRPCStreamConnection_IsHealthy_WhenClosed(t *testing.T) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://localhost:9090",
		sanitizedURL: "grpc://localhost:9090",
		createdAt:    time.Now(),
	}
	conn.healthy.Store(true)
	assert.True(t, conn.IsHealthy())

	// Close the connection
	conn.closed.Store(true)

	// Should be unhealthy when closed even if healthy flag is true
	assert.False(t, conn.IsHealthy())
}

func TestUpstreamGRPCStreamConnection_MarkUnhealthy_WithError(t *testing.T) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://localhost:9090",
		sanitizedURL: "grpc://localhost:9090",
		createdAt:    time.Now(),
	}
	conn.healthy.Store(true)

	testErr := context.DeadlineExceeded
	conn.MarkUnhealthy(testErr)

	assert.False(t, conn.IsHealthy())

	// Check error was stored
	storedErr := conn.lastError.Load()
	assert.Equal(t, testErr, storedErr)
}

func TestUpstreamGRPCStreamConnection_GetEndpoint(t *testing.T) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://user:pass@localhost:9090",
		sanitizedURL: "grpc://[REDACTED]@localhost:9090",
	}

	// GetEndpoint should return sanitized URL
	assert.Equal(t, "grpc://[REDACTED]@localhost:9090", conn.GetEndpoint())
}

func TestUpstreamGRPCStreamConnection_GetNodeUrl(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	conn := &UpstreamGRPCStreamConnection{
		nodeUrl: nodeUrl,
	}

	assert.Equal(t, nodeUrl, conn.GetNodeUrl())
}

func TestUpstreamGRPCStreamConnection_Close_Idempotent(t *testing.T) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://localhost:9090",
		sanitizedURL: "grpc://localhost:9090",
		createdAt:    time.Now(),
	}

	// First close
	err1 := conn.Close()
	assert.NoError(t, err1)
	assert.True(t, conn.closed.Load())

	// Second close should be no-op
	err2 := conn.Close()
	assert.NoError(t, err2)
}

func TestUpstreamGRPCStreamConnection_ConcurrentStreamOperations(t *testing.T) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://localhost:9090",
		sanitizedURL: "grpc://localhost:9090",
		createdAt:    time.Now(),
	}
	conn.healthy.Store(true)

	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 100

	// Concurrently increment and decrement streams
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				conn.IncrementStreams()
				conn.StreamCount()
				conn.DecrementStreams()
			}
		}()
	}

	wg.Wait()

	// Final count should be 0
	assert.Equal(t, int32(0), conn.StreamCount())
}

func TestUpstreamGRPCPool_NotifyStreamRemoved_NilConn(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	// Should not panic with nil connection
	pool.NotifyStreamRemoved(nil)
}

func TestUpstreamGRPCPool_MaybeScaleDown_BelowMinConnections(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)
	pool.minConnections = 2

	// Add one mock connection
	mockConn := &UpstreamGRPCStreamConnection{
		endpoint:     "grpc://localhost:9090",
		sanitizedURL: "grpc://localhost:9090",
		createdAt:    time.Now(),
	}
	mockConn.healthy.Store(true)

	pool.lock.Lock()
	pool.connections = []*UpstreamGRPCStreamConnection{mockConn}
	pool.lock.Unlock()

	// Scale down should not remove connections below minConnections
	pool.maybeScaleDown()

	assert.Equal(t, 1, pool.ConnectionCount())
}

// installReconnectAttempt puts an attempt in flight on pool, standing for another caller's
// reconnect. The test ends it with pool.endReconnect.
func installReconnectAttempt(t *testing.T, pool *UpstreamGRPCPool) *reconnectAttempt {
	t.Helper()
	attempt := &reconnectAttempt{done: make(chan struct{})}
	require.True(t, pool.reconnectAttempt.CompareAndSwap(nil, attempt), "an attempt was already in flight")
	return attempt
}

// A caller arriving while a reconnect is in flight waits for it instead of failing: every
// stream on a dead connection restores through ReconnectWithBackoff at once, and a failure
// here tears that stream's subscription down.
func TestUpstreamGRPCPool_ReconnectWithBackoff_AlreadyReconnecting(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	// Another caller's attempt is in flight
	attempt := installReconnectAttempt(t, pool)

	done := make(chan error, 1)
	go func() { done <- pool.ReconnectWithBackoff(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("returned while the other attempt was still in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// That attempt ends and succeeds
	pool.endReconnect(attempt, nil, false)
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting after the other attempt ended")
	}
	assert.Equal(t, 0, pool.ConnectionCount(), "a waiting caller dials nothing itself")
}

// A waiting caller gets the outcome of the attempt it waited for: when that attempt failed,
// it must not go on as if the pool had reconnected.
func TestUpstreamGRPCPool_ReconnectWithBackoff_WaiterGetsTheFailure(t *testing.T) {
	pool := NewUpstreamGRPCPool(&common.NodeUrl{Url: "grpc://localhost:9090"})
	attempt := installReconnectAttempt(t, pool)

	done := make(chan error, 1)
	go func() { done <- pool.ReconnectWithBackoff(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("returned while the other attempt was still in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	dialErr := errors.New("connection refused")
	pool.endReconnect(attempt, dialErr, false)
	select {
	case err := <-done:
		assert.ErrorIs(t, err, dialErr)
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting after the other attempt ended")
	}
}

// An attempt that ended because its own caller left says nothing about the upstream. A waiter
// whose context is still live dials itself instead of failing with the other caller's
// cancellation.
func TestUpstreamGRPCPool_ReconnectWithBackoff_WaiterRetriesAnAbandonedAttempt(t *testing.T) {
	upstream := startRestoreUpstream(t)
	pool := NewUpstreamGRPCPool(&common.NodeUrl{
		Url:        "grpc://" + upstream.addr,
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	})
	defer pool.Close()
	attempt := installReconnectAttempt(t, pool)

	done := make(chan error, 1)
	go func() { done <- pool.ReconnectWithBackoff(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("returned while the other attempt was still in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// The other caller's context ended mid-attempt
	pool.endReconnect(attempt, context.Canceled, true)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("still waiting after the other attempt was abandoned")
	}
	assert.Equal(t, 1, pool.ConnectionCount(), "the waiter dials the connection itself")
}

func TestUpstreamGRPCPool_ReconnectWithBackoff_WaitEndsWithTheContext(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)
	attempt := installReconnectAttempt(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	assert.ErrorIs(t, pool.ReconnectWithBackoff(ctx), context.DeadlineExceeded)
	assert.Same(t, attempt, pool.reconnectAttempt.Load(), "a waiting caller must not uninstall the other attempt")
}

func TestUpstreamGRPCPool_ReconnectWithBackoff_ContextCanceled(t *testing.T) {
	nodeUrl := &common.NodeUrl{
		Url: "grpc://localhost:9090",
	}
	pool := NewUpstreamGRPCPool(nodeUrl)

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pool.ReconnectWithBackoff(ctx)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	// The attempt is uninstalled, so the next caller dials
	assert.Nil(t, pool.reconnectAttempt.Load())
}

// startSilentListener accepts TCP connections and never answers on them, so a gRPC dial to
// it waits out its whole timeout.
func startSilentListener(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var lock sync.Mutex
	var accepted []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // listener closed
			}
			lock.Lock()
			accepted = append(accepted, conn)
			lock.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		lock.Lock()
		defer lock.Unlock()
		for _, conn := range accepted {
			_ = conn.Close()
		}
	})
	return listener.Addr().String()
}

// A reconnect dials outside the pool lock. A dial can take up to connectTimeout, and every
// stream opened or released on the pool takes that lock, so a dial under it stalled them all.
func TestUpstreamGRPCPool_ReconnectWithBackoff_DialsOutsideThePoolLock(t *testing.T) {
	pool := NewUpstreamGRPCPool(&common.NodeUrl{
		Url:        "grpc://" + startSilentListener(t),
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	})
	pool.connectTimeout = 2 * time.Second
	defer pool.Close()

	done := make(chan error, 1)
	go func() { done <- pool.ReconnectWithBackoff(context.Background()) }()
	// Past the first backoff (at most 130ms), well inside the dial
	time.Sleep(400 * time.Millisecond)

	counted := make(chan int, 1)
	go func() { counted <- pool.ConnectionCount() }()
	select {
	case count := <-counted:
		assert.Equal(t, 0, count)
	case <-time.After(time.Second):
		t.Fatal("the pool lock is held while the reconnect dials")
	}
	assert.Error(t, <-done, "nothing answers, so the dial times out")
}

// A reconnect that completes once the pool has closed must not leave its connection in the
// pool, where nothing would ever close it.
func TestUpstreamGRPCPool_ReconnectWithBackoff_ClosedPoolKeepsNoConnection(t *testing.T) {
	upstream := startRestoreUpstream(t)
	pool := NewUpstreamGRPCPool(&common.NodeUrl{
		Url:        "grpc://" + upstream.addr,
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	})
	require.NoError(t, pool.Close())

	assert.ErrorContains(t, pool.ReconnectWithBackoff(context.Background()), "pool closed")
	assert.Equal(t, 0, pool.ConnectionCount())
}
