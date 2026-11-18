package rpcsmartrouter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePooledConn is a pooledConn with no socket behind it. A non-nil closeGate makes Close wait
// until the test closes the gate, and closing signals closing as Close begins.
type fakePooledConn struct {
	healthy   atomic.Bool
	closed    atomic.Bool
	count     atomic.Int32
	closeGate chan struct{}
	closing   chan struct{}
}

func newFakePooledConn() *fakePooledConn {
	conn := &fakePooledConn{}
	conn.healthy.Store(true)
	return conn
}

func (c *fakePooledConn) IsHealthy() bool { return c.healthy.Load() && !c.closed.Load() }

func (c *fakePooledConn) Close() error {
	if c.closing != nil {
		close(c.closing)
	}
	if c.closeGate != nil {
		<-c.closeGate
	}
	c.closed.Store(true)
	return nil
}

func (c *fakePooledConn) slots() int32 { return c.count.Load() }

func (c *fakePooledConn) addSlots(delta int32) int32 { return c.count.Add(delta) }

func newFakeConnPool(policy reconnectPolicy, dial func(context.Context) (*fakePooledConn, error)) *connPool[*fakePooledConn] {
	return newConnPool(poolVocabulary{name: "test", slotAttr: "existingSlots"}, "ws://upstream.test", dial, policy, 1, 5, 100)
}

// TestConnPool_TheSharedLadderWaitsOutTheRetryAfterTheLastDialNamed pins the Retry-After floor
// on the gRPC pool's reconnect, which only the WebSocket pool's had. That reconnect waits
// once on the shared ladder and dials once, so the Retry-After a failed dial carried is honoured
// by the next reconnect's wait rather than by a retry inside the same call.
func TestConnPool_TheSharedLadderWaitsOutTheRetryAfterTheLastDialNamed(t *testing.T) {
	const retryAfter = time.Second
	var dials atomic.Int32
	var secondDialAt atomic.Int64
	pool := newFakeConnPool(grpcReconnectPolicy, func(context.Context) (*fakePooledConn, error) {
		if dials.Add(1) == 1 {
			return nil, &common.RateLimitedError{RetryAfter: retryAfter}
		}
		secondDialAt.Store(time.Now().UnixNano())
		return newFakePooledConn(), nil
	})

	require.Error(t, pool.ReconnectWithBackoff(context.Background()))

	start := time.Now()
	require.NoError(t, pool.ReconnectWithBackoff(context.Background()))
	waited := time.Duration(secondDialAt.Load() - start.UnixNano())
	assert.GreaterOrEqual(t, waited, retryAfter,
		"the upstream named its wait; the ladder's own second rung is far shorter")
	assert.Equal(t, 1, pool.ConnectionCount())
}

// TestConnPool_CloseDoesNotHoldThePoolLockWhileConnectionsClose pins the gRPC pool's Close,
// which the WebSocket pool's lacked: closing a WebSocket waits on the socket, and the
// WebSocket pool did it under the pool lock, so every pick and release on the endpoint waited
// for the slowest socket to go.
func TestConnPool_CloseDoesNotHoldThePoolLockWhileConnectionsClose(t *testing.T) {
	pool := newFakeConnPool(wsReconnectPolicy, func(context.Context) (*fakePooledConn, error) {
		return nil, errors.New("not dialed in this test")
	})
	slow := newFakePooledConn()
	slow.closeGate = make(chan struct{})
	slow.closing = make(chan struct{})
	pool.connections = []*fakePooledConn{slow}

	closed := make(chan struct{})
	go func() {
		_ = pool.Close()
		close(closed)
	}()
	receiveWithin(t, slow.closing, "the pool to start closing its connection")

	counted := make(chan struct{})
	go func() {
		pool.ConnectionCount()
		close(counted)
	}()
	receiveWithin(t, counted, "the pool lock while a connection is still closing")

	close(slow.closeGate)
	receiveWithin(t, closed, "Close to return")
}

// TestConnPool_ReleaseOfANilConnectionIsANoOp keeps the guard both pools had: a subscription
// that never got a connection still releases its slot on cleanup.
func TestConnPool_ReleaseOfANilConnectionIsANoOp(t *testing.T) {
	pool := newFakeConnPool(grpcReconnectPolicy, nil)
	pool.release(nil)
	assert.Zero(t, pool.totalSlots())
}

// newDepartureTestPool is a pool whose dial hands out fake connections, one slot per
// connection, so each take lands on a connection of its own.
func newDepartureTestPool(departed bool) *connPool[*fakePooledConn] {
	pool := newFakeConnPool(wsReconnectPolicy, func(context.Context) (*fakePooledConn, error) {
		return newFakePooledConn(), nil
	}) // minConnections 1
	pool.perConn = 1
	pool.departed = func() bool { return departed }
	return pool
}

// TestConnPool_ADepartedEndpointClosesItsConnectionsWithItsLastSubscription pins the floor of
// an endpoint that left its manager's tiers. While a subscription remains, the pool scales
// down as before; once the last one leaves, its last connection closes too. minConnections
// kept that one open to an upstream nothing is placed on any more, for the life of the process
// (MAG-4924). An endpoint still served keeps its minimum.
func TestConnPool_ADepartedEndpointClosesItsConnectionsWithItsLastSubscription(t *testing.T) {
	for _, tc := range []struct {
		name     string
		departed bool
		kept     int
	}{
		{name: "still served", departed: false, kept: 1},
		{name: "departed", departed: true, kept: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := newDepartureTestPool(tc.departed)
			first, err := pool.take(context.Background(), true)
			require.NoError(t, err)
			second, err := pool.take(context.Background(), true)
			require.NoError(t, err)
			require.NotSame(t, first, second)

			pool.release(second)
			require.Equal(t, 1, pool.ConnectionCount(),
				"the connection still carrying a subscription stays, departed or not")
			require.Eventually(t, second.closed.Load, time.Second, time.Millisecond)

			pool.release(first)
			require.Equal(t, tc.kept, pool.ConnectionCount())
			if tc.departed {
				require.Eventually(t, first.closed.Load, time.Second, time.Millisecond)
			} else {
				require.False(t, first.closed.Load())
			}
		})
	}
}

// TestConnPool_ADepartedEndpointKeepsTheConnectionARestoreReopened: a subscription on a departed
// endpoint loses its connection and restores. The reconnect drops the dead connection, which
// the subscription still holds its slot on, and dials one the restore has not taken a slot on
// yet. A scale-down in that window must leave the new connection alone: the WebSocket waiters on
// the reconnect judge it by whether the pool holds a healthy connection, and would end their
// subscriptions on finding none.
func TestConnPool_ADepartedEndpointKeepsTheConnectionARestoreReopened(t *testing.T) {
	pool := newDepartureTestPool(true)
	dead, err := pool.take(context.Background(), true)
	require.NoError(t, err)

	dead.healthy.Store(false)
	require.NoError(t, pool.ReconnectWithBackoff(context.Background()))
	require.Equal(t, 1, pool.ConnectionCount(), "the reconnect replaced the dead connection")

	pool.maybeScaleDown()
	require.True(t, pool.IsHealthy(), "the restore's subscription still holds a slot")

	restored, err := pool.take(context.Background(), true)
	require.NoError(t, err)
	pool.release(dead)
	require.Equal(t, 1, pool.ConnectionCount())
	require.False(t, restored.closed.Load())

	pool.release(restored)
	require.Equal(t, 0, pool.ConnectionCount(), "the last subscription left")
	require.Eventually(t, restored.closed.Load, time.Second, time.Millisecond)
}
