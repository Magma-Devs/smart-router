package rpcsmartrouter

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// droppedClientsCounter counts the slow-consumer drops the manager reports.
type droppedClientsCounter struct {
	metrics.NoOpConsumerMetrics
	dropped atomic.Int32
}

func (c *droppedClientsCounter) SetSubscriptionClientDroppedMetric(string, string) { c.dropped.Add(1) }

// drainedUntilClosed reads the channel until it closes, or gives up after timeout.
func drainedUntilClosed(replies <-chan *pairingtypes.RelayReply, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-replies:
			if !ok {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// TestSlowClientIsDroppedNotWaitedOnEveryPush is MAG-4242 item 8 on the WebSocket manager. Two
// clients share one upstream subscription; one never reads. Before the fix the fan-out waited up
// to 10 ms on every push for the slow client (stalling the fast one), then lost the push with
// nothing counted and nothing told. Now the slow client is dropped once it is a buffer behind and
// frees no room within subscriptionClientSendGrace — counted, its channel closed so its
// subscription ends — and the fast client waits for it once, for the grace, not on every push.
func TestSlowClientIsDroppedNotWaitedOnEveryPush(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = time.Millisecond
	t.Cleanup(srv.Close)
	counter := &droppedClientsCounter{}
	manager := NewDirectWSSubscriptionManager(counter, "jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil)
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}

	fast := h.subscribe("ws-fast")
	slow := h.subscribe("ws-slow")
	require.Equal(t, 1, h.activeSubscriptionCount(), "both clients share one upstream subscription")

	var fastReceived atomic.Int64
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		for range fast.replies {
			fastReceived.Add(1)
		}
	}()

	require.Eventually(t, func() bool { return !h.attached(slow.clientKey) },
		10*time.Second, 10*time.Millisecond, "the client that never reads must be dropped")
	require.EqualValues(t, 1, counter.dropped.Load(), "the drop is counted")
	require.True(t, drainedUntilClosed(slow.replies, 5*time.Second), "the dropped client's channel closes, so its subscription visibly ends")

	require.True(t, h.attached(fast.clientKey), "the client that keeps up stays")
	require.Equal(t, 1, h.activeSubscriptionCount())
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load())
	before := fastReceived.Load()
	require.Eventually(t, func() bool { return fastReceived.Load() > before },
		5*time.Second, 10*time.Millisecond, "the fast client keeps receiving after the slow one is dropped")

	fast.disconnect()
	select {
	case <-fastDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the fast client's channel did not close on disconnect")
	}
}

// readEvery drains a client's channel, spending perFrame on each push the way the ingress
// spends time handing a frame to the client's socket, and counts what arrives. The returned
// channel closes when the client's channel does.
func readEvery(replies <-chan *pairingtypes.RelayReply, perFrame time.Duration) (*atomic.Int64, <-chan struct{}) {
	var received atomic.Int64
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for range replies {
			received.Add(1)
			for start := time.Now(); time.Since(start) < perFrame; {
			}
		}
	}()
	return &received, closed
}

// requireEveryPushOrEnd waits until the client has every one of want pushes, and fails as soon as
// its channel is closed under it instead.
func requireEveryPushOrEnd(t *testing.T, received *atomic.Int64, closed <-chan struct{}, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case <-closed:
			return true
		default:
			return received.Load() == want
		}
	}, 10*time.Second, time.Millisecond, "the client got %d of %d pushes", received.Load(), want)
	select {
	case <-closed:
		t.Fatalf("the client's subscription was ended under it after %d of %d pushes", received.Load(), want)
	default:
	}
}

// TestBurstyClientThatReadsIsNotDropped: a node publishes a block's logs back to back, and a
// client that reads every push, only a little slower than they arrive, falls a buffer behind
// within microseconds of the burst's start. It is keeping up; it must get every push and keep
// its subscription. Dropped at the first full channel, a client reading 20,000 pushes a second
// lost its subscription a few hundred pushes into a 1000-log burst.
func TestBurstyClientThatReadsIsNotDropped(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = time.Hour // only the burst below
	t.Cleanup(srv.Close)
	counter := &droppedClientsCounter{}
	manager := NewDirectWSSubscriptionManager(counter, "jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil)
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}

	sub := h.subscribe("ws-1")
	_, upstreamID := h.upstreamOf(h.subscriptionOf(sub.clientKey))
	received, closed := readEvery(sub.replies, 50*time.Microsecond)

	const burst = 1000
	h.srv.writeToRouter(t, func(write func([]byte)) {
		for i := range burst {
			frame, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params":  map[string]any{"subscription": upstreamID, "result": map[string]any{"logIndex": i}},
			})
			require.NoError(t, err)
			write(frame)
		}
	})
	requireEveryPushOrEnd(t, received, closed, burst)
	require.True(t, h.attached(sub.clientKey))
	require.Zero(t, counter.dropped.Load(), "a client that reads every push was counted as a slow consumer")
}
