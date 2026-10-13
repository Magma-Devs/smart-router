package rpcsmartrouter

import (
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

// TestSlowClientIsDroppedNotWaitedFor is MAG-4242 item 8 on the WebSocket manager. Two clients
// share one upstream subscription; one never reads. Before the fix the fan-out waited up to
// 10 ms per push for the slow client (stalling the fast one), then lost the push with nothing
// counted and nothing told. Now the slow client is dropped once it is a buffer behind — counted,
// its channel closed so its subscription visibly ends — and the fast client is never stalled.
func TestSlowClientIsDroppedNotWaitedFor(t *testing.T) {
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
