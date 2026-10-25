package rpcsmartrouter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGRPCSlowClientStreamEndsWithStatus is MAG-4242 item 8 on the gRPC manager. A joiner that
// never reads falls a buffer behind; its stream must end with a status that says so, the drop
// must be counted, and the creator, which keeps up, must keep receiving.
func TestGRPCSlowClientStreamEndsWithStatus(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	counter := &droppedClientsCounter{}
	manager.metricsManager = counter
	defer manager.Stop()

	message := newGrpcSubscriptionMessage(t)
	_, creatorReplies, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", "creator-conn", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)
	_, laggardReplies, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", "laggard-conn", nil)
	require.NoError(t, err)
	laggardKey := manager.ClientKey("dapp", "1.1.1.1", "laggard-conn")

	var creatorReceived atomic.Int64
	go func() {
		for range creatorReplies {
			creatorReceived.Add(1)
		}
	}()

	// One push more than the laggard's buffer holds.
	for i := 0; i <= subscriptionClientReplyBuffer; i++ {
		upstream.messages <- marshalStreamPayload(t, fmt.Sprintf("checkpoint-%d", i))
	}

	// Only once the drop is counted may the test read the laggard's channel: reading earlier
	// would make it a client that keeps up.
	require.Eventually(t, func() bool { return counter.dropped.Load() == 1 },
		10*time.Second, 10*time.Millisecond, "the client that never reads must be dropped")
	require.True(t, drainedUntilClosed(laggardReplies, 10*time.Second), "the laggard's channel must close")
	endErr := manager.SubscriptionEndError(laggardKey)
	require.Error(t, endErr, "the laggard's stream must end with a status, not OK")
	require.Equal(t, codes.Unavailable, status.Code(endErr))
	require.Contains(t, status.Convert(endErr).Message(), "keep up")

	require.Equal(t, int64(1), manager.totalSubscriptions.Load(), "the creator's subscription lives on")
	before := creatorReceived.Load()
	upstream.messages <- marshalStreamPayload(t, "after-the-drop")
	require.Eventually(t, func() bool { return creatorReceived.Load() > before },
		5*time.Second, 10*time.Millisecond, "the creator keeps receiving after the laggard is dropped")
}

// TestGRPCDepartedJoinerIsNotCountedAsSlow: a joiner's reply channel is bound to its stream's
// context. When the client hangs up, the stream's Close releases it through UnsubscribeAll, but
// a push can reach the fan-out first and find the sender's context ended. That client left; it
// did not fall behind, so it is neither counted as a slow consumer nor given its status.
func TestGRPCDepartedJoinerIsNotCountedAsSlow(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	counter := &droppedClientsCounter{}
	manager.metricsManager = counter
	defer manager.Stop()

	message := newGrpcSubscriptionMessage(t)
	_, creatorReplies, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", "creator-conn", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)
	joinerCtx, hangUp := context.WithCancel(context.Background())
	_, _, err = manager.StartSubscription(joinerCtx, message, "dapp", "1.1.1.1", "joiner-conn", nil)
	require.NoError(t, err)
	joinerKey := manager.ClientKey("dapp", "1.1.1.1", "joiner-conn")

	hangUp() // the joiner's stream ended; its UnsubscribeAll has not run yet
	upstream.messages <- marshalStreamPayload(t, "after-hang-up")
	require.Equal(t, "after-hang-up", awaitStreamPayload(t, creatorReplies))
	require.Zero(t, counter.dropped.Load(), "a client that hung up was counted as one that did not keep up")
	require.NoError(t, manager.SubscriptionEndError(joinerKey), "a client that hung up was given the slow-consumer status")
}

// TestGRPCBurstyClientThatReadsIsNotDropped: the gRPC side of TestBurstyClientThatReadsIsNotDropped.
// A client reading every message at 20,000 a second lost its stream to a 300-message burst when
// the first full channel dropped it.
func TestGRPCBurstyClientThatReadsIsNotDropped(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	counter := &droppedClientsCounter{}
	manager.metricsManager = counter
	defer manager.Stop()

	_, replies, err := manager.StartSubscription(context.Background(), newGrpcSubscriptionMessage(t), "dapp", "1.1.1.1", "conn-1", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)
	received, closed := readEvery(replies, 50*time.Microsecond)

	const burst = 300
	for i := range burst {
		select {
		case upstream.messages <- marshalStreamPayload(t, fmt.Sprintf("checkpoint-%d", i)):
		case <-closed:
			t.Fatalf("the client's stream was ended under it after %d of %d messages", received.Load(), burst)
		}
	}
	requireEveryPushOrEnd(t, received, closed, burst)
	require.Zero(t, counter.dropped.Load(), "a client that reads every message was counted as a slow consumer")
}
