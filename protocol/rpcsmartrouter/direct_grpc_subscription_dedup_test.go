package rpcsmartrouter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// TestGRPCStartSubscription_ConcurrentIdenticalSubscribesShareOneStream is MAG-4242 item 9. The
// manager allocated a pending-subscription map it never used, so clients arriving with the same
// params while the first was still dialling, reflecting and opening its stream each opened a
// stream of their own, and the last one to finish overwrote the map entry: every earlier stream
// lived on upstream with a client that could never unsubscribe it. Now the first client creates
// and the rest wait and join, so one upstream stream serves them all.
func TestGRPCStartSubscription_ConcurrentIdenticalSubscribesShareOneStream(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	const clients = 4
	message := newGrpcSubscriptionMessage(t)
	replies := make([]<-chan *pairingtypes.RelayReply, clients)
	errs := make([]error, clients)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, replies[i], errs[i] = manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", fmt.Sprintf("conn-%d", i), nil)
		}()
	}
	wg.Wait()
	for i := range clients {
		require.NoError(t, errs[i], "client %d", i)
		require.NotNil(t, replies[i], "client %d must get a channel of its own", i)
	}

	streams := 0
	settle := time.After(500 * time.Millisecond)
counting:
	for {
		select {
		case <-upstream.streamsOpen:
			streams++
		case <-settle:
			break counting
		}
	}
	require.Equal(t, 1, streams, "identical concurrent subscribes must share one upstream stream")
	require.Equal(t, int64(1), manager.totalSubscriptions.Load())
	manager.lock.RLock()
	trackedClients := len(manager.clientSubscriptions)
	manager.lock.RUnlock()
	require.Equal(t, clients, trackedClients, "every client is tracked on the one subscription")

	upstream.messages <- marshalStreamPayload(t, "shared-push")
	for i := range clients {
		require.Equal(t, "shared-push", awaitStreamPayload(t, replies[i]), "client %d receives the shared push", i)
	}
}
