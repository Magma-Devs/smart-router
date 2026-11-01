package chainlib

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// endingSubscriptions acknowledges every subscribe, pushes once and ends the subscription, and
// reports endErr for the connection as a WSSubscriptionEndReporter does.
type endingSubscriptions struct {
	WSSubscriptionManager
	endErr error
}

func (endingSubscriptions) StartSubscription(context.Context, ProtocolMessage, string, string, string, *metrics.RelayMetrics) (*pairingtypes.RelayReply, <-chan *pairingtypes.RelayReply, error) {
	replies := make(chan *pairingtypes.RelayReply, 1)
	replies <- &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":{"number":1}}}`)}
	close(replies)
	return &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}, replies, nil
}

// UnsubscribeAll runs when the connection closes; there is nothing to tear down.
func (endingSubscriptions) UnsubscribeAll(context.Context, string, string, string, *metrics.RelayMetrics) error {
	return nil
}

func (m endingSubscriptions) SubscriptionEndError(string, string, string) error { return m.endErr }

func dialSubscriptionListener(t *testing.T, manager WSSubscriptionManager) *websocket.Conn {
	t.Helper()
	serveCtx, cancelServe := context.WithCancel(context.Background())
	t.Cleanup(cancelServe)
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener, addr := startTestJsonRPCListenerWithSubscriptions(t, serveCtx, common.DEFAULT_HEALTH_PATH, wsSubscribeStub{}, logger, manager)
	t.Cleanup(func() {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		_ = listener.Shutdown(shutdownCtx)
	})
	client, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", http.Header{"project-id": {"acme"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

const subscribeFrame = `{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":1}`

// MAG-4242: a subscription the manager ended because the client did not keep up closes the
// client's socket with 1013 (try again later), through the connection's writer, so the client
// reconnects instead of waiting on a socket nothing will push to again.
func TestConsumerWebsocketManager_SlowSubscriberIsClosedWithTryAgainLater(t *testing.T) {
	client := dialSubscriptionListener(t, endingSubscriptions{endErr: ErrSubscriptionClientTooSlow})
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte(subscribeFrame)))

	for frames := 0; ; frames++ {
		_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _, err := client.ReadMessage()
		if err == nil {
			require.Less(t, frames, 2, "an acknowledgement and one push, then the close")
			continue
		}
		var closeErr *websocket.CloseError
		require.ErrorAs(t, err, &closeErr, "the client got %v instead of a close frame", err)
		require.Equal(t, websocket.CloseTryAgainLater, closeErr.Code)
		require.Equal(t, ErrSubscriptionClientTooSlow.Error(), closeErr.Text)
		return
	}
}

// Every other end of a subscription leaves the socket open: the client subscribes again on it.
func TestConsumerWebsocketManager_OrdinarySubscriptionEndKeepsTheSocket(t *testing.T) {
	for _, endErr := range []error{nil, errors.New("some other end")} {
		client := dialSubscriptionListener(t, endingSubscriptions{endErr: endErr})
		for round := range 2 {
			require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte(subscribeFrame)))
			for frame := range 2 { // the acknowledgement and the push, in either order
				_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
				_, _, err := client.ReadMessage()
				require.NoError(t, err, "end %v, round %d, frame %d: the socket must stay open", endErr, round, frame)
			}
		}
	}
}
