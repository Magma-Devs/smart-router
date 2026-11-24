package rpcclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cometCancelData is the reason CometBFT 0.37 and 0.38 give when they cancel a subscription
// whose event buffer is out of capacity (rpc/core/events.go).
const cometCancelData = "subscription was canceled (reason: internal subscription event buffer is out of capacity)"

// openTendermintSubscription subscribes to query on client and answers the subscribe as
// CometBFT does: an empty result under the request's id. It returns the subscription, its
// channel and the id of the request that opened it.
func openTendermintSubscription(t *testing.T, client *Client, codec *scriptedCodec, query string) (*ClientSubscription, chan *JsonrpcMessage, json.RawMessage) {
	t.Helper()
	written := codec.writtenCount()
	requestID := make(chan json.RawMessage, 1)
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for codec.writtenCount() == written && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		req := codec.lastWritten()
		requestID <- req.ID
		codec.incoming <- []*JsonrpcMessage{{Version: Vsn, ID: req.ID, Result: json.RawMessage(`{}`)}}
	}()
	ch := make(chan *JsonrpcMessage, 16)
	sub, _, err := client.Subscribe(context.Background(), nil, "subscribe", ch, map[string]interface{}{"query": query})
	require.NoError(t, err)
	require.NotNil(t, sub)
	return sub, ch, <-requestID
}

// tendermintEvent is an event CometBFT pushes on the subscription to query.
func tendermintEvent(requestID json.RawMessage, query string, height int) *JsonrpcMessage {
	return &JsonrpcMessage{Version: Vsn, ID: requestID, Result: json.RawMessage(fmt.Sprintf(
		`{"query":%q,"data":{"type":"tendermint/event/NewBlock","value":{"height":%d}}}`, query, height))}
}

// cometCancelFrame is CometBFT's cancel: a JSON-RPC error under id, on a socket it keeps open.
func cometCancelFrame(id json.RawMessage) *JsonrpcMessage {
	return &JsonrpcMessage{Version: Vsn, ID: id, Error: &JsonError{Code: -32000, Message: "Server error", Data: cometCancelData}}
}

func requireEventDelivered(t *testing.T, ch <-chan *JsonrpcMessage) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("an event the node pushed did not reach the subscription")
	}
}

func requireStillOpen(t *testing.T, sub *ClientSubscription) {
	t.Helper()
	select {
	case err := <-sub.Err():
		t.Fatalf("the subscription ended: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

func newScriptedClient(t *testing.T) (*Client, *scriptedCodec) {
	t.Helper()
	codec := newScriptedCodec()
	client, err := newClient(context.Background(), func(context.Context) (ServerCodec, error) { return codec, nil })
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client, codec
}

// TestTendermintCancel_EndsTheSubscription is MAG-5327's regression. CometBFT cancels a
// subscription with a JSON-RPC error under the subscribe request's id, older Tendermint under
// that id plus "#event", and keeps the socket open. The frame names no subscription and no
// pending call, so the client dropped it as an unsolicited response: the subscription stalled,
// open and silent, and nothing noticed. It has to end with the node's error.
func TestTendermintCancel_EndsTheSubscription(t *testing.T) {
	const query = "tm.event='NewBlock'"
	for _, tc := range []struct {
		name     string
		cancelID func(requestID json.RawMessage) json.RawMessage
	}{
		{"under the subscribe request's id (CometBFT 0.37, 0.38)", func(id json.RawMessage) json.RawMessage { return id }},
		{"under the id plus #event (older Tendermint)", func(id json.RawMessage) json.RawMessage {
			return json.RawMessage(`"` + string(id) + `#event"`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, codec := newScriptedClient(t)
			sub, ch, requestID := openTendermintSubscription(t, client, codec, query)
			for height := 1; height <= 3; height++ {
				codec.incoming <- []*JsonrpcMessage{tendermintEvent(requestID, query, height)}
				requireEventDelivered(t, ch)
			}

			codec.incoming <- []*JsonrpcMessage{cometCancelFrame(tc.cancelID(requestID))}

			select {
			case err := <-sub.Err():
				var nodeErr *JsonError
				require.True(t, errors.As(err, &nodeErr), "the subscription must end with the node's error, got %v", err)
				require.Equal(t, -32000, nodeErr.Code)
				require.Equal(t, cometCancelData, nodeErr.Data)
			case <-time.After(time.Second):
				t.Fatal("the node cancelled the subscription, but it did not end within 1 s")
			}
		})
	}
}

// TestTendermintCancel_LeavesTheOtherSubscriptionAlone: a cancel ends only the subscription
// whose request it names. Another subscription on the same connection keeps receiving.
func TestTendermintCancel_LeavesTheOtherSubscriptionAlone(t *testing.T) {
	const cancelled, kept = "tm.event='NewBlock'", "tm.event='Tx'"
	client, codec := newScriptedClient(t)
	cancelledSub, _, cancelledID := openTendermintSubscription(t, client, codec, cancelled)
	keptSub, keptCh, keptID := openTendermintSubscription(t, client, codec, kept)

	codec.incoming <- []*JsonrpcMessage{cometCancelFrame(cancelledID)}
	select {
	case <-cancelledSub.Err():
	case <-time.After(time.Second):
		t.Fatal("the cancelled subscription did not end")
	}

	codec.incoming <- []*JsonrpcMessage{tendermintEvent(keptID, kept, 1)}
	requireEventDelivered(t, keptCh)
	requireStillOpen(t, keptSub)
}

// TestUnmatchedErrorFrame_IsStillDropped: an error frame whose id names no pending call and
// no live subscription's request is dropped as before. It ends nothing.
func TestUnmatchedErrorFrame_IsStillDropped(t *testing.T) {
	const query = "tm.event='NewBlock'"
	client, codec := newScriptedClient(t)
	sub, ch, requestID := openTendermintSubscription(t, client, codec, query)

	codec.incoming <- []*JsonrpcMessage{
		cometCancelFrame(json.RawMessage(`999`)),
		cometCancelFrame(json.RawMessage(`"999#event"`)),
	}

	codec.incoming <- []*JsonrpcMessage{tendermintEvent(requestID, query, 1)}
	requireEventDelivered(t, ch)
	requireStillOpen(t, sub)
}

// TestForgetClientSub_DropsTheRequestIDEntry: releasing a subscription also drops it from the
// request-id lookup, and a stale release does not evict a later subscription under that id.
func TestForgetClientSub_DropsTheRequestIDEntry(t *testing.T) {
	h := newTestHandler()
	ch := make(chan *JsonrpcMessage)
	old := newClientSubscription(nil, "tendermint", reflect.ValueOf(ch))
	old.requestID = "7"
	h.subsByRequestID[old.requestID] = old

	h.forgetClientSub(old)
	require.NotContains(t, h.subsByRequestID, "7")

	replacement := newClientSubscription(nil, "tendermint", reflect.ValueOf(ch))
	replacement.requestID = "7"
	h.subsByRequestID[replacement.requestID] = replacement
	h.forgetClientSub(old) // stale
	require.Same(t, replacement, h.subsByRequestID["7"], "a stale release must not evict the live subscription")
}
