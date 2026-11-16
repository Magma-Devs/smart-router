package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/stretchr/testify/require"
)

// writeSubscribeMethod and writeSubscribeParams stand in for Substrate's
// author_submitAndWatchExtrinsic: a subscribe whose params are the write itself (a signed
// extrinsic), on a chain that otherwise looks exactly like the mock server's default EVM-ish
// subscribe/notify dance. The mock server classifies by method name alone — it answers any
// subscribe it has not seen before — so standing in for Substrate here needs nothing beyond
// mockWSProtocolMessage.stateful.
const writeSubscribeMethod = "author_submitAndWatchExtrinsic"

var writeSubscribeParams = []interface{}{"0xdeadbeef"}

// subscribeWrite is releaseHarness.subscribe for a write subscription: the API's spec category
// is stateful, the way Category.Stateful is for author_submitAndWatchExtrinsic and
// transactionWatch_v1_submitAndWatch (MAG-5331).
func (h *releaseHarness) subscribeWrite(uid string) subscription {
	h.t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  writeSubscribeMethod,
		"params":  writeSubscribeParams,
		"id":      1,
	})
	require.NoError(h.t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method:   writeSubscribeMethod,
			params:   writeSubscribeParams,
			stateful: true,
		},
		rawData: body,
	}
	ctx, disconnect := context.WithCancel(context.Background())
	reply, replies, err := h.manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", uid, nil)
	require.NoError(h.t, err)
	require.NotNil(h.t, reply)
	require.NotNil(h.t, replies)
	var resp struct {
		Result string `json:"result"`
	}
	require.NoError(h.t, json.Unmarshal(reply.Data, &resp))
	require.NotEmpty(h.t, resp.Result, "the subscribe reply carries the router id")
	return subscription{
		ctx:        ctx,
		disconnect: disconnect,
		replies:    replies,
		clientKey:  h.manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", uid),
		routerID:   resp.Result,
		uid:        uid,
	}
}

// finalWriteFrame is what the client got on replies, decoded as endWriteSubscription's frame:
// {"jsonrpc":"2.0","method":"...","params":{"subscription":"<routerID>","error":{"code":...,"message":"..."}}}.
type finalWriteFrame struct {
	Method string `json:"method"`
	Params struct {
		Subscription string `json:"subscription"`
		Error        struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	} `json:"params"`
}

// writePushMethod is the name a Substrate node gives the status pushes of
// author_submitAndWatchExtrinsic, and the name a client routes them by.
const writePushMethod = "author_extrinsicUpdate"

// newWriteHarness is newReleaseHarness with the mock node naming a write subscription's pushes
// the way a Substrate node does. Set before anything subscribes, so the node only ever reads it.
func newWriteHarness(t *testing.T) *releaseHarness {
	t.Helper()
	h := newReleaseHarness(t)
	h.srv.notificationMethods = map[string]string{writeSubscribeMethod: writePushMethod}
	return h
}

// requireFirstPush waits for an ordinary push on sub, the way a node sends "ready" right after
// it accepts the extrinsic, and requires it to carry the node's push name.
func requireFirstPush(t *testing.T, sub subscription) {
	t.Helper()
	select {
	case reply, ok := <-sub.replies:
		require.True(t, ok, "the subscription ended before its first push")
		var push finalWriteFrame
		require.NoError(t, json.Unmarshal(reply.Data, &push))
		require.Equal(t, writePushMethod, push.Method)
		require.Zero(t, push.Params.Error.Code, "an ordinary push, not the final frame")
	case <-time.After(5 * time.Second):
		t.Fatal("no push arrived on the write subscription")
	}
}

// requireFinalWriteFrame reads replies up to the ended-write notification, the first frame that
// carries params.error, skipping the ordinary pushes the node sent before it went away. It then
// requires the channel to close: the subscription ends with that frame.
func requireFinalWriteFrame(t *testing.T, sub subscription) finalWriteFrame {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case reply, ok := <-sub.replies:
			require.True(t, ok, "the client must get a final frame, not a silent close")
			var frame finalWriteFrame
			require.NoError(t, json.Unmarshal(reply.Data, &frame))
			if frame.Params.Error.Code == 0 {
				continue // an ordinary push from before the drop
			}
			require.True(t, channelClosed(sub.replies, 5*time.Second),
				"the subscription must end with its final frame")
			return frame
		case <-deadline:
			t.Fatal("no final frame arrived for the ended write subscription")
		}
	}
}

// TestWriteSubscriptionDrop_EndsInsteadOfResubscribing is MAG-5331's headline scenario: one
// client holds a submit-and-watch subscription, the upstream socket drops, and the router must
// not replay the stored params — the client's signed extrinsic — back to the node. Before the
// fix, handleUpstreamDisconnect re-subscribed on every restore with no regard for what the
// subscribe method actually did, so the node received the submission again.
func TestWriteSubscriptionDrop_EndsInsteadOfResubscribing(t *testing.T) {
	h := newWriteHarness(t)

	creator := h.subscribeWrite("ws-1")
	joiner := h.subscribeWrite("ws-2") // dedupes onto the same upstream watch; both must be ended
	require.Equal(t, 1, h.upstreamCalls(writeSubscribeMethod), "the second client must join, not resubscribe")
	// The watch has pushed its first status by the time the socket drops, as a live one has.
	requireFirstPush(t, creator)
	requireFirstPush(t, joiner)

	h.srv.dropConnections()

	creatorFrame := requireFinalWriteFrame(t, creator)
	require.Equal(t, writePushMethod, creatorFrame.Method,
		"the final frame carries the node's push name, which the client routes by, not the subscribe call's")
	require.Equal(t, creator.routerID, creatorFrame.Params.Subscription,
		"the final frame must name the creator's own subscription id, not the joiner's or the upstream's")
	require.NotZero(t, creatorFrame.Params.Error.Code)
	require.NotEmpty(t, creatorFrame.Params.Error.Message)

	joinerFrame := requireFinalWriteFrame(t, joiner)
	require.Equal(t, joiner.routerID, joinerFrame.Params.Subscription,
		"every attached client gets its own frame on its own id, not just the creator")
	require.NotEqual(t, creator.routerID, joiner.routerID)

	require.Equal(t, 1, h.upstreamCalls(writeSubscribeMethod),
		"the node must never receive the write a second time, whether as a restore or as anything else")
	require.Equal(t, 0, h.upstreamCalls("author_unwatchExtrinsic"),
		"the mock server speaks only the method it was given; nothing here claims a declared unsubscribe")

	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond,
		"the manager must hold nothing for an ended write subscription")
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond,
		"the pool must not keep counting a subscription that will never restore")
	require.Eventually(t, func() bool { return goroutinesIn("listenForUpstreamMessages") == 0 }, 5*time.Second, 10*time.Millisecond,
		"no upstream listener may outlive an ended write subscription")

	require.Nil(t, creator.ctx.Err(), "the client's own socket must stay open — only the subscription ends")
	require.Nil(t, joiner.ctx.Err())
}

// TestWriteSubscriptionEndsForEveryDisconnectCause drives handleUpstreamDisconnect directly, the
// same technique TestSolanaRouterIDSurvivesUpstreamReconnect uses, for every cause it handles:
// a plain socket error, ErrClientReconnected, and ErrSubscriptionQueueOverflow. MAG-5331's
// acceptance criteria name all three; the guard sits before handleUpstreamDisconnect ever reads
// cause, so this proves it is not reached by one case and missed by another.
func TestWriteSubscriptionEndsForEveryDisconnectCause(t *testing.T) {
	causes := []struct {
		name  string
		cause error
	}{
		{"a plain socket error", errSentinelSocketClosed()},
		{"ErrClientReconnected", rpcclient.ErrClientReconnected},
		{"ErrSubscriptionQueueOverflow", rpcclient.ErrSubscriptionQueueOverflow},
	}

	for _, tc := range causes {
		t.Run(tc.name, func(t *testing.T) {
			h := newWriteHarness(t)
			sub := h.subscribeWrite("ws-1")

			activeSub := h.subscriptionOf(sub.clientKey)
			require.NotNil(t, activeSub)
			h.manager.lock.RLock()
			hashedParams := activeSub.hashedParams
			h.manager.lock.RUnlock()

			h.manager.handleUpstreamDisconnect(sub.ctx, hashedParams, activeSub, tc.cause)

			frame := requireFinalWriteFrame(t, sub)
			require.Equal(t, sub.routerID, frame.Params.Subscription)
			require.Equal(t, 1, h.upstreamCalls(writeSubscribeMethod), "no restore must have called the node again")
			require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
		})
	}
}

// TestWriteSubscriptionEndedBeforeAnyPush_NamesTheSubscribeCall: a drop that comes before the
// node pushed anything leaves the router no push name to use, so the final frame falls back to
// the subscribe call's name, the only one it has.
func TestWriteSubscriptionEndedBeforeAnyPush_NamesTheSubscribeCall(t *testing.T) {
	h := newWriteHarness(t)
	h.srv.messageInterval = time.Hour // the node accepts the submit and never pushes
	sub := h.subscribeWrite("ws-1")

	h.srv.dropConnections()

	frame := requireFinalWriteFrame(t, sub)
	require.Equal(t, writeSubscribeMethod, frame.Method)
	require.Equal(t, sub.routerID, frame.Params.Subscription)
	require.Equal(t, 1, h.upstreamCalls(writeSubscribeMethod))
}

// errSentinelSocketClosed stands in for the plain, non-JsonError the listener passes
// handleUpstreamDisconnect on an ordinary socket drop (io.EOF, a reset, a gateway hangup —
// anything that is not one of rpcclient's two named causes).
func errSentinelSocketClosed() error {
	return context.DeadlineExceeded
}

// TestWriteSubscriptionEndedFrame_Shape pins the wire shape endWriteSubscription sends: a
// notification (method + params.subscription), matching the shape every other push on this
// subscription would carry, with "error" standing in for "result" instead of a close with
// nothing on the wire at all.
func TestWriteSubscriptionEndedFrame_Shape(t *testing.T) {
	frame, err := writeSubscriptionEndedFrame(writeSubscribeMethod, "rs_abc123", false)
	require.NoError(t, err)

	var decoded finalWriteFrame
	require.NoError(t, json.Unmarshal(frame, &decoded))
	require.Equal(t, writeSubscribeMethod, decoded.Method)
	require.Equal(t, "rs_abc123", decoded.Params.Subscription)
	require.Equal(t, -32000, decoded.Params.Error.Code)
	require.Equal(t, errUnclearWriteAnswerLost.Error(), decoded.Params.Error.Message)
}
