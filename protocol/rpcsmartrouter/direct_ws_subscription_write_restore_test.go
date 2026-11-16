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

// requireFinalWriteFrame reads exactly one frame from replies and decodes it as the ended-write
// notification, then requires the channel to close right after — nothing else is ever sent to a
// client whose write subscription just ended.
func requireFinalWriteFrame(t *testing.T, sub subscription) finalWriteFrame {
	t.Helper()
	var frame finalWriteFrame
	select {
	case reply, ok := <-sub.replies:
		require.True(t, ok, "the client must get a final frame, not a silent close")
		require.NoError(t, json.Unmarshal(reply.Data, &frame))
	case <-time.After(5 * time.Second):
		t.Fatal("no final frame arrived for the ended write subscription")
	}
	require.True(t, channelClosed(sub.replies, 5*time.Second),
		"nothing else may be sent to a client once its write subscription has ended")
	return frame
}

// TestWriteSubscriptionDrop_EndsInsteadOfResubscribing is MAG-5331's headline scenario: one
// client holds a submit-and-watch subscription, the upstream socket drops, and the router must
// not replay the stored params — the client's signed extrinsic — back to the node. Before the
// fix, handleUpstreamDisconnect re-subscribed on every restore with no regard for what the
// subscribe method actually did, so the node received the submission again.
func TestWriteSubscriptionDrop_EndsInsteadOfResubscribing(t *testing.T) {
	h := newReleaseHarness(t)

	creator := h.subscribeWrite("ws-1")
	joiner := h.subscribeWrite("ws-2") // dedupes onto the same upstream watch; both must be ended
	require.Equal(t, 1, h.upstreamCalls(writeSubscribeMethod), "the second client must join, not resubscribe")

	h.srv.dropConnections()

	creatorFrame := requireFinalWriteFrame(t, creator)
	require.Equal(t, writeSubscribeMethod, creatorFrame.Method)
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
			h := newReleaseHarness(t)
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
