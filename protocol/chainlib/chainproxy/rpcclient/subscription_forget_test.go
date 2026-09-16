package rpcclient

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestForgetClientSub_DropsOnlyThatSubscription pins the identity check: a later
// subscription registered under a reused id must not be removed by a stale unsubscribe.
func TestForgetClientSub_DropsOnlyThatSubscription(t *testing.T) {
	h := newTestHandler()
	ch := make(chan *JsonrpcMessage)
	old := newClientSubscription(nil, "eth", reflect.ValueOf(ch))
	old.subid = "0xabc"
	h.clientSubs[old.subid] = old

	h.forgetClientSub(old)
	require.NotContains(t, h.clientSubs, "0xabc")

	replacement := newClientSubscription(nil, "eth", reflect.ValueOf(ch))
	replacement.subid = "0xabc"
	h.clientSubs[replacement.subid] = replacement
	h.forgetClientSub(old) // stale
	require.Same(t, replacement, h.clientSubs["0xabc"], "a stale unsubscribe must not evict the live subscription")

	h.forgetClientSub(nil) // must not panic
}

// scriptedCodec is an in-memory ServerCodec: the test feeds it frames to read and
// records what the client writes.
type scriptedCodec struct {
	incoming chan []*JsonrpcMessage
	closedCh chan interface{}
	once     sync.Once

	mu      sync.Mutex
	written []*JsonrpcMessage
}

func newScriptedCodec() *scriptedCodec {
	return &scriptedCodec{incoming: make(chan []*JsonrpcMessage, 16), closedCh: make(chan interface{})}
}

func (c *scriptedCodec) peerInfo() PeerInfo { return PeerInfo{} }
func (c *scriptedCodec) readBatch() ([]*JsonrpcMessage, bool, error) {
	select {
	case msgs := <-c.incoming:
		return msgs, false, nil
	case <-c.closedCh:
		return nil, false, ErrClientQuit
	}
}
func (c *scriptedCodec) close()                     { c.once.Do(func() { close(c.closedCh) }) }
func (c *scriptedCodec) closed() <-chan interface{} { return c.closedCh }
func (c *scriptedCodec) remoteAddr() string         { return "scripted" }
func (c *scriptedCodec) writeJSON(_ context.Context, v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if msg, ok := v.(*JsonrpcMessage); ok {
		c.written = append(c.written, msg)
	}
	return nil
}

func (c *scriptedCodec) writtenCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.written)
}

func (c *scriptedCodec) lastWritten() *JsonrpcMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.written) == 0 {
		return nil
	}
	return c.written[len(c.written)-1]
}

// TestUnsubscribe_ForgetsDispatchEntry is the regression for MAG-3722's rpcclient leak:
// the dispatch table kept an entry for every subscription ever unsubscribed until the
// connection closed, and on a pooled upstream connection that is the life of the process.
//
// The entry is observed through behaviour rather than a hook. A push that names the
// subscription and carries a request id is claimed silently while the entry exists, and
// handed to the call path once it is gone, where the handler answers it. So the answer
// appearing on the wire is the entry disappearing.
func TestUnsubscribe_ForgetsDispatchEntry(t *testing.T) {
	codec := newScriptedCodec()
	client, err := newClient(context.Background(), func(context.Context) (ServerCodec, error) { return codec, nil })
	require.NoError(t, err)
	defer client.Close()

	// Answer the subscribe request as the node would.
	go func() {
		require.Eventually(t, func() bool { return codec.writtenCount() == 1 }, 5*time.Second, 5*time.Millisecond)
		req := codec.lastWritten()
		codec.incoming <- []*JsonrpcMessage{{Version: Vsn, ID: req.ID, Result: json.RawMessage(`"0xabc"`)}}
	}()
	ch := make(chan *JsonrpcMessage, 8)
	sub, _, err := client.Subscribe(context.Background(), nil, "eth_subscribe", ch, []interface{}{"newHeads"})
	require.NoError(t, err)
	require.NotNil(t, sub)

	// A Substrate-shaped push: the handler routes it by the params envelope, and that
	// route is the one that hands an unmatched id-bearing frame back to the call path.
	// (An eth_subscription frame is claimed by its method name whether or not the
	// table still has the entry, so it cannot show the entry going away.)
	push := func() {
		codec.incoming <- []*JsonrpcMessage{{
			Version: Vsn,
			ID:      json.RawMessage(`99`),
			Method:  "chain_newHead",
			Params:  json.RawMessage(`{"subscription":"0xabc","result":{"number":"0x1"}}`),
		}}
	}

	push()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("a push must reach the subscriber while it is live")
	}
	require.Equal(t, 1, codec.writtenCount(), "a claimed push is not answered")

	sub.Unsubscribe()

	require.Eventually(t, func() bool {
		push()
		return codec.writtenCount() > 1
	}, 5*time.Second, 10*time.Millisecond,
		"once the entry is forgotten an id-bearing push is handed to the call path and answered")
	answer := codec.lastWritten()
	require.NotNil(t, answer.Error, "the call path answers an unknown method with an error")
}
