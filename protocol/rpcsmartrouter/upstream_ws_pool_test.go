package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stallingWSUpstream is a WebSocket upstream whose upgrades wait until the test releases them,
// so a test can act while a pool's dial is in flight.
type stallingWSUpstream struct {
	url string
	// dialing receives once per upgrade request, as it arrives.
	dialing chan struct{}
	// socketClosed receives once per upgraded socket, when the router side closes it.
	socketClosed chan struct{}
	release      chan struct{}
	releaseOnce  sync.Once
}

func newStallingWSUpstream(t *testing.T) *stallingWSUpstream {
	t.Helper()
	upstream := &stallingWSUpstream{
		dialing:      make(chan struct{}, 8),
		socketClosed: make(chan struct{}, 8),
		release:      make(chan struct{}),
	}
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.dialing <- struct{}{}
		select {
		case <-upstream.release:
		case <-r.Context().Done():
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				upstream.socketClosed <- struct{}{}
				return
			}
		}
	}))
	t.Cleanup(func() {
		upstream.releaseUpgrades()
		server.CloseClientConnections()
		server.Close()
	})
	upstream.url = "ws" + strings.TrimPrefix(server.URL, "http")
	return upstream
}

func (u *stallingWSUpstream) releaseUpgrades() {
	u.releaseOnce.Do(func() { close(u.release) })
}

func receiveWithin(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestUpstreamWSPool_AReconnectRacingCloseLeavesNoSocketOpen pins the closed-check after a
// reconnect's dial, which the gRPC pool had and the WebSocket pool did not. The reconnect
// dials outside the pool lock; a Close in the meantime takes the connection list, and the
// connection the dial returns was then added to the closed pool and never closed — a live
// socket to the node with no owner.
func TestUpstreamWSPool_AReconnectRacingCloseLeavesNoSocketOpen(t *testing.T) {
	upstream := newStallingWSUpstream(t)
	pool := NewUpstreamWSPool(&common.NodeUrl{Url: upstream.url})

	reconnected := make(chan error, 1)
	go func() { reconnected <- pool.ReconnectWithBackoff(context.Background()) }()
	receiveWithin(t, upstream.dialing, "the reconnect's dial")

	require.NoError(t, pool.Close())
	upstream.releaseUpgrades()

	select {
	case err := <-reconnected:
		require.Error(t, err, "a reconnect that finishes on a closed pool must not report a connection")
	case <-time.After(5 * time.Second):
		t.Fatal("the reconnect did not return")
	}
	assert.Zero(t, pool.ConnectionCount(), "a closed pool holds no connection")
	receiveWithin(t, upstream.socketClosed, "the socket dialed during Close to be closed")
}

// TestUpstreamWSPool_ADialToAnUpstreamThatNeverAnswersTimesOut pins the WebSocket dial
// deadline. The pool dials under its lock when it scales up, and the WebSocket dial had no
// deadline of its own: a node that accepts the connection and never answers the upgrade held
// the lock for as long as the subscribing caller's context allowed, and every subscribe and
// release on the endpoint waited behind it. The gRPC pool bounds the same dial with its
// connection timeout.
func TestUpstreamWSPool_ADialToAnUpstreamThatNeverAnswersTimesOut(t *testing.T) {
	upstream := newStallingWSUpstream(t)
	pool := NewUpstreamWSPool(&common.NodeUrl{Url: upstream.url})
	pool.dialTimeout = 200 * time.Millisecond

	reserved := make(chan error, 1)
	go func() {
		_, err := pool.ReserveConnectionForSubscription(context.Background())
		reserved <- err
	}()

	select {
	case err := <-reserved:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the dial to an upstream that never answers the upgrade is still blocking the pool")
	}
	assert.Zero(t, pool.ConnectionCount())
}
