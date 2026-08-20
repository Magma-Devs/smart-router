package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// recordingOptimizer observes AppendRelayFailure calls, which the shared
// fakeSubscriptionOptimizer deliberately no-ops.
type recordingOptimizer struct {
	fakeSubscriptionOptimizer
	mu       sync.Mutex
	failures []string
}

func (r *recordingOptimizer) AppendRelayFailure(providerAddress string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, providerAddress)
}

// A rate-limited subscribe is held off, not scored: no availability sample, a registry
// hold-off with the upstream's Retry-After as the floor.
func TestWSNoteSubscribeFailure_RateLimitHoldsOffInsteadOfScoring(t *testing.T) {
	reg := withFreshRelayHoldoff(t)
	opt := &recordingOptimizer{}
	dwsm := &DirectWSSubscriptionManager{optimizer: opt}

	dwsm.noteSubscribeFailure("wss://busy.example/ws", common.RateLimited(errors.New("handshake refused"), 2*time.Minute))

	require.Empty(t, opt.failures, "a rate-limited subscribe must not produce an availability sample")
	require.True(t, reg.HeldOff("wss://busy.example/ws", "wss://busy.example/ws"))
}

// Any other failure keeps today's scoring.
func TestWSNoteSubscribeFailure_GenuineFailureStillScores(t *testing.T) {
	reg := withFreshRelayHoldoff(t)
	opt := &recordingOptimizer{}
	dwsm := &DirectWSSubscriptionManager{optimizer: opt}

	dwsm.noteSubscribeFailure("wss://broken.example/ws", errors.New("connection refused"))

	require.Equal(t, []string{"wss://broken.example/ws"}, opt.failures)
	require.False(t, reg.HeldOff("wss://broken.example/ws", "wss://broken.example/ws"))
}

// Selection skips held-off endpoints while something ready remains, and keeps the full
// tier when nothing does — a subscription must still be served.
func TestWSSelectFromTier_SkipsHeldOffEndpoints(t *testing.T) {
	reg := withFreshRelayHoldoff(t)
	dwsm := &DirectWSSubscriptionManager{} // no optimizer: first-non-ignored selection
	tier := []*common.NodeUrl{{Url: "wss://a.example/ws"}, {Url: "wss://b.example/ws"}}

	reg.RecordRateLimit("wss://a.example/ws", "wss://a.example/ws", time.Minute)
	ep, err := dwsm.selectFromTier(context.Background(), tier, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "wss://b.example/ws", ep.Url, "the held-off endpoint must not be picked")

	reg.RecordRateLimit("wss://b.example/ws", "wss://b.example/ws", time.Minute)
	ep, err = dwsm.selectFromTier(context.Background(), tier, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, ep, "with everything held off the tier still serves")
}

// The reconnect ladder honours a rate-limited dial's Retry-After, through the exact
// wrapping production builds (rpcclient handshake error wrapped by connect's %w).
func TestReconnectDelay_FloorsWithRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		http.Error(w, "too many requests", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, dialErr := rpcclient.DialWebsocket(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.Error(t, dialErr)
	wrapped := fmt.Errorf("failed to dial WebSocket %s: %w", srv.URL, dialErr)

	require.Equal(t, 30*time.Second, reconnectDelay(100*time.Millisecond, wrapped),
		"the vendor's ask floors the ladder")
	require.Equal(t, time.Minute, reconnectDelay(time.Minute, wrapped),
		"a ladder delay already past the ask is kept")
	require.Equal(t, 100*time.Millisecond, reconnectDelay(100*time.Millisecond, errors.New("connection refused")),
		"a plain dial failure keeps the ladder")
}

// The interface stub must stay in sync: recordingOptimizer embeds the shared fake.
var _ WebSocketEndpointOptimizer = (*recordingOptimizer)(nil)
