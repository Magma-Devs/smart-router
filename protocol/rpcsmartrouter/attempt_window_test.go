package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The per-attempt timeout used to do two jobs with one number: it decided when to dispatch the
// next endpoint AND it killed the attempt in flight. Because both jobs read the same value, the
// moment the next endpoint was dispatched was the moment the current one died, so a method
// genuinely slower than that number could never succeed on ANY endpoint — three attempts killed at
// the window, the error tolerance exhausted by timeouts our own timer manufactured, and the
// request abandoned with most of its budget unspent.
//
// The window now only triggers. The attempt lives on the request's budget.

// An upstream slower than the window but well inside the budget must now be allowed to answer.
// This is the headline regression: before the split, passing the window here killed the relay.
func TestAttemptOutlivesItsWindow(t *testing.T) {
	const upstreamDelay = 300 * time.Millisecond
	const window = 100 * time.Millisecond
	const attemptBudget = 3 * time.Second

	var served atomic.Int32
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		time.Sleep(upstreamDelay)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`))
	}))
	defer mockServer.Close()

	ctx := context.Background()
	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: mockServer.URL}, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "slower-than-the-window",
		chainFamily:      common.ChainFamilyEVM,
	}
	chainMessage := createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

	require.Greater(t, upstreamDelay, window,
		"the upstream must be slower than the window or this test proves nothing")

	result, err := sender.SendDirectRelay(ctx, chainMessage, attemptBudget)

	require.NoError(t, err, "an attempt must live on the request budget, not on the dispatch window")
	require.NotNil(t, result)
	require.EqualValues(t, 1, served.Load())

	var reply map[string]any
	require.NoError(t, json.Unmarshal(result.Reply.Data, &reply))
	assert.Equal(t, "0x1234", reply["result"])
}

// The budget is still a real bound. Removing the window as a killer must not remove every limit,
// or a hung upstream would hold its goroutine and session indefinitely.
func TestAttemptStillBoundedByItsBudget(t *testing.T) {
	// Held open by the test, not by the request context: a client-side context cancellation does
	// not necessarily reach the server handler, so waiting on r.Context() here would leave the
	// handler running and deadlock httptest's Close on its outstanding-request WaitGroup.
	release := make(chan struct{})
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer mockServer.Close()
	defer close(release)

	ctx := context.Background()
	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: mockServer.URL}, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "hangs-forever",
		chainFamily:      common.ChainFamilyEVM,
	}
	chainMessage := createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

	started := time.Now()
	result, err := sender.SendDirectRelay(ctx, chainMessage, 200*time.Millisecond)
	elapsed := time.Since(started)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "deadline exceeded")
	assert.Less(t, elapsed, 3*time.Second, "the budget must still stop a hung upstream")
}

// The fairness test behind the availability verdict for a relay that produced nothing.
//
// The obvious test — "was it given its full window" — was tried first and is wrong. A hedge is
// dispatched AT the window, so a race loser has always been silent for longer than its window by the
// time it is cancelled; the test is true for every loser, always. Measured against the real router: a
// healthy endpoint answering in 55s, cancelled at 35s when a faster one won, on a request that
// SUCCEEDED, was blamed at waited=35s window=28s. That is the structural penalty MAG-2648 removed —
// every node but the fastest, on a broadcast — and the pre-change code never did it, because an
// attempt could not outlive its window in the first place.
//
// Budget exhaustion is the honest question and needs no threshold: did this endpoint run out of road,
// or did we cut it short while it was still working?
func TestRequestRanOutOfRoad(t *testing.T) {
	for _, tc := range []struct {
		name       string
		stopReason string
		want       bool
		why        string
	}{
		{
			name: "budget expired", stopReason: relaycore.StopReasonProcessingTimeout, want: true,
			why: "the request used everything it had and this endpoint was still silent — it hung",
		},
		{
			name: "another endpoint answered", stopReason: "Success", want: false,
			why: "MAG-2648: a loser cancelled while still inside its budget was cut short, not shown to be unavailable",
		},
		{
			name: "policy stopped the request", stopReason: "Stateful", want: false,
			why: "the request ended on a decision, not on time running out; nothing was proved about the endpoint",
		},
		{
			name: "no providers to try", stopReason: "AllProvidersExhausted", want: false,
			why: "nothing was in flight to blame",
		},
		{
			name: "first message never sent", stopReason: "FirstMessageFailed", want: false,
			why: "the endpoint was never actually exercised",
		},
		{
			name: "no stop reason recorded", stopReason: "", want: false,
			why: "absence of a reason is not evidence of a hang — withhold blame rather than invent it",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, requestRanOutOfRoad(tc.stopReason), tc.why)
		})
	}
}
