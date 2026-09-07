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

// The per-attempt timeout used to serve as both the hedge interval and the attempt deadline, so an
// attempt was killed the instant the next endpoint was dispatched. The window now only triggers.

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

// The fairness test for a relay that produced nothing. "Was it given its full window" is the wrong
// question — a hedge is dispatched AT the window, so every race loser passes it, and blaming those
// is the MAG-2648 penalty. Budget exhaustion asks the answerable question instead.
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
