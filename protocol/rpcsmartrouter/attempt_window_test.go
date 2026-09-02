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
// Elapsed time is the input rather than why the request ended, and both consequences are wanted:
// an endpoint that hung is blamed even when the request succeeded through someone else, and an
// endpoint dispatched just before the budget expired is not blamed, because it never had a fair
// chance.
func TestEndpointGotItsWindow(t *testing.T) {
	const window = 10 * time.Second

	for _, tc := range []struct {
		name    string
		elapsed time.Duration
		window  time.Duration
		want    bool
		why     string
	}{
		{
			name: "hung for the whole window", elapsed: window, window: window, want: true,
			why: "exactly the window is a full window — the endpoint had every moment it was promised",
		},
		{
			name: "hung well past the window", elapsed: 3 * window, window: window, want: true,
			why: "an endpoint cancelled long after its window answered nothing and must be blamed",
		},
		{
			name: "race loser stopped early", elapsed: 500 * time.Millisecond, window: window, want: false,
			why: "MAG-2648: a relay we stopped before its window was never actually tested",
		},
		{
			name: "cancelled one tick short", elapsed: window - time.Nanosecond, window: window, want: false,
			why: "below the window is below the window; blame needs the full promise to have been kept",
		},
		{
			name: "dispatched just before the budget expired", elapsed: 2 * time.Second, window: window, want: false,
			why: "the end-of-budget case falls out of elapsed time without a special case",
		},
		{
			name: "no window configured", elapsed: time.Hour, window: 0, want: false,
			why: "nothing was promised, so nothing can be judged against it — withhold blame rather than invent it",
		},
		{
			name: "negative window", elapsed: time.Hour, window: -time.Second, want: false,
			why: "a nonsensical window must not become a blanket blame rule",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, endpointGotItsWindow(tc.elapsed, tc.window), tc.why)
		})
	}
}
