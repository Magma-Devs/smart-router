package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// A client that closes its socket without unsubscribing must not lower the score of the
// node that was serving it. Browsers close tabs, phones lose signal, processes get killed:
// if each of those counted against the node, the router would slowly route away from its
// healthiest nodes for no reason.
//
// Nothing on the disconnect or teardown path scores today, but only by construction. The
// other disconnect tests use fakeSubscriptionOptimizer, whose AppendRelayFailure discards
// the call, so they could not see one being added. This test uses a double that records
// both score inputs: a failure, and a success whose latency could be made to count against
// the node.
//
// A closed socket reaches the manager two ways, and each case drives one:
//   - The ingress (chainlib's consumer WebSocket manager) calls UnsubscribeAll when the
//     connection ends, then cancels the connection context. This is the usual production
//     path; handleClientDisconnect then finds the client already released and returns.
//   - Only the context is cancelled. handleClientDisconnect releases the client itself.
func TestClientDisconnect_DoesNotLowerServingProviderScore(t *testing.T) {
	const (
		dappID = "dapp-score"
		ip     = "10.0.0.1"
		connID = "ws-score"
	)

	cases := []struct {
		name  string
		close func(t *testing.T, manager *DirectWSSubscriptionManager, cancel context.CancelFunc)
	}{
		{
			name: "ingress releases the client, then cancels the context",
			close: func(t *testing.T, manager *DirectWSSubscriptionManager, cancel context.CancelFunc) {
				require.NoError(t, manager.UnsubscribeAll(context.Background(), dappID, ip, connID, nil))
				cancel()
			},
		},
		{
			name: "only the client context is cancelled",
			close: func(_ *testing.T, _ *DirectWSSubscriptionManager, cancel context.CancelFunc) {
				cancel()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockSrv := newMockSubscriptionServer()
			mockSrv.messageInterval = 20 * time.Millisecond
			defer mockSrv.Close()

			opt := &scoreRecordingOptimizer{}

			nodeUrl := &common.NodeUrl{Url: mockSrv.URL()}
			manager := NewDirectWSSubscriptionManager(
				getTestMetricsManager(),
				"jsonrpc", "ETH", "jsonrpc",
				[]*common.NodeUrl{nodeUrl},
				nil, opt, nil,
			)

			// The client's connection context.
			clientCtx, cancel := context.WithCancel(context.Background())
			defer cancel()

			subscribeBody, err := json.Marshal(map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscribe",
				"params":  []interface{}{"newHeads"},
				"id":      1,
			})
			require.NoError(t, err)

			subscribeMsg := &mockWSProtocolMessageWithRawData{
				mockWSProtocolMessage: mockWSProtocolMessage{
					method: "eth_subscribe",
					params: []interface{}{"newHeads"},
				},
				rawData: subscribeBody,
			}

			reply, repliesChan, err := manager.StartSubscription(clientCtx, subscribeMsg, dappID, ip, connID, nil)
			require.NoError(t, err, "subscribe must succeed")
			require.NotNil(t, reply)
			require.NotNil(t, repliesChan)

			successesBefore := opt.snapshotSuccesses()
			require.Equal(t, []string{nodeUrl.Url}, successesBefore,
				"precondition: the subscribe must have reported to this optimizer for the serving node, "+
					"or an unchanged score proves nothing")
			require.Contains(t, mockSrv.ObservedMethods(), "eth_subscribe",
				"precondition: the node must be serving the subscription")
			require.Empty(t, opt.snapshotFailures(), "precondition: a successful subscribe scores no failure")

			// The client goes away WITHOUT sending eth_unsubscribe.
			tc.close(t, manager, cancel)

			// Teardown has reached the node once it sees eth_unsubscribe.
			require.Eventually(t, func() bool {
				return slices.Contains(mockSrv.ObservedMethods(), "eth_unsubscribe")
			}, 15*time.Second, 50*time.Millisecond, "the disconnect must tear the upstream subscription down")

			// The unsubscribe is sent from a goroutine, so give anything after it time to land.
			scored := func() bool {
				return len(opt.snapshotFailures()) > 0 || len(opt.snapshotSuccesses()) != len(successesBefore)
			}
			require.Never(t, scored, 500*time.Millisecond, 25*time.Millisecond,
				"a client hanging up must not change the score of the node that was serving it")
			require.Empty(t, opt.snapshotFailures(), "AppendRelayFailure calls after the disconnect")
			require.Equal(t, successesBefore, opt.snapshotSuccesses(), "AppendRelayData calls after the disconnect")
		})
	}
}

// scoreRecordingOptimizer also records AppendRelayData, so a test can show the optimizer is
// wired to the manager before it relies on the absence of AppendRelayFailure calls.
type scoreRecordingOptimizer struct {
	recordingOptimizer
	successes []string
}

func (r *scoreRecordingOptimizer) AppendRelayData(provider string, _ time.Duration, _, _ uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.successes = append(r.successes, provider)
}

func (r *scoreRecordingOptimizer) snapshotSuccesses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.successes...)
}
