package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

const (
	stellarPending  = `{"jsonrpc":"2.0","id":1,"result":{"status":"PENDING","hash":"abc"}}`
	stellarDup      = `{"jsonrpc":"2.0","id":1,"result":{"status":"DUPLICATE","hash":"abc"}}`
	stellarTryLater = `{"jsonrpc":"2.0","id":1,"result":{"status":"TRY_AGAIN_LATER","hash":"abc"}}`
	stellarError    = `{"jsonrpc":"2.0","id":1,"result":{"status":"ERROR","errorResultXdr":"AAAAAAAAACr////7","hash":"abc"}}`
	stellarLoaded   = "loaded@test" // answers TRY_AGAIN_LATER or ERROR first (busy, or already judged the tx)
	stellarQueuer   = "queuer@test" // the node that actually queues the transaction
)

// newStatefulStellarProcessor builds a relay processor for a Stellar sendTransaction broadcast to
// two providers, loaded@test and queuer@test unless named, parsed against the Stellar mock spec so
// the reply goes through CheckStellarResponseError.
func newStatefulStellarProcessor(t *testing.T, providers ...string) *RelayProcessor {
	t.Helper()
	ctx := context.Background()
	chainParser, err := chainlib.NewChainParser("jsonrpc")
	require.NoError(t, err)
	chainParser.SetSpec(chainlib.CreateMockStellarSpec("XLMT"))
	chainMsg, err := chainParser.ParseMsg("", []byte(`{"method":"sendTransaction","params":{"transaction":"AAAAAg=="}}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "127.0.0.1")
	usedProviders := routersession.NewUsedProviders(nil)
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateful))

	require.NoError(t, usedProviders.TryLockSelection(ctx))
	if len(providers) == 0 {
		providers = []string{stellarLoaded, stellarQueuer}
	}
	sessions := routersession.ConsumerSessionsMap{}
	for _, provider := range providers {
		sessions[provider] = &routersession.SessionInfo{}
	}
	usedProviders.AddUsed(sessions, nil)
	return relayProcessor
}

// TestStatefulStellarBroadcastWaitsThroughTryAgainLater reproduces MAG-5448: a sendTransaction
// reaches two nodes, a loaded one answers TRY_AGAIN_LATER first, and the one that queued it
// answers PENDING after. The caller must get the PENDING.
func TestStatefulStellarBroadcastWaitsThroughTryAgainLater(t *testing.T) {
	relayProcessor := newStatefulStellarProcessor(t)

	deliverJsonrpcReply(relayProcessor, stellarLoaded, stellarTryLater)
	requireStillWaiting(t, relayProcessor, 1)

	deliverJsonrpcReply(relayProcessor, stellarQueuer, stellarPending)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx))
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, stellarPending, string(returnedResult.Reply.Data))
	require.Equal(t, stellarQueuer, returnedResult.ProviderInfo.ProviderAddress)
}

// TestStatefulStellarBroadcastWaitsThroughError is the same fault for ERROR: a node that already
// judged the transaction (or one lagging behind the account's sequence) must not outrank a
// sibling's PENDING.
func TestStatefulStellarBroadcastWaitsThroughError(t *testing.T) {
	relayProcessor := newStatefulStellarProcessor(t)

	deliverJsonrpcReply(relayProcessor, stellarLoaded, stellarError)
	requireStillWaiting(t, relayProcessor, 1)

	deliverJsonrpcReply(relayProcessor, stellarQueuer, stellarPending)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx))

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, stellarPending, string(returnedResult.Reply.Data))
	require.Equal(t, stellarQueuer, returnedResult.ProviderInfo.ProviderAddress)
}

// TestStatefulStellarBroadcastAllRefuseReturnsTheFirstRefusal: every node answers TRY_AGAIN_LATER or
// ERROR. The caller gets the first of them as-is, HTTP 200, unchanged.
//
// Both replies are delivered before the one wait, as the state machine's single wait would read
// them: a wait counts only the replies it reads itself toward "every provider answered".
func TestStatefulStellarBroadcastAllRefuseReturnsTheFirstRefusal(t *testing.T) {
	for name, refusal := range map[string]string{"TRY_AGAIN_LATER": stellarTryLater, "ERROR": stellarError} {
		t.Run(name, func(t *testing.T) {
			relayProcessor := newStatefulStellarProcessor(t)

			deliverJsonrpcReply(relayProcessor, stellarLoaded, refusal)
			deliverJsonrpcReply(relayProcessor, stellarQueuer, refusal)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, relayProcessor.WaitForResults(ctx), "both answered, nothing left in flight")
			hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
			require.False(t, hasResults, "two refusals are not a success")
			successes, nodeErrors, _, _ := relayProcessor.GetResults()
			require.Equal(t, 0, successes)
			require.Equal(t, 2, nodeErrors)

			returnedResult, err := relayProcessor.ProcessingResult()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, returnedResult.StatusCode)
			require.Equal(t, refusal, string(returnedResult.Reply.Data))
			require.Equal(t, stellarLoaded, returnedResult.ProviderInfo.ProviderAddress)
		})
	}
}

// TestStatefulStellarBroadcastEndsOnADuplicate: DUPLICATE says the node already holds the
// transaction, so it is in the network. It is a success, as on every other path, and ends the
// broadcast without waiting for the other node.
func TestStatefulStellarBroadcastEndsOnADuplicate(t *testing.T) {
	relayProcessor := newStatefulStellarProcessor(t)

	deliverJsonrpcReply(relayProcessor, stellarLoaded, stellarDup)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx))
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults, "a DUPLICATE is the answer; the broadcast does not wait for the other node")

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, returnedResult.StatusCode)
	require.Equal(t, stellarDup, string(returnedResult.Reply.Data))
	require.False(t, returnedResult.IsNodeError)
}
