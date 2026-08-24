package relaycore

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

func newTwoProviderProcessor(t *testing.T) (*RelayProcessor, *lavasession.UsedProviders, func()) {
	t.Helper()
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "123.11")
	usedProviders := lavasession.NewUsedProviders(nil)
	rp := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, newMockRelayStateMachine(protocolMessage, usedProviders))

	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	require.Nil(t, usedProviders.TryLockSelection(lockCtx))
	usedProviders.AddUsed(lavasession.ConsumerSessionsMap{"lava@a": &lavasession.SessionInfo{}, "lava@b": &lavasession.SessionInfo{}}, nil)
	closer := func() {
		if closeServer != nil {
			closeServer()
		}
	}
	return rp, usedProviders, closer
}

// startBatch opens a new selection batch of the given providers, so a follow-up
// WaitForResults expects exactly their responses — the shape of a retry round.
func startBatch(t *testing.T, usedProviders *lavasession.UsedProviders, providers ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.Nil(t, usedProviders.TryLockSelection(ctx))
	sessions := lavasession.ConsumerSessionsMap{}
	for _, provider := range providers {
		sessions[provider] = &lavasession.SessionInfo{}
	}
	usedProviders.AddUsed(sessions, nil)
}

// Every attempt refused for rate: the chain is temporarily unservable, not broken — the
// client gets 503, and the policy summary says the failures were only rate limits.
func TestProcessingResult_AllRateLimitedIs503(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()

	go SendProtocolError(rp, "lava@a", time.Millisecond, common.RateLimited(errors.New("HTTP 429"), 30*time.Second))
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, common.RateLimited(errors.New("HTTP 429"), 0))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = rp.WaitForResults(ctx)

	summary := rp.GetResultsSummary()
	require.True(t, summary.OnlyRateLimited)
	require.False(t, summary.HasPermanentProtocolError, "a typed 429 must stay retryable")

	result, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, result.StatusCode)
}

// Mixed failures keep today's shape — the best error's own result, status untouched (the
// listener applies its default) — and the summary does not claim only-rate-limited.
func TestProcessingResult_MixedFailuresUnchanged(t *testing.T) {
	rp, _, closer := newTwoProviderProcessor(t)
	defer closer()

	go SendProtocolError(rp, "lava@a", time.Millisecond, common.RateLimited(errors.New("HTTP 429"), 0))
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, errors.New("connection refused"))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = rp.WaitForResults(ctx)

	require.False(t, rp.GetResultsSummary().OnlyRateLimited)
	result, err := rp.ProcessingResult()
	require.Error(t, err)
	require.NotEqual(t, http.StatusServiceUnavailable, result.StatusCode, "only an all-rate-limited failure is 503")
	require.Equal(t, 0, result.StatusCode, "the protocol error's own result is returned as before")
}

// The 503 is stamped on a copy, never on the stored result. sendRelayWithRetries reuses one
// processor and calls ProcessingResult() per iteration, so an all-429 iteration must not
// leave a 503 behind for a later, mixed one to inherit.
func TestProcessingResult_RateLimited503DoesNotStick(t *testing.T) {
	rp, usedProviders, closer := newTwoProviderProcessor(t)
	defer closer()

	// Iteration 1: every failure is a rate limit.
	go SendProtocolError(rp, "lava@a", time.Millisecond, common.RateLimited(errors.New("HTTP 429"), 0))
	go SendProtocolError(rp, "lava@b", 2*time.Millisecond, common.RateLimited(errors.New("HTTP 429"), 0))
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelFirst()
	_ = rp.WaitForResults(firstCtx)

	result, err := rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, result.StatusCode)

	// Iteration 2: the retry lands on an endpoint that is down rather than capped, so the
	// chain is no longer merely unservable and the 429 result must report its own status.
	startBatch(t, usedProviders, "lava@c")
	go SendProtocolError(rp, "lava@c", time.Millisecond, errors.New("connection refused"))
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelSecond()
	_ = rp.WaitForResults(secondCtx)

	require.False(t, rp.GetResultsSummary().OnlyRateLimited)
	result, err = rp.ProcessingResult()
	require.Error(t, err)
	require.Equal(t, 0, result.StatusCode, "the earlier 503 must not have been stamped onto the stored result")
}
