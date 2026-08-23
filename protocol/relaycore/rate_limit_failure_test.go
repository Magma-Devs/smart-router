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
