package chaintracker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// timeoutChainFetcher is a minimal ChainFetcher whose latest-block fetch always
// fails with the exact error shape a wss upstream produces on a deadline: a
// wrappedRouterError (from utils.FormatWarning) wrapping context.DeadlineExceeded.
//
// This mirrors production: SVMChainTracker.FetchLatestBlockNum wraps the raw
// CallContext error returned by WebSocketDirectRPCConnection.SendRequest via
// FormatWarning, yielding a *wrappedRouterError whose single Unwrap() exposes a
// net.Error — the shape that crashed the tracker in MAG-2219. (HTTP upstreams were
// shielded because HTTPDirectRPCConnection.SendRequest wraps with fmt.Errorf, so one
// Unwrap() yields a *fmt.wrapError, not a net.Error.)
type timeoutChainFetcher struct{}

func (timeoutChainFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	return 0, utils.FormatWarning("[test] simulated wss latest-block timeout", context.DeadlineExceeded)
}

func (timeoutChainFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	return "", nil
}

func (timeoutChainFetcher) FetchEndpoint() routersession.RPCProviderEndpoint {
	return routersession.RPCProviderEndpoint{}
}

func (timeoutChainFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	return nil, nil
}

// TestFetchAllPreviousBlocksIfNecessary_UpstreamTimeoutIsHandled guards MAG-2219.
//
// When a wss upstream's latest-block fetch timed out, the error unwrapped to a net.Error,
// and fetchAllPreviousBlocksIfNecessary answered that shape by calling an optional
// "old block" callback with no nil guard. Nothing ever set the callback, so the poll
// goroutine dereferenced a nil func (SIGSEGV) and crash-looped the router. The branch and
// the callback have since been removed as dead (MAG-4876); this keeps the contract that
// mattered: an upstream latest-block timeout is a real, failed poll that surfaces its
// deadline cause, and the tracker does not panic.
func TestFetchAllPreviousBlocksIfNecessary_UpstreamTimeoutIsHandled(t *testing.T) {
	config := ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      time.Millisecond,
		ServerBlockMemory:     10,
		ParseDirectiveEnabled: true,
	}

	tracker, err := NewChainTracker(context.Background(), timeoutChainFetcher{}, config)
	require.NoError(t, err)

	cs, ok := tracker.(*ChainTracker)
	require.True(t, ok, "expected NewChainTracker to return a *ChainTracker")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("MAG-2219: fetchAllPreviousBlocksIfNecessary panicked on a wss upstream timeout instead of handling it: %v", r)
		}
	}()

	gotSkipped, gotErr := cs.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
	require.False(t, gotSkipped, "an upstream timeout is a real poll attempt, not a gate skip")
	require.Error(t, gotErr, "an upstream latest-block timeout should surface as an error, not crash the router")
	require.True(t, errors.Is(gotErr, context.DeadlineExceeded), "the returned error should preserve the upstream deadline cause")
}
