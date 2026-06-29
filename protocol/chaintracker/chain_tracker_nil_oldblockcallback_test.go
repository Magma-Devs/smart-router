package chaintracker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// timeoutChainFetcher is a minimal ChainFetcher whose latest-block fetch always
// fails with the exact error shape a wss upstream produces on a deadline: a
// wrappedLavaError (from utils.LavaFormatWarning) wrapping context.DeadlineExceeded.
//
// This mirrors production: SVMChainTracker.FetchLatestBlockNum wraps the raw
// CallContext error returned by WebSocketDirectRPCConnection.SendRequest via
// LavaFormatWarning, yielding a *wrappedLavaError whose single Unwrap() exposes a
// net.Error. That is precisely what drives the net.Error branch of
// fetchAllPreviousBlocksIfNecessary. (HTTP upstreams are shielded because
// HTTPDirectRPCConnection.SendRequest wraps with fmt.Errorf, so one Unwrap() yields
// a *fmt.wrapError, not a net.Error.)
type timeoutChainFetcher struct{}

func (timeoutChainFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	return 0, utils.LavaFormatWarning("[test] simulated wss latest-block timeout", context.DeadlineExceeded)
}

func (timeoutChainFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	return "", nil
}

func (timeoutChainFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{}
}

func (timeoutChainFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	return nil, nil
}

// TestFetchAllPreviousBlocksIfNecessary_UpstreamTimeoutIsHandled reproduces MAG-2219.
//
// When a wss upstream's latest-block fetch times out, the error reaching
// fetchAllPreviousBlocksIfNecessary unwraps to a net.Error, so the net.Error branch
// calls notUpdated() WITHOUT the `oldBlockCallback != nil` guard that the sibling
// call has. OldBlockCallback is never assigned anywhere in the codebase, so it is
// always nil and notUpdated() dereferences a nil func -> SIGSEGV, crash-looping the
// router.
//
// Desired behaviour: an upstream latest-block timeout is handled and surfaced as an
// error; the tracker keeps running. This test fails today (nil-pointer panic) and
// passes once the call site is guarded. OldBlockCallback is intentionally left unset
// to reflect every real deployment.
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

	gotErr := cs.fetchAllPreviousBlocksIfNecessary(context.Background())
	require.Error(t, gotErr, "an upstream latest-block timeout should surface as an error, not crash the router")
	require.True(t, errors.Is(gotErr, context.DeadlineExceeded), "the returned error should preserve the upstream deadline cause")
}
