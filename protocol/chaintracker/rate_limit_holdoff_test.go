package chaintracker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/stretchr/testify/require"
)

const (
	holdoffTestProvider = "vendor"
	holdoffTestURL      = "ws://node.example/v1"
)

// rateLimitingFetcher answers every head fetch with the typed rate-limit error a JSON-RPC 429
// body produces (no Retry-After), counting the attempts the tracker makes.
type rateLimitingFetcher struct {
	advancingFetcher
	calls atomic.Int32
}

func (f *rateLimitingFetcher) FetchLatestBlockNum(context.Context) (int64, error) {
	f.calls.Add(1)
	return 0, common.RateLimited(errors.New("json-rpc error 429: Too Many Requests"), 0)
}

func newHoldoffTracker(t *testing.T, fetcher ChainFetcher, registry *holdoff.Registry) *ChainTracker {
	t.Helper()
	tracker := newCustomChainTracker(fetcher, ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      20 * time.Millisecond,
		ServerBlockMemory:     100,
		ChainId:               "ETH1",
		ParseDirectiveEnabled: true,
		FlatPollInterval:      10 * time.Millisecond,
		RateLimitHoldoff:      registry,
		RateLimitProvider:     holdoffTestProvider,
		RateLimitURL:          holdoffTestURL,
	})
	ct, ok := tracker.(*ChainTracker)
	require.True(t, ok)
	return ct
}

// A rate-limited poll with no Retry-After used to clear the floor as if the upstream had
// answered. It must record a strike into the registry and floor the next poll with the
// hold-off the registry applied; the first answered poll afterwards clears both (MAG-4165).
func TestNoteFetchOutcome_RecordsIntoRegistryAndFloors(t *testing.T) {
	registry := holdoff.NewRegistryWithClock(time.Now) // no jitter: the hold-off is exact
	cs := &ChainTracker{
		flatPollInterval: 6 * time.Second,
		rateLimitHoldoff: registry,
		holdoffProvider:  holdoffTestProvider,
		holdoffURL:       holdoffTestURL,
	}

	heldFor := cs.noteFetchOutcome(common.RateLimited(errors.New("json-rpc error 429"), 0))
	require.Equal(t, holdoff.InitialHoldoff, heldFor, "the applied hold-off is reported back")
	require.True(t, registry.HeldOff(holdoffTestURL, holdoffTestURL), "the 429 must land in the shared registry under the url alone")
	got := cs.computePollInterval(0, 0)
	require.Greater(t, got, holdoff.InitialHoldoff-time.Second, "the next poll waits the applied hold-off out")
	require.LessOrEqual(t, got, holdoff.InitialHoldoff)

	// An upstream that said how long to wait is honoured over the strike schedule.
	cs.noteFetchOutcome(common.RateLimited(errors.New("HTTP 429"), 5*time.Minute))
	got = cs.computePollInterval(0, 0)
	require.Greater(t, got, 4*time.Minute)

	// The first answer after the tracker's own 429 clears its floor and its registry entry.
	cs.noteFetchOutcome(nil)
	require.False(t, registry.HeldOff(holdoffTestURL, holdoffTestURL))
	require.Equal(t, 6*time.Second, cs.computePollInterval(0, 0))
}

// A hold-off the relay path recorded for the endpoint floors the poll too, and a poll that
// merely fit under the cap must not clear it: the tracker only clears what its own 429 set.
func TestNoteFetchOutcome_LeavesRelayPathHoldoffInPlace(t *testing.T) {
	registry := holdoff.NewRegistryWithClock(time.Now)
	cs := &ChainTracker{
		flatPollInterval: 6 * time.Second,
		rateLimitHoldoff: registry,
		holdoffProvider:  holdoffTestProvider,
		holdoffURL:       holdoffTestURL,
	}
	registry.RecordRateLimit(holdoffTestProvider, holdoffTestURL, 0) // as relayInnerDirect would

	got := cs.computePollInterval(0, 0)
	require.Greater(t, got, holdoff.InitialHoldoff-time.Second, "the poll honours a hold-off recorded elsewhere")

	cs.noteFetchOutcome(nil)
	require.True(t, registry.HeldOff(holdoffTestProvider, holdoffTestURL), "an answered poll must not clear the relay path's hold-off")
}

// The first fetch used to burn all five attempts one poll interval apart, each one re-arming a
// ban-type limit. A rate-limited attempt now ends the burst with the typed error and a recorded
// hold-off.
func TestFetchInitDataWithRetry_RateLimitEndsTheBurst(t *testing.T) {
	registry := holdoff.NewRegistryWithClock(time.Now)
	fetcher := &rateLimitingFetcher{}
	ct := newHoldoffTracker(t, fetcher, registry)

	err := ct.fetchInitDataWithRetry(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429, "the typed cause must reach the start loop")
	require.Equal(t, int32(1), fetcher.calls.Load(), "one refused attempt ends the burst")
	require.True(t, registry.HeldOff(holdoffTestURL, holdoffTestURL))

	// The applied hold-off rides the error as its Retry-After: the caller retrying the start
	// waits it out with the tracker's state naming the 429, instead of this tracker sleeping it
	// out inside StartAndServe where the state would read "starting" with no error.
	heldFor, ok := common.RetryAfterFrom(err)
	require.True(t, ok, "the start error must carry the applied hold-off")
	require.InDelta(t, time.Until(registry.ReadyAt(holdoffTestURL, holdoffTestURL)), heldFor, float64(time.Second))
}

// With the endpoint held off — by this tracker's previous start or by anyone else — the first
// fetch waits before asking again, instead of asking at once. The wait respects the context.
func TestFetchInitDataWithRetry_WaitsOutAnExistingHoldoff(t *testing.T) {
	registry := holdoff.NewRegistryWithClock(time.Now)
	registry.RecordRateLimit(holdoffTestProvider, holdoffTestURL, 0)
	fetcher := &rateLimitingFetcher{}
	ct := newHoldoffTracker(t, fetcher, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := ct.fetchInitDataWithRetry(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "the fetch waited rather than returning")
	require.Equal(t, int32(0), fetcher.calls.Load(), "no attempt is made while held off")
}

// A tracker built without a URL to key on has nothing to record under; the nil registry must
// leave init on its plain failure path.
func TestFetchInitDataWithRetry_NoKeyNoRegistry(t *testing.T) {
	fetcher := &rateLimitingFetcher{}
	tracker := newCustomChainTracker(fetcher, ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      20 * time.Millisecond,
		ServerBlockMemory:     100,
		ChainId:               "ETH1",
		ParseDirectiveEnabled: true,
		FlatPollInterval:      time.Millisecond,
	})
	ct, ok := tracker.(*ChainTracker)
	require.True(t, ok)
	require.Nil(t, ct.rateLimitHoldoff, "advancingFetcher reports no URL")

	err := ct.fetchInitDataWithRetry(context.Background())
	require.ErrorIs(t, err, common.StatusCodeError429)
	require.Equal(t, int32(1), fetcher.calls.Load(), "the burst still ends on a rate limit")
	_, ok = common.RetryAfterFrom(err)
	require.False(t, ok, "no registry applied a hold-off, so none is claimed")
}

// A poll that timed out or could not connect got no answer, so it must not clear the url's entry:
// WS and gRPC subscriptions record under the same url-only key, and a gateway enforcing a ban often
// drops connections instead of answering. Clearing here would reset the strike ladder and send the
// next poll — and the subscriptions — straight back into the limit (MAG-4165).
func TestNoteFetchOutcome_NoAnswerKeepsTheHoldoff(t *testing.T) {
	now := time.Now()
	registry := holdoff.NewRegistryWithClock(func() time.Time { return now })
	cs := &ChainTracker{
		flatPollInterval: 6 * time.Second,
		rateLimitHoldoff: registry,
		holdoffProvider:  holdoffTestProvider,
		holdoffURL:       holdoffTestURL,
	}
	cs.noteFetchOutcome(common.RateLimited(errors.New("json-rpc error 429"), 0))

	cs.noteFetchOutcome(context.DeadlineExceeded)
	cs.noteFetchOutcome(errors.New("dial tcp 10.0.0.1:443: connect: connection refused"))
	require.True(t, registry.HeldOff(holdoffTestURL, holdoffTestURL), "a fetch with no answer must not clear the hold-off")

	// The strike survived: the next refusal escalates instead of starting over.
	require.Equal(t, 2*holdoff.InitialHoldoff, cs.noteFetchOutcome(common.RateLimited(errors.New("json-rpc error 429"), 0)))

	cs.noteFetchOutcome(nil)
	require.False(t, registry.HeldOff(holdoffTestURL, holdoffTestURL), "an answer still clears")
}

// The tracker's own 429 must not hold the provider off for relay selection. Relay selection asks
// ProviderReadyAt, which holds a provider while every url the registry knows for it is held — and
// it only knows urls that answered 429. Recorded under the relay path's (provider, url) key, one
// rate-limited polled leg (say a backup provider's ws:// url, which no relay ever answers on to
// clear it) pulled the provider's healthy https leg out of selection for as long as the start
// retry kept re-arming it (MAG-4165).
func TestNoteFetchOutcome_OwnRateLimitDoesNotHoldTheProviderForRelays(t *testing.T) {
	registry := holdoff.NewRegistryWithClock(time.Now)
	cs := &ChainTracker{
		flatPollInterval: 6 * time.Second,
		rateLimitHoldoff: registry,
		holdoffProvider:  holdoffTestProvider,
		holdoffURL:       holdoffTestURL,
	}

	for range 3 {
		cs.noteFetchOutcome(common.RateLimited(errors.New("json-rpc error 429"), 0))
	}
	_, providerHeld := registry.ProviderReadyAt(holdoffTestProvider)
	require.False(t, providerHeld, "a polled leg's 429 must not take the provider out of relay selection")
	require.False(t, registry.HeldOff(holdoffTestProvider, holdoffTestURL), "nothing lands under the relay path's key")
	require.True(t, registry.HeldOff(holdoffTestURL, holdoffTestURL), "the url itself is held, as the ws and gRPC paths key it")
	require.Greater(t, cs.computePollInterval(0, 0), holdoff.InitialHoldoff, "and the poll still waits its own hold-off out")
}
