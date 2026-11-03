package chaintracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestComputePollInterval_FlatCadence guards the MAG-2159 per-endpoint cadence: the poll runs at
// EXACTLY flatPollInterval, and failure backoff is the only thing that slows it. The base-time
// argument this used to be handed, and the adaptive tiers that read it, are gone (MAG-4398).
func TestComputePollInterval_FlatCadence(t *testing.T) {
	const avg = 400 * time.Millisecond
	flat := avg / 2 // 200ms
	ct := &ChainTracker{
		averageBlockTime: avg,
		flatPollInterval: flat,
	}

	require.Equal(t, flat, ct.computePollInterval(0),
		"a healthy tracker polls at exactly its flat interval")
	// Failure backoff doubles per failure (1<<fails) and can only slow polling.
	require.Equal(t, flat*8, ct.computePollInterval(3),
		"failure backoff slows the flat interval (flat << 3)")
}

// TestNextFetchFails locks the skip-vs-success distinction that keeps a broken poll path in
// backoff (MAG-2159 follow-up): a traffic-gate skip PRESERVES the consecutive-failure counter,
// only a genuine successful poll resets it, and a real failure increments it. The regression this
// guards: a skip used to look like a success (err==nil), resetting the counter every gated cycle
// so a doomed poll never backed off and its recovery evidence never accrued.
func TestNextFetchFails(t *testing.T) {
	cases := []struct {
		name    string
		current uint64
		skipped bool
		failed  bool
		want    uint64
	}{
		{"failure increments", 2, false, true, 3},
		{"failure from zero", 0, false, true, 1},
		{"successful poll resets", 5, false, false, 0},
		{"skip preserves a clean counter", 0, true, false, 0},
		{"skip preserves an accrued backoff (the fix)", 4, true, false, 4},
		{"failure precedence over skip", 3, true, true, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, nextFetchFails(tc.current, tc.skipped, tc.failed))
		})
	}
}

// TestNewChainTracker_FloorsAnUnsetPollInterval pins what replaced the legacy adaptive cadence.
//
// A config that left FlatPollInterval at 0 used to select a second scheduler — the adaptive tiers,
// whose only test PR #522 removed and whose last caller was the `smartrouter test rpcsmartrouter`
// command. With the tiers gone a 0 would schedule nothing at all, so the constructor floors it to
// one poll per average block time: the conservative end of the range endpointstate resolves, and
// the value resolveFlatPollInterval already fell back to.
func TestNewChainTracker_FloorsAnUnsetPollInterval(t *testing.T) {
	const avg = 700 * time.Millisecond
	for _, tc := range []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{"unset", 0, avg},
		{"negative", -time.Second, avg},
		{"set", avg / 4, avg / 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := newCustomChainTracker(&advancingFetcher{}, ChainTrackerConfig{
				BlocksToSave:          1,
				AverageBlockTime:      avg,
				ServerBlockMemory:     100,
				ChainId:               "ETH1",
				ParseDirectiveEnabled: true,
				FlatPollInterval:      tc.configured,
			})
			ct, ok := tracker.(*ChainTracker)
			require.True(t, ok)
			require.Equal(t, tc.want, ct.flatPollInterval)
			// Published for /debug/endpoint-state before the first poll runs (MAG-2395).
			require.Equal(t, tc.want, ct.CurrentPollInterval())
			require.Equal(t, HealthyPollInterval(tc.want), ct.computePollInterval(0))
		})
	}
}
