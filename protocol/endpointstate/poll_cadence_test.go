package endpointstate

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestResolvePollDivisor pins the validation contract: absent means default, in-range passes
// through untouched, and anything outside the range REVERTS to the default rather than clamping
// to the nearest bound. The distinction matters operationally — a clamp would let someone who
// asked for 20 polls per block read the resulting metric as confirmation they got it.
func TestResolvePollDivisor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provided int
		want     int
	}{
		{name: "absent (0) takes the built-in default", provided: 0, want: DefaultPollDivisor},
		{name: "1 is the slowest supported cadence: one poll per block time", provided: 1, want: 1},
		{name: "the default may be passed explicitly", provided: DefaultPollDivisor, want: DefaultPollDivisor},
		{name: "the upper bound is inclusive", provided: MaxPollDivisor, want: MaxPollDivisor},
		{name: "above the range reverts, it does not clamp to Max", provided: MaxPollDivisor + 1, want: DefaultPollDivisor},
		{name: "far above the range reverts", provided: 100, want: DefaultPollDivisor},
		// A negative divisor would produce a negative interval, which time.NewTimer fires
		// immediately on — a hot loop against the upstream. It must never reach the tracker.
		{name: "negative reverts", provided: -1, want: DefaultPollDivisor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, resolvePollDivisor(tc.provided, "ETH1", spectypes.APIInterfaceJsonRPC))
		})
	}
}

// TestResolveFlatPollInterval covers the derivation itself, including on a sub-second chain where
// the divisor scales the interval down rather than up — the property that lets ONE process-wide
// ratio serve chains whose block times differ by 30x.
func TestResolveFlatPollInterval(t *testing.T) {
	for _, tc := range []struct {
		name         string
		avgBlockTime time.Duration
		divisor      int
		want         time.Duration
	}{
		{name: "ethereum at the default divisor", avgBlockTime: 12 * time.Second, divisor: DefaultPollDivisor, want: 6 * time.Second},
		{name: "ethereum at divisor 1 halves the request rate", avgBlockTime: 12 * time.Second, divisor: 1, want: 12 * time.Second},
		{name: "solana at divisor 1 stays sub-second", avgBlockTime: 400 * time.Millisecond, divisor: 1, want: 400 * time.Millisecond},
		{name: "solana at the fastest allowed divisor", avgBlockTime: 400 * time.Millisecond, divisor: MaxPollDivisor, want: 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, resolveFlatPollInterval(tc.avgBlockTime, tc.divisor))
		})
	}
}

// TestNewEndpointMonitor_ResolvesFlatPollInterval checks the resolution happens at construction,
// against the FLOORED block time — a chain whose spec omits average_block_time must divide
// DefaultAverageBlockTime, not zero, or the divisor would produce a 0 interval and silently switch
// the tracker back to the legacy adaptive scheduler.
func TestNewEndpointMonitor_ResolvesFlatPollInterval(t *testing.T) {
	for _, tc := range []struct {
		name         string
		avgBlockTime time.Duration
		divisor      int
		want         time.Duration
	}{
		{name: "default divisor", avgBlockTime: 12 * time.Second, divisor: 0, want: 6 * time.Second},
		{name: "divisor 1", avgBlockTime: 12 * time.Second, divisor: 1, want: 12 * time.Second},
		{name: "rejected divisor falls back to the default cadence", avgBlockTime: 12 * time.Second, divisor: 99, want: 6 * time.Second},
		{name: "spec omits average_block_time: divides the floored default", avgBlockTime: 0, divisor: 1, want: DefaultAverageBlockTime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
				ChainID:             "ETH1",
				ApiInterface:        spectypes.APIInterfaceJsonRPC,
				AverageBlockTime:    tc.avgBlockTime,
				PollIntervalDivisor: tc.divisor,
			})
			require.NotNil(t, m)
			defer m.Stop()

			require.Equal(t, tc.want, m.flatPollInterval)
			require.Positive(t, m.flatPollInterval, "a zero interval would re-enable the adaptive scheduler")
		})
	}
}

// TestEndpointMonitor_PollDivisor_ReachesLiveTracker is the production-path proof. The tests above
// exercise the resolution in isolation, and the chaintracker cadence tests set FlatPollInterval
// directly — so without this one the wiring between them could be missing entirely and every other
// test would still pass. It reads the cadence back through the same accessor /debug/endpoint-state
// reports as PollIntervalMs.
//
// Both cases use a block time long enough that the poll timer cannot fire inside the test, so what
// is asserted is the CONFIGURED cadence and never a backed-off or re-scheduled one.
func TestEndpointMonitor_PollDivisor_ReachesLiveTracker(t *testing.T) {
	for _, tc := range []struct {
		name    string
		divisor int
		want    time.Duration
	}{
		{name: "default divisor gives half the block time", divisor: 0, want: 30 * time.Second},
		{name: "divisor 1 gives a full block time", divisor: 1, want: time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			ensureRandSeeded()

			const url = "http://eth-poll-divisor:8545"
			m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
				ChainParser:         newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
				ChainID:             "ETH1",
				ApiInterface:        spectypes.APIInterfaceJsonRPC,
				AverageBlockTime:    time.Minute,
				BlocksToSave:        1,
				PollIntervalDivisor: tc.divisor,
			})
			require.NotNil(t, m)
			defer m.Stop()

			conn := &pollNowConn{url: url}
			conn.block.Store(1000)
			_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				state, _, exists := m.GetTrackerState(url)
				return exists && state == EndpointChainTrackerPolling
			}, 10*time.Second, 20*time.Millisecond, "the tracker must reach its poll loop before its cadence is read")

			require.Equal(t, tc.want, m.BackoffSnapshot()[url])
		})
	}
}
