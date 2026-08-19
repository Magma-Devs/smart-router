package endpointstate

import (
	"time"

	"github.com/magma-Devs/smart-router/utils"
)

// PollDivisorFlagName is the operator knob for the per-endpoint dedicated-poll cadence.
//
// Each per-endpoint ChainTracker polls its upstream for the latest block on a FIXED
// interval of avgBlockTime/divisor (chaintracker.ChainTrackerConfig.FlatPollInterval).
// The built-in divisor is DefaultPollDivisor — two polls per block time. Lowering it to 1
// halves the tracker's request volume against every upstream, which is the point: on a
// metered customer node the dedicated poll is pure overhead the moment served traffic is
// already keeping the tip current.
//
// It is a RATIO, not an absolute interval, on purpose. One process can serve several
// chains (config/smartrouter_examples/smartrouter_multichain.yml) and each resolves its own
// average_block_time from its spec, so a single ratio stays chain-relative — 12s/D on
// Ethereum and 400ms/D on Solana from the same flag. An absolute "--poll-every=6s" would be
// right for one chain and wrong for every other one the process serves.
const PollDivisorFlagName = "chain-tracker-poll-divisor"

const (
	// DefaultPollDivisor is the built-in cadence: avgBlockTime/2, two polls per block time.
	// Used when the operator supplies nothing (0) or an out-of-range value.
	DefaultPollDivisor = 2

	// MinPollDivisor floors the cadence at ONE poll per block time. Polling slower than the
	// chain produces blocks is deliberately out of reach here, because two windows derived
	// from avgBlockTime elsewhere start to bind:
	//
	//   - the traffic gate may skip up to chaintracker.defaultMaxRelaySkipsBeforePoll (4)
	//     consecutive cycles, so the poll atomic that consistency pre-validation falls back
	//     to can be (skips+1) x interval stale. At divisor 1 that is ~5 block times against
	//     a default 10-block EndpointLagThreshold — half the margin the built-in cadence has,
	//     and the last value that keeps a comfortable one.
	//   - chainstate.StalenessWindow and the probe's alive horizon are both ~10 x avgBlockTime;
	//     an interval that approaches them scores healthy endpoints stale/not-alive.
	//
	// Going below 1 is therefore not a tuning decision but a redesign of those windows, and
	// should arrive with them rather than through this knob.
	MinPollDivisor = 1

	// MaxPollDivisor caps how much FASTER than the built-in cadence an operator can poll.
	// The cap is relative to the chain's own block time, which is the frame that matters:
	// divisor 8 is 8 polls per block either way, but that is 8 requests per 12s on Ethereum
	// and 8 per 400ms on Solana.
	MaxPollDivisor = 8
)

// resolvePollDivisor validates an operator-supplied divisor, reverting to the built-in
// default on 0 (nothing supplied) or an out-of-range value. Out-of-range warns rather than
// silently clamping — an operator who asked for 20 polls per block should learn the request
// was refused, not quietly receive 8 and read the metric as confirmation.
//
// Validation lives here, not in the command's RunE, so every construction path is covered
// identically: the CLI flag, a config.yml key resolved through viper, and an embedded server
// that sets the field directly.
func resolvePollDivisor(divisor int, chainID, apiInterface string) int {
	if divisor == 0 {
		return DefaultPollDivisor
	}
	if divisor < MinPollDivisor || divisor > MaxPollDivisor {
		utils.LavaFormatWarning("--"+PollDivisorFlagName+" out of allowed range; reverting to default", nil,
			utils.LogAttr("provided", divisor),
			utils.LogAttr("allowed", []int{MinPollDivisor, MaxPollDivisor}),
			utils.LogAttr("default", DefaultPollDivisor),
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface),
		)
		return DefaultPollDivisor
	}
	return divisor
}

// resolveFlatPollInterval turns a (already floored) average block time and a validated
// divisor into the tracker's flat poll interval. averageBlockTime is guaranteed positive by
// the caller (NewEndpointMonitor floors a zero spec value to DefaultAverageBlockTime), and
// divisor is guaranteed within [MinPollDivisor, MaxPollDivisor], so the result is always
// positive — a zero would silently switch the tracker back to the legacy adaptive scheduler.
func resolveFlatPollInterval(averageBlockTime time.Duration, divisor int) time.Duration {
	return averageBlockTime / time.Duration(divisor)
}
