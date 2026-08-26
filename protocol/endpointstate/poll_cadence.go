package endpointstate

import (
	"math"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"

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
	DefaultPollDivisor = 2.0

	// MinPollDivisor allows polling as slowly as one poll per FOUR block times
	// (avgBlockTime/0.25). It is fractional on purpose: the knob is a ratio to block time, and
	// the useful relief on a fast chain lies below one poll per block, not above it.
	//
	// What actually bounds it is chainstate.StalenessWindow (max(10 x avgBlockTime, 2s)) — the
	// horizon past which an observation stops counting for consensus, the tip reads "unknown",
	// and the probe's alive check (which reuses the same constant by design) scores a healthy
	// endpoint not-alive. The window does NOT move with this knob; the knob moves how long the
	// tip can go unrefreshed, which is the other side of that comparison:
	//
	//   idle endpoint  — nothing but the poll refreshes the tip, so the gap IS the interval.
	//                    At 0.25 that is 4 x avgBlockTime, comfortably inside the 10x window.
	//   served endpoint — relay harvest refreshes the same tip, so the gap is bounded by traffic,
	//                    not by this knob.
	//
	// The exposure is the seam between them: an endpoint that trips the traffic gate and then
	// goes quiet is refreshed by neither, and the worst-case gap becomes
	// (chaintracker.DefaultMaxRelaySkipsBeforePoll + 1) x interval — 20 x avgBlockTime at 0.25,
	// which is past the window. That coupling is a property of the PRODUCT of this knob and the
	// skip budget, not of either alone, so warnIfCadenceOutrunsStaleness reports it at startup
	// rather than this constant pretending to prevent it.
	MinPollDivisor = 0.25

	// MaxPollDivisor caps how much FASTER than the built-in cadence an operator can poll.
	// The cap is relative to the chain's own block time, which is the frame that matters:
	// divisor 8 is 8 polls per block either way, but that is 8 requests per 12s on Ethereum
	// and 8 per 400ms on Solana.
	MaxPollDivisor = 8.0
)

// resolvePollDivisor validates an operator-supplied divisor, reverting to the built-in
// default on 0 (nothing supplied) or an out-of-range value. Out-of-range warns rather than
// silently clamping — an operator who asked for 20 polls per block should learn the request
// was refused, not quietly receive 8 and read the metric as confirmation.
//
// Validation lives here, not in the command's RunE, so every construction path is covered
// identically: the CLI flag, a config.yml key resolved through viper, and an embedded server
// that sets the field directly.
func resolvePollDivisor(divisor float64, chainID, apiInterface string) float64 {
	if divisor == 0 {
		return DefaultPollDivisor
	}
	// NaN fails every comparison below, so it would slip through an ordinary range check and
	// then make the interval NaN — which time.Duration turns into a huge negative, i.e. a timer
	// that fires immediately and hot-loops the upstream. Reject it explicitly.
	if math.IsNaN(divisor) {
		utils.LavaFormatWarning("--"+PollDivisorFlagName+" is not a number; reverting to default", nil,
			utils.LogAttr("default", DefaultPollDivisor),
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface),
		)
		return DefaultPollDivisor
	}
	if divisor < MinPollDivisor || divisor > MaxPollDivisor {
		utils.LavaFormatWarning("--"+PollDivisorFlagName+" out of allowed range; reverting to default", nil,
			utils.LogAttr("provided", divisor),
			utils.LogAttr("allowed", []float64{MinPollDivisor, MaxPollDivisor}),
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
//
// The division is done in float64 and only then converted: time.Duration(divisor) would
// truncate every fractional divisor to an integer, turning 0.25 into 0 and the whole
// expression into a divide-by-zero panic.
func resolveFlatPollInterval(averageBlockTime time.Duration, divisor float64) time.Duration {
	interval := time.Duration(float64(averageBlockTime) / divisor)
	// Defensive: a sub-nanosecond result would round to 0 and re-enable the adaptive scheduler.
	// Unreachable with the validated bounds above (the smallest is avgBlockTime/8), but the
	// cost of being wrong here is a silent scheduler swap, so it is not left to reasoning.
	if interval <= 0 {
		return averageBlockTime
	}
	return interval
}

// warnIfCadenceOutrunsStaleness reports, once per chain at startup, a cadence whose worst-case
// gap between real polls exceeds chainstate.StalenessWindow — the horizon past which an
// observation stops counting for consensus, the tip reads "unknown", and the probe's alive check
// (which reuses the same constant) scores a healthy endpoint not-alive.
//
// The hazard is a PRODUCT, not a single setting: the traffic gate may skip up to
// chaintracker.DefaultMaxRelaySkipsBeforePoll consecutive cycles, so the longest a tip can go
// unrefreshed by a poll is (skips+1) x interval. Neither the divisor nor the skip budget is
// unsafe alone, which is exactly why a range check on either one cannot catch this.
//
// It warns rather than rejects. The gap is only reachable in one seam — an endpoint that trips
// the gate and then goes quiet, so neither relays nor polls refresh it — and an idle endpoint
// (gap == interval) stays well inside the window at every allowed divisor. Refusing a
// configuration that is safe for both of the common cases would cost more than it protects.
func warnIfCadenceOutrunsStaleness(interval, averageBlockTime time.Duration, chainID, apiInterface string) {
	window := chainstate.StalenessWindow(averageBlockTime)
	worstCaseGap := time.Duration(chaintracker.DefaultMaxRelaySkipsBeforePoll+1) * interval
	if worstCaseGap <= window {
		return
	}
	utils.LavaFormatWarning("poll cadence can outrun the staleness window when the traffic gate skips", nil,
		utils.LogAttr("chainID", chainID),
		utils.LogAttr("apiInterface", apiInterface),
		utils.LogAttr("pollInterval", interval),
		utils.LogAttr("maxRelaySkips", chaintracker.DefaultMaxRelaySkipsBeforePoll),
		utils.LogAttr("worstCaseGapBetweenPolls", worstCaseGap),
		utils.LogAttr("stalenessWindow", window),
		utils.LogAttr("impact", "an endpoint that stops serving relays right after the gate skips can read stale: no consensus vote, sync scoring off, probe scores it not-alive"),
		utils.LogAttr("note", "an idle endpoint is unaffected (its gap is one interval); a served endpoint is refreshed by relay harvest"),
	)
}
