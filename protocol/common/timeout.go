package common

import (
	"context"
	"math"
	"time"

	"github.com/magma-Devs/smart-router/utils"
)

const (
	TimePerCU             = uint64(100 * time.Millisecond)
	CacheWriteTimeout     = 5 * time.Second
	AverageWorldLatency   = 300 * time.Millisecond
	DefaultTimeoutSeconds = 30 // default timeout in seconds, can be overridden by flag
	DefaultCacheTimeout   = 50 * time.Millisecond
	// On subscriptions we must use context.Background(),
	// we cant have a context.WithTimeout() context, meaning we can hang for ever.
	// to avoid that we introduced a first reply timeout using a routine.
	// if the first reply doesn't return after the specified timeout a timeout error will occur
	SubscriptionFirstReplyTimeout = 10 * time.Second
)

// DefaultTimeout is the configurable default timeout for relay processing.
// It can be overridden via the --default-processing-timeout flag on consumer and smart router commands.
var DefaultTimeout = time.Duration(DefaultTimeoutSeconds) * time.Second

// CacheTimeout is the per-relay cache LOOKUP budget (reads only; writes are
// asynchronous under CacheWriteTimeout). The default is sized for a same-zone
// backend; it can be overridden via the --cache-timeout flag on the smart
// router command for backends a network away — a RESP backend such as
// ElastiCache in another region needs at least one round trip per lookup, so
// a budget below the RTT turns every read into a timeout while writes still
// land. Raising it trades added miss latency (a miss now waits up to this
// budget before falling through to the upstream) for the ability to hit at
// all; the secondary tier's --secondary-cache-timeout exists for the same
// reason.
var CacheTimeout = DefaultCacheTimeout

// MinimumTimePerRelayDelay is the minimum relay timeout floor used by GetTimePerCu.
// It can be overridden via the --min-relay-timeout flag on consumer and smart router commands.
var MinimumTimePerRelayDelay = time.Second

// MaxCallerRelayTimeout is the longest a caller's lava-relay-timeout header may make the router
// hold one request. It can be overridden via the --max-caller-relay-timeout flag on the smart
// router command.
//
// The header sets the attempt window, and a request's processing budget is never shorter than its
// window (GetTimeoutForProcessing), so an unbounded header was an unbounded hold (MAG-3600). The
// bound is max(the request's own budget, MaxCallerRelayTimeout), where the own budget is the one
// the request gets with no header: the header can reshape the window anywhere inside that budget,
// and can stretch it only as far as this setting. The default, 0, means it cannot stretch it at
// all. A value below --default-processing-timeout has the same effect, because this setting never
// shortens a budget.
var MaxCallerRelayTimeout time.Duration

// MinCallerRelayTimeout is the shortest attempt window a caller's lava-relay-timeout header can ask
// for. The window is the hedge interval, so a value far below one round trip makes the state
// machine dispatch every attempt the retry limits allow before any endpoint could have answered:
// a per-request fan-out, whatever the endpoints' health. It sits well under --min-relay-timeout on
// purpose. That flag floors only the CU-derived window, and callers do use the header to hedge
// sooner than it (deployments run it at several seconds; the automation suite sends 300ms).
const MinCallerRelayTimeout = AverageWorldLatency

// ValidateAndCapMinRelayTimeout ensures both DefaultTimeout and MinimumTimePerRelayDelay
// are positive and that MinimumTimePerRelayDelay < DefaultTimeout. It also resets a
// non-positive CacheTimeout and a negative MaxCallerRelayTimeout. Called once at startup
// after flags are parsed.
func ValidateAndCapMinRelayTimeout() {
	// Guard DefaultTimeout < 1s: GetTimeoutForProcessing feeds into
	// CapContextTimeout — values below 1s cause immediate DeadlineExceeded on every relay.
	reset := time.Duration(DefaultTimeoutSeconds) * time.Second
	if DefaultTimeout < time.Second {
		utils.LavaFormatWarning("default-processing-timeout is unreasonably small, resetting to default",
			nil,
			utils.LogAttr("invalid_value", DefaultTimeout),
			utils.LogAttr("reset_to", reset),
		)
		DefaultTimeout = reset
	}

	if MinimumTimePerRelayDelay >= DefaultTimeout {
		capped := DefaultTimeout / 2
		// Integer division of a very small DefaultTimeout (< 2ns) rounds to 0.
		// Clamp to at least 1ms so the floor never becomes zero.
		if capped <= 0 {
			capped = time.Millisecond
		}
		utils.LavaFormatWarning("min-relay-timeout >= default-processing-timeout, capping to 50% of processing timeout",
			nil,
			utils.LogAttr("min_relay_timeout", MinimumTimePerRelayDelay),
			utils.LogAttr("default_processing_timeout", DefaultTimeout),
			utils.LogAttr("capped_to", capped),
		)
		MinimumTimePerRelayDelay = capped
	}

	// Guard MinimumTimePerRelayDelay <= 0: GetTimePerCu returns 0 for low-CU methods,
	// which feeds into CapContextTimeout and causes immediate timeouts.
	if MinimumTimePerRelayDelay <= 0 {
		utils.LavaFormatWarning("min-relay-timeout is zero or negative, resetting to 1s",
			nil,
			utils.LogAttr("invalid_value", MinimumTimePerRelayDelay),
		)
		MinimumTimePerRelayDelay = time.Second
	}

	// Guard CacheTimeout <= 0: the lookup context would be born expired and
	// every cache read would fail immediately — a silently disabled cache.
	if CacheTimeout <= 0 {
		utils.LavaFormatWarning("cache-timeout is zero or negative, resetting to default",
			nil,
			utils.LogAttr("invalid_value", CacheTimeout),
			utils.LogAttr("reset_to", DefaultCacheTimeout),
		)
		CacheTimeout = DefaultCacheTimeout
	}

	// Guard MaxCallerRelayTimeout < 0: a negative ceiling has no meaning. 0 is the default, and
	// means a caller cannot extend a request's budget.
	if MaxCallerRelayTimeout < 0 {
		utils.LavaFormatWarning("max-caller-relay-timeout is negative, resetting to 0 (a caller's lava-relay-timeout cannot extend a request's budget)",
			nil,
			utils.LogAttr("invalid_value", MaxCallerRelayTimeout),
		)
		MaxCallerRelayTimeout = 0
	}

	// Kept as set, but said out loud: this can read like a cap on every request, and it is not one.
	// Below the default budget it cannot extend anything, and it never shortens anything, so an
	// operator who set it to cut requests short would otherwise see no effect and no reason.
	if MaxCallerRelayTimeout > 0 && MaxCallerRelayTimeout <= DefaultTimeout {
		utils.LavaFormatWarning("max-caller-relay-timeout is not above default-processing-timeout, so it has no effect: it only lets a caller's lava-relay-timeout extend a request's budget, it never shortens one",
			nil,
			utils.LogAttr("max_caller_relay_timeout", MaxCallerRelayTimeout),
			utils.LogAttr("default_processing_timeout", DefaultTimeout),
		)
	}
}

func LocalNodeTimePerCu(cu uint64) time.Duration {
	return BaseTimePerCU(cu)
}

func BaseTimePerCU(cu uint64) time.Duration {
	return time.Duration(cu * TimePerCU)
}

func GetTimePerCu(cu uint64) time.Duration {
	base := LocalNodeTimePerCu(cu)
	if base < MinimumTimePerRelayDelay {
		return MinimumTimePerRelayDelay
	}
	return base
}

func GetRemainingTimeoutFromContext(ctx context.Context) (timeRemaining time.Duration) {
	deadline, ok := ctx.Deadline()
	if ok {
		return time.Until(deadline)
	}
	return time.Duration(math.MaxInt64)
}

func CapContextTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if GetRemainingTimeoutFromContext(ctx) > timeout {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}

type TimeoutInfo struct {
	CU       uint64
	Hanging  bool
	Stateful uint32
}

func GetTimeoutForProcessing(relayTimeout time.Duration, timeoutInfo TimeoutInfo) time.Duration {
	ctxTimeout := DefaultTimeout
	if timeoutInfo.CU >= 50 {
		ctxTimeout = DefaultTimeout * 2
	}
	if timeoutInfo.Hanging || timeoutInfo.CU >= 100 || timeoutInfo.Stateful == CONSISTENCY_SELECT_ALL_PROVIDERS {
		ctxTimeout = DefaultTimeout * 6
	}
	if relayTimeout > ctxTimeout {
		ctxTimeout = relayTimeout
	}
	return ctxTimeout
}

// BoundCallerRelayTimeout turns the value of a caller's lava-relay-timeout header into the attempt
// window the router will use, and reports false when the value must be ignored in favour of
// ownWindow, the window the router computes for the request by itself.
//
// A value that is not positive is ignored. It means nothing as a window, and the window reaches
// time.NewTicker in the relay state machine's goroutine, which panics on it; a panic there ends the
// process, for every request on it (MAG-3600). A positive value is held inside
// [MinCallerRelayTimeout, max(the request's own budget, MaxCallerRelayTimeout)], where the request's
// own budget is the one it gets with no header: GetTimeoutForProcessing(ownWindow). It has to be
// measured from ownWindow, not from the call's category alone. A hanging call on a slow-block chain
// waits twice the block time, so Bitcoin's sendrawtransaction gets ~20 minutes with no header, and
// a bound built from its category (180s) would leave a caller who asked for more with less. Were
// the floor and the bound ever to cross (a budget under 300ms, which ValidateAndCapMinRelayTimeout
// rules out), the floor wins, so the result is always positive.
func BoundCallerRelayTimeout(requested, ownWindow time.Duration, timeoutInfo TimeoutInfo) (window time.Duration, ok bool) {
	if requested <= 0 {
		return 0, false
	}
	ceiling := max(GetTimeoutForProcessing(ownWindow, timeoutInfo), MaxCallerRelayTimeout)
	return max(min(requested, ceiling), MinCallerRelayTimeout), true
}
