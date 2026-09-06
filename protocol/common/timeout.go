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

// ValidateAndCapMinRelayTimeout ensures both DefaultTimeout and MinimumTimePerRelayDelay
// are positive and that MinimumTimePerRelayDelay < DefaultTimeout. Called once at startup
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
