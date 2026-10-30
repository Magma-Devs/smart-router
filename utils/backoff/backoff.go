// Package backoff computes exponential retry ladders. It is pure: callers keep their own
// attempt counters, random sources and floors, and ask only for the delay of an attempt.
package backoff

import (
	"math"
	"time"
)

// Config is an exponential ladder: Initial grown by Multiplier per attempt, capped at Max.
// JitterFactor > 0 lets Jittered spread a delay over delay × (1 ± JitterFactor).
type Config struct {
	Initial      time.Duration
	Max          time.Duration
	Multiplier   float64
	JitterFactor float64
}

// Delay is Initial × Multiplier^attempt, capped at Max. A negative attempt counts as 0.
func Delay(attempt int, cfg Config) time.Duration {
	return time.Duration(delay(attempt, cfg))
}

// Jittered is Delay spread over [delay×(1-JitterFactor), delay×(1+JitterFactor)) by r, a
// uniform draw from [0, 1). The spread applies after the cap, so a capped delay can exceed Max.
func Jittered(attempt int, cfg Config, r float64) time.Duration {
	d := delay(attempt, cfg)
	if cfg.JitterFactor > 0 {
		jitter := d * cfg.JitterFactor
		d = d - jitter + (r * 2 * jitter)
	}
	return time.Duration(d)
}

// delay stays in float64 until the caller truncates, so a fractional multiplier keeps its
// sub-nanosecond part through the jitter. A non-positive Initial skips the power, which would
// otherwise turn 0 × +Inf at a huge attempt into NaN.
func delay(attempt int, cfg Config) float64 {
	if attempt < 0 {
		attempt = 0
	}
	d := float64(cfg.Initial)
	if d > 0 {
		d *= math.Pow(cfg.Multiplier, float64(attempt))
	}
	if d > float64(cfg.Max) {
		d = float64(cfg.Max)
	}
	return d
}
