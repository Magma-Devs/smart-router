package rpcsmartrouter

import (
	"math/rand"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/utils/backoff"
)

// WebSocket backoff constants for reconnection
const (
	// WebSocketInitialBackoff is the initial delay before first retry
	WebSocketInitialBackoff = 100 * time.Millisecond

	// WebSocketMaxBackoff is the maximum delay between retries (cap)
	WebSocketMaxBackoff = 30 * time.Second

	// WebSocketBackoffMultiplier is the multiplier for exponential growth
	WebSocketBackoffMultiplier = 2.0

	// WebSocketMaxRetries is the maximum number of retry attempts (0 = unlimited)
	WebSocketMaxRetries = 10

	// WebSocketJitterFactor randomizes delay to prevent thundering herd (0.0 to 1.0)
	WebSocketJitterFactor = 0.3
)

// ExponentialBackoff calculates the next backoff duration with jitter
// for WebSocket reconnection attempts. It uses exponential growth with
// configurable jitter to prevent thundering herd problems.
type ExponentialBackoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	JitterFactor    float64
	MaxRetries      int

	currentAttempt int
	rng            *rand.Rand
	lock           sync.Mutex
}

// NewWebSocketBackoff creates a backoff configured for WebSocket reconnection
// with sensible defaults: 100ms initial, 30s max, 2x multiplier, 30% jitter, 10 retries
func NewWebSocketBackoff() *ExponentialBackoff {
	return &ExponentialBackoff{
		InitialInterval: WebSocketInitialBackoff,
		MaxInterval:     WebSocketMaxBackoff,
		Multiplier:      WebSocketBackoffMultiplier,
		JitterFactor:    WebSocketJitterFactor,
		MaxRetries:      WebSocketMaxRetries,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NextBackoff returns the next backoff duration, or (0, false) if max retries exceeded.
// The backoff grows exponentially: initial * multiplier^attempt, capped at MaxInterval.
// Jitter is applied to spread retries: delay * (1 ± jitter)
func (b *ExponentialBackoff) NextBackoff() (time.Duration, bool) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.MaxRetries > 0 && b.currentAttempt >= b.MaxRetries {
		return 0, false // Max retries exceeded
	}

	cfg := backoff.Config{
		Initial:      b.InitialInterval,
		Max:          b.MaxInterval,
		Multiplier:   b.Multiplier,
		JitterFactor: b.JitterFactor,
	}
	var delay time.Duration
	if b.JitterFactor > 0 && b.rng != nil {
		delay = backoff.Jittered(b.currentAttempt, cfg, b.rng.Float64())
	} else {
		delay = backoff.Delay(b.currentAttempt, cfg)
	}

	b.currentAttempt++
	return delay, true
}

// Reset resets the backoff to initial state (call after successful connection)
func (b *ExponentialBackoff) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.currentAttempt = 0
}

// Attempt returns the current attempt number (0-indexed)
func (b *ExponentialBackoff) Attempt() int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.currentAttempt
}

// Clone creates a copy of the backoff configuration with reset state
func (b *ExponentialBackoff) Clone() *ExponentialBackoff {
	b.lock.Lock()
	defer b.lock.Unlock()

	return &ExponentialBackoff{
		InitialInterval: b.InitialInterval,
		MaxInterval:     b.MaxInterval,
		Multiplier:      b.Multiplier,
		JitterFactor:    b.JitterFactor,
		MaxRetries:      b.MaxRetries,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		currentAttempt:  0, // Reset on clone
	}
}
