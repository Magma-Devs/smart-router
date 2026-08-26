package chaintracker

import (
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// The poll cadence must honour an upstream's Retry-After when it names a longer wait
// than the generic failure backoff — and fall back to the backoff once it passes or the
// upstream answers again.
func TestComputePollInterval_RetryAfterFloors(t *testing.T) {
	cs := &ChainTracker{flatPollInterval: 6 * time.Second}

	// Baseline: flat cadence with backoff, no floor.
	require.Equal(t, 6*time.Second, cs.computePollInterval(0, 0))
	require.Equal(t, 12*time.Second, cs.computePollInterval(0, 1))

	// A rate-limited poll with Retry-After far above the backoff ceiling floors the
	// interval to (about) the requested wait.
	cs.noteFetchOutcome(common.RateLimited(errors.New("HTTP 429"), 5*time.Minute))
	got := cs.computePollInterval(0, 1)
	require.Greater(t, got, 4*time.Minute, "interval must honour the upstream's Retry-After")
	require.LessOrEqual(t, got, 5*time.Minute)

	// A Retry-After shorter than the computed interval never shortens it.
	cs.noteFetchOutcome(common.RateLimited(errors.New("HTTP 429"), time.Second))
	require.Equal(t, 12*time.Second, cs.computePollInterval(0, 1))

	// Any answered poll clears the floor.
	cs.noteFetchOutcome(common.RateLimited(errors.New("HTTP 429"), 5*time.Minute))
	cs.noteFetchOutcome(nil)
	require.Equal(t, 6*time.Second, cs.computePollInterval(0, 0))

	// A non-rate-limit failure clears it too — the upstream answered, its ask is spent.
	cs.noteFetchOutcome(common.RateLimited(errors.New("HTTP 429"), 5*time.Minute))
	cs.noteFetchOutcome(errors.New("connection refused"))
	require.Equal(t, 6*time.Second, cs.computePollInterval(0, 0))
}
