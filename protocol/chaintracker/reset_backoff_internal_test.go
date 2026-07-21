package chaintracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestChainTracker_CurrentPollInterval_ReflectsBackoff verifies updateTimer publishes the live
// dedicated-poll interval to the atomic that CurrentPollInterval reads (MAG-2395): base cadence
// when healthy (fetchFails == 0), exponentialBackoff-stretched when failing. This is the value
// /debug/endpoint-state surfaces as PollIntervalMs — the observable the reset returns to base.
func TestChainTracker_CurrentPollInterval_ReflectsBackoff(t *testing.T) {
	flat := 6 * time.Second
	ct := &ChainTracker{flatPollInterval: flat}

	// Healthy → base cadence. computePollInterval ignores the tickerBaseTime arg for flat trackers.
	ct.updateTimer(flat, 0)
	require.Equal(t, flat, ct.CurrentPollInterval())

	// Backed off → base * 2^fetchFails.
	ct.updateTimer(flat, 3)
	require.Equal(t, flat*8, ct.CurrentPollInterval())

	ct.timer.Stop()
}

// TestChainTracker_ResetBackoff_NonBlockingCoalesces verifies ResetBackoff never blocks on the poll
// goroutine and coalesces repeated requests on its buffered(1) channel (MAG-2395), so the
// /debug/reset-probe-backoff handler is always fast even while a reset is already pending.
func TestChainTracker_ResetBackoff_NonBlockingCoalesces(t *testing.T) {
	ct := &ChainTracker{resetBackoffCh: make(chan struct{}, 1)}

	ct.ResetBackoff()
	ct.ResetBackoff()
	ct.ResetBackoff()
	require.Len(t, ct.resetBackoffCh, 1, "repeated resets coalesce into one buffered signal")

	<-ct.resetBackoffCh // the poll goroutine drains one signal per loop iteration
	require.Empty(t, ct.resetBackoffCh)

	ct.ResetBackoff()
	require.Len(t, ct.resetBackoffCh, 1, "reset works again once the pending signal is drained")
}
