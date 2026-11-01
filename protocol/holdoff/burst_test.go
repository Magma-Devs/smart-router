package holdoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A vendor's per-second quota answers a burst with many 429s inside one window. Each used to add
// a strike, so seven 429s in one second escalated a 30 s hold-off to the 30-minute cap
// (MAG-4386, measured on QuickNode: 28 real 429s in ~2 s held the url off for 31-35 min). A 429
// that lands while the url is already held off adds nothing: the burst is one strike, and the
// hold-off still ends when the first one said.
func TestBurstWithinOneHoldoffIsOneStrike(t *testing.T) {
	r, now := newTestRegistry(t0)

	first := r.RecordRateLimit("vendor", "u", 0)
	require.Equal(t, InitialHoldoff, first)

	for i := 1; i < 7; i++ {
		*now = t0.Add(time.Duration(i) * 100 * time.Millisecond)
		remaining := r.RecordRateLimit("vendor", "u", 0)
		require.Equal(t, t0.Add(InitialHoldoff).Sub(*now), remaining,
			"429 #%d inside the window reports what is left of the hold-off, not a new one", i+1)
	}
	require.Equal(t, t0.Add(InitialHoldoff), r.ReadyAt("vendor", "u"),
		"the burst did not move the end of the hold-off")

	// The window passes; the next 429 is the second strike, not the eighth.
	*now = t0.Add(InitialHoldoff + time.Second)
	require.Equal(t, 2*InitialHoldoff, r.RecordRateLimit("vendor", "u", 0))
}

// An upstream that asks for longer than what is left gets it, and that is still not a strike.
func TestLongerRetryAfterInsideTheWindowExtendsWithoutAStrike(t *testing.T) {
	r, now := newTestRegistry(t0)
	r.RecordRateLimit("vendor", "u", 0) // strike 1: 30 s

	*now = t0.Add(time.Second)
	require.Equal(t, 5*time.Minute, r.RecordRateLimit("vendor", "u", 5*time.Minute))
	require.Equal(t, t0.Add(time.Second+5*time.Minute), r.ReadyAt("vendor", "u"))

	// A shorter ask inside the window never shortens it.
	*now = t0.Add(2 * time.Second)
	require.Equal(t, 5*time.Minute-time.Second, r.RecordRateLimit("vendor", "u", time.Second))
	require.Equal(t, t0.Add(time.Second+5*time.Minute), r.ReadyAt("vendor", "u"))

	// Past it, the next 429 is the second strike: the extension did not count as one.
	*now = t0.Add(time.Second + 5*time.Minute + time.Second)
	require.Equal(t, 2*InitialHoldoff, r.RecordRateLimit("vendor", "u", 0))
}

// A 429 that changes nothing is not a recorded hold-off: the burst shows once on the
// recorded counter and once in the duration histogram, an extension shows as one more.
func TestMetrics_BurstRecordsOneHoldoff(t *testing.T) {
	r, now := newTestRegistry(t0)
	base := eventCount(t, "vendorBurst", eventRecorded)

	r.RecordRateLimit("vendorBurst", "u", 0)
	for i := 1; i < 7; i++ {
		*now = t0.Add(time.Duration(i) * 100 * time.Millisecond)
		r.RecordRateLimit("vendorBurst", "u", 0)
	}
	require.Equal(t, base+1, eventCount(t, "vendorBurst", eventRecorded))

	r.RecordRateLimit("vendorBurst", "u", 10*time.Minute)
	require.Equal(t, base+2, eventCount(t, "vendorBurst", eventRecorded), "an extension is a hold-off applied")
}
