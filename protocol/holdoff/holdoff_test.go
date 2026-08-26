package holdoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestRegistry pins the clock through the public consumer-test constructor. Tests
// that exercise jitter override randFloat explicitly.
func newTestRegistry(start time.Time) (*Registry, *time.Time) {
	now := start
	r := NewRegistryWithClock(func() time.Time { return now })
	return r, &now
}

var t0 = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

func TestFirstStrikeUsesInitialHoldoff(t *testing.T) {
	r, _ := newTestRegistry(t0)
	d := r.RecordRateLimit("vendor", "https://a.example/eth", 0)
	require.Equal(t, InitialHoldoff, d)
	require.True(t, r.HeldOff("vendor", "https://a.example/eth"))
	require.Equal(t, t0.Add(InitialHoldoff), r.ReadyAt("vendor", "https://a.example/eth"))
}

func TestConsecutiveStrikesDoubleUpToCap(t *testing.T) {
	r, _ := newTestRegistry(t0)
	want := []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute,
		8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute,
	}
	for i, w := range want {
		d := r.RecordRateLimit("vendor", "u", 0)
		require.Equal(t, w, d, "strike %d", i+1)
	}
}

func TestRetryAfterIsTheFloor(t *testing.T) {
	r, _ := newTestRegistry(t0)

	// Larger than the exponential: the vendor's ask wins.
	d := r.RecordRateLimit("vendor", "u", 10*time.Minute)
	require.Equal(t, 10*time.Minute, d)

	// Smaller than the current exponential: the exponential wins (a vendor asking for
	// 1s on the second consecutive strike does not shrink the penalty).
	d = r.RecordRateLimit("vendor", "u", time.Second)
	require.Equal(t, time.Minute, d)
}

func TestRetryAfterIsBounded(t *testing.T) {
	r, _ := newTestRegistry(t0)
	r.randFloat = func() float64 { return 1 } // worst case must not exceed the hard bound
	d := r.RecordRateLimit("vendor", "u", 24*time.Hour)
	require.Equal(t, maxUpstreamHoldoff, d, "the bound includes jitter")
}

func TestJitterOnlyExtends(t *testing.T) {
	r, _ := newTestRegistry(t0)
	r.randFloat = func() float64 { return 1 } // worst case
	d := r.RecordRateLimit("vendor", "u", 100*time.Second)
	require.Equal(t, 120*time.Second, d, "jitter extends by at most JitterFactor")
	require.GreaterOrEqual(t, d, 100*time.Second, "never shorter than the vendor's ask")
}

func TestAnswerClearsStrikesAndHoldoff(t *testing.T) {
	r, _ := newTestRegistry(t0)
	r.RecordRateLimit("vendor", "u", 0)
	r.RecordRateLimit("vendor", "u", 0)
	require.True(t, r.HeldOff("vendor", "u"))

	r.RecordAnswer("vendor", "u")
	require.False(t, r.HeldOff("vendor", "u"))
	require.True(t, r.ReadyAt("vendor", "u").IsZero())

	// Strikes reset: the next 429 starts from the initial penalty again.
	d := r.RecordRateLimit("vendor", "u", 0)
	require.Equal(t, InitialHoldoff, d)
}

func TestEscalationCoversTheWholeProvider(t *testing.T) {
	r, now := newTestRegistry(t0)

	r.RecordRateLimit("vendor", "https://a.example/eth", 5*time.Minute)
	require.False(t, r.HeldOff("vendor", "https://b.example/arb"),
		"one held URL must not lock the provider")

	r.RecordRateLimit("vendor", "https://b.example/arb", 10*time.Minute)
	require.True(t, r.HeldOff("vendor", "https://c.example/base"),
		"two held URLs escalate to the provider name")
	require.Equal(t, t0.Add(10*time.Minute), r.ReadyAt("vendor", "https://c.example/base"),
		"the provider hold-off ends when its longest-held member does")

	// Another provider is untouched.
	require.False(t, r.HeldOff("other", "https://d.example/eth"))

	// An expired escalation must not leave an unrelated URL with a stale non-zero
	// ReadyAt value.
	*now = t0.Add(10*time.Minute + time.Second)
	require.True(t, r.ReadyAt("vendor", "https://never-limited.example/base").IsZero())
}

func TestAnswerDropsTheEscalation(t *testing.T) {
	r, _ := newTestRegistry(t0)
	r.RecordRateLimit("vendor", "a", 5*time.Minute)
	r.RecordRateLimit("vendor", "b", 5*time.Minute)
	require.True(t, r.HeldOff("vendor", "c"))

	r.RecordAnswer("vendor", "a")
	require.False(t, r.HeldOff("vendor", "c"), "an answering account is not capped")
	require.True(t, r.HeldOff("vendor", "b"), "the other URL keeps its own hold-off")
}

func TestHoldoffExpires(t *testing.T) {
	r, now := newTestRegistry(t0)
	r.RecordRateLimit("vendor", "u", 0)
	require.True(t, r.HeldOff("vendor", "u"))

	*now = t0.Add(InitialHoldoff + time.Second)
	require.False(t, r.HeldOff("vendor", "u"))
	require.True(t, r.ReadyAt("vendor", "u").IsZero(),
		"an expired URL hold-off must use the same zero value as an absent one")
}

func TestStrikeMemoryAgesOut(t *testing.T) {
	r, now := newTestRegistry(t0)
	r.RecordRateLimit("vendor", "u", 0)
	r.RecordRateLimit("vendor", "u", 0) // 2 strikes, next would be 2m

	// Past the retention window with no further 429s, the URL starts clean.
	*now = t0.Add(entryRetention + 2*time.Minute)
	d := r.RecordRateLimit("vendor", "u", 0)
	require.Equal(t, InitialHoldoff, d)
}

func TestUnknownIsNotHeldOff(t *testing.T) {
	r, _ := newTestRegistry(t0)
	require.False(t, r.HeldOff("vendor", "u"))
	require.True(t, r.ReadyAt("vendor", "u").IsZero())
}

func TestProviderReadyAt(t *testing.T) {
	t.Run("unknown provider is not held", func(t *testing.T) {
		r, _ := newTestRegistry(t0)
		readyAt, held := r.ProviderReadyAt("vendor")
		require.False(t, held)
		require.True(t, readyAt.IsZero())
	})

	t.Run("all limited URLs held means the provider is held", func(t *testing.T) {
		r, _ := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 5*time.Minute)
		readyAt, held := r.ProviderReadyAt("vendor")
		require.True(t, held)
		require.Equal(t, t0.Add(5*time.Minute), readyAt)
	})

	t.Run("a free URL frees the provider", func(t *testing.T) {
		r, now := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 0) // 30s
		*now = t0.Add(InitialHoldoff + time.Second)
		r.RecordRateLimit("vendor", "b", 5*time.Minute) // b held, a expired
		_, held := r.ProviderReadyAt("vendor")
		require.False(t, held, "an expired URL is worth trying, so the provider is not held")
	})

	t.Run("escalation holds even past a free URL", func(t *testing.T) {
		r, now := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 2*time.Minute)
		r.RecordRateLimit("vendor", "b", 10*time.Minute) // escalates until t0+10m
		*now = t0.Add(3 * time.Minute)                   // a expired, escalation active
		readyAt, held := r.ProviderReadyAt("vendor")
		require.True(t, held)
		require.Equal(t, t0.Add(10*time.Minute), readyAt)
	})

	t.Run("soonest URL bounds the ready time", func(t *testing.T) {
		r, _ := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 3*time.Minute)
		readyAt, held := r.ProviderReadyAt("vendor")
		require.True(t, held)
		require.Equal(t, t0.Add(3*time.Minute), readyAt)
	})
}
