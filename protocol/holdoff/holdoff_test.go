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

// A strike is a hold-off that ended in another 429, so each 429 here lands just after the
// previous hold-off ran out (a 429 inside one is no strike: TestBurstWithinOneHoldoffIsOneStrike).
func TestConsecutiveStrikesDoubleUpToCap(t *testing.T) {
	r, now := newTestRegistry(t0)
	want := []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute,
		8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute,
	}
	for i, w := range want {
		d := r.RecordRateLimit("vendor", "u", 0)
		require.Equal(t, w, d, "strike %d", i+1)
		*now = now.Add(d + time.Second)
	}
}

func TestRetryAfterIsTheFloor(t *testing.T) {
	r, now := newTestRegistry(t0)

	// Larger than the exponential: the vendor's ask wins.
	d := r.RecordRateLimit("vendor", "u", 10*time.Minute)
	require.Equal(t, 10*time.Minute, d)

	// Smaller than the current exponential: the exponential wins (a vendor asking for
	// 1s on the second consecutive strike does not shrink the penalty).
	*now = t0.Add(10*time.Minute + time.Second)
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
	r, now := newTestRegistry(t0)
	r.RecordRateLimit("vendor", "u", 0)
	*now = t0.Add(InitialHoldoff + time.Second)
	r.RecordRateLimit("vendor", "u", 0) // second strike: held off until t0+31s+1m
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
	*now = t0.Add(InitialHoldoff + time.Second)
	r.RecordRateLimit("vendor", "u", 0) // 2 strikes, next would be 2m

	// Past the retention window with no further 429s, the URL starts clean.
	*now = t0.Add(InitialHoldoff + time.Second + entryRetention + 2*time.Minute)
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
		readyAt, held := r.ProviderReadyAt("vendor", []string{"a"})
		require.False(t, held)
		require.True(t, readyAt.IsZero())
	})

	t.Run("every listed URL held means the provider is held", func(t *testing.T) {
		r, _ := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 5*time.Minute)
		readyAt, held := r.ProviderReadyAt("vendor", []string{"a"})
		require.True(t, held)
		require.Equal(t, t0.Add(5*time.Minute), readyAt)
	})

	// MAG-4732: the registry only knows urls that answered 429. A listed url with no entry is a url
	// the request can go to.
	t.Run("a listed URL that never answered 429 frees the provider", func(t *testing.T) {
		r, _ := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 5*time.Minute)
		_, held := r.ProviderReadyAt("vendor", []string{"a", "b"})
		require.False(t, held)
	})

	t.Run("a held URL that is not listed does not count", func(t *testing.T) {
		r, _ := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "rest-url", 5*time.Minute)
		_, held := r.ProviderReadyAt("vendor", []string{"jsonrpc-url"})
		require.False(t, held, "a 429 on another entry's url under the same name says nothing about this one")
	})

	t.Run("an expired URL frees the provider", func(t *testing.T) {
		r, now := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 0) // 30s
		*now = t0.Add(InitialHoldoff + time.Second)
		r.RecordRateLimit("vendor", "b", 5*time.Minute) // b held, a expired
		_, held := r.ProviderReadyAt("vendor", []string{"a", "b"})
		require.False(t, held, "an expired URL is worth trying, so the provider is not held")
	})

	t.Run("escalation holds even past a free URL", func(t *testing.T) {
		r, now := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 2*time.Minute)
		r.RecordRateLimit("vendor", "b", 10*time.Minute) // escalates until t0+10m
		*now = t0.Add(3 * time.Minute)                   // a expired, escalation active
		readyAt, held := r.ProviderReadyAt("vendor", []string{"a", "b", "c"})
		require.True(t, held)
		require.Equal(t, t0.Add(10*time.Minute), readyAt)
	})

	t.Run("soonest URL bounds the ready time", func(t *testing.T) {
		r, _ := newTestRegistry(t0)
		r.RecordRateLimit("vendor", "a", 3*time.Minute)
		readyAt, held := r.ProviderReadyAt("vendor", []string{"a"})
		require.True(t, held)
		require.Equal(t, t0.Add(3*time.Minute), readyAt)
	})
}

// The chain tracker and the WS / gRPC subscription paths record a 429 under the URL alone; the
// relay path answers under the provider's name. An answer on the URL must clear both, or a URL
// relays are serving fine keeps its url-only strikes (MAG-4165). Another URL's entry, and another
// provider's, are untouched.
func TestRecordAnswer_ClearsTheURLOnlyEntryToo(t *testing.T) {
	r := NewRegistryWithClock(time.Now)
	r.RecordRateLimit("https://a", "https://a", 0)
	r.RecordRateLimit("wss://b", "wss://b", 0)
	r.RecordRateLimit("other", "https://a", 0)

	r.RecordAnswer("prov", "https://a")

	require.False(t, r.HeldOff("https://a", "https://a"))
	require.True(t, r.HeldOff("wss://b", "wss://b"), "another url keeps its hold-off")
	require.True(t, r.HeldOff("other", "https://a"), "another provider's entry is that provider's to clear")
}
