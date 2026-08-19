package common

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func hdr(v string) http.Header {
	h := http.Header{}
	if v != "" {
		h.Set("Retry-After", v)
	}
	return h
}

// RFC 9110 permits two wire forms and both are in use. Anything else must read as absent —
// a malformed header pushing a caller to retry immediately is the one outcome worse than
// having no header at all.
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	t.Run("delta-seconds", func(t *testing.T) {
		d, ok := ParseRetryAfter(hdr("120"), now)
		require.True(t, ok)
		require.Equal(t, 2*time.Minute, d)
	})

	t.Run("http-date in the future", func(t *testing.T) {
		d, ok := ParseRetryAfter(hdr(now.Add(90*time.Second).Format(http.TimeFormat)), now)
		require.True(t, ok)
		require.InDelta(t, float64(90*time.Second), float64(d), float64(time.Second))
	})

	t.Run("treated as absent", func(t *testing.T) {
		for _, tc := range []struct{ name, value string }{
			{"no header", ""},
			{"not a number or date", "soon"},
			{"zero seconds", "0"},
			{"negative seconds", "-30"},
			{"float seconds", "1.5"},
			{"http-date already past", now.Add(-time.Minute).Format(http.TimeFormat)},
			{"http-date exactly now", now.Format(http.TimeFormat)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, ok := ParseRetryAfter(hdr(tc.value), now)
				require.False(t, ok, "%q must read as absent", tc.value)
			})
		}
	})

	t.Run("nil header", func(t *testing.T) {
		_, ok := ParseRetryAfter(nil, now)
		require.False(t, ok)
	})

	// RFC 9110 sets no ceiling, so both wire forms can name a delay long enough to park a
	// provider indefinitely — and a large enough delta-seconds overflows int64 nanoseconds
	// into a negative duration. Both clamp.
	t.Run("clamped to MaxRetryAfter", func(t *testing.T) {
		for _, tc := range []struct{ name, value string }{
			{"delta-seconds over the cap", strconv.Itoa(int(MaxRetryAfter/time.Second) + 1)},
			{"delta-seconds that would overflow", "10000000000"},
			{"delta-seconds at max int64", "9223372036854775807"},
			{"http-date beyond the cap", now.Add(365 * 24 * time.Hour).Format(http.TimeFormat)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				d, ok := ParseRetryAfter(hdr(tc.value), now)
				require.True(t, ok, "%q is a usable delay, just an excessive one", tc.value)
				require.Equal(t, MaxRetryAfter, d)
			})
		}
	})

	// An HTTP-date is absolute, so it is only as good as the two clocks agreeing. The
	// upstream's Date header states the clock that wrote the deadline, so the gap between
	// them is skew-free; without it the local clock is all there is.
	t.Run("http-date measured against the upstream Date header", func(t *testing.T) {
		skewed := func(skew time.Duration) http.Header {
			h := hdr(now.Add(skew + 90*time.Second).Format(http.TimeFormat))
			h.Set("Date", now.Add(skew).Format(http.TimeFormat))
			return h
		}

		t.Run("upstream clock ahead", func(t *testing.T) {
			d, ok := ParseRetryAfter(skewed(10*time.Minute), now)
			require.True(t, ok)
			require.InDelta(t, float64(90*time.Second), float64(d), float64(time.Second))
		})

		t.Run("upstream clock behind", func(t *testing.T) {
			d, ok := ParseRetryAfter(skewed(-10*time.Minute), now)
			require.True(t, ok, "a backdated header must not read as already past")
			require.InDelta(t, float64(90*time.Second), float64(d), float64(time.Second))
		})

		t.Run("unparseable Date falls back to the local clock", func(t *testing.T) {
			h := hdr(now.Add(90 * time.Second).Format(http.TimeFormat))
			h.Set("Date", "not a date")
			d, ok := ParseRetryAfter(h, now)
			require.True(t, ok)
			require.InDelta(t, float64(90*time.Second), float64(d), float64(time.Second))
		})
	})
}

// The enriched error must stay a 429 to everything that already recognises one — the
// classification in the parent PRs matches on the sentinel and must not care that a duration
// rode along.
func TestWithRetryAfter_PreservesTheSentinel(t *testing.T) {
	now := time.Now()

	enriched := WithRetryAfter(StatusCodeError429, hdr("60"), now)
	require.ErrorIs(t, enriched, StatusCodeError429, "must still satisfy errors.Is on the sentinel")

	d, ok := RetryAfterFrom(enriched)
	require.True(t, ok)
	require.Equal(t, time.Minute, d)

	t.Run("survives further wrapping", func(t *testing.T) {
		wrapped := fmt.Errorf("verify failed: %w", enriched)
		require.ErrorIs(t, wrapped, StatusCodeError429)
		d, ok := RetryAfterFrom(wrapped)
		require.True(t, ok)
		require.Equal(t, time.Minute, d)
	})

	t.Run("no header leaves the error alone", func(t *testing.T) {
		same := WithRetryAfter(StatusCodeError429, hdr(""), now)
		require.Equal(t, StatusCodeError429, same)
		_, ok := RetryAfterFrom(same)
		require.False(t, ok, "no header means no opinion, not zero delay")
	})

	t.Run("passes non-rate-limit errors through untouched", func(t *testing.T) {
		other := errors.New("connection refused")
		require.Equal(t, other, WithRetryAfter(other, hdr("60"), now))
		require.Equal(t, StatusCodeError504, WithRetryAfter(StatusCodeError504, hdr("60"), now))
		require.Nil(t, WithRetryAfter(nil, hdr("60"), now))
	})

	t.Run("RetryAfterFrom is false for a plain rate-limit", func(t *testing.T) {
		_, ok := RetryAfterFrom(StatusCodeError429)
		require.False(t, ok)
	})

	// Enrichment is additive: whatever context was already wrapped around the sentinel has to
	// survive, since the call sites apply it to whatever error they were handed.
	t.Run("keeps the context wrapped around the sentinel", func(t *testing.T) {
		inner := fmt.Errorf("upstream %s: %w", "eth-mainnet", StatusCodeError429)
		enriched := WithRetryAfter(inner, hdr("60"), now)

		require.ErrorIs(t, enriched, StatusCodeError429)
		require.ErrorIs(t, enriched, inner)
		require.Contains(t, enriched.Error(), "upstream eth-mainnet")
		require.Contains(t, enriched.Error(), "retry after 1m0s")
	})
}
