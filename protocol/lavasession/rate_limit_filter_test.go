package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/stretchr/testify/require"
)

func filterFixture(t *testing.T) (*ConsumerSessionManager, *holdoff.Registry, *time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })
	csm := &ConsumerSessionManager{rateLimitHoldoff: reg}
	return csm, reg, &now
}

func TestFilterRateLimitedProviders_NoHoldsPassesThrough(t *testing.T) {
	csm, _, _ := filterFixture(t)
	in := []string{"a", "b", "c"}
	require.Equal(t, in, csm.filterRateLimitedProviders(context.Background(), in, nil))
}

func TestFilterRateLimitedProviders_NilRegistryPassesThrough(t *testing.T) {
	csm := &ConsumerSessionManager{}
	in := []string{"a", "b"}
	require.Equal(t, in, csm.filterRateLimitedProviders(context.Background(), in, nil))
}

func TestFilterRateLimitedProviders_DropsHeldProviders(t *testing.T) {
	csm, reg, _ := filterFixture(t)
	reg.RecordRateLimit("b", "url-b", 5*time.Minute)

	got := csm.filterRateLimitedProviders(context.Background(), []string{"a", "b", "c"}, nil)
	require.ElementsMatch(t, []string{"a", "c"}, got)
}

func TestFilterRateLimitedProviders_ExpiredHoldIsChoosableAgain(t *testing.T) {
	csm, reg, now := filterFixture(t)
	reg.RecordRateLimit("b", "url-b", time.Minute)
	*now = now.Add(2 * time.Minute)

	got := csm.filterRateLimitedProviders(context.Background(), []string{"a", "b"}, nil)
	require.ElementsMatch(t, []string{"a", "b"}, got)
}

// When every candidate is held off, the soonest-to-expire one stays in — the request
// must still be served, and the customer is never answered with a synthesized 429.
func TestFilterRateLimitedProviders_AllHeldKeepsSoonestToExpire(t *testing.T) {
	csm, reg, _ := filterFixture(t)
	reg.RecordRateLimit("a", "url-a", 10*time.Minute)
	reg.RecordRateLimit("b", "url-b", 2*time.Minute)
	reg.RecordRateLimit("c", "url-c", 30*time.Minute)

	got := csm.filterRateLimitedProviders(context.Background(), []string{"a", "b", "c"}, nil)
	require.Equal(t, []string{"b"}, got)
}

// Ready-but-ignored providers do not count as choosable: if the only non-held providers
// are already ignored for this request, the soonest-to-expire held one is kept.
func TestFilterRateLimitedProviders_IgnoredReadyDoesNotCount(t *testing.T) {
	csm, reg, _ := filterFixture(t)
	reg.RecordRateLimit("b", "url-b", 2*time.Minute)
	reg.RecordRateLimit("c", "url-c", 10*time.Minute)
	ignored := map[string]struct{}{"a": {}}

	got := csm.filterRateLimitedProviders(context.Background(), []string{"a", "b", "c"}, ignored)
	require.ElementsMatch(t, []string{"a", "b"}, got, "ignored 'a' stays for downstream bookkeeping, held 'b' is the soonest-to-expire fallback")
}

// A held provider that is also ignored this request cannot be the fallback.
func TestFilterRateLimitedProviders_HeldAndIgnoredCannotFallBack(t *testing.T) {
	csm, reg, _ := filterFixture(t)
	reg.RecordRateLimit("a", "url-a", 2*time.Minute)
	reg.RecordRateLimit("b", "url-b", 10*time.Minute)
	ignored := map[string]struct{}{"a": {}}

	got := csm.filterRateLimitedProviders(context.Background(), []string{"a", "b"}, ignored)
	require.Equal(t, []string{"b"}, got, "'a' is ignored, so 'b' is the only possible fallback despite expiring later")
}
