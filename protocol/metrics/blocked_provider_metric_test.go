package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

var blockedProviderLabels = []string{"spec", "apiInterface", "provider_address"}

func newSmartRouterForBlockedProviderTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		csmProviderBlocked: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "t_sr_provider_blocked"}, blockedProviderLabels),
	}
}

// SetBlockedProvider was an empty function body, so no call site published anything. Assert the
// value in both directions — the previous test only asserted that the call did not panic, which
// an empty body also satisfies, so it passed against the very stub it was named after.
func TestSetBlockedProvider_PublishesBothDirections(t *testing.T) {
	m := newSmartRouterForBlockedProviderTest()
	g := m.csmProviderBlocked.WithLabelValues("LAVA", "tendermintrpc", "provider-1")

	m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "http://127.0.0.1:1", true)
	require.Equal(t, float64(1), testutil.ToFloat64(g), "a blocked provider must publish 1")

	m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "http://127.0.0.1:1", false)
	require.Equal(t, float64(0), testutil.ToFloat64(g), "a restored provider must publish 0")
}

// The node URL must never become a label: it can carry an API key in its path or query, and
// docs/METRICS.md makes that a rule for every series. Two calls that differ ONLY by endpoint
// must land on one series rather than creating a second one keyed by the URL.
func TestSetBlockedProvider_DoesNotLabelByNodeURL(t *testing.T) {
	m := newSmartRouterForBlockedProviderTest()

	m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "https://node.example/v2/SECRET-KEY", true)
	m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "https://node.example/v2/ROTATED-KEY", false)

	require.Equal(t, 1, testutil.CollectAndCount(m.csmProviderBlocked),
		"the endpoint must not be a label — a changed URL must not fork a second series")
	require.Equal(t, float64(0),
		testutil.ToFloat64(m.csmProviderBlocked.WithLabelValues("LAVA", "tendermintrpc", "provider-1")),
		"the unblock must land on the same series as the block")
}

// ResetBlockedProvidersMetrics rebases the gauge onto a new pairing at an epoch transition.
// A provider that was blocked and is no longer paired must not leave a series stuck at 1:
// the unblock path only publishes for addresses still in the standing block list, so nothing
// else can ever clear it.
func TestResetBlockedProvidersMetrics_DropsProvidersNoLongerPaired(t *testing.T) {
	m := newSmartRouterForBlockedProviderTest()
	m.SetBlockedProvider("LAVA", "tendermintrpc", "stays", "", true)
	m.SetBlockedProvider("LAVA", "tendermintrpc", "leaves", "", true)
	require.Equal(t, 2, testutil.CollectAndCount(m.csmProviderBlocked))

	m.ResetBlockedProvidersMetrics("LAVA", "tendermintrpc", map[string]string{"stays": "http://127.0.0.1:1"})

	require.Equal(t, 1, testutil.CollectAndCount(m.csmProviderBlocked),
		"the departed provider's series must be dropped, not frozen at 1")
	require.Equal(t, float64(0),
		testutil.ToFloat64(m.csmProviderBlocked.WithLabelValues("LAVA", "tendermintrpc", "stays")),
		"a provider carried into the new pairing is seeded as serving")
}

// The rebase is scoped to one chain: a reset on one spec/apiInterface must not wipe another's.
func TestResetBlockedProvidersMetrics_ScopedToOneChain(t *testing.T) {
	m := newSmartRouterForBlockedProviderTest()
	m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "", true)
	m.SetBlockedProvider("ETH1", "jsonrpc", "provider-1", "", true)

	m.ResetBlockedProvidersMetrics("LAVA", "tendermintrpc", map[string]string{})

	require.Equal(t, 1, testutil.CollectAndCount(m.csmProviderBlocked))
	require.Equal(t, float64(1),
		testutil.ToFloat64(m.csmProviderBlocked.WithLabelValues("ETH1", "jsonrpc", "provider-1")),
		"another chain's blocked provider must survive this chain's epoch rebase")
}

// Every setter on this manager is nil-safe: the constructor returns nil when metrics are disabled.
func TestBlockedProviderMetrics_NilManagerIsSafe(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "", true)
		m.ResetBlockedProvidersMetrics("LAVA", "tendermintrpc", map[string]string{"provider-1": ""})
	})
}
