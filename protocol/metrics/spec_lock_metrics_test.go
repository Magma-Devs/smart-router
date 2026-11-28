package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newSmartRouterForSpecLockTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		specLockEnforced: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "t_sr_spec_lock_enforced"},
			[]string{"spec", "apiInterface", "rule"},
		),
		urlToProviderNames: make(map[string][]string),
	}
}

// smartrouter_spec_lock_enforced is read as a value, not an increase: the lock runs before the first
// scrape, so a zero is published for every endpoint and a non-zero stays until the spec is fixed and
// the router restarts (PRD §9, MAG-5593).
func TestSetSpecLockEnforced_PublishesAValuePerEndpointAndRule(t *testing.T) {
	m := newSmartRouterForSpecLockTest()

	m.SetSpecLockEnforced("ETH1", "jsonrpc", SpecLockRuleStateful, 2)
	m.SetSpecLockEnforced("ETH1", "rest", SpecLockRuleStateful, 0)
	require.Equal(t, 2.0, testutil.ToFloat64(m.specLockEnforced.WithLabelValues("ETH1", "jsonrpc", "stateful")))
	require.Equal(t, 0.0, testutil.ToFloat64(m.specLockEnforced.WithLabelValues("ETH1", "rest", "stateful")),
		"an endpoint the lock forced nothing on still has a series, at 0")
	require.Equal(t, 2, testutil.CollectAndCount(m.specLockEnforced), "one series per endpoint and rule")

	var disabled *SmartRouterMetricsManager
	disabled.SetSpecLockEnforced("ETH1", "jsonrpc", SpecLockRuleStateful, 1) // metrics off: no panic
}
