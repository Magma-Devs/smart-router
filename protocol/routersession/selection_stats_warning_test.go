package routersession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// statsGapOptimizer picks the first candidate it is offered and answers the selection stats the
// test sets, standing in for an optimizer that returned no stats or stats without scores.
type statsGapOptimizer struct {
	*fixedScoreOptimizer
	stats *provideroptimizer.SelectionStats
}

func (o *statsGapOptimizer) ChooseUpstreamWithStats(_ context.Context, allAddresses []string, ignoredProviders map[string]struct{}, _ uint64, _ int64) ([]string, *provideroptimizer.SelectionStats) {
	for _, address := range allAddresses {
		if _, ignored := ignoredProviders[address]; !ignored {
			return []string{address}, o.stats
		}
	}
	return []string{}, nil
}

// A sticky pick and a fresh pick each say, in their own words, that the optimizer gave no
// selection stats or stats without scores, and name the provider picked.
func TestSelectProviderAddresses_WarnsOnMissingSelectionStats(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct {
		name, stickiness string
		stats            *provideroptimizer.SelectionStats
		message          string
		selectedProvider bool
	}{
		{"sticky, no stats", "sticky-id", nil, "Selection stats missing for sticky session provider", false},
		{"sticky, no scores", "sticky-id", &provideroptimizer.SelectionStats{SelectedProvider: "elsewhere"}, "Selection stats missing provider scores for sticky session", true},
		{"fresh, no stats", "", nil, "Selection stats missing for provider selection", false},
		{"fresh, no scores", "", &provideroptimizer.SelectionStats{SelectedProvider: "elsewhere"}, "Selection stats missing provider scores", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := &statsGapOptimizer{fixedScoreOptimizer: newFixedScoreOptimizer(provideroptimizer.SelectionModeBest), stats: tc.stats}
			csm := NewConsumerSessionManager(&RPCEndpoint{"stub", "stub", "stub", false, "/"}, opt, nil, "provider@test", NewActiveSubscriptionProvidersStorage())
			require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
			time.Sleep(5 * time.Millisecond) // let probes finish, as the sibling tests do

			var picked []string
			records := captureLogs(t, func() {
				var err error
				picked, err = csm.getValidProviderAddresses(context.Background(), 1, map[string]struct{}{}, nil, 10, 100, "", nil, nil, common.NO_STATE, tc.stickiness, "")
				require.NoError(t, err)
			})
			require.Len(t, picked, 1)

			record := findLogRecord(records, tc.message)
			require.NotNil(t, record, "no %q line", tc.message)
			require.Equal(t, "warn", record["level"])
			require.Equal(t, picked[0], record["provider"])
			if tc.selectedProvider {
				require.Equal(t, "elsewhere", record["selectedProvider"])
			} else {
				require.NotContains(t, record, "selectedProvider")
			}
			for _, key := range []string{"chainId", "addon", "extensions", "GUID"} {
				require.Contains(t, record, key)
			}
		})
	}
}
