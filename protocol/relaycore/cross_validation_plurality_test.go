package relaycore

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// TestCrossValidationPlurality pins the plurality behind the MAG-2192 failure headers: the largest set of
// providers that returned the same response, empty on a tie, and blind to nil/empty replies.
func TestCrossValidationPlurality(t *testing.T) {
	reply := func(provider, data string) common.RelayResult {
		return common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: provider},
			Reply:        &pairingtypes.RelayReply{Data: []byte(data)},
			ResponseHash: responseContentHash([]byte(data)),
		}
	}
	empty := func(provider string) common.RelayResult {
		return common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: provider},
			Reply:        &pairingtypes.RelayReply{},
		}
	}

	tests := []struct {
		name          string
		results       []common.RelayResult
		wantSize      int
		wantProviders []string
	}{
		{
			// The ticket's repro: 2-of-3 agree, below a threshold of 3.
			name:          "two of three agree",
			results:       []common.RelayResult{reply("p3", `"0xBBBB"`), reply("p2", `"0xAAAA"`), reply("p1", `"0xAAAA"`)},
			wantSize:      2,
			wantProviders: []string{"p1", "p2"},
		},
		{
			name:     "tie names no plurality",
			results:  []common.RelayResult{reply("p1", `"0xA"`), reply("p2", `"0xA"`), reply("p3", `"0xB"`), reply("p4", `"0xB"`)},
			wantSize: 2,
		},
		{
			name:     "all distinct is a tie of one",
			results:  []common.RelayResult{reply("p1", `"0xA"`), reply("p2", `"0xB"`)},
			wantSize: 1,
		},
		{
			name:          "a lone response is the plurality",
			results:       []common.RelayResult{reply("p1", `"0xA"`)},
			wantSize:      1,
			wantProviders: []string{"p1"},
		},
		{
			// Three empty replies outnumber the pair, but an empty reply is not agreement on a value.
			name:          "empty replies never form the plurality",
			results:       []common.RelayResult{empty("p1"), empty("p2"), empty("p3"), reply("p4", `"0xA"`), reply("p5", `"0xA"`)},
			wantSize:      2,
			wantProviders: []string{"p4", "p5"},
		},
		{
			name:    "only empty replies",
			results: []common.RelayResult{empty("p1"), empty("p2")},
		},
		{
			name: "no results",
		},
		{
			// A result whose hash was never cached is hashed with the same rule, so it still joins its peers.
			name: "uncached hash joins the cached group",
			results: []common.RelayResult{
				reply("p1", `"0xA"`),
				{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2"}, Reply: &pairingtypes.RelayReply{Data: []byte(`"0xA"`)}},
				reply("p3", `"0xB"`),
			},
			wantSize:      2,
			wantProviders: []string{"p1", "p2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, providers := CrossValidationPlurality(tt.results)
			require.Equal(t, tt.wantSize, size)
			require.Equal(t, tt.wantProviders, providers)
		})
	}
}
