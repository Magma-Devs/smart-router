package relaycore

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// TestResponsesCrossValidation_GroupDiversity covers the Phase 1.2c diversity gate: a quorum that meets
// the count threshold but spans too few distinct groups is a failure, not a success. The empty group
// label folds into "default", and MinGroups <= 1 disables the gate (pre-1.2 behavior).
func TestResponsesCrossValidation_GroupDiversity(t *testing.T) {
	mkResult := func(data, group string) common.RelayResult {
		return common.RelayResult{
			Reply:        &pairingtypes.RelayReply{Data: []byte(data)},
			ProviderInfo: common.ProviderInfo{ProviderAddress: "p-" + group, ProviderGroup: group},
		}
	}

	cases := []struct {
		name      string
		minGroups int
		threshold int
		results   []common.RelayResult
		wantOK    bool
	}{
		{
			name: "count met and diversity met -> success", minGroups: 2, threshold: 2,
			results: []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g2")},
			wantOK:  true,
		},
		{
			// The case that proves the gate does something the old code didn't: enough matching
			// responses, but all from one group.
			name: "count met but all one group -> diversity-unmet failure", minGroups: 2, threshold: 2,
			results: []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g1"), mkResult("A", "g1")},
			wantOK:  false,
		},
		{
			name: "empty labels fold to a single default group -> fail", minGroups: 2, threshold: 2,
			results: []common.RelayResult{mkResult("A", ""), mkResult("A", "")},
			wantOK:  false,
		},
		{
			name: "min-groups 1 disables the gate -> count alone suffices", minGroups: 1, threshold: 2,
			results: []common.RelayResult{mkResult("A", ""), mkResult("A", "")},
			wantOK:  true,
		},
		{
			name: "three groups, need two -> success", minGroups: 2, threshold: 3,
			results: []common.RelayResult{mkResult("A", "g1"), mkResult("A", "g2"), mkResult("A", "g3")},
			wantOK:  true,
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rp := &RelayProcessor{
				crossValidationParams: &common.CrossValidationParams{AgreementThreshold: tc.threshold, MaxParticipants: 5, MinGroups: tc.minGroups},
				selection:             CrossValidation,
			}
			result, err := rp.responsesCrossValidation(tc.results, tc.threshold)
			if tc.wantOK {
				require.NoError(t, err, "tc #%d, i #%d", i, i)
				require.NotNil(t, result, "tc #%d, i #%d", i, i)
			} else {
				require.Error(t, err, "tc #%d, i #%d", i, i)
			}
		})
	}
}

// TestCrossValidationQuorumReached_Diversity covers the Phase 1.2b diversity-aware early-exit predicate:
// it must NOT report quorum on count alone when the matching responses span too few groups, so processing
// continues until a later same-hash response from a new group arrives.
func TestCrossValidationQuorumReached_Diversity(t *testing.T) {
	hash := [32]byte{0x01}
	rp := &RelayProcessor{
		crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 2},
		selection:             CrossValidation,
		quorumMap: map[[32]byte]*quorumStat{
			hash: {count: 2, groups: map[string]struct{}{"g1": {}}}, // count met, only one group
		},
	}
	require.False(t, rp.crossValidationQuorumReached(), "count met but diversity unmet must NOT early-exit")

	// A later response with the same hash from a new group satisfies diversity.
	rp.quorumMap[hash].count++
	rp.quorumMap[hash].groups["g2"] = struct{}{}
	require.True(t, rp.crossValidationQuorumReached(), "count + diversity met must early-exit")

	// With MinGroups <= 1 the predicate reduces to count alone (pre-1.2 behavior).
	rpNoGroups := &RelayProcessor{
		crossValidationParams: &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 1},
		selection:             CrossValidation,
		quorumMap: map[[32]byte]*quorumStat{
			hash: {count: 2, groups: map[string]struct{}{"g1": {}}},
		},
	}
	require.True(t, rpNoGroups.crossValidationQuorumReached(), "MinGroups<=1 must reach quorum on count alone")
}
