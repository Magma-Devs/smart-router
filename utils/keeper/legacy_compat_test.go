package keeper

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLegacyProposalStillLoads is the backward-compatibility guard for the spec
// field-level cleanup (see agent_docs/spec_field_cleanup_plan.md, step 1).
//
// testdata/legacy_spec_proposal.json carries every field the cleanup removes
// from the Go model: the nine spec-level governance fields (min_stake_provider
// with a string amount, providers_types as null / 1 / "static" across the three
// specs, contributor, contributor_percentage as a string, shares, identity,
// block_last_updated, reliability_threshold, data_reliability_enabled), the
// api-level fields (extra_compute_units, category.local, category.subscription)
// and the envelope metadata (proposal.title, proposal.description, deposit).
//
// It asserts only ACTIVE fields, so it compiles and passes both before the
// removal (fields present) and after it (fields ignored as unknown JSON by
// encoding/json). It must keep passing once the removal lands.
func TestLegacyProposalStillLoads(t *testing.T) {
	specs, err := GetAllSpecsFromFile(filepath.Join("testdata", "legacy_spec_proposal.json"))
	require.NoError(t, err, "legacy proposal carrying removed fields must still decode")
	require.Len(t, specs, 3)

	base, ok := specs["LEGACYBASE"]
	require.True(t, ok, "LEGACYBASE must load")
	require.Equal(t, "Legacy Base Chain", base.Name)
	require.True(t, base.Enabled)
	// active finalization / block-timing fields survive untouched
	require.Equal(t, uint32(8), base.BlockDistanceForFinalizedData)
	require.Equal(t, uint32(3), base.BlocksInFinalizationProof)
	require.Equal(t, int64(13000), base.AverageBlockTime)
	require.Equal(t, int64(2), base.AllowedBlockLagForQosSync)

	// active api + category fields survive; compute_units is the only CU input
	require.Len(t, base.ApiCollections, 1)
	require.Len(t, base.ApiCollections[0].Apis, 1)
	api := base.ApiCollections[0].Apis[0]
	require.Equal(t, "base_method", api.Name)
	require.Equal(t, uint64(10), api.ComputeUnits)
	require.True(t, api.Category.Deterministic)
	require.Equal(t, uint32(1), api.Category.Stateful)
	require.False(t, api.Category.HangingApi)

	// imports (active inheritance driver) survive
	child, ok := specs["LEGACYCHILD"]
	require.True(t, ok, "LEGACYCHILD must load")
	require.Equal(t, []string{"LEGACYBASE"}, child.Imports)
}

// TestBundledSpecsLoadAndExpand guards invariant #6: every bundled spec still
// loads and expands to the same active runtime model. Run it before and after
// the field removal — the set of expandable indexes must not change. No prior
// test exercised the bundled specs/ directory through the expansion path.
func TestBundledSpecsLoadAndExpand(t *testing.T) {
	specsDir := filepath.Join("..", "..", "specs")
	all, err := GetAllSpecsFromLocalDir(specsDir)
	require.NoError(t, err)
	require.NotEmpty(t, all, "expected bundled specs under %s", specsDir)

	for index := range all {
		t.Run(index, func(t *testing.T) {
			expanded, err := ExpandSpecWithDependencies(all, index)
			require.NoError(t, err, "bundled spec %q must expand", index)
			require.Equal(t, index, expanded.Index)
		})
	}
}
