package specfetcher

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseSpecProposalIgnoresRemovedFields is the remote-path counterpart to
// keeper.TestLegacyProposalStillLoads: the specfetcher parse path
// (parseSpecProposal, used by the remote fetchers) must also accept legacy spec
// JSON containing the fields the cleanup removes. It reuses the canonical
// retained fixture so both loader entry points are proven against identical
// bytes, and asserts only active fields so it holds before and after removal.
func TestParseSpecProposalIgnoresRemovedFields(t *testing.T) {
	// The canonical legacy fixture is owned by utils/keeper (the local loader);
	// reuse it here so the remote path is proven against the exact same bytes.
	content, err := os.ReadFile(filepath.Join("..", "keeper", "testdata", "legacy_spec_proposal.json"))
	require.NoError(t, err)

	specs, err := parseSpecProposal(content)
	require.NoError(t, err, "remote parse must accept legacy fields")
	require.Len(t, specs, 3)

	base, ok := specs["LEGACYBASE"]
	require.True(t, ok)
	require.Equal(t, "Legacy Base Chain", base.Name)
	require.True(t, base.Enabled)
	require.Equal(t, int64(13000), base.AverageBlockTime)
	require.Len(t, base.ApiCollections, 1)
	require.Len(t, base.ApiCollections[0].Apis, 1)
	require.Equal(t, uint64(10), base.ApiCollections[0].Apis[0].ComputeUnits)

	child, ok := specs["LEGACYCHILD"]
	require.True(t, ok)
	require.Equal(t, []string{"LEGACYBASE"}, child.Imports)
}
