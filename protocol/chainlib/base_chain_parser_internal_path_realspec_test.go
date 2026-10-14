package chainlib

import (
	"strings"
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// TestRealSpecInternalPathHeadDirective ties the per-path lookup to the container
// shape the REAL parser builds from a REAL spec file — taggedApis first-wins over
// getServiceApis's iteration, CollectionKey as it is actually constructed — rather
// than the hand-built maps of the unit fixture.
//
// testdata/ton_internal_paths.json is lava-specs ton.json (origin/main,
// 2026-10-04) trimmed to its four collections with their parse_directives and a
// handful of apis. It is loaded as TONT, which declares four EMPTY collections and
// imports TON, so the test also proves that import expansion carries each path's
// directives into the child — the shape a production TONT tenant runs on.
func TestRealSpecInternalPathHeadDirective(t *testing.T) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{"testdata"}, "TONT")
	require.NoError(t, err)
	require.Equal(t, "TONT", spec.Index)
	require.Equal(t, []string{"TON"}, spec.Imports)

	parser, err := NewChainParser(spectypes.APIInterfaceRest)
	require.NoError(t, err)
	parser.SetSpec(spec)

	// The spec-wide answer is still the first collection in the file: /v2's. Every
	// unscoped caller (craftRelay, resolveTipApiNames, specRequiresHeadOnly) keeps it.
	parsing, collection, ok := parser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	require.Equal(t, "/getMasterchainInfo", parsing.ApiName)
	require.Equal(t, "/v2", collection.CollectionData.InternalPath)

	for _, tc := range []struct {
		internalPath string
		tag          spectypes.FUNCTION_TAG
		wantApiName  string
		wantTemplate string
		wantArgs     []string
	}{
		{"/v3", spectypes.FUNCTION_TAG_GET_BLOCKNUM, "/masterchainInfo", "/masterchainInfo", []string{"0", "last", "seqno"}},
		{"/v2", spectypes.FUNCTION_TAG_GET_BLOCKNUM, "/getMasterchainInfo", "/getMasterchainInfo", []string{"0", "result", "last", "seqno"}},
		{"/v3", spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM, "/blocks", "/blocks?workchain=-1&seqno=%d", []string{"0", "blocks", "0", "root_hash"}},
		{"/v2", spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM, "/lookupBlock", "/lookupBlock?workchain=-1&shard=-9223372036854775808&seqno=%d", []string{"0", "result", "root_hash"}},
	} {
		// No leading slash: `go test -run 'TestRealSpecInternalPathHeadDirective/v3'` must select it.
		t.Run(strings.TrimPrefix(tc.internalPath, "/")+" "+tc.tag.String(), func(t *testing.T) {
			// nil addons, base allowed: exactly what EndpointPoller passes for a
			// node-url configured with `internal-path` and no `addons`.
			parsing, collection, ok := parser.GetParsingByTagForCollection(tc.tag, nil, tc.internalPath, true)
			require.True(t, ok)
			require.Equal(t, tc.wantApiName, parsing.ApiName)
			require.Equal(t, tc.wantTemplate, parsing.FunctionTemplate)
			require.Equal(t, tc.wantArgs, parsing.ResultParsing.ParserArg,
				"the response shape differs per path; the parser args must be the path's own")
			require.Equal(t, tc.internalPath, collection.CollectionData.InternalPath,
				"the poller crafts the request from CollectionData, so it must be the path's own collection")
			require.Equal(t, "GET", collection.CollectionData.Type)
		})
	}
}
