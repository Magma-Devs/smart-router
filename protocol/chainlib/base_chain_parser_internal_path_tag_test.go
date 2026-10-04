package chainlib

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// tonShapedParser is the shape that broke a TON tenant on 2026-10-03 (MAG-4105):
// one REST spec serving two api surfaces on two internal paths — /v2 (the liteserver api) and
// /v3 (the indexer api) — each declaring its OWN head and block-hash directives,
// and no add-ons anywhere. Both collections are base collections (Addon ""), so
// the add-on-keyed lookup MAG-3296 added has nothing to key on, and a url on /v3
// used to be handed /v2's directive — the first one in spec-file order.
//
// Two add-on collections on /v3 are included for contrast: `indexer-plus`
// declares its own head directive, `metrics` declares none.
func tonShapedParser(t *testing.T) (parser *BaseChainParser, v2, v3 *spectypes.ApiCollection) {
	t.Helper()

	collectionData := func(connectionType, internalPath, addon string) spectypes.CollectionData {
		return spectypes.CollectionData{
			ApiInterface: spectypes.APIInterfaceRest,
			InternalPath: internalPath,
			Type:         connectionType,
			AddOn:        addon,
		}
	}

	v2 = &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("GET", "/v2", ""),
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "/getMasterchainInfo", FunctionTemplate: "/getMasterchainInfo"},
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM, ApiName: "/lookupBlock", FunctionTemplate: "/lookupBlock?workchain=-1&seqno=%d"},
		},
	}
	v3 = &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("GET", "/v3", ""),
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "/masterchainInfo", FunctionTemplate: "/masterchainInfo"},
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM, ApiName: "/blocks", FunctionTemplate: "/blocks?workchain=-1&seqno=%d"},
		},
	}
	v2Post := &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("POST", "/v2", ""),
	}
	v3Post := &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("POST", "/v3", ""),
	}
	indexerPlus := &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("GET", "/v3", "indexer-plus"),
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "/plus/masterchainInfo", FunctionTemplate: "/plus/masterchainInfo"},
		},
	}
	metrics := &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("GET", "/v3", "metrics"),
	}

	collections := map[CollectionKey]*spectypes.ApiCollection{}
	for _, collection := range []*spectypes.ApiCollection{v2, v3, v2Post, v3Post, indexerPlus, metrics} {
		collections[CollectionKey{
			ConnectionType: collection.CollectionData.Type,
			InternalPath:   collection.CollectionData.InternalPath,
			Addon:          collection.CollectionData.AddOn,
		}] = collection
	}

	return &BaseChainParser{
		apiCollections: collections,
		// First-wins in spec-file order, exactly as getServiceApis builds it: /v2 is
		// listed first in ton.json, so both tags resolve to /v2's directives here.
		taggedApis: map[spectypes.FUNCTION_TAG]TaggedContainer{
			spectypes.FUNCTION_TAG_GET_BLOCKNUM: {
				Parsing:       v2.ParseDirectives[0],
				ApiCollection: v2,
			},
			spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM: {
				Parsing:       v2.ParseDirectives[1],
				ApiCollection: v2,
			},
		},
	}, v2, v3
}

// TestGetParsingByTagForCollection_InternalPathBaseCollection pins that a url on
// an internal path is answered from its OWN path's base collection, not from the
// first collection in the file that happens to declare the tag.
//
// This is the lookup a per-url ChainTracker makes (endpointstate.EndpointPoller
// passes the url's InternalPath). In MAG-4105 the /v3 poller was handed /v2's
// GET_BLOCKNUM, sent GET /api/v3/getMasterchainInfo, got 500/404 from both
// vendors, and never started — so the consistency gate never ran on the chain.
func TestGetParsingByTagForCollection_InternalPathBaseCollection(t *testing.T) {
	parser, v2, v3 := tonShapedParser(t)

	// The unscoped lookup is still first-wins. Pinned because every other caller
	// (craftRelay, resolveTipApiNames, specRequiresHeadOnly) relies on it and this
	// change must not move it.
	parsing, collection, ok := parser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	require.Equal(t, "/getMasterchainInfo", parsing.ApiName)
	require.Same(t, v2, collection)

	for _, tc := range []struct {
		name           string
		tag            spectypes.FUNCTION_TAG
		addons         []string
		internalPath   string
		allowBase      bool
		wantApiName    string
		wantCollection *spectypes.ApiCollection
	}{
		{
			name:           "a /v3 url is polled with /v3's own head directive",
			tag:            spectypes.FUNCTION_TAG_GET_BLOCKNUM,
			internalPath:   "/v3",
			allowBase:      true,
			wantApiName:    "/masterchainInfo",
			wantCollection: v3,
		},
		{
			name:           "a /v2 url is polled with /v2's — indistinguishable from the bug, which is why it went unnoticed",
			tag:            spectypes.FUNCTION_TAG_GET_BLOCKNUM,
			internalPath:   "/v2",
			allowBase:      true,
			wantApiName:    "/getMasterchainInfo",
			wantCollection: v2,
		},
		{
			name:           "the block-hash directive resolves per path the same way",
			tag:            spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM,
			internalPath:   "/v3",
			allowBase:      true,
			wantApiName:    "/blocks",
			wantCollection: v3,
		},
		{
			name:           "a path the spec does not define inherits the first-declared directive",
			tag:            spectypes.FUNCTION_TAG_GET_BLOCKNUM,
			internalPath:   "/nope",
			allowBase:      true,
			wantApiName:    "/getMasterchainInfo",
			wantCollection: v2,
		},
		{
			name:           "the root, which this spec has no collection for, inherits the first-declared directive",
			tag:            spectypes.FUNCTION_TAG_GET_BLOCKNUM,
			internalPath:   "",
			allowBase:      true,
			wantApiName:    "/getMasterchainInfo",
			wantCollection: v2,
		},
		{
			name:         "an add-on on /v3 that declares the tag still wins over /v3's base collection",
			tag:          spectypes.FUNCTION_TAG_GET_BLOCKNUM,
			addons:       []string{"indexer-plus"},
			internalPath: "/v3",
			allowBase:    true,
			wantApiName:  "/plus/masterchainInfo",
		},
		{
			name:           "an add-on on /v3 that declares nothing inherits /v3's base, not /v2's",
			tag:            spectypes.FUNCTION_TAG_GET_BLOCKNUM,
			addons:         []string{"metrics"},
			internalPath:   "/v3",
			allowBase:      true,
			wantApiName:    "/masterchainInfo",
			wantCollection: v3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsing, collection, ok := parser.GetParsingByTagForCollection(tc.tag, tc.addons, tc.internalPath, tc.allowBase)
			require.True(t, ok)
			require.NotNil(t, parsing)
			require.Equal(t, tc.wantApiName, parsing.ApiName)
			if tc.wantCollection != nil {
				// The poller crafts the request from the returned collection's
				// CollectionData, so it has to be the path's own, not /v2's.
				require.Same(t, tc.wantCollection, collection)
			}
		})
	}

	t.Run("a standalone-addons url is not handed its own path's base collection either", func(t *testing.T) {
		// `metrics` declares no head directive. An ordinary url inherits /v3's base
		// (above); a url that serves ONLY its add-ons opted out of every base
		// collection, the one at its own path included, so it must fail loudly.
		_, _, ok := parser.GetParsingByTagForCollection(
			spectypes.FUNCTION_TAG_GET_BLOCKNUM, []string{"metrics"}, "/v3", false)
		require.False(t, ok)
	})

	t.Run("a disabled path collection is not a source of directives", func(t *testing.T) {
		disabledParser, _, disabledV3 := tonShapedParser(t)
		disabledV3.Enabled = false
		parsing, collection, ok := disabledParser.GetParsingByTagForCollection(
			spectypes.FUNCTION_TAG_GET_BLOCKNUM, nil, "/v3", true)
		require.True(t, ok)
		require.Equal(t, "/getMasterchainInfo", parsing.ApiName)
		require.NotSame(t, disabledV3, collection)
	})

	t.Run("a tag no collection declares stays absent", func(t *testing.T) {
		_, _, ok := parser.GetParsingByTagForCollection(
			spectypes.FUNCTION_TAG_GET_EARLIEST_BLOCK, nil, "/v3", true)
		require.False(t, ok)
	})

	t.Run("resolution is deterministic across calls", func(t *testing.T) {
		// apiCollections is a map; the lookup must be keyed, never a scan, or
		// consecutive polls of one url could read the head from different
		// collections and the endpoint would look like its tip flaps.
		for i := 0; i < 200; i++ {
			parsing, _, ok := parser.GetParsingByTagForCollection(
				spectypes.FUNCTION_TAG_GET_BLOCKNUM, nil, "/v3", true)
			require.True(t, ok)
			require.Equal(t, "/masterchainInfo", parsing.ApiName)
		}
	})
}
