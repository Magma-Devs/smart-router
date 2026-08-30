package chainlib

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3296. A spec whose add-on is a DISJOINT api surface rather than a superset
// of the base one: Acala serves Substrate in the base collection and EVM in an
// `evm` add-on, and the two have no method in common. `txpool` is the ordinary
// shape for contrast — a real add-on (ASTAR has one) that declares no tagged
// directives of its own and is meant to inherit the base collection's.
func acalaShapedParser(t *testing.T) (parser *BaseChainParser, base, evm *spectypes.ApiCollection) {
	t.Helper()

	collectionData := func(addon, internalPath string) spectypes.CollectionData {
		return spectypes.CollectionData{
			ApiInterface: spectypes.APIInterfaceJsonRPC,
			InternalPath: internalPath,
			Type:         "POST",
			AddOn:        addon,
		}
	}

	base = &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("", ""),
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "chain_getHeader"},
		},
	}
	evm = &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("evm", ""),
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "eth_blockNumber"},
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM, ApiName: "eth_getBlockByNumber"},
		},
	}
	txpool := &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("txpool", ""),
	}
	disabled := &spectypes.ApiCollection{
		Enabled:        false,
		CollectionData: collectionData("disabled", ""),
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "never_reached"},
		},
	}
	otherPath := &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: collectionData("evm", "/v2"),
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "v2_blockNumber"},
		},
	}

	collections := map[CollectionKey]*spectypes.ApiCollection{}
	for _, collection := range []*spectypes.ApiCollection{base, evm, txpool, disabled, otherPath} {
		collections[CollectionKey{
			ConnectionType: collection.CollectionData.Type,
			InternalPath:   collection.CollectionData.InternalPath,
			Addon:          collection.CollectionData.AddOn,
		}] = collection
	}

	return &BaseChainParser{
		apiCollections: collections,
		// taggedApis holds one entry per tag — the first collection to declare it.
		// For a spec written base-first that is the base collection, which is the
		// whole of the bug this resolver exists to route around.
		taggedApis: map[spectypes.FUNCTION_TAG]TaggedContainer{
			spectypes.FUNCTION_TAG_GET_BLOCKNUM: {
				Parsing:       base.ParseDirectives[0],
				ApiCollection: base,
			},
		},
	}, base, evm
}

func TestGetParsingByTagForCollection(t *testing.T) {
	parser, base, evm := acalaShapedParser(t)

	// The unscoped lookup answers "base" for everyone. Pinned because it is the
	// behaviour every other caller still relies on, and the reason the scoped
	// lookup had to be added rather than the old one changed.
	parsing, collection, ok := parser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	require.Equal(t, "chain_getHeader", parsing.ApiName)
	require.Same(t, base, collection)

	for _, tc := range []struct {
		name         string
		addons       []string
		internalPath string
		wantApiName  string
		wantExisted  bool
	}{
		{
			name:        "no addons falls back to the base collection",
			addons:      nil,
			wantApiName: "chain_getHeader",
			wantExisted: true,
		},
		{
			name:        "an evm-only node is probed with the evm collection's directive",
			addons:      []string{"evm"},
			wantApiName: "eth_blockNumber",
			wantExisted: true,
		},
		{
			name:        "an addon that declares no directive inherits the base one",
			addons:      []string{"txpool"},
			wantApiName: "chain_getHeader",
			wantExisted: true,
		},
		{
			name:        "an addon with no collection at all inherits the base one",
			addons:      []string{"archive"},
			wantApiName: "chain_getHeader",
			wantExisted: true,
		},
		{
			name:        "an extension name mixed in with the addons is skipped, not matched",
			addons:      []string{"debug", "trace", "evm"},
			wantApiName: "eth_blockNumber",
			wantExisted: true,
		},
		{
			name:        "a disabled collection is not a source of directives",
			addons:      []string{"disabled"},
			wantApiName: "chain_getHeader",
			wantExisted: true,
		},
		{
			name:        "the empty addon means the base collection and is not re-searched",
			addons:      []string{""},
			wantApiName: "chain_getHeader",
			wantExisted: true,
		},
		{
			name:         "an internal path scopes the lookup to that path's collection",
			addons:       []string{"evm"},
			internalPath: "/v2",
			wantApiName:  "v2_blockNumber",
			wantExisted:  true,
		},
		{
			name:         "an addon on an internal path the spec does not define inherits the base one",
			addons:       []string{"evm"},
			internalPath: "/nope",
			wantApiName:  "chain_getHeader",
			wantExisted:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsing, _, existed := parser.GetParsingByTagForCollection(
				spectypes.FUNCTION_TAG_GET_BLOCKNUM, tc.addons, tc.internalPath)
			require.Equal(t, tc.wantExisted, existed)
			require.NotNil(t, parsing)
			require.Equal(t, tc.wantApiName, parsing.ApiName)
		})
	}

	t.Run("the resolved collection is returned alongside the directive", func(t *testing.T) {
		_, collection, ok := parser.GetParsingByTagForCollection(
			spectypes.FUNCTION_TAG_GET_BLOCKNUM, []string{"evm"}, "")
		require.True(t, ok)
		require.Same(t, evm, collection,
			"the caller crafts the probe from CollectionData, so it must be the add-on's")
	})

	t.Run("a tag no collection declares stays absent", func(t *testing.T) {
		// GET_BLOCK_BY_NUM is declared by the evm collection but has no taggedApis
		// entry, so there is no base answer to fall back to. Reporting it as found
		// would hand callers a directive the base surface cannot serve.
		_, _, existed := parser.GetParsingByTagForCollection(
			spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM, []string{"evm"}, "")
		require.False(t, existed)
	})

	t.Run("resolution is deterministic across calls", func(t *testing.T) {
		// apiCollections is a map. A scan of it would iterate randomly and could
		// return a different collection per call, which reads downstream as a node
		// whose tip source flaps.
		for i := 0; i < 200; i++ {
			parsing, _, ok := parser.GetParsingByTagForCollection(
				spectypes.FUNCTION_TAG_GET_BLOCKNUM, []string{"txpool", "evm"}, "")
			require.True(t, ok)
			require.Equal(t, "eth_blockNumber", parsing.ApiName)
		}
	})
}

// TestGetParsingByTagForCollection_AddonOrderIsTheNodesOwn asserts the node's
// declaration order decides between two add-on collections that both carry the
// tag, rather than map order deciding for it.
func TestGetParsingByTagForCollection_AddonOrderIsTheNodesOwn(t *testing.T) {
	parser, _, _ := acalaShapedParser(t)

	second := &spectypes.ApiCollection{
		Enabled: true,
		CollectionData: spectypes.CollectionData{
			ApiInterface: spectypes.APIInterfaceJsonRPC,
			Type:         "POST",
			AddOn:        "other",
		},
		ParseDirectives: []*spectypes.ParseDirective{
			{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "other_blockNumber"},
		},
	}
	parser.apiCollections[CollectionKey{ConnectionType: "POST", Addon: "other"}] = second

	parsing, _, ok := parser.GetParsingByTagForCollection(
		spectypes.FUNCTION_TAG_GET_BLOCKNUM, []string{"evm", "other"}, "")
	require.True(t, ok)
	require.Equal(t, "eth_blockNumber", parsing.ApiName)

	parsing, _, ok = parser.GetParsingByTagForCollection(
		spectypes.FUNCTION_TAG_GET_BLOCKNUM, []string{"other", "evm"}, "")
	require.True(t, ok)
	require.Equal(t, "other_blockNumber", parsing.ApiName)
}
