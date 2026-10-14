package chainlib

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// rootServedByVersionedPathCollections is STRK's shape, already expanded: a
// root base collection (with a subscription api), a root add-on collection
// declared after it, a versioned path answering v1Apis, and a path answering
// apis of its own.
func rootServedByVersionedPathCollections(apiInterface string, v1Apis []string) []*spectypes.ApiCollection {
	apis := func(names ...string) []*spectypes.Api {
		out := make([]*spectypes.Api, 0, len(names))
		for _, name := range names {
			out = append(out, &spectypes.Api{Name: name, Enabled: true})
		}
		return out
	}
	collection := func(path, addon string, names ...string) *spectypes.ApiCollection {
		return &spectypes.ApiCollection{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: apiInterface, InternalPath: path, Type: "POST", AddOn: addon},
			Apis:           apis(names...),
		}
	}
	root := collection("", "", "a", "b", "sub")
	root.ParseDirectives = []*spectypes.ParseDirective{{FunctionTag: spectypes.FUNCTION_TAG_SUBSCRIBE, ApiName: "sub"}}
	return []*spectypes.ApiCollection{
		root,
		collection("", "trace", "t"),
		collection("/V1", "", v1Apis...),
		collection("/P", "", "p"),
	}
}

func parserFor(t *testing.T, apiInterface string, collections []*spectypes.ApiCollection) ChainParser {
	t.Helper()
	chainParser, err := NewChainParser(apiInterface)
	require.NoError(t, err)
	spec := CreateMockSpec()
	spec.ApiCollections = collections
	chainParser.SetSpec(spec)
	return chainParser
}

func TestInternalPathServesRootCollection(t *testing.T) {
	for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC} {
		t.Run(apiInterface, func(t *testing.T) {
			chainParser := parserFor(t, apiInterface, rootServedByVersionedPathCollections(apiInterface, []string{"a", "b"}))

			require.True(t, chainParser.IsInternalPathEnabled("", apiInterface, ""),
				"the root base collection owns the root entry though an add-on collection follows it")
			require.True(t, chainParser.ServesRootCollection("/V1"), "/V1 answers every root api but the subscription one")
			require.False(t, chainParser.ServesRootCollection("/P"), "/P answers none of the root apis")
			require.False(t, chainParser.ServesRootCollection(""), "the root does not stand in for itself")
			require.False(t, chainParser.ServesRootCollection("/missing"))

			short := parserFor(t, apiInterface, rootServedByVersionedPathCollections(apiInterface, []string{"a"}))
			require.False(t, short.ServesRootCollection("/V1"), "/V1 lacks root api b")
		})
	}
}

func TestInternalPathRootWithNoApisIsServedByNothing(t *testing.T) {
	apiInterface := spectypes.APIInterfaceJsonRPC
	collections := []*spectypes.ApiCollection{
		{Enabled: true, CollectionData: spectypes.CollectionData{ApiInterface: apiInterface, InternalPath: "", Type: "POST"}},
		{Enabled: true, CollectionData: spectypes.CollectionData{ApiInterface: apiInterface, InternalPath: "/X", Type: "POST"},
			Apis: []*spectypes.Api{{Name: "x", Enabled: true}}},
	}
	require.False(t, parserFor(t, apiInterface, collections).ServesRootCollection("/X"))
}

// The real ETH1 spec declares its root debug, bundler and trace collections
// after the base one; the root entry still describes the base collection.
func TestRealSpecRootEntryIsTheBaseCollection(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "specs", "ethereum.json"))
	require.NoError(t, err, "specs/ethereum.json ships in this repo")

	var proposal struct {
		Proposal struct {
			Specs []spectypes.Spec `json:"specs"`
		} `json:"proposal"`
	}
	require.NoError(t, json.Unmarshal(raw, &proposal))

	var eth1 spectypes.Spec
	for _, spec := range proposal.Proposal.Specs {
		if spec.Index == "ETH1" {
			eth1 = spec
		}
	}
	require.Equal(t, "ETH1", eth1.Index)

	internalPaths, _, _, _, _, _ := getServiceApis(eth1, spectypes.APIInterfaceJsonRPC)
	require.Equal(t, "", internalPaths[""].Addon)
	require.True(t, internalPaths[""].Enabled)
}
