package chainlib

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// headDependentVerification is a verification that cannot be evaluated without the
// chain head, which is what makes Validate spend a head probe before verifying.
func headDependentVerification(name string) VerificationContainer {
	return VerificationContainer{
		Name:           name,
		LatestDistance: 100,
		Severity:       spectypes.ParseValue_Fail,
	}
}

// TestValidateProbesHeadWithTheUrlsOwnCollections is the MAG-3296 assertion at the
// level the bug was reported from: admission must ask for the head using the
// directives of the collections THIS node url serves.
//
// It stops at the lookup — the mock reports the tag as absent, so the probe fails
// and Validate returns — because the argument passed to the lookup is the whole
// defect. Before the fix, admission asked GetParsingByTag, which cannot be told
// which node is asking and answers with the base collection for all of them.
func TestValidateProbesHeadWithTheUrlsOwnCollections(t *testing.T) {
	for _, tc := range []struct {
		name         string
		addons       []string
		internalPath string
	}{
		{name: "an evm-only node", addons: []string{"evm"}},
		{name: "a node declaring several addons", addons: []string{"evm", "archive"}},
		{name: "a node on an internal path", addons: []string{"evm"}, internalPath: "/v2"},
		{name: "a plain node with no addons", addons: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			parser := NewMockChainParser(ctrl)
			endpoint := &lavasession.RPCProviderEndpoint{
				ChainID:      "ACA",
				ApiInterface: spectypes.APIInterfaceJsonRPC,
				NodeUrls: []common.NodeUrl{{
					Url:          "https://eth-rpc-acala.aca-api.network",
					Addons:       tc.addons,
					InternalPath: tc.internalPath,
				}},
			}

			parser.EXPECT().
				GetVerifications(tc.addons, tc.internalPath, spectypes.APIInterfaceJsonRPC).
				Return([]VerificationContainer{headDependentVerification("pruning")}, nil)

			// The assertion: scoped to this url's collections, never the unscoped
			// lookup. Returning "not found" ends Validate here, which is all this
			// test needs — the call itself is the subject.
			parser.EXPECT().
				GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, tc.addons, tc.internalPath, true).
				Return(nil, nil, false).
				MinTimes(1)

			fetcher := &ChainFetcher{endpoint: endpoint, chainParser: parser}

			err := fetcher.Validate(context.Background())
			require.Error(t, err, "a node whose head cannot be resolved still fails admission")
		})
	}
}

// TestFetchLatestBlockNumKeepsBaseCollectionSemantics pins the unscoped entry
// point. It is on the chaintracker's fetcher interface and has no node url to
// scope by, so it must keep asking for the base collection's directive — which
// GetParsingByTagForCollection returns for an empty addon set.
func TestFetchLatestBlockNumKeepsBaseCollectionSemantics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parser := NewMockChainParser(ctrl)
	parser.EXPECT().
		GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, nil, "", true).
		Return(nil, nil, false)

	fetcher := &ChainFetcher{
		endpoint:    &lavasession.RPCProviderEndpoint{ChainID: "ACA", ApiInterface: spectypes.APIInterfaceJsonRPC},
		chainParser: parser,
	}

	block, err := fetcher.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.Equal(t, spectypes.NOT_APPLICABLE, block)
}

// acalaVerifications is the set GetVerifications returns for a node declaring
// addons ["evm"] and no archive extension, taken from aca.json (lava-specs #121).
// The base entry is the one that excluded an EVM-only provider: it fires a
// Substrate chain_getBlockHash, and an omitted severity in a spec means Fail.
func acalaVerifications() []VerificationContainer {
	return []VerificationContainer{
		{ // base collection, inherited by every node
			Name:            "chain-id",
			Value:           "0xfc41b9bd8ef8fe53d58c7ea67c794c7ec9a73daf05e6d54b14ff6342c99ba64c",
			VerificationKey: VerificationKey{Addon: ""},
		},
		{ // evm collection
			Name:            "chain-id",
			Value:           "0x313",
			VerificationKey: VerificationKey{Addon: "evm"},
		},
		{ // evm collection, head-dependent
			Name:            "pruning",
			LatestDistance:  7200,
			VerificationKey: VerificationKey{Addon: "evm"},
		},
	}
}

func TestVerificationsForNodeUrl(t *testing.T) {
	t.Run("a standalone-addons url does not answer for the base collection", func(t *testing.T) {
		url := common.NodeUrl{
			Url:              "https://eth-rpc-acala.aca-api.network",
			Addons:           []string{"evm"},
			StandaloneAddons: true,
		}

		kept := verificationsForNodeUrl(url, acalaVerifications())

		require.Len(t, kept, 2)
		for _, verification := range kept {
			require.Equal(t, "evm", verification.Addon)
		}
		// The check that actually validates an Acala EVM node survives. Skipping
		// by name could not do this — both collections name theirs "chain-id",
		// and ShouldSkipVerification matches on the name alone.
		require.Equal(t, "0x313", kept[0].Value)
	})

	t.Run("the default is unchanged for every existing deployment", func(t *testing.T) {
		url := common.NodeUrl{Url: "https://archive.example.com", Addons: []string{"archive"}}
		require.Len(t, verificationsForNodeUrl(url, acalaVerifications()), 3,
			"an add-on that extends the base surface still inherits its verifications")
	})

	t.Run("the flag is ignored on a url declaring no addons", func(t *testing.T) {
		url := common.NodeUrl{Url: "https://plain.example.com", StandaloneAddons: true}
		require.Len(t, verificationsForNodeUrl(url, acalaVerifications()), 3,
			"there would be nothing left to serve")
	})
}

// TestStandaloneAddonsUrlSkipsTheHeadProbeItCannotAnswer pins the ordering: the
// base-collection filter runs before needsLatestBlock, so an inherited
// verification that is not going to run cannot drag a head probe out to the
// upstream on its way past. Same lesson as the skip-verifications filter.
func TestStandaloneAddonsUrlSkipsTheHeadProbeItCannotAnswer(t *testing.T) {
	base := []VerificationContainer{{
		Name:            "pruning",
		LatestDistance:  100,
		VerificationKey: VerificationKey{Addon: ""},
	}}

	standalone := common.NodeUrl{Addons: []string{"evm"}, StandaloneAddons: true}
	require.False(t, needsLatestBlock(standalone, verificationsForNodeUrl(standalone, base)),
		"the only head-dependent verification belongs to a collection this url does not serve")

	inheriting := common.NodeUrl{Addons: []string{"evm"}}
	require.True(t, needsLatestBlock(inheriting, verificationsForNodeUrl(inheriting, base)))
}
