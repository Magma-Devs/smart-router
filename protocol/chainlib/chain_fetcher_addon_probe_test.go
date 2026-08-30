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
				GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, tc.addons, tc.internalPath).
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
		GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, nil, "").
		Return(nil, nil, false)

	fetcher := &ChainFetcher{
		endpoint:    &lavasession.RPCProviderEndpoint{ChainID: "ACA", ApiInterface: spectypes.APIInterfaceJsonRPC},
		chainParser: parser,
	}

	block, err := fetcher.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.Equal(t, int64(spectypes.NOT_APPLICABLE), block)
}
