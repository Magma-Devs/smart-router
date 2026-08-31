package chainlib

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// failingVerification is a Fail-severity verification for one collection. Fail is
// the zero value of the severity enum, so a spec that omits the field gets it.
func failingVerification(name, addon string) VerificationContainer {
	return VerificationContainer{
		Name:            name,
		Value:           "0xdead",
		Severity:        spectypes.ParseValue_Fail,
		VerificationKey: VerificationKey{Addon: addon},
		ParseDirective:  spectypes.ParseDirective{FunctionTag: spectypes.FUNCTION_TAG_VERIFICATION, ApiName: name},
	}
}

// newValidationFetcher wires a fetcher whose every verification relay fails, so
// which verifications are ATTEMPTED and how their failures are attributed is the
// only thing under test.
func newValidationFetcher(t *testing.T, url common.NodeUrl, verifications []VerificationContainer) *ChainFetcher {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	parser := NewMockChainParser(ctrl)
	parser.EXPECT().GetVerifications(gomock.Any(), gomock.Any(), gomock.Any()).Return(verifications, nil).AnyTimes()
	parser.EXPECT().CraftMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("upstream unreachable")).AnyTimes()

	router := NewMockChainRouter(ctrl)
	router.EXPECT().SendNodeMsg(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, common.NodeUrl{}, "", errors.New("upstream unreachable")).AnyTimes()

	return &ChainFetcher{
		endpoint: &lavasession.RPCProviderEndpoint{
			ChainID:      "ACA",
			ApiInterface: spectypes.APIInterfaceJsonRPC,
			NodeUrls:     []common.NodeUrl{url},
		},
		chainParser: parser,
		chainRouter: router,
	}
}

// TestValidateCollections_AddonFailureKeepsTheProvider is the MAG-3326 case that
// affects deployments having nothing to do with add-on splits: attach an archive
// url to an otherwise healthy provider, have it fail the archive verification,
// and today you lose the whole provider.
func TestValidateCollections_AddonFailureKeepsTheProvider(t *testing.T) {
	url := common.NodeUrl{Url: "https://a.example.com", Addons: []string{"archive"}}
	verifications := []VerificationContainer{failingVerification("pruning", "archive")}

	t.Run("per-collection refuses only the addon", func(t *testing.T) {
		fetcher := newValidationFetcher(t, url, verifications)
		admission, err := fetcher.ValidateCollections(context.Background())
		require.NoError(t, err, "the provider survives a failure that belongs to one add-on")
		require.True(t, admission.Any())
		require.Empty(t, admission.Apply([]common.NodeUrl{url})[0].Addons,
			"the url keeps serving, minus the add-on it could not answer for")
	})

	t.Run("the all-or-nothing path is unchanged", func(t *testing.T) {
		// Validate still excludes the whole provider — applyReverification depends
		// on that until the epoch path grows per-add-on hysteresis.
		fetcher := newValidationFetcher(t, url, verifications)
		require.Error(t, fetcher.Validate(context.Background()))
	})
}

// TestValidateCollections_BaseFailureIsStillFatal pins the deliberate limit: a
// failure with no add-on to attribute it to excludes the provider, and does NOT
// quietly turn the url into a standalone-addons one. Only an operator can tell
// "serves only EVM by design" from "its Substrate side is down right now".
func TestValidateCollections_BaseFailureIsStillFatal(t *testing.T) {
	url := common.NodeUrl{Url: "https://a.example.com", Addons: []string{"archive"}}
	verifications := []VerificationContainer{
		failingVerification("chain-id", ""),
		failingVerification("pruning", "archive"),
	}

	fetcher := newValidationFetcher(t, url, verifications)
	_, err := fetcher.ValidateCollections(context.Background())
	require.Error(t, err, "a node that cannot serve the base collection has nothing narrower to fall back to")
}

func TestValidateCollections_CleanProviderRefusesNothing(t *testing.T) {
	url := common.NodeUrl{Url: "https://a.example.com", Addons: []string{"archive"}}
	// Skipped verifications never run, so nothing can be refused.
	url.SkipVerifications = []string{common.SkipVerificationsWildcard}

	fetcher := newValidationFetcher(t, url, []VerificationContainer{failingVerification("pruning", "archive")})
	admission, err := fetcher.ValidateCollections(context.Background())
	require.NoError(t, err)
	require.False(t, admission.Any())
}
