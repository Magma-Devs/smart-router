package rpcsmartrouter

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// A name in a url's addons that the spec does not define is ignored: the url serves the
// rest, and the name is refused as a service with check unsupported-addon, which the
// alarm metric publishes. It used to take the url out (MAG-3950).
func TestValidateProviderCollections_AnUnknownAddonIsIgnored(t *testing.T) {
	up := newFakeUpstreams(t)
	parser := liveParser(t, "ETH1", true)
	validate := func(url commonlib.NodeUrl) (chainlib.ProviderAdmission, error) {
		return validateProviderCollections(t.Context(), staticProvider("ETH1", url), parser, 20*time.Second)
	}

	t.Run("beside a defined name, the url keeps that name", func(t *testing.T) {
		url := commonlib.NodeUrl{Url: up.httpURL("/u/" + upArchive), Addons: []string{"archiv", "archive"}}
		provider := staticProvider("ETH1", url, commonlib.NodeUrl{Url: up.wsURL("/u/" + upArchive)})
		admission, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
		require.NoError(t, err)
		services, keep := admittedServicesFor(parser, admission, url)
		require.True(t, keep)
		require.Equal(t, []string{"archive"}, services)
		require.Equal(t, []chainlib.RefusedService{{Service: "archiv", Check: chainlib.CheckUnsupportedAddon}}, admission.Refused())
		require.Equal(t, []metrics.ProviderVerificationFailure{
			{Scope: metrics.VerificationScopeService, Service: "archiv", Check: chainlib.CheckUnsupportedAddon},
		}, refusalsOf(parser, provider, admission))
	})

	t.Run("alone, the url serves the base collection", func(t *testing.T) {
		url := commonlib.NodeUrl{Url: up.httpURL("/v/" + upArchive), Addons: []string{"archiv"}}
		admission, err := validate(url)
		require.NoError(t, err)
		services, keep := admittedServicesFor(parser, admission, url)
		require.True(t, keep)
		require.Empty(t, services)
	})

	t.Run("beside a defined add-on, a standalone url serves that add-on", func(t *testing.T) {
		url := commonlib.NodeUrl{Url: up.httpURL("/w/" + upDebug), Addons: []string{"debug", "debugg"}, StandaloneAddons: true}
		admission, err := validate(url)
		require.NoError(t, err)
		services, keep := admittedServicesFor(parser, admission, url)
		require.True(t, keep)
		require.Equal(t, []string{"debug"}, services)
	})

	// The unknown name may have been the url's add-on, and a standalone url takes no
	// base traffic, so the url keeps nothing: verification, the serving check and the
	// endpoint builder all refuse it, as one url refusal and no service refusal.
	for name, addons := range map[string][]string{
		"a standalone url with no defined add-on keeps nothing":                {"debugg"},
		"a standalone url with only an extension beside it keeps nothing, too": {"archive", "debugg"},
	} {
		t.Run(name, func(t *testing.T) {
			url := commonlib.NodeUrl{Url: up.httpURL("/y/" + upArchive), Addons: addons, StandaloneAddons: true}
			admission, err := validate(url)
			require.ErrorContains(t, err, "no node url left serving the root path")
			require.True(t, admission.URLRefused(0))
			require.Equal(t, []string{chainlib.CheckUnsupportedAddon}, admission.RefusedURLChecks())
			require.Empty(t, admission.Refused(), "published once, as the url's refusal")
			_, keep := admittedServicesFor(parser, chainlib.ProviderAdmission{}, url)
			require.False(t, keep, "the builder drops it even where no verification ran")
		})
	}
}

// health shows an ignored name as a refused service of an admitted url, and says why.
// The row itself fails, naming it, so the pre-deploy check still catches the typo.
func TestHealth_AnUnknownAddonIsIgnored(t *testing.T) {
	up := newFakeUpstreams(t)
	rows := healthProbe(t, "ETH1", true, 20*time.Second, commonlib.NodeUrl{Url: up.httpURL("/h/" + upArchive), Addons: []string{"archiv", "archive"}})
	require.Len(t, rows, 1)
	require.Equal(t, routerAdmitted, rows[0].Router.Verdict)
	require.Equal(t, []string{"archiv"}, rows[0].Router.RefusedServices)
	require.Equal(t, "not defined by this spec, ignored: archiv; "+noWebsocketReason, rows[0].Router.Reason)
	require.False(t, rows[0].Ok, "a typo still fails the row, as it did when the router refused the url")
	require.Equal(t, "addons the spec does not define: archiv", rows[0].Error)

	rows = healthProbe(t, "ETH1", true, 20*time.Second, commonlib.NodeUrl{Url: up.httpURL("/h/" + upArchive), Addons: []string{"debugg"}, StandaloneAddons: true})
	require.Len(t, rows, 1)
	require.Equal(t, routerExcluded, rows[0].Router.Verdict)
	require.Contains(t, rows[0].Error, "declares no add-on this spec defines (debugg")
}

// The subscription probe reads a url's collections from the add-ons the spec defines,
// as admission does, instead of reading every path as an http collection.
func TestSubscriptionCollectionProbe_IgnoresAnUnknownAddon(t *testing.T) {
	ctrl := gomock.NewController(t)
	parser := chainlib.NewMockChainParser(ctrl)
	parser.EXPECT().SeparateAddonsExtensions(gomock.Any(), []string{"debugg", "debug"}).
		Return([]string{"debug"}, nil, errors.New("debugg is neither an addon nor an extension")).AnyTimes()
	parser.EXPECT().IsTagInCollection(spectypes.FUNCTION_TAG_SUBSCRIBE, gomock.Any()).
		DoAndReturn(func(_ spectypes.FUNCTION_TAG, key chainlib.CollectionKey) bool { return key.Addon == "debug" }).AnyTimes()

	require.True(t, subscriptionCollectionProbe(parser)("/ws", []string{"debugg", "debug"}))
}
