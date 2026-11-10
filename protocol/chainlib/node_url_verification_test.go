package chainlib

import (
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

var errNotServed = errors.New("chain-id verification failed: 401")

func throttledErr() error {
	return fmt.Errorf("chain-id verification failed: %w", common.StatusCodeError429)
}

// A provider has nothing to serve from when no path it declares has a url left. A path
// whose urls all failed outright is a failure, whatever else is throttled; only when
// every empty path is merely throttled is the answer the rate limit.
func TestServingError(t *testing.T) {
	urls := []common.NodeUrl{
		{Url: "https://a/v2", InternalPath: "/v2"},
		{Url: "https://a/v3", InternalPath: "/v3"},
		{Url: "wss://a/ws"},
	}

	var allFine ProviderAdmission
	require.NoError(t, allFine.servingError(urls[:2]))

	var failed ProviderAdmission
	failed.throttleURL(0, common.NodeUrl{}, throttledErr())
	failed.refuseURL(1, errNotServed)
	err := failed.servingError(urls)
	require.ErrorIs(t, err, errNotServed, "a failed path outranks a throttled one in either order")
	require.False(t, IsRateLimitFailure(err))

	var throttled ProviderAdmission
	throttled.throttleURL(0, common.NodeUrl{}, throttledErr())
	require.True(t, IsRateLimitFailure(throttled.servingError(urls[:1])), "nothing but a throttle: inconclusive")

	var socketsOnly ProviderAdmission
	require.Error(t, socketsOnly.servingError(urls[2:]), "a socket serves no path")
}

// A failed path costs that path, not the provider, while another path is served
// (MAG-4214): TON's /v2 url failing chain-id must not take the healthy /v3 url with it.
func TestServingError_AFailedPathCostsOnlyThatPath(t *testing.T) {
	urls := []common.NodeUrl{
		{Url: "https://a/v2", InternalPath: "/v2"},
		{Url: "https://a/v3", InternalPath: "/v3"},
		{Url: "https://a/v4", InternalPath: "/v4"},
	}

	var v2Refused ProviderAdmission
	v2Refused.refuseURL(0, errNotServed)
	require.NoError(t, v2Refused.servingError(urls[:2]), "/v3 is still served")

	var noneServed ProviderAdmission
	noneServed.refuseURL(0, errNotServed)
	noneServed.throttleURL(1, throttledErr())
	err := noneServed.servingError(urls[:2])
	require.ErrorIs(t, err, errNotServed, "no path served: the failure, not the throttle")
	require.False(t, IsRateLimitFailure(err))

	var servedAndUnknown ProviderAdmission
	servedAndUnknown.refuseURL(0, errNotServed)
	servedAndUnknown.throttleURL(2, throttledErr())
	require.True(t, IsRateLimitFailure(servedAndUnknown.servingError(urls)),
		"a lost path is no failure while /v3 is served; the throttled /v4 leaves the pass inconclusive, as before")
}

// RefusesURL finds a url by value in the list the pass verified, so a caller holding
// the configured list and one holding the generated per-path list agree (MAG-4214).
func TestProviderAdmission_RefusesURL(t *testing.T) {
	base := common.NodeUrl{Url: "https://a"}
	v2 := common.NodeUrl{Url: "https://a/v2", InternalPath: "/v2"}
	v3 := common.NodeUrl{Url: "https://a/v3", InternalPath: "/v3"}
	admission, err := admissionFrom([]nodeUrlOutcome{
		{url: base, served: base},
		{url: v2, served: v2, err: errNotServed},
		{url: v3, served: v3},
	})
	require.NoError(t, err)

	require.True(t, admission.RefusesURL(v2))
	require.False(t, admission.RefusesURL(v3))
	require.False(t, admission.RefusesURL(base))
	require.False(t, admission.RefusesURL(common.NodeUrl{Url: "https://a/v2"}), "same url, other path: another endpoint")
	require.False(t, admission.RefusesURL(common.NodeUrl{Url: "https://a/v2", InternalPath: "/v2", Addons: []string{"archive"}}),
		"declared add-ons are part of what was verified")
	require.False(t, ProviderAdmission{}.RefusesURL(v2), "no pass, nothing refused")

	require.False(t, admission.WithoutURLRefusals([]int{1}).RefusesURL(v2), "the url grace admits it again")
	require.True(t, admission.KeepingThrottledFrom(ProviderAdmission{}).RefusesURL(v2), "a copy keeps the verified list")

	keyed := func(key string) common.NodeUrl {
		url := v2
		url.AuthConfig.AuthHeaders = map[string]string{"X-Api-Key": key}
		return url
	}
	revoked, good := keyed("revoked"), keyed("good")
	twoKeys, err := admissionFrom([]nodeUrlOutcome{
		{url: revoked, served: revoked, err: errNotServed},
		{url: good, served: good},
	})
	require.NoError(t, err)
	require.True(t, twoKeys.RefusesURL(revoked))
	require.False(t, twoKeys.RefusesURL(good), "same url and path, other key: verified apart (MAG-4214 review)")
}

// A path that declares a url serving the base collection needs one of those left: a standalone-
// addons url carries no base traffic. A provider whose urls are all standalone serves its add-ons
// as long as one of them keeps something (MAG-4231).
func TestServingError_ABasePathNeedsABaseUrl(t *testing.T) {
	base := common.NodeUrl{Url: "https://a"}
	debugOnly := common.NodeUrl{Url: "https://b", Addons: []string{"debug"}, StandaloneAddons: true}

	var none ProviderAdmission
	require.NoError(t, none.servingError([]common.NodeUrl{base, debugOnly}))
	require.NoError(t, none.servingError([]common.NodeUrl{debugOnly}), "an add-on-only provider serves its add-ons")

	var baseRefused ProviderAdmission
	baseRefused.refuseURL(0, errNotServed)
	require.ErrorIs(t, baseRefused.servingError([]common.NodeUrl{base, debugOnly}), errNotServed, "the standalone url carries no base traffic")

	var debugRefused ProviderAdmission
	debugRefused.fail(debugOnly, "debug", "enabled")
	require.ErrorIs(t, debugRefused.servingError([]common.NodeUrl{debugOnly}), errKeepsNothing, "a standalone url that keeps nothing serves nothing")
	require.NoError(t, debugRefused.servingError([]common.NodeUrl{base, debugOnly}), "the base url still serves the path")
}

// A rate limit settles nothing, so the verdict the last pass gave stands for it.
func TestKeepingThrottledFrom(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"archive", "trace"}}

	var previous ProviderAdmission
	previous.refuseURL(1, errNotServed)
	previous.fail(url, "trace", "trace")

	var now ProviderAdmission
	now.throttleURL(1, common.NodeUrl{}, throttledErr())
	now.throttleURL(2, common.NodeUrl{}, throttledErr())
	now.throttleService(url, "trace", throttledErr())
	now.throttleService(url, "archive", throttledErr())
	now.refuseURL(3, errNotServed)

	kept := now.KeepingThrottledFrom(previous)
	require.True(t, kept.URLRefused(1), "refused before, only throttled now: still refused")
	require.False(t, kept.URLRefused(2), "admitted before, only throttled now: still admitted")
	require.True(t, kept.URLRefused(3), "a refusal of this pass stands")
	services, keep := kept.AdmittedServices(url)
	require.True(t, keep)
	require.Equal(t, []string{"archive"}, services)
	require.NoError(t, kept.Throttled(), "what was kept is settled")

	kept.refuseURL(2, errNotServed)
	require.False(t, now.URLRefused(2), "the copy shares no map with its source")
}

// A url this pass could not ask — held off, or rate-limited at the url level —
// re-checked none of its services, so the services it was refused stay refused.
// Another url's refusals are not carried over by it.
func TestKeepingThrottledFrom_AURLNotCheckedKeepsItsServiceRefusals(t *testing.T) {
	full := common.NodeUrl{Url: "https://full"}
	pruned := common.NodeUrl{Url: "https://pruned", Addons: []string{"archive"}}
	other := common.NodeUrl{Url: "https://other", Addons: []string{"archive"}}

	var previous ProviderAdmission
	previous.fail(pruned, "archive", "archive")
	previous.fail(other, "archive", "archive")

	var now ProviderAdmission
	now.throttleURL(1, pruned, ErrNotAskedHeldOff)

	kept := now.KeepingThrottledFrom(previous)
	services, keep := kept.AdmittedServices(pruned)
	require.True(t, keep, "the url still serves its base collection")
	require.Empty(t, services, "archive stays refused: nothing re-checked it")
	services, _ = kept.AdmittedServices(other)
	require.Equal(t, []string{"archive"}, services, "a url this pass did check settles its own services")
	services, _ = kept.AdmittedServices(full)
	require.Empty(t, services)
}

func TestWithoutURLRefusals(t *testing.T) {
	var admission ProviderAdmission
	admission.refuseURL(0, errNotServed)
	admission.refuseURL(1, errNotServed)
	graced := admission.WithoutURLRefusals([]int{1})
	require.Equal(t, []int{0}, graced.RefusedURLs())
	require.Equal(t, []int{0, 1}, admission.RefusedURLs(), "the source is untouched")
}

func TestEqualIgnoresWhatWasOnlyThrottled(t *testing.T) {
	var a, b ProviderAdmission
	a.refuseURL(0, errNotServed)
	b.refuseURL(0, errNotServed)
	b.throttleURL(1, common.NodeUrl{}, throttledErr())
	require.True(t, a.Equal(b))
	b.refuseURL(2, errNotServed)
	require.False(t, a.Equal(b))
}

// Entries share a connection when they differ only in the services they declare.
func TestConnectionGroups(t *testing.T) {
	keyed := common.AuthConfig{AuthHeaders: map[string]string{"x-api-key": "k"}}
	urls := []common.NodeUrl{
		{Url: "https://a"},
		{Url: "https://a", Addons: []string{"archive"}, SkipVerifications: []string{"pruning"}},
		{Url: "https://a", AuthConfig: keyed},
		{Url: "wss://a"},
		{Url: "https://a", Addons: []string{"debug"}, StandaloneAddons: true},
		{Url: "https://a", InternalPath: "/v3"},
	}
	require.Equal(t, [][]int{{0, 1, 4}, {2}, {3}, {5}}, connectionGroups(urls))
}

// A report reads a url's height only where the spec asks one at the url's own path.
func TestReadsOwnHead(t *testing.T) {
	ctrl := gomock.NewController(t)
	parser := NewMockChainParser(ctrl)
	parser.EXPECT().SkipWebsocketVerification().Return(false).AnyTimes()
	atPath := func(path string) *spectypes.ApiCollection {
		return &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{InternalPath: path}}
	}
	root := common.NodeUrl{Url: "https://a"}
	parser.EXPECT().GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, root.Addons, "", true).
		Return(&spectypes.ParseDirective{}, atPath("/C/rpc"), true)
	require.False(t, readsOwnHead(parser, root, false), "the root url of an internal-path spec")

	versioned := common.NodeUrl{Url: "https://a/C/rpc", InternalPath: "/C/rpc"}
	parser.EXPECT().GetParsingByTagForCollection(spectypes.FUNCTION_TAG_GET_BLOCKNUM, versioned.Addons, "/C/rpc", true).
		Return(&spectypes.ParseDirective{}, atPath("/C/rpc"), true)
	require.True(t, readsOwnHead(parser, versioned, false))
}
