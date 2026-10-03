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

// A provider has nothing to serve from when a path it declares has no url left. A path
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
	failed.throttleURL(0, throttledErr())
	failed.refuseURL(1, errNotServed)
	err := failed.servingError(urls)
	require.ErrorIs(t, err, errNotServed, "a failed path outranks a throttled one in either order")
	require.False(t, IsRateLimitFailure(err))

	var throttled ProviderAdmission
	throttled.throttleURL(0, throttledErr())
	require.True(t, IsRateLimitFailure(throttled.servingError(urls[:1])), "nothing but a throttle: inconclusive")

	var socketsOnly ProviderAdmission
	require.Error(t, socketsOnly.servingError(urls[2:]), "a socket serves no path")
}

// A rate limit settles nothing, so the verdict the last pass gave stands for it.
func TestKeepingThrottledFrom(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"archive", "trace"}}

	var previous ProviderAdmission
	previous.refuseURL(1, errNotServed)
	previous.fail(url, "trace")

	var now ProviderAdmission
	now.throttleURL(1, throttledErr())
	now.throttleURL(2, throttledErr())
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
	b.throttleURL(1, throttledErr())
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
