package relaycore

import (
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// Which requests are strict pins (MAG-5711): router-stickiness always, whatever else is sent;
// router-select-provider unless router-select-provider-allow-retry: true comes with it. The
// allow-retry header pins nothing alone and never loosens a sticky request.
func TestPinnedByDirectives(t *testing.T) {
	const (
		sel        = common.SELECT_PROVIDER_HEADER_NAME
		sticky     = common.STICKINESS_HEADER_NAME
		allowRetry = common.SELECT_PROVIDER_ALLOW_RETRY_HEADER_NAME
	)
	for name, tc := range map[string]struct {
		headers map[string]string
		pinned  bool
	}{
		"sticky alone is strict":                                  {map[string]string{sticky: "ingest-42"}, true},
		"plain select-provider is strict":                         {map[string]string{sel: "upstream-a"}, true},
		"select-provider with allow-retry true keeps failover":    {map[string]string{sel: "upstream-a", allowRetry: "true"}, false},
		"allow-retry false leaves select-provider strict":         {map[string]string{sel: "upstream-a", allowRetry: "false"}, true},
		"allow-retry alone pins nothing":                          {map[string]string{allowRetry: "true"}, false},
		"sticky with select-provider is strict":                   {map[string]string{sticky: "ingest-42", sel: "upstream-a"}, true},
		"allow-retry does not loosen sticky":                      {map[string]string{sticky: "ingest-42", allowRetry: "true"}, true},
		"allow-retry does not loosen sticky with select-provider": {map[string]string{sticky: "ingest-42", sel: "upstream-a", allowRetry: "true"}, true},
		"unpinned":   {map[string]string{"router-force-cache-refresh": "true"}, false},
		"no headers": {nil, false},
		// An empty value names no upstream. Selection ignores it, so the request keeps its failover.
		"an empty select-provider is not a pin": {map[string]string{sel: ""}, false},
		// Two headers that no longer exist. Neither is read, so neither changes the answer.
		"the removed router-select-provider-strict false does not loosen select-provider": {map[string]string{sel: "upstream-a", "router-select-provider-strict": "false"}, true},
		"the removed router-select-provider-strict alone pins nothing":                    {map[string]string{"router-select-provider-strict": "true"}, false},
		"the removed router-pin-allow-retry does not loosen select-provider":              {map[string]string{sel: "upstream-a", "router-pin-allow-retry": "true"}, true},
		"the removed router-pin-allow-retry does not loosen sticky":                       {map[string]string{sticky: "ingest-42", "router-pin-allow-retry": "true"}, true},
	} {
		require.Equal(t, tc.pinned, PinnedByDirectives(tc.headers), name)
	}
}

// Only the value "true" (any case, surrounding space ignored) allows a retry on another upstream.
// Anything else, "false" and an empty value included, leaves router-select-provider a strict pin.
func TestSelectProviderAllowsRetry(t *testing.T) {
	for _, tc := range []struct {
		value  string
		allows bool
	}{{"true", true}, {"TRUE", true}, {" True ", true}, {"false", false}, {"", false}, {"1", false}, {"yes", false}} {
		headers := map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "x", common.SELECT_PROVIDER_ALLOW_RETRY_HEADER_NAME: tc.value}
		require.Equal(t, tc.allows, SelectProviderAllowsRetry(headers), "%q", tc.value)
		require.Equal(t, !tc.allows, PinnedByDirectives(headers), "%q", tc.value)
	}
	require.False(t, SelectProviderAllowsRetry(map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "x"}), "absent does not allow a retry")
}

// Only the two errors that name the pin are terminal at the send stage; an empty pool or a
// transport failure keeps the ordinary resend path.
func TestPinnedUpstreamUnservable(t *testing.T) {
	require.True(t, PinnedUpstreamUnservable(fmt.Errorf("wrapped: %w", routersession.SelectedProviderUnavailableError)))
	require.True(t, PinnedUpstreamUnservable(routersession.SelectedProviderAlreadyFailedError))
	require.False(t, PinnedUpstreamUnservable(routersession.PairingListEmptyError))
	require.False(t, PinnedUpstreamUnservable(errors.New("dial tcp: connection refused")))
	require.False(t, PinnedUpstreamUnservable(nil))
}
