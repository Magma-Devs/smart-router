package relaycore

import (
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// Either pin header makes the request pinned; presence is what counts, as in resolvePinDirectives.
func TestPinnedByDirectives(t *testing.T) {
	require.True(t, PinnedByDirectives(map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "upstream-a"}))
	require.True(t, PinnedByDirectives(map[string]string{common.STICKINESS_HEADER_NAME: "ingest-42"}))
	require.False(t, PinnedByDirectives(map[string]string{"lava-force-cache-refresh": "true"}))
	require.False(t, PinnedByDirectives(nil))
}

// lava-pin-allow-retry: true turns a pin back into a first-attempt preference, so the request is
// not pinned for the policy or the consistency gate. Only "true" counts: "false", an empty value
// or anything else leaves the pin hard.
func TestPinAllowsRetry(t *testing.T) {
	for _, pin := range []string{common.SELECT_PROVIDER_HEADER_NAME, common.STICKINESS_HEADER_NAME} {
		for value, allows := range map[string]bool{"true": true, "TRUE": true, " true ": true, "false": false, "": false, "1": false, "yes": false} {
			headers := map[string]string{pin: "x", common.PIN_ALLOW_RETRY_HEADER_NAME: value}
			require.Equal(t, allows, PinAllowsRetry(headers), "%s with %q", pin, value)
			require.Equal(t, !allows, PinnedByDirectives(headers), "%s with %q", pin, value)
		}
	}
	require.False(t, PinAllowsRetry(map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "x"}))
	require.False(t, PinnedByDirectives(map[string]string{common.PIN_ALLOW_RETRY_HEADER_NAME: "true"}), "the header alone pins nothing")
}

// Only the two errors that name the pin are terminal at the send stage; an empty pool or a
// transport failure keeps the ordinary resend path.
func TestPinnedUpstreamUnservable(t *testing.T) {
	require.True(t, PinnedUpstreamUnservable(fmt.Errorf("wrapped: %w", lavasession.SelectedProviderUnavailableError)))
	require.True(t, PinnedUpstreamUnservable(lavasession.SelectedProviderAlreadyFailedError))
	require.False(t, PinnedUpstreamUnservable(lavasession.PairingListEmptyError))
	require.False(t, PinnedUpstreamUnservable(errors.New("dial tcp: connection refused")))
	require.False(t, PinnedUpstreamUnservable(nil))
}
