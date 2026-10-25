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

// Only the two errors that name the pin are terminal at the send stage; an empty pool or a
// transport failure keeps the ordinary resend path.
func TestPinnedUpstreamUnservable(t *testing.T) {
	require.True(t, PinnedUpstreamUnservable(fmt.Errorf("wrapped: %w", lavasession.SelectedProviderUnavailableError)))
	require.True(t, PinnedUpstreamUnservable(lavasession.SelectedProviderAlreadyFailedError))
	require.False(t, PinnedUpstreamUnservable(lavasession.PairingListEmptyError))
	require.False(t, PinnedUpstreamUnservable(errors.New("dial tcp: connection refused")))
	require.False(t, PinnedUpstreamUnservable(nil))
}
