package relaycore

import (
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// Which requests are strict pins (MAG-4826): lava-stickiness always (D1), whatever else is sent
// (D4); lava-select-provider only with lava-select-provider-strict: true (D3); a plain
// lava-select-provider keeps its failover and is not pinned in this sense (D2).
func TestPinnedByDirectives(t *testing.T) {
	const (
		sel    = common.SELECT_PROVIDER_HEADER_NAME
		sticky = common.STICKINESS_HEADER_NAME
		strict = common.SELECT_PROVIDER_STRICT_HEADER_NAME
	)
	for name, tc := range map[string]struct {
		headers map[string]string
		pinned  bool
	}{
		"D1 sticky alone is strict":                     {map[string]string{sticky: "ingest-42"}, true},
		"D2 plain select-provider keeps failover":       {map[string]string{sel: "upstream-a"}, false},
		"D3 select-provider with strict true is strict": {map[string]string{sel: "upstream-a", strict: "true"}, true},
		"D3 strict false leaves select-provider loose":  {map[string]string{sel: "upstream-a", strict: "false"}, false},
		"D3 strict alone pins nothing":                  {map[string]string{strict: "true"}, false},
		"D4 sticky with select-provider is strict":      {map[string]string{sticky: "ingest-42", sel: "upstream-a"}, true},
		"D4 sticky stays strict even with strict false": {map[string]string{sticky: "ingest-42", sel: "upstream-a", strict: "false"}, true},
		"D4 sticky with strict and select-provider":     {map[string]string{sticky: "ingest-42", sel: "upstream-a", strict: "true"}, true},
		"D6 unpinned":   {map[string]string{"lava-force-cache-refresh": "true"}, false},
		"D6 no headers": {nil, false},
		"the removed lava-pin-allow-retry does not loosen sticky": {map[string]string{sticky: "ingest-42", "lava-pin-allow-retry": "true"}, true},
	} {
		require.Equal(t, tc.pinned, PinnedByDirectives(tc.headers), name)
	}
}

// Only the value "true" (any case, surrounding space ignored) turns strict selection on.
func TestSelectProviderStrict(t *testing.T) {
	for _, tc := range []struct {
		value  string
		strict bool
	}{{"true", true}, {"TRUE", true}, {" True ", true}, {"false", false}, {"", false}, {"1", false}, {"yes", false}} {
		headers := map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "x", common.SELECT_PROVIDER_STRICT_HEADER_NAME: tc.value}
		require.Equal(t, tc.strict, SelectProviderStrict(headers), "%q", tc.value)
		require.Equal(t, tc.strict, PinnedByDirectives(headers), "%q", tc.value)
	}
	require.False(t, SelectProviderStrict(map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "x"}), "absent is not strict")
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
