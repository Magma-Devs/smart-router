package rpcsmartrouter

import (
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/stretchr/testify/require"
)

// withFreshRelayHoldoff swaps the package's hold-off registry for a fresh one for the
// duration of a test, restoring the shared instance on cleanup.
func withFreshRelayHoldoff(t *testing.T) *holdoff.Registry {
	t.Helper()
	reg := holdoff.NewRegistry()
	swapRelayHoldoff(t, reg)
	return reg
}

// swapRelayHoldoff makes reg the hold-off registry relays read and record into until the test
// ends, then puts back whatever was there before.
func swapRelayHoldoff(t *testing.T, reg *holdoff.Registry) {
	t.Helper()
	restore := relayHoldoffOverride.Swap(reg)
	t.Cleanup(func() { relayHoldoffOverride.Store(restore) })
}

// The session-release decision keys on this: a rate-limit in either arriving shape must
// be neither failure nor success.
func TestIsRateLimitedRelayOutcome(t *testing.T) {
	t.Run("typed 429 error", func(t *testing.T) {
		err := httpStatusRelayError(429, nil)
		require.True(t, isRateLimitedRelayOutcome(err, &common.RelayResult{}))
	})
	t.Run("body-shape classification flag", func(t *testing.T) {
		require.True(t, isRateLimitedRelayOutcome(nil, &common.RelayResult{IsRateLimited: true}))
	})
	t.Run("5xx is not a rate limit", func(t *testing.T) {
		require.False(t, isRateLimitedRelayOutcome(httpStatusRelayError(503, nil), &common.RelayResult{}))
	})
	t.Run("plain transport error is not a rate limit", func(t *testing.T) {
		require.False(t, isRateLimitedRelayOutcome(errors.New("connection refused"), &common.RelayResult{}))
	})
	t.Run("clean result is not a rate limit", func(t *testing.T) {
		require.False(t, isRateLimitedRelayOutcome(nil, &common.RelayResult{StatusCode: 200}))
	})
}
