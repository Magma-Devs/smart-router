package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestResolveKnob_EnabledDefaultIsAFloor is MAG-2784's direct acceptance test: an enabled policy
// with no explicit floor on a knob must still floor that knob at its default when a caller sends
// a weaker value — "enabled: true" with nothing else configured must not be weakenable below what
// that same policy gives a caller who sends no headers at all.
func TestResolveKnob_EnabledDefaultIsAFloor(t *testing.T) {
	t.Run("caller below the default floors at the default", func(t *testing.T) {
		require.Equal(t, defaultEnabledAgreementThreshold,
			resolveKnob(1, true, Bound{}, defaultEnabledAgreementThreshold),
			"agreement-threshold: a caller sending 1 with no floor configured must not resolve below the default (2)")
		require.Equal(t, defaultEnabledMaxParticipants,
			resolveKnob(1, true, Bound{}, defaultEnabledMaxParticipants),
			"max-participants: same floor-at-default behavior")
	})

	t.Run("caller above the default is still honored — stricter is always allowed", func(t *testing.T) {
		require.Equal(t, 3, resolveKnob(3, true, Bound{}, defaultEnabledAgreementThreshold),
			"a caller asking for MORE than the default must get it")
	})

	t.Run("no caller present: default alone, unaffected by the fix", func(t *testing.T) {
		require.Equal(t, defaultEnabledMinGroups, resolveKnob(0, false, Bound{}, defaultEnabledMinGroups),
			"min-groups has no caller header; its no-caller resolution was already correct and must stay so")
	})

	t.Run("an explicit floor below the default still wins — the fix only fills in a MISSING floor", func(t *testing.T) {
		low := 1
		require.Equal(t, 1, resolveKnob(1, true, Bound{Floor: &low}, defaultEnabledAgreementThreshold),
			"an operator who deliberately configured a floor below the default must not be overridden by it")
		require.Equal(t, 1, resolveKnob(0, false, Bound{Floor: &low}, defaultEnabledAgreementThreshold),
			"same, with no caller present")
	})

	t.Run("an explicit floor above the default still wins, as before", func(t *testing.T) {
		high := 5
		require.Equal(t, 5, resolveKnob(1, true, Bound{Floor: &high}, defaultEnabledAgreementThreshold))
	})

	t.Run("cap behavior is unchanged", func(t *testing.T) {
		cap5 := 5
		require.Equal(t, 5, resolveKnob(10, true, Bound{Cap: &cap5}, defaultEnabledAgreementThreshold),
			"a cap still overrides a stricter caller, exactly as before")
	})
}

// TestCrossValidationPolicyResolver_Resolve_EnabledFloorsEveryKnob is the same acceptance scenario
// exercised end-to-end through Resolve, for a policy with no floors configured at all — the exact
// shape the ticket's "Today" section describes (enabled: true, no floors set).
func TestCrossValidationPolicyResolver_Resolve_EnabledFloorsEveryKnob(t *testing.T) {
	r := newResolver(t, "eth_getLogs", CrossValidationPolicy{Enabled: true})

	t.Run("caller sends agreement-threshold: 1 -> effective threshold is the default (2), not 1", func(t *testing.T) {
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getLogs", common.CrossValidationParams{AgreementThreshold: 1}, true)
		require.True(t, applies)
		require.Equal(t, defaultEnabledAgreementThreshold, got.AgreementThreshold)
	})

	t.Run("caller sends agreement-threshold: 3 -> stricter is still allowed", func(t *testing.T) {
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getLogs", common.CrossValidationParams{AgreementThreshold: 3}, true)
		require.True(t, applies)
		require.Equal(t, 3, got.AgreementThreshold)
	})

	t.Run("caller sends max-participants: 1 -> effective max is the default (3), not 1", func(t *testing.T) {
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getLogs", common.CrossValidationParams{MaxParticipants: 1, AgreementThreshold: 1}, true)
		require.True(t, applies)
		require.Equal(t, defaultEnabledMaxParticipants, got.MaxParticipants)
	})

	t.Run("no caller headers at all -> every knob is exactly its default", func(t *testing.T) {
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getLogs", common.CrossValidationParams{}, false)
		require.True(t, applies)
		require.Equal(t, common.CrossValidationParams{
			MaxParticipants:    defaultEnabledMaxParticipants,
			AgreementThreshold: defaultEnabledAgreementThreshold,
			MinGroups:          defaultEnabledMinGroups,
		}, got)
	})
}
