package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestParseCrossValidationConfig_MaxParticipantsSentinel is MAG-2783: max-participants accepts the
// string shorthand "primary" or "all" alongside the existing numeric shorthand and object form.
func TestParseCrossValidationConfig_MaxParticipantsSentinel(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  string
	}{
		{"primary", participantsPrimary},
		{"all", participantsAll},
	} {
		t.Run(tc.value, func(t *testing.T) {
			yamlBody := "cross-validation:\n" +
				"  policies:\n" +
				"    - chain-id: ETH1\n" +
				"      api-interface: jsonrpc\n" +
				"      method: eth_getBalance\n" +
				"      enabled: true\n" +
				"      max-participants: " + tc.value + "\n"

			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

			cfg, err := ParseCrossValidationConfig(v)
			require.NoError(t, err)
			require.Len(t, cfg.Policies, 1)
			mp := cfg.Policies[0].MaxParticipants
			require.Equal(t, tc.want, mp.Participants)
			require.Nil(t, mp.Floor, "a sentinel carries no floor until SetParticipantCounts resolves it")
			require.Nil(t, mp.Cap)

			// A resolver built straight from this config must still construct cleanly: Validate defers
			// the max-participants-dependent feasibility checks rather than reading a meaningless
			// placeholder default.
			_, err = NewCrossValidationPolicyResolver(cfg)
			require.NoError(t, err)
		})
	}
}

// TestParseCrossValidationConfig_MaxParticipantsSentinel_RejectsUnknownString pins the error message
// for a typo'd string value — distinct from the existing float/quoted-string rejection, which this must
// not regress (see TestParseCrossValidationConfig_RejectsNonInteger).
func TestParseCrossValidationConfig_MaxParticipantsSentinel_RejectsUnknownString(t *testing.T) {
	yamlBody := "cross-validation:\n" +
		"  policies:\n" +
		"    - chain-id: ETH1\n" +
		"      api-interface: jsonrpc\n" +
		"      method: eth_getBalance\n" +
		"      enabled: true\n" +
		"      max-participants: everyone\n"

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

	_, err := ParseCrossValidationConfig(v)
	require.Error(t, err)
	require.Contains(t, err.Error(), `"primary"`)
	require.Contains(t, err.Error(), `"all"`)
}

// TestCrossValidationPolicy_Validate_RejectsSentinelOnWrongKnob: the parse hook has no field name to
// check (mapstructure hooks see only types), so "primary"/"all" on agreement-threshold or min-groups
// must be caught here instead — the one place that does know which field it is.
func TestCrossValidationPolicy_Validate_RejectsSentinelOnWrongKnob(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy CrossValidationPolicy
	}{
		{"agreement-threshold", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Participants: participantsPrimary}}},
		{"min-groups", CrossValidationPolicy{Enabled: true, MinGroups: Bound{Participants: participantsAll}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "only max-participants accepts")
		})
	}
}

// TestCrossValidationPolicy_Validate_SentinelSkipsFeasibilityCheck: an agreement-threshold floor that
// would be unsatisfiable against the *default* max-participants (3) must not reject a policy whose
// max-participants is "primary"/"all" — the real count is unknowable at config-load time, so Validate
// defers to the runtime advisory/guard instead of judging against a meaningless placeholder.
func TestCrossValidationPolicy_Validate_SentinelSkipsFeasibilityCheck(t *testing.T) {
	policy := CrossValidationPolicy{
		Enabled:            true,
		MaxParticipants:    Bound{Participants: participantsPrimary},
		AgreementThreshold: Bound{Floor: intPtr(50)}, // would exceed the default-3 noCallerMax
	}
	require.NoError(t, policy.Validate())
}

// TestCrossValidationPolicyResolver_SetParticipantCounts is MAG-2783's acceptance test: "primary" fans
// out to the configured primaries, "all" also counts backups, and a plain numeric policy is untouched —
// each exercised end-to-end through Resolve, which is what the capacity guards actually read.
func TestCrossValidationPolicyResolver_SetParticipantCounts(t *testing.T) {
	t.Run("primary resolves to the primary count", func(t *testing.T) {
		r := newResolver(t, "eth_getBalance", CrossValidationPolicy{
			Enabled:         true,
			MaxParticipants: Bound{Participants: participantsPrimary},
		})
		r.SetParticipantCounts(7, 10)
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
		require.True(t, applies)
		require.Equal(t, 7, got.MaxParticipants)
		require.False(t, got.IncludeBackups, "primary IS the primary count; there is never a shortfall for selection to top up")
	})

	t.Run("all resolves to the combined primary+backup count and sets IncludeBackups", func(t *testing.T) {
		r := newResolver(t, "eth_getBalance", CrossValidationPolicy{
			Enabled:         true,
			MaxParticipants: Bound{Participants: participantsAll},
		})
		r.SetParticipantCounts(7, 10)
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
		require.True(t, applies)
		require.Equal(t, 10, got.MaxParticipants)
		require.True(t, got.IncludeBackups, "selection must know to draw on backups to reach this count")
	})

	t.Run("a numeric policy is unaffected", func(t *testing.T) {
		r := newResolver(t, "eth_getBalance", CrossValidationPolicy{
			Enabled:         true,
			MaxParticipants: Bound{Floor: intPtr(4)},
		})
		r.SetParticipantCounts(7, 10)
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
		require.True(t, applies)
		require.Equal(t, 4, got.MaxParticipants, "existing configs using a number must be unaffected")
		require.False(t, got.IncludeBackups)
	})

	t.Run("resolved sentinel still floors agreement-threshold and min-groups like an ordinary max", func(t *testing.T) {
		// A caller-shrunk max-participants must not be able to drop the operator's no-caller minimums
		// below floor — the same guard Resolve already applies to a numeric max-participants (see
		// TestCrossValidationPolicyResolver_Resolve's "no max floor" cases), now exercised with the
		// resolved count standing in for a configured number.
		r := newResolver(t, "eth_getBalance", CrossValidationPolicy{
			Enabled:            true,
			MaxParticipants:    Bound{Participants: participantsPrimary},
			AgreementThreshold: Bound{Floor: intPtr(2)},
		})
		r.SetParticipantCounts(1, 1) // primary count smaller than the operator's own threshold floor
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{MaxParticipants: 1}, true)
		require.True(t, applies)
		require.Equal(t, 2, got.MaxParticipants, "max-participants must float up to hold the operator's agreement-threshold floor")
		require.Equal(t, 2, got.AgreementThreshold)
	})

	t.Run("nil resolver is a no-op", func(t *testing.T) {
		var r *CrossValidationPolicyResolver
		require.NotPanics(t, func() { r.SetParticipantCounts(5, 8) })
	})
}

// TestPreflightValidateCrossValidationConfig_SentinelPolicyTargetIsChecked: a policy whose only knob is
// max-participants: primary/all names its target the way max-participants: 3 does, so a chain-id no
// endpoint serves is refused at startup for it too (MAG-3604, governsRequests). Until SetParticipantCounts
// runs, such a Bound has neither a floor nor a cap, so it read as a policy with no bounds and was skipped.
func TestPreflightValidateCrossValidationConfig_SentinelPolicyTargetIsChecked(t *testing.T) {
	for _, value := range []string{"3", participantsPrimary, participantsAll} {
		t.Run(value, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader("cross-validation:\n"+
				"  policies:\n"+
				"    - chain-id: ETH1X\n"+
				"      api-interface: jsonrpc\n"+
				"      method: eth_getBalance\n"+
				"      max-participants: "+value+"\n")))
			err := PreflightValidateCrossValidationConfig(v, []*routersession.RPCEndpoint{{ChainID: "ETH1", ApiInterface: "jsonrpc"}})
			require.ErrorContains(t, err, "no endpoint serves")
		})
	}
}
