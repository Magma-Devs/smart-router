package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(i int) *int { return &i }

// newResolver builds a single-policy resolver for ETH1/jsonrpc/<method>.
func newResolver(t *testing.T, method string, policy CrossValidationPolicy) *CrossValidationPolicyResolver {
	t.Helper()
	r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{
		Policies: map[string]map[string]map[string]CrossValidationPolicy{
			"ETH1": {"jsonrpc": {method: policy}},
		},
	})
	require.NoError(t, err)
	return r
}

// TestCrossValidationPolicyResolver_Resolve covers the clamp(caller, floor, cap) precedence matrix,
// the enabled/disabled/no-policy branches, and key scoping (review points 4 and 5).
func TestCrossValidationPolicyResolver_Resolve(t *testing.T) {
	cases := []struct {
		name          string
		method        string // policy is configured for "eth_getBalance"
		policy        CrossValidationPolicy
		reqChain      string
		reqAPI        string
		reqMethod     string
		caller        common.CrossValidationParams
		callerPresent bool
		wantApplies   bool
		wantParams    common.CrossValidationParams
	}{
		{
			name:     "enabled policy, no caller headers -> floor values, CV on",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3)}, AgreementThreshold: Bound{Floor: intPtr(2)}, MinGroups: Bound{Floor: intPtr(2)}},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_getBalance",
			callerPresent: false,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 2, MinGroups: 2},
		},
		{
			name:     "caller stricter than floor -> caller wins (floor is a minimum)",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3)}, AgreementThreshold: Bound{Floor: intPtr(2)}},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_getBalance",
			caller:        common.CrossValidationParams{MaxParticipants: 5, AgreementThreshold: 4},
			callerPresent: true,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 5, AgreementThreshold: 4, MinGroups: 1},
		},
		{
			name:     "caller looser than floor -> floor wins (cannot go below operator minimum)",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(3)}, MaxParticipants: Bound{Floor: intPtr(3)}},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_getBalance",
			caller:        common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 1},
			callerPresent: true,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 3, MinGroups: 1},
		},
		{
			name:     "cap overrides a stricter caller (the approved loosening)",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3), Cap: intPtr(3)}, AgreementThreshold: Bound{Floor: intPtr(2), Cap: intPtr(2)}},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_getBalance",
			caller:        common.CrossValidationParams{MaxParticipants: 9, AgreementThreshold: 9},
			callerPresent: true,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 2, MinGroups: 1},
		},
		{
			name:     "floor == cap -> exact/authoritative regardless of caller",
			method:   "eth_getTransactionReceipt",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3), Cap: intPtr(3)}, AgreementThreshold: Bound{Floor: intPtr(3), Cap: intPtr(3)}, MinGroups: Bound{Floor: intPtr(2), Cap: intPtr(2)}},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_getTransactionReceipt",
			caller:        common.CrossValidationParams{MaxParticipants: 1, AgreementThreshold: 1},
			callerPresent: true,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 3, MinGroups: 2},
		},
		{
			name:     "disabled policy with caller headers -> caller CV still runs",
			method:   "eth_syncing",
			policy:   CrossValidationPolicy{Enabled: false},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_syncing",
			caller:        common.CrossValidationParams{MaxParticipants: 4, AgreementThreshold: 2},
			callerPresent: true,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 4, AgreementThreshold: 2},
		},
		{
			name:     "disabled policy, no caller headers -> CV off",
			method:   "eth_syncing",
			policy:   CrossValidationPolicy{Enabled: false},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_syncing",
			callerPresent: false,
			wantApplies:   false,
		},
		{
			name:     "no policy for method -> pure caller passthrough",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3)}, AgreementThreshold: Bound{Floor: intPtr(2)}},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_blockNumber", // different method
			caller:        common.CrossValidationParams{MaxParticipants: 2, AgreementThreshold: 2},
			callerPresent: true,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 2, AgreementThreshold: 2},
		},
		{
			name:     "policy keyed to ETH1 does not apply to a different chain",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3)}, AgreementThreshold: Bound{Floor: intPtr(2)}},
			reqChain: "POLYGON", reqAPI: "jsonrpc", reqMethod: "eth_getBalance",
			callerPresent: false,
			wantApplies:   false,
		},
		{
			name:     "chain/api match is case-insensitive",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3)}, AgreementThreshold: Bound{Floor: intPtr(2)}},
			reqChain: "eth1", reqAPI: "JSONRPC", reqMethod: "eth_getBalance",
			callerPresent: false,
			wantApplies:   true,
			wantParams:    common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 2, MinGroups: 1},
		},
		{
			name:     "structural invariant: caller threshold clamped to effective max-participants",
			method:   "eth_getBalance",
			policy:   CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3), Cap: intPtr(3)}},
			reqChain: "ETH1", reqAPI: "jsonrpc", reqMethod: "eth_getBalance",
			caller:        common.CrossValidationParams{MaxParticipants: 9, AgreementThreshold: 9},
			callerPresent: true,
			wantApplies:   true,
			// participants capped to 3, threshold (caller 9, no cap) clamped down to participants
			wantParams: common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 3, MinGroups: 1},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newResolver(t, tc.method, tc.policy)
			got, applies := r.Resolve(tc.reqChain, tc.reqAPI, tc.reqMethod, tc.caller, tc.callerPresent)
			require.Equal(t, tc.wantApplies, applies, "applies, tc #%d, i #%d", i, i)
			if tc.wantApplies {
				assert.Equal(t, tc.wantParams, got, "params, tc #%d, i #%d", i, i)
			}
		})
	}
}

// TestCrossValidationPolicy_Validate covers config-load validation of a single policy.
func TestCrossValidationPolicy_Validate(t *testing.T) {
	cases := []struct {
		name    string
		policy  CrossValidationPolicy
		wantErr bool
	}{
		{"valid floor only", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}}, false},
		{"valid floor and cap", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3), Cap: intPtr(5)}}, false},
		{"valid floor==cap", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(3), Cap: intPtr(3)}}, false},
		{"floor below 1", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(0)}}, true},
		{"cap below 1", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Cap: intPtr(0)}}, true},
		{"floor exceeds cap", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: intPtr(5), Cap: intPtr(3)}}, true},
		{"threshold floor exceeds participants cap (unsatisfiable)", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(5)}, MaxParticipants: Bound{Cap: intPtr(3)}}, true},
		{"min-groups floor exceeds participants cap (unsatisfiable)", CrossValidationPolicy{Enabled: true, MinGroups: Bound{Floor: intPtr(4)}, MaxParticipants: Bound{Cap: intPtr(3)}}, true},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			if tc.wantErr {
				require.Error(t, err, "tc #%d, i #%d", i, i)
			} else {
				require.NoError(t, err, "tc #%d, i #%d", i, i)
			}
		})
	}
}

// TestCrossValidationPolicyResolver_StatefulGuard covers the config-load guard that rejects an enabled
// policy on a stateful (write) method.
func TestCrossValidationPolicyResolver_StatefulGuard(t *testing.T) {
	r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{
		Policies: map[string]map[string]map[string]CrossValidationPolicy{
			"ETH1": {"jsonrpc": {
				"eth_getBalance":         {Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}},
				"eth_sendRawTransaction": {Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}},
				"eth_disabledWrite":      {Enabled: false}, // disabled write policy is allowed
			}},
		},
	})
	require.NoError(t, err)

	isStateful := func(_, _ string, method string) bool {
		return method == "eth_sendRawTransaction" || method == "eth_disabledWrite"
	}

	err = r.ValidateNoStatefulPolicies(isStateful)
	require.Error(t, err, "enabled policy on a stateful method must be rejected")
	assert.Contains(t, err.Error(), "eth_sendRawTransaction")

	// With only read policies enabled, the guard passes.
	rReads := newResolver(t, "eth_getBalance", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}})
	require.NoError(t, rReads.ValidateNoStatefulPolicies(isStateful))
}

// TestCrossValidationPolicyResolver_EmptyConfig confirms an empty config is fully backwards compatible:
// no policies, every request resolves to pure caller-driven behavior.
func TestCrossValidationPolicyResolver_EmptyConfig(t *testing.T) {
	r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{})
	require.NoError(t, err)
	require.False(t, r.HasPolicies())

	caller := common.CrossValidationParams{MaxParticipants: 4, AgreementThreshold: 2}
	got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", caller, true)
	assert.True(t, applies)
	assert.Equal(t, caller, got)

	gotOff, appliesOff := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
	assert.False(t, appliesOff)
	assert.Equal(t, common.CrossValidationParams{}, gotOff)
}
