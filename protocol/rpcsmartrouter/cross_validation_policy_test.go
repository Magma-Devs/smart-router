package rpcsmartrouter

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(i int) *int { return &i }

// newResolver builds a single-policy resolver for ETH1/jsonrpc/<method>.
func newResolver(t *testing.T, method string, policy CrossValidationPolicy) *CrossValidationPolicyResolver {
	t.Helper()
	r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{
		Policies: []CrossValidationPolicyEntry{
			{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: method, CrossValidationPolicy: policy},
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

// TestParseCrossValidationConfig covers YAML parsing: absent key (backwards compatible), the object
// form {floor, cap}, and the scalar shorthand (N == {floor: N}).
func TestParseCrossValidationConfig(t *testing.T) {
	t.Run("absent key yields empty config", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader("direct-rpc:\n  - name: x\n")))
		cfg, err := ParseCrossValidationConfig(v)
		require.NoError(t, err)
		r, err := NewCrossValidationPolicyResolver(cfg)
		require.NoError(t, err)
		assert.False(t, r.HasPolicies())
	})

	t.Run("object form and scalar shorthand both parse", func(t *testing.T) {
		const yamlBody = "cross-validation:\n" +
			"  policies:\n" +
			"    - chain-id: ETH1\n" +
			"      api-interface: jsonrpc\n" +
			"      method: eth_getBalance\n" + // preserved casing (string value, not a map key)
			"      enabled: true\n" +
			"      agreement-threshold: 2\n" + // scalar shorthand -> {floor: 2}
			"      max-participants:\n" + // object form
			"        floor: 3\n" +
			"        cap: 5\n" +
			"      min-groups: 2\n" // scalar shorthand -> {floor: 2}

		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

		cfg, err := ParseCrossValidationConfig(v)
		require.NoError(t, err)

		require.Len(t, cfg.Policies, 1)
		entry := cfg.Policies[0]
		assert.Equal(t, "ETH1", entry.ChainID)
		assert.Equal(t, "jsonrpc", entry.ApiInterface)
		assert.Equal(t, "eth_getBalance", entry.Method, "method casing must be preserved")
		policy := entry.CrossValidationPolicy
		require.True(t, policy.Enabled)
		require.NotNil(t, policy.AgreementThreshold.Floor)
		assert.Equal(t, 2, *policy.AgreementThreshold.Floor)
		assert.Nil(t, policy.AgreementThreshold.Cap)
		require.NotNil(t, policy.MaxParticipants.Floor)
		require.NotNil(t, policy.MaxParticipants.Cap)
		assert.Equal(t, 3, *policy.MaxParticipants.Floor)
		assert.Equal(t, 5, *policy.MaxParticipants.Cap)
		require.NotNil(t, policy.MinGroups.Floor)
		assert.Equal(t, 2, *policy.MinGroups.Floor)

		// And it resolves end-to-end.
		r, err := NewCrossValidationPolicyResolver(cfg)
		require.NoError(t, err)
		got, applies := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
		require.True(t, applies)
		assert.Equal(t, common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 2, MinGroups: 2}, got)
	})
}

// TestParseCrossValidationConfig_RejectsFractional covers P2: a fractional knob value must be a config
// error, not silently truncated.
func TestParseCrossValidationConfig_RejectsFractional(t *testing.T) {
	const yamlBody = "cross-validation:\n" +
		"  policies:\n" +
		"    - chain-id: ETH1\n" +
		"      api-interface: jsonrpc\n" +
		"      method: eth_getBalance\n" +
		"      enabled: true\n" +
		"      agreement-threshold: 2.9\n" // fractional -> must be rejected

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

	_, err := ParseCrossValidationConfig(v)
	require.Error(t, err, "fractional knob value must be rejected, not truncated to an int")
}

// TestValidateCrossValidationStartup covers the startup guards: the stateful-write guard (with a real
// parser and the fail-closed path when the parser cannot classify) and the min-groups capacity bound.
func TestValidateCrossValidationStartup(t *testing.T) {
	ctx := context.Background()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	realParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	mkResolver := func(t *testing.T, entries ...CrossValidationPolicyEntry) *CrossValidationPolicyResolver {
		t.Helper()
		r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: entries})
		require.NoError(t, err)
		return r
	}
	readPolicy := CrossValidationPolicyEntry{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_getBalance", CrossValidationPolicy: CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}, MaxParticipants: Bound{Floor: intPtr(2)}}}
	writePolicy := CrossValidationPolicyEntry{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_sendRawTransaction", CrossValidationPolicy: CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}, MaxParticipants: Bound{Floor: intPtr(2)}}}
	groupPolicy := CrossValidationPolicyEntry{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_getBalance", CrossValidationPolicy: CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}, MaxParticipants: Bound{Floor: intPtr(3)}, MinGroups: Bound{Floor: intPtr(3)}}}

	t.Run("no policies -> ok", func(t *testing.T) {
		require.NoError(t, validateCrossValidationStartup(mkResolver(t), realParser, "ETH1", "jsonrpc", 5))
	})
	t.Run("read policy, enough groups -> ok", func(t *testing.T) {
		require.NoError(t, validateCrossValidationStartup(mkResolver(t, readPolicy), realParser, "ETH1", "jsonrpc", 2))
	})
	t.Run("write policy -> rejected by stateful guard", func(t *testing.T) {
		require.Error(t, validateCrossValidationStartup(mkResolver(t, writePolicy), realParser, "ETH1", "jsonrpc", 5))
	})
	t.Run("min-groups 3 but only 2 configured groups -> rejected", func(t *testing.T) {
		require.Error(t, validateCrossValidationStartup(mkResolver(t, groupPolicy), realParser, "ETH1", "jsonrpc", 2))
	})
	t.Run("min-groups 3 with 3 configured groups -> ok", func(t *testing.T) {
		require.NoError(t, validateCrossValidationStartup(mkResolver(t, groupPolicy), realParser, "ETH1", "jsonrpc", 3))
	})
	t.Run("parser cannot classify stateful -> fail closed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		// MockChainParser implements chainlib.ChainParser but NOT ApiHasStatefulCategory.
		mockParser := chainlib.NewMockChainParser(ctrl)
		require.Error(t, validateCrossValidationStartup(mkResolver(t, readPolicy), mockParser, "ETH1", "jsonrpc", 5),
			"policies + a parser that cannot classify stateful methods must fail closed")
	})
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
		// Fix 4: an enabled policy must be satisfiable with NO caller — reject instead of silently
		// clamping agreement-threshold / min-groups down to max-participants.
		{"enabled: threshold floor 5 > participants floor 3", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(5)}, MaxParticipants: Bound{Floor: intPtr(3)}}, true},
		{"enabled: threshold floor 5 > default participants (3)", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(5)}}, true},
		{"enabled: min-groups floor 4 > participants floor 3", CrossValidationPolicy{Enabled: true, MinGroups: Bound{Floor: intPtr(4)}, MaxParticipants: Bound{Floor: intPtr(3)}}, true},
		{"enabled: threshold floor 5 with participants floor 5 is fine", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(5)}, MaxParticipants: Bound{Floor: intPtr(5)}}, false},
		// A disabled policy never resolves on its own, so an unsatisfiable no-caller shape is allowed.
		{"disabled: unsatisfiable floors allowed (dormant)", CrossValidationPolicy{Enabled: false, AgreementThreshold: Bound{Floor: intPtr(5)}, MaxParticipants: Bound{Floor: intPtr(3)}}, false},
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

// TestCrossValidationFinality covers the tri-state finality classifier used to label mismatch metrics.
func TestCrossValidationFinality(t *testing.T) {
	cases := []struct {
		name                                          string
		requestedBlock, latestBlock, finalizationDist int64
		want                                          string
	}{
		{"old enough -> finalized", 100, 200, 10, "finalized"},
		{"boundary -> finalized", 190, 200, 10, "finalized"},
		{"too recent -> not_finalized", 195, 200, 10, "not_finalized"},
		{"sentinel requested block (latest) -> unknown", -2, 200, 10, "unknown"},
		{"not-applicable requested block -> unknown", -1, 200, 10, "unknown"},
		{"latest unknown -> unknown", 100, 0, 10, "unknown"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, crossValidationFinality(tc.requestedBlock, tc.latestBlock, tc.finalizationDist), "tc #%d, i #%d", i, i)
		})
	}
}

// TestCrossValidationPolicy_StatefulGuard_ProductionParser exercises the real production path of the
// stateful guard (Fix 2): a real chain parser's ApiHasStatefulCategory lookup, fed through the exact
// predicate ServeRPCRequests builds, must reject an enabled CV policy on a write method and allow one on
// a read method.
func TestCrossValidationPolicy_StatefulGuard_ProductionParser(t *testing.T) {
	ctx := context.Background()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	checker, ok := chainParser.(interface{ ApiHasStatefulCategory(string) bool })
	require.True(t, ok, "real chain parser must expose ApiHasStatefulCategory")
	require.True(t, checker.ApiHasStatefulCategory("eth_sendRawTransaction"), "eth_sendRawTransaction must be stateful in the ETH1 spec")
	require.False(t, checker.ApiHasStatefulCategory("eth_getBalance"), "eth_getBalance must be a read")

	// Mirror the predicate built in ServeRPCRequests.
	isStateful := func(chainID, apiInterface, method string) bool {
		if !strings.EqualFold(chainID, "ETH1") || !strings.EqualFold(apiInterface, "jsonrpc") {
			return false
		}
		return checker.ApiHasStatefulCategory(method)
	}

	writePolicy := []CrossValidationPolicyEntry{{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_sendRawTransaction", CrossValidationPolicy: CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}, MaxParticipants: Bound{Floor: intPtr(2)}}}}
	rWrite, err := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: writePolicy})
	require.NoError(t, err)
	require.Error(t, rWrite.ValidateNoStatefulPolicies(isStateful), "enabled CV policy on a write method must be rejected at startup")

	readPolicy := []CrossValidationPolicyEntry{{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_getBalance", CrossValidationPolicy: CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}, MaxParticipants: Bound{Floor: intPtr(2)}}}}
	rRead, err := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: readPolicy})
	require.NoError(t, err)
	require.NoError(t, rRead.ValidateNoStatefulPolicies(isStateful), "CV policy on a read method must be allowed")
}

// TestGroupLabel_ConfigToSession_InertWithoutPolicy strengthens Phase 0.2 (Fix 5): it follows the real
// path group-label (config) -> RPCStaticProviderEndpoint.GroupLabel -> ConsumerSessionsWithProvider.
// GroupLabel, and confirms that with no group-diversity policy configured the label is inert (no CV).
func TestGroupLabel_ConfigToSession_InertWithoutPolicy(t *testing.T) {
	const yamlBody = "direct-rpc:\n" +
		"  - name: p1\n" +
		"    group-label: tier-1\n" +
		"    chain-id: ETH1\n" +
		"    api-interface: jsonrpc\n" +
		"    node-urls:\n" +
		"      - url: https://a.example.com\n"
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

	// config -> RPCStaticProviderEndpoint.GroupLabel
	endpoints, err := ParseStaticProviderEndpoints(v, common.DirectRPCConfigName, 1)
	require.NoError(t, err)
	require.Len(t, endpoints, 1)
	require.Equal(t, "tier-1", endpoints[0].GroupLabel)

	// -> ConsumerSessionsWithProvider.GroupLabel (mirrors the provider build in rpcsmartrouter.go)
	session := lavasession.NewConsumerSessionWithProvider(endpoints[0].Name,
		[]*lavasession.Endpoint{{NetworkAddress: "http://a", Enabled: true}}, 1, 1, 0)
	session.GroupLabel = endpoints[0].GroupLabel
	require.Equal(t, "tier-1", session.GroupLabel, "group label must flow config -> session record")

	// Inert: with no policy configured, group labels never trigger cross-validation.
	r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{})
	require.NoError(t, err)
	require.False(t, r.HasPolicies())
	_, applies := r.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
	require.False(t, applies, "no policy => no cross-validation regardless of group labels")
}

// TestCrossValidationPolicyResolver_StatefulGuard covers the config-load guard that rejects an enabled
// policy on a stateful (write) method.
func TestCrossValidationPolicyResolver_StatefulGuard(t *testing.T) {
	r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{
		Policies: []CrossValidationPolicyEntry{
			{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_getBalance", CrossValidationPolicy: CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}}},
			{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_sendRawTransaction", CrossValidationPolicy: CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: intPtr(2)}}},
			{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_disabledWrite", CrossValidationPolicy: CrossValidationPolicy{Enabled: false}}, // disabled write policy is allowed
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
