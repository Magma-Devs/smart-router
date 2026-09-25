package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3751. The cross-validation startup check counted the groups of the primaries that passed boot
// verification. When the only member of a group answered 503 at boot, a min-groups 2 policy saw one group,
// the endpoint failed to start, and every restart met the same node. The check now judges the configured
// primaries. A shortfall among the verified ones is logged, the endpoint starts, and the request-time
// guard refuses only the cross-validated requests the verified groups cannot meet.

// fleetTestParser returns a real ETH1 JSON-RPC parser; the stateful-write guard needs one.
func fleetTestParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	parser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	return parser
}

func fleetTestResolver(t *testing.T, entries ...CrossValidationPolicyEntry) *CrossValidationPolicyResolver {
	t.Helper()
	r, err := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: entries})
	require.NoError(t, err)
	return r
}

func fleetTestPolicy(method string, policy CrossValidationPolicy) CrossValidationPolicyEntry {
	return CrossValidationPolicyEntry{ChainID: "ETH1", ApiInterface: "jsonrpc", Method: method, CrossValidationPolicy: policy}
}

func TestValidateCrossValidationFleet(t *testing.T) {
	parser := fleetTestParser(t)
	minGroups := fleetTestPolicy("eth_getBalance", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: new(2)}, MaxParticipants: Bound{Floor: new(3)}, MinGroups: Bound{Floor: new(2)}})
	perGroup := fleetTestPolicy("eth_getTransactionCount", CrossValidationPolicy{Enabled: true, PerGroupQuorum: true, AgreementThreshold: Bound{Floor: new(2)}, MaxParticipants: Bound{Floor: new(4)}, MinGroups: Bound{Floor: new(2)}})
	write := fleetTestPolicy("eth_sendRawTransaction", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: new(2)}, MaxParticipants: Bound{Floor: new(2)}})

	// The eth-sim layout from the ticket: sim-3 is the only member of its group.
	configured := map[string][]string{"group-a": {"sim-1", "sim-2"}, "group-b": {"sim-3"}}

	t.Run("the only primary of a group failed verification -> starts", func(t *testing.T) {
		verified := map[string][]string{"group-a": {"sim-1", "sim-2"}}
		require.NoError(t, validateCrossValidationFleet(fleetTestResolver(t, minGroups), parser, "ETH1", "jsonrpc", configured, verified))
	})
	t.Run("every primary failed verification -> starts", func(t *testing.T) {
		require.NoError(t, validateCrossValidationFleet(fleetTestResolver(t, minGroups), parser, "ETH1", "jsonrpc", configured, map[string][]string{}))
	})
	t.Run("too few configured groups -> refused, even with every primary verified", func(t *testing.T) {
		oneGroup := map[string][]string{"group-a": {"sim-1", "sim-2", "sim-3"}}
		err := validateCrossValidationFleet(fleetTestResolver(t, minGroups), parser, "ETH1", "jsonrpc", oneGroup, oneGroup)
		require.ErrorContains(t, err, "min-groups policy cannot be satisfied")
	})
	t.Run("per-group: a group below the threshold only among verified primaries -> starts", func(t *testing.T) {
		configured := map[string][]string{"group-a": {"sim-1", "sim-2"}, "group-b": {"sim-3", "sim-4"}}
		verified := map[string][]string{"group-a": {"sim-1", "sim-2"}, "group-b": {"sim-3"}}
		require.NoError(t, validateCrossValidationFleet(fleetTestResolver(t, perGroup), parser, "ETH1", "jsonrpc", configured, verified))
	})
	t.Run("per-group: a configured group below the threshold -> refused", func(t *testing.T) {
		err := validateCrossValidationFleet(fleetTestResolver(t, perGroup), parser, "ETH1", "jsonrpc", configured, configured)
		require.ErrorContains(t, err, "per-group-quorum policy cannot be satisfied")
	})
	t.Run("a policy on a stateful method -> refused, whatever the fleet", func(t *testing.T) {
		require.Error(t, validateCrossValidationFleet(fleetTestResolver(t, write), parser, "ETH1", "jsonrpc", configured, configured))
	})
}

// TestCrossValidationFleet_GroupDownAtBoot replays the ticket's boot over a real session manager: the
// endpoint starts, a request whose policy needs both groups is refused with insufficient-groups while sim-3
// is out, a request without cross-validation is not, and the policy is met again once sim-3 is re-admitted.
func TestCrossValidationFleet_GroupDownAtBoot(t *testing.T) {
	parser := fleetTestParser(t)
	resolver := fleetTestResolver(t, fleetTestPolicy("eth_getBalance", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: new(2)}, MaxParticipants: Bound{Floor: new(2)}, MinGroups: Bound{Floor: new(2)}}))
	configured := staticProviderGroupAssignments([]*lavasession.RPCStaticProviderEndpoint{
		{Name: "sim-1", GroupLabel: "group-a"},
		{Name: "sim-2", GroupLabel: "group-a"},
		{Name: "sim-3", GroupLabel: "group-b"},
	})

	// sim-3 answered 503 at boot, so only sim-1 and sim-2 reached the session manager.
	degraded := newCapacityTestServer(t, map[string]string{"sim-1": "group-a", "sim-2": "group-a"})
	verified := degraded.sessionManager.ProviderGroupAssignments()

	require.Error(t, validateCrossValidationStartup(resolver, parser, "ETH1", "jsonrpc", len(verified), groupSizesOf(verified)),
		"judged by the verified primaries, the policy looks unsatisfiable: this is the error that ended the process")
	require.NoError(t, validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", configured, verified),
		"judged by the configured primaries, the endpoint starts")
	require.Equal(t, 2, unmetCrossValidationGroupRequirement(resolver, "ETH1", "jsonrpc", groupSizesOf(verified)),
		"and the startup warning reports the requirement the verified groups cannot meet")
	require.Equal(t, []string{"sim-3"}, providersMissingFrom(configured, verified), "naming the provider that is out")

	ctx := context.Background()
	params, _ := resolver.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
	reason, err := degraded.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &params, "", nil)
	require.Error(t, err)
	require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)

	reason, err = degraded.validateCrossValidationCapacity(ctx, relaycore.Stateless, nil, "", nil)
	require.NoError(t, err, "a request without cross-validation is served while sim-3 is out")
	require.Empty(t, reason)

	// retryFailedProviders re-admits sim-3 once it answers again.
	recovered := newCapacityTestServer(t, map[string]string{"sim-1": "group-a", "sim-2": "group-a", "sim-3": "group-b"})
	reason, err = recovered.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &params, "", nil)
	require.NoError(t, err)
	require.Empty(t, reason)
}

// TestUnmetCrossValidationGroupRequirement pins the prediction behind the startup warning: the largest
// min-groups the layout cannot meet, judged the way the request-time guards judge the candidate set.
func TestUnmetCrossValidationGroupRequirement(t *testing.T) {
	minGroups2 := fleetTestPolicy("eth_getBalance", CrossValidationPolicy{Enabled: true, MinGroups: Bound{Floor: new(2)}})
	minGroups3 := fleetTestPolicy("eth_getCode", CrossValidationPolicy{Enabled: true, MinGroups: Bound{Floor: new(3)}})
	perGroup := fleetTestPolicy("eth_getTransactionCount", CrossValidationPolicy{Enabled: true, PerGroupQuorum: true, AgreementThreshold: Bound{Floor: new(2)}, MaxParticipants: Bound{Floor: new(4)}, MinGroups: Bound{Floor: new(2)}})
	noDiversity := fleetTestPolicy("eth_getStorageAt", CrossValidationPolicy{Enabled: true})

	for _, tc := range []struct {
		desc     string
		policies []CrossValidationPolicyEntry
		sizes    map[string]int
		want     int
	}{
		{"two groups meet min-groups 2", []CrossValidationPolicyEntry{minGroups2}, map[string]int{"a": 2, "b": 1}, 0},
		{"one group does not", []CrossValidationPolicyEntry{minGroups2}, map[string]int{"a": 2}, 2},
		{"no verified primary at all", []CrossValidationPolicyEntry{minGroups2}, map[string]int{}, 2},
		{"the largest unmet requirement is reported", []CrossValidationPolicyEntry{minGroups2, minGroups3}, map[string]int{"a": 2}, 3},
		{"a met requirement is not reported", []CrossValidationPolicyEntry{minGroups2, minGroups3}, map[string]int{"a": 1, "b": 1}, 3},
		{"per-group: a group below the threshold", []CrossValidationPolicyEntry{perGroup}, map[string]int{"a": 2, "b": 1}, 2},
		{"per-group: both groups at the threshold", []CrossValidationPolicyEntry{perGroup}, map[string]int{"a": 2, "b": 2}, 0},
		{"a policy without min-groups needs no diversity", []CrossValidationPolicyEntry{noDiversity}, map[string]int{}, 0},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.want, unmetCrossValidationGroupRequirement(fleetTestResolver(t, tc.policies...), "ETH1", "jsonrpc", tc.sizes))
		})
	}
}

func TestStaticProviderGroupAssignments(t *testing.T) {
	providers := []*lavasession.RPCStaticProviderEndpoint{
		{Name: "sim-3", GroupLabel: "group-b"},
		{Name: "sim-2", GroupLabel: "group-a"},
		{Name: "sim-1", GroupLabel: "group-a"},
		{Name: "sim-4"}, // no label: the implicit default group, as in the session manager
	}
	require.Equal(t, map[string][]string{
		"group-a":                   {"sim-1", "sim-2"},
		"group-b":                   {"sim-3"},
		common.DefaultProviderGroup: {"sim-4"},
	}, staticProviderGroupAssignments(providers))
}
