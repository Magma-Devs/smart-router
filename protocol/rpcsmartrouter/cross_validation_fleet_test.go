package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// MAG-3751. The cross-validation startup check counted the groups of the primaries that passed boot
// verification. When the only member of a group answered 503 at boot, a min-groups 2 policy saw one group,
// the endpoint failed to start, and every restart met the same node. The check now judges the configured
// primaries. A shortfall among the verified ones is logged, the endpoint starts, and the request-time
// guard refuses only the cross-validated requests the verified primaries cannot meet.

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

// fleetTestUpstream answers the ETH1 JSON-RPC calls a boot makes (verification, the crafted latest-block
// relay, the chain trackers), echoing each request's id.
func fleetTestUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		var result any = "0x0"
		switch req.Method {
		case "eth_blockNumber":
			result = "0x1312d00"
		case "eth_getBlockByNumber":
			result = map[string]any{"number": "0x1312d00", "hash": "0x" + strings.Repeat("ab", 32)}
		case "eth_getCode":
			result = "0x"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func fleetTestPrimary(name, group, url string) *lavasession.RPCStaticProviderEndpoint {
	return &lavasession.RPCStaticProviderEndpoint{
		Name: name, ChainID: "ETH1", ApiInterface: "jsonrpc", GroupLabel: group,
		NodeUrls: []common.NodeUrl{{Url: url, SkipVerifications: []string{"chain-id", "pruning"}}},
	}
}

// bootFleetTestEndpoint runs the real CreateSmartRouterEndpoint for an ETH1 JSON-RPC endpoint with the given
// primaries and a min-groups 2 policy on eth_getBalance.
func bootFleetTestEndpoint(t *testing.T, primaries ...*lavasession.RPCStaticProviderEndpoint) (*RPCSmartRouter, error) {
	t.Helper()
	rand.InitRandomSeed()
	// The fake upstreams are HTTP only, as the helm chart's --skip-websocket-verification allows.
	prevSkipWS := chainlib.SkipWebsocketVerificationDefault
	chainlib.SkipWebsocketVerificationDefault = true
	t.Cleanup(func() { chainlib.SkipWebsocketVerificationDefault = prevSkipWS })
	prevPolicies := viper.Get(common.CrossValidationConfigName)
	viper.Set(common.CrossValidationConfigName, map[string]any{"policies": []any{map[string]any{
		"chain-id": "ETH1", "api-interface": "jsonrpc", "method": "eth_getBalance",
		"enabled": true, "agreement-threshold": 2, "max-participants": 2, "min-groups": 2,
	}}})
	t.Cleanup(func() { viper.Set(common.CrossValidationConfigName, prevPolicies) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rpsr := createTestRPCSmartRouter()
	rpsr.reverifyInputs = make(map[string]*chainReverifyInputs)
	endpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc", NetworkAddress: "127.0.0.1:0"}
	metricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	logs, err := metrics.NewRPCConsumerLogs(metricsManager, nil, nil)
	require.NoError(t, err)
	options := &rpcSmartRouterStartOptions{
		rpcEndpoints:        []*lavasession.RPCEndpoint{endpoint},
		strategy:            provideroptimizer.StrategyBalanced,
		cmdFlags:            common.ConsumerCmdFlags{StaticSpecPaths: []string{"../../specs/ethereum.json"}},
		staticProvidersList: primaries,
	}
	err = rpsr.CreateSmartRouterEndpoint(ctx, endpoint, make(chan error, 1), &common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{},
		map[string]*sync.Mutex{"ETH1": {}}, options, "test-router", logs, nil, metricsManager, nil)
	return rpsr, err
}

// TestCreateSmartRouterEndpoint_GroupDownAtBoot drives the real boot path of the ticket: three primaries in
// two groups, the only member of group-b answering 503, and a min-groups 2 policy. Before MAG-3751 the
// endpoint failed with "min-groups policy cannot be satisfied" and the process exited.
func TestCreateSmartRouterEndpoint_GroupDownAtBoot(t *testing.T) {
	up1, up2 := fleetTestUpstream(t), fleetTestUpstream(t)
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "Node temporarily unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)

	t.Run("a group whose only primary is down at boot -> the endpoint starts", func(t *testing.T) {
		rpsr, err := bootFleetTestEndpoint(t,
			fleetTestPrimary("sim-1", "group-a", up1.URL),
			fleetTestPrimary("sim-2", "group-a", up2.URL),
			fleetTestPrimary("sim-3", "group-b", down.URL))
		require.NoError(t, err)
		key := (&lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}).Key()
		require.Equal(t, map[string][]string{"group-a": {"sim-1", "sim-2"}}, rpsr.sessionManagers[key].ProviderGroupAssignments(),
			"sim-3 failed verification, so only group-a is serving")
		require.Len(t, rpsr.failedStaticProviders[key], 1, "and sim-3 is queued for the background retry")
		require.Equal(t, "sim-3", rpsr.failedStaticProviders[key][0].Name)
	})
	// The control: the same boot with a config that can never meet the policy still fails, which shows the
	// policy was loaded and checked above rather than skipped.
	t.Run("too few configured groups -> the endpoint does not start", func(t *testing.T) {
		_, err := bootFleetTestEndpoint(t,
			fleetTestPrimary("sim-1", "group-a", up1.URL),
			fleetTestPrimary("sim-2", "group-a", up2.URL))
		require.ErrorContains(t, err, "min-groups policy cannot be satisfied")
	})
}

// TestCrossValidationFleet_GroupDownAtBoot replays the ticket's boot over a real session manager: the
// endpoint starts, the capacity guard refuses a request whose policy needs both groups with
// insufficient-groups while sim-3 is out but lets a request without cross-validation through, and the
// policy is met again in the pairing that holds sim-3.
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
	requiredProviders, requiredGroups := crossValidationShortfall(resolver, "ETH1", "jsonrpc", groupSizesOf(verified))
	require.Equal(t, [2]int{0, 2}, [2]int{requiredProviders, requiredGroups},
		"and the startup warning reports the requirement the verified primaries cannot meet")
	require.Equal(t, []string{"sim-3"}, providersMissingFrom(configured, verified), "naming the provider that is out")

	ctx := context.Background()
	params, _ := resolver.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
	reason, err := degraded.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &params, "", nil)
	require.Error(t, err)
	require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)

	reason, err = degraded.validateCrossValidationCapacity(ctx, relaycore.Stateless, nil, "", nil)
	require.NoError(t, err, "the guard never holds back a request without cross-validation")
	require.Empty(t, reason)

	// The pairing after sim-3 is re-admitted (by retryFailedProviders or the epoch re-verification).
	recovered := newCapacityTestServer(t, map[string]string{"sim-1": "group-a", "sim-2": "group-a", "sim-3": "group-b"})
	reason, err = recovered.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &params, "", nil)
	require.NoError(t, err)
	require.Empty(t, reason)
}

// TestCrossValidationShortfall pins the prediction behind the startup warning: the largest max-participants
// and min-groups the primaries cannot meet, judged the way the request-time guard judges a header-less request.
func TestCrossValidationShortfall(t *testing.T) {
	minGroups2 := fleetTestPolicy("eth_getBalance", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: new(2)}, MinGroups: Bound{Floor: new(2)}})
	minGroups3 := fleetTestPolicy("eth_getCode", CrossValidationPolicy{Enabled: true, MinGroups: Bound{Floor: new(3)}})
	perGroup := fleetTestPolicy("eth_getTransactionCount", CrossValidationPolicy{Enabled: true, PerGroupQuorum: true, AgreementThreshold: Bound{Floor: new(2)}, MaxParticipants: Bound{Floor: new(4)}, MinGroups: Bound{Floor: new(2)}})
	noDiversity := fleetTestPolicy("eth_getStorageAt", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: new(2)}})
	// The eth-sim router's policy: max-participants pinned at 3, min-groups 2.
	ethSim := fleetTestPolicy("eth_getBlockByNumber", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: new(2), Cap: new(3)}, MaxParticipants: Bound{Floor: new(3), Cap: new(3)}, MinGroups: Bound{Floor: new(2)}})

	for _, tc := range []struct {
		desc                string
		policies            []CrossValidationPolicyEntry
		sizes               map[string]int
		providers, groupsIn int
	}{
		{"two groups meet min-groups 2", []CrossValidationPolicyEntry{minGroups2}, map[string]int{"a": 1, "b": 1}, 0, 0},
		{"one group does not", []CrossValidationPolicyEntry{minGroups2}, map[string]int{"a": 2}, 0, 2},
		{"no primary at all", []CrossValidationPolicyEntry{minGroups2}, map[string]int{}, 2, 2},
		{"the largest unmet requirement is reported", []CrossValidationPolicyEntry{minGroups2, minGroups3}, map[string]int{"a": 3}, 0, 3},
		{"a met requirement is not reported", []CrossValidationPolicyEntry{minGroups2, minGroups3}, map[string]int{"a": 2, "b": 1}, 0, 3},
		{"per-group: a group below the threshold", []CrossValidationPolicyEntry{perGroup}, map[string]int{"a": 2, "b": 1}, 4, 2},
		{"per-group: both groups at the threshold", []CrossValidationPolicyEntry{perGroup}, map[string]int{"a": 2, "b": 2}, 0, 0},
		{"a policy without min-groups needs no diversity", []CrossValidationPolicyEntry{noDiversity}, map[string]int{"a": 2}, 0, 0},
		{"eth-sim, EthPrimaryProvider1 out: two groups, too few providers", []CrossValidationPolicyEntry{ethSim}, map[string]int{"voting-group-1": 1, "voting-group-2": 1}, 3, 0},
		{"eth-sim, EthPrimaryProvider3 out: one group, too few providers", []CrossValidationPolicyEntry{ethSim}, map[string]int{"voting-group-1": 2}, 3, 2},
		{"eth-sim, every primary up", []CrossValidationPolicyEntry{ethSim}, map[string]int{"voting-group-1": 2, "voting-group-2": 1}, 0, 0},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			providers, groups := crossValidationShortfall(fleetTestResolver(t, tc.policies...), "ETH1", "jsonrpc", tc.sizes)
			require.Equal(t, [2]int{tc.providers, tc.groupsIn}, [2]int{providers, groups}, "[requiredProviders, requiredGroups]")
		})
	}
}

// TestHeaderlessParams pins the shapes the prediction starts from: one per enabled policy of the endpoint,
// each exactly what Resolve gives a request without cross-validation headers.
func TestHeaderlessParams(t *testing.T) {
	resolver := fleetTestResolver(t,
		fleetTestPolicy("eth_getBalance", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: new(3)}, MinGroups: Bound{Floor: new(2)}}),
		fleetTestPolicy("eth_call", CrossValidationPolicy{Enabled: false}),
		fleetTestPolicy("eth_gasPrice", CrossValidationPolicy{ForbidCallerCV: true}),
		CrossValidationPolicyEntry{ChainID: "SOLANA", ApiInterface: "jsonrpc", Method: "getEpochInfo", CrossValidationPolicy: CrossValidationPolicy{Enabled: true}},
	)
	want, applies := resolver.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
	require.True(t, applies)
	require.Equal(t, []common.CrossValidationParams{want}, resolver.HeaderlessParams("ETH1", "jsonrpc"),
		"only the enabled ETH1 policy; a disabled, a forbidding and another chain's policy are not shapes a request is held to")
}

// captureFleetLog runs fn with the global logger writing into a buffer and returns what it wrote.
func captureFleetLog(t *testing.T, fn func()) string {
	t.Helper()
	prev := zerologlog.Logger
	var buf strings.Builder
	var mu sync.Mutex
	zerologlog.Logger = zerolog.New(zerolog.SyncWriter(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	})))
	defer func() { zerologlog.Logger = prev }()
	fn()
	mu.Lock()
	defer mu.Unlock()
	return buf.String()
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// TestValidateCrossValidationFleet_Warning pins the ATTENTION line, the "router says so" of the ticket: it
// names the provider that is out and the requirement it breaks, for both reasons the request-time guard
// refuses, and it is absent when every primary verified. Every capture must also hold the "policies loaded"
// line, or an empty capture would pass the absence check.
func TestValidateCrossValidationFleet_Warning(t *testing.T) {
	parser := fleetTestParser(t)
	resolver := fleetTestResolver(t, fleetTestPolicy("eth_getBlockByNumber", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: new(2), Cap: new(3)}, MaxParticipants: Bound{Floor: new(3), Cap: new(3)}, MinGroups: Bound{Floor: new(2)}}))
	configured := map[string][]string{"voting-group-1": {"EthPrimaryProvider1", "EthPrimaryProvider2"}, "voting-group-2": {"EthPrimaryProvider3"}}
	boot := func(verified map[string][]string) string {
		return captureFleetLog(t, func() {
			require.NoError(t, validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", configured, verified))
		})
	}
	const attention = "ATTENTION: the providers that passed startup verification cannot meet a cross-validation policy"

	t.Run("the only member of a group is out", func(t *testing.T) {
		log := boot(map[string][]string{"voting-group-1": {"EthPrimaryProvider1", "EthPrimaryProvider2"}})
		require.Contains(t, log, "cross-validation per-method policies loaded")
		require.Contains(t, log, attention)
		require.Contains(t, log, `"unavailableProviders":"EthPrimaryProvider3"`)
		require.Contains(t, log, `"requiredGroups":"2"`)
		require.Contains(t, log, `"requiredProviders":"3"`)
	})
	t.Run("one of two members of a group is out: groups still met, providers are not", func(t *testing.T) {
		log := boot(map[string][]string{"voting-group-1": {"EthPrimaryProvider2"}, "voting-group-2": {"EthPrimaryProvider3"}})
		require.Contains(t, log, "cross-validation per-method policies loaded")
		require.Contains(t, log, attention)
		require.Contains(t, log, `"unavailableProviders":"EthPrimaryProvider1"`)
		require.Contains(t, log, `"requiredProviders":"3"`)
		require.NotContains(t, log, `"requiredGroups"`)
	})
	t.Run("every primary verified: nothing to report", func(t *testing.T) {
		log := boot(configured)
		require.Contains(t, log, "cross-validation per-method policies loaded")
		require.NotContains(t, log, "ATTENTION")
	})
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
