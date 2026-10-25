package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
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
		_, err := validateCrossValidationFleet(fleetTestResolver(t, minGroups), parser, "ETH1", "jsonrpc", configured, verified)
		require.NoError(t, err)
	})
	t.Run("every primary failed verification -> starts", func(t *testing.T) {
		_, err := validateCrossValidationFleet(fleetTestResolver(t, minGroups), parser, "ETH1", "jsonrpc", configured, map[string][]string{})
		require.NoError(t, err)
	})
	t.Run("too few configured groups -> refused, even with every primary verified", func(t *testing.T) {
		oneGroup := map[string][]string{"group-a": {"sim-1", "sim-2", "sim-3"}}
		_, err := validateCrossValidationFleet(fleetTestResolver(t, minGroups), parser, "ETH1", "jsonrpc", oneGroup, oneGroup)
		require.ErrorContains(t, err, "min-groups policy cannot be satisfied")
	})
	t.Run("per-group: a group below the threshold only among verified primaries -> starts", func(t *testing.T) {
		configured := map[string][]string{"group-a": {"sim-1", "sim-2"}, "group-b": {"sim-3", "sim-4"}}
		verified := map[string][]string{"group-a": {"sim-1", "sim-2"}, "group-b": {"sim-3"}}
		_, err := validateCrossValidationFleet(fleetTestResolver(t, perGroup), parser, "ETH1", "jsonrpc", configured, verified)
		require.NoError(t, err)
	})
	t.Run("per-group: a configured group below the threshold -> refused", func(t *testing.T) {
		_, err := validateCrossValidationFleet(fleetTestResolver(t, perGroup), parser, "ETH1", "jsonrpc", configured, configured)
		require.ErrorContains(t, err, "per-group-quorum policy cannot be satisfied")
	})
	t.Run("a policy on a stateful method -> refused, whatever the fleet", func(t *testing.T) {
		_, err := validateCrossValidationFleet(fleetTestResolver(t, write), parser, "ETH1", "jsonrpc", configured, configured)
		require.Error(t, err)
	})
	// A backup-only endpoint has no configured primary, so no capacity guard can run: it starts, and the
	// advisory says its policies can never be met (cross-validation never draws on backups).
	t.Run("no configured primary (backup-only endpoint) -> starts, with an advisory", func(t *testing.T) {
		advisory, err := validateCrossValidationFleet(fleetTestResolver(t, minGroups), parser, "ETH1", "jsonrpc", map[string][]string{}, map[string][]string{})
		require.NoError(t, err)
		require.NotNil(t, advisory)
		require.True(t, advisory.configuredShort, "no provider can ever be re-admitted into an empty primary list")
		require.Empty(t, advisory.unavailable)
		require.Empty(t, advisory.configuredSizes)
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
		// The debug ring buffer is a second, process-global log sink that every LavaFormat* call feeds
		// through an atomic pointer, so the boot's own goroutines can keep logging while it is read.
		utils.EnableDebugLogBuffer(5000)
		t.Cleanup(utils.DisableDebugLogBuffer)
		utils.ClearDebugLogBuffer()

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

		// The router said so: the ATTENTION line names the provider that is out and the requirement it
		// breaks, and the layout line kept its meaning (distinctGroups counts the groups serving now).
		boot := bytes.Join(utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 5000), []byte("\n"))
		require.Contains(t, string(boot), "ATTENTION: the providers that passed startup verification cannot meet a cross-validation policy")
		require.Contains(t, string(boot), `"unavailableProviders":"sim-3"`)
		require.Contains(t, string(boot), `"requiredGroups":"2"`)
		require.Contains(t, string(boot), `"distinctGroups":"1"`)
		require.Contains(t, string(boot), `"configuredGroups":"2"`)

		// And the endpoint serves: through the policy the booted server loaded, a governed request is
		// refused by the request-time guard with insufficient-groups while a plain request is not.
		server := rpsr.rpcServers[key]
		require.NotNil(t, server)
		params, applies := server.crossValidationResolver.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
		require.True(t, applies, "the policy from the config applies to the booted endpoint")
		reason, err := server.validateCrossValidationCapacity(context.Background(), relaycore.CrossValidation, &params, "", nil)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)
		reason, err = server.validateCrossValidationCapacity(context.Background(), relaycore.Stateless, nil, "", nil)
		require.NoError(t, err, "a request without cross-validation is served")
		require.Empty(t, reason)
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

	// sim-3 answered 503 at boot, so only sim-1 and sim-2 reached the session manager. The server is the
	// endpoint the policy is written for, and it carries the resolver, so the params below reach the guard
	// the way a request's do: resolved for the server's own chain/api-interface.
	degraded := newCapacityTestServerFor(t, "ETH1", "jsonrpc", map[string]string{"sim-1": "group-a", "sim-2": "group-a"})
	degraded.crossValidationResolver = resolver
	verified := degraded.sessionManager.ProviderGroupAssignments()

	require.Error(t, validateCrossValidationStartup(resolver, parser, "ETH1", "jsonrpc", len(verified), groupSizesOf(verified)),
		"judged by the verified primaries, the policy looks unsatisfiable: this is the error that ended the process")
	advisory, err := validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", configured, verified)
	require.NoError(t, err, "judged by the configured primaries, the endpoint starts")
	require.NotNil(t, advisory, "and the startup warning reports the requirement the verified primaries cannot meet")
	require.Equal(t, [2]int{0, 2}, [2]int{advisory.requiredProviders, advisory.requiredGroups}, "[requiredProviders, requiredGroups]")
	require.Equal(t, []string{"sim-3"}, advisory.unavailable, "naming the provider that is out")
	require.False(t, advisory.configuredShort, "re-admitting sim-3 closes the gap")

	ctx := context.Background()
	params, applies := degraded.crossValidationResolver.Resolve(degraded.listenEndpoint.ChainID, degraded.listenEndpoint.ApiInterface, "eth_getBalance", common.CrossValidationParams{}, false)
	require.True(t, applies)
	reason, err := degraded.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &params, "", nil)
	require.Error(t, err)
	require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)

	reason, err = degraded.validateCrossValidationCapacity(ctx, relaycore.Stateless, nil, "", nil)
	require.NoError(t, err, "the guard never holds back a request without cross-validation")
	require.Empty(t, reason)

	// The pairing after sim-3 is re-admitted (by retryFailedProviders or the epoch re-verification).
	recovered := newCapacityTestServerFor(t, "ETH1", "jsonrpc", map[string]string{"sim-1": "group-a", "sim-2": "group-a", "sim-3": "group-b"})
	recovered.crossValidationResolver = resolver
	reason, err = recovered.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &params, "", nil)
	require.NoError(t, err)
	require.Empty(t, reason)
	advisory, err = validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", configured, recovered.sessionManager.ProviderGroupAssignments())
	require.NoError(t, err)
	require.Nil(t, advisory, "nothing to report once the fleet is whole")
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
	require.Equal(t, []common.CrossValidationParams{want}, resolver.headerlessParams("ETH1", "jsonrpc"),
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

// TestValidateCrossValidationFleet_Warning pins the ATTENTION line, the "router says so" of the ticket. It
// fires for every shortfall the request-time guard will refuse on, worded by what closes the gap: the
// provider that is out recovering, or a change to the policy or the fleet when the configured primaries fall
// short themselves. It is absent only when every enabled policy is met. Every capture must also hold the
// "policies loaded" line, or an empty capture would pass the absence check.
func TestValidateCrossValidationFleet_Warning(t *testing.T) {
	parser := fleetTestParser(t)
	// The eth-sim router's policy: max-participants pinned at 3, min-groups 2.
	ethSim := fleetTestResolver(t, fleetTestPolicy("eth_getBlockByNumber", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: new(2), Cap: new(3)}, MaxParticipants: Bound{Floor: new(3), Cap: new(3)}, MinGroups: Bound{Floor: new(2)}}))
	// The same fleet asked for more participants than it has.
	fiveOfThree := fleetTestResolver(t, fleetTestPolicy("eth_getBlockByNumber", CrossValidationPolicy{Enabled: true, AgreementThreshold: Bound{Floor: new(2)}, MaxParticipants: Bound{Floor: new(5)}, MinGroups: Bound{Floor: new(2)}}))
	configured := map[string][]string{"voting-group-1": {"EthPrimaryProvider1", "EthPrimaryProvider2"}, "voting-group-2": {"EthPrimaryProvider3"}}
	boot := func(resolver *CrossValidationPolicyResolver, configured, verified map[string][]string) string {
		return captureFleetLog(t, func() {
			advisory, err := validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", configured, verified)
			require.NoError(t, err)
			advisory.log()
		})
	}
	const (
		loaded      = "cross-validation per-method policies loaded"
		recovers    = "ATTENTION: the providers that passed startup verification cannot meet a cross-validation policy"
		configShort = "ATTENTION: the configured primaries cannot meet a cross-validation policy"
		backupOnly  = "ATTENTION: this endpoint has no configured primary, and cross-validation never draws on backup providers"
	)

	t.Run("the only member of a group is out", func(t *testing.T) {
		log := boot(ethSim, configured, map[string][]string{"voting-group-1": {"EthPrimaryProvider1", "EthPrimaryProvider2"}})
		require.Contains(t, log, loaded)
		require.Contains(t, log, recovers)
		require.Contains(t, log, `"unavailableProviders":"EthPrimaryProvider3"`)
		require.Contains(t, log, `"requiredGroups":"2"`)
		require.Contains(t, log, `"requiredProviders":"3"`)
		require.NotContains(t, log, configShort)
	})
	t.Run("one of two members of a group is out: groups still met, providers are not", func(t *testing.T) {
		log := boot(ethSim, configured, map[string][]string{"voting-group-1": {"EthPrimaryProvider2"}, "voting-group-2": {"EthPrimaryProvider3"}})
		require.Contains(t, log, loaded)
		require.Contains(t, log, recovers)
		require.Contains(t, log, `"unavailableProviders":"EthPrimaryProvider1"`)
		require.Contains(t, log, `"requiredProviders":"3"`)
		require.NotContains(t, log, `"requiredGroups"`)
	})
	t.Run("every primary verified and the policy is satisfiable: nothing to report", func(t *testing.T) {
		log := boot(ethSim, configured, configured)
		require.Contains(t, log, loaded)
		require.NotContains(t, log, "ATTENTION")
	})
	t.Run("every primary verified but max-participants exceeds the configured fleet: warns", func(t *testing.T) {
		log := boot(fiveOfThree, configured, configured)
		require.Contains(t, log, loaded)
		require.Contains(t, log, configShort)
		require.Contains(t, log, `"requiredProviders":"5"`)
		require.NotContains(t, log, `"unavailableProviders"`, "nothing is out; recovery cannot close this gap")
		require.NotContains(t, log, recovers)
	})
	t.Run("a provider is out AND max-participants exceeds the configured fleet: the permanent wording, naming the provider", func(t *testing.T) {
		log := boot(fiveOfThree, configured, map[string][]string{"voting-group-1": {"EthPrimaryProvider1", "EthPrimaryProvider2"}})
		require.Contains(t, log, loaded)
		require.Contains(t, log, configShort)
		require.Contains(t, log, `"unavailableProviders":"EthPrimaryProvider3"`)
		require.Contains(t, log, `"requiredProviders":"5"`)
		require.NotContains(t, log, recovers, "re-admitting EthPrimaryProvider3 still leaves three of five")
	})
	t.Run("no configured primary: the policies can never be met", func(t *testing.T) {
		log := boot(ethSim, map[string][]string{}, map[string][]string{})
		require.Contains(t, log, loaded)
		require.Contains(t, log, backupOnly)
		require.NotContains(t, log, `"unavailableProviders"`)
	})
	t.Run("the layout line keeps its keys: distinctGroups counts the groups serving now", func(t *testing.T) {
		log := boot(ethSim, configured, map[string][]string{"voting-group-1": {"EthPrimaryProvider1", "EthPrimaryProvider2"}})
		var layout string
		for _, line := range strings.Split(log, "\n") {
			if strings.Contains(line, loaded) {
				layout = line
			}
		}
		require.NotEmpty(t, layout)
		require.Contains(t, layout, `"distinctGroups":"1"`, "the groups serving now, as before MAG-3751 (the cv demo script greps this key)")
		require.Contains(t, layout, `"groupSizes":"map[voting-group-1:2]"`)
		require.Contains(t, layout, `"configuredGroups":"2"`)
		require.Contains(t, layout, `"configuredGroupSizes":"map[voting-group-1:2 voting-group-2:1]"`)
		require.NotContains(t, layout, `"verifiedGroupSizes"`, "the layout line has no second name for what groupSizes already says")
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
