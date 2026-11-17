package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bootParticipantsTestEndpoint runs the real CreateSmartRouterEndpoint for an ETH1 JSON-RPC endpoint with
// the given primaries and backups and one enabled policy on eth_getBalance (agreement-threshold 2), whose
// max-participants is the given value, and returns the endpoint's server. extra adds policy fields.
func bootParticipantsTestEndpoint(t *testing.T, maxParticipants any, primaries, backups []*routersession.RPCStaticProviderEndpoint, extra ...map[string]any) *RPCSmartRouterServer {
	t.Helper()
	rand.InitRandomSeed()
	prevSkipWS := chainlib.SkipWebsocketVerificationDefault
	chainlib.SkipWebsocketVerificationDefault = true
	t.Cleanup(func() { chainlib.SkipWebsocketVerificationDefault = prevSkipWS })
	prevPolicies := viper.Get(common.CrossValidationConfigName)
	policy := map[string]any{
		"chain-id": "ETH1", "api-interface": "jsonrpc", "method": "eth_getBalance",
		"enabled": true, "agreement-threshold": 2, "max-participants": maxParticipants,
	}
	for _, fields := range extra {
		maps.Copy(policy, fields)
	}
	viper.Set(common.CrossValidationConfigName, map[string]any{"policies": []any{policy}})
	t.Cleanup(func() { viper.Set(common.CrossValidationConfigName, prevPolicies) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rpsr := createTestRPCSmartRouter()
	rpsr.reverifyInputs = make(map[string]*chainReverifyInputs)
	endpoint := &routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc", NetworkAddress: "127.0.0.1:0"}
	metricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	logs, err := metrics.NewRPCConsumerLogs(metricsManager, nil, nil)
	require.NoError(t, err)
	options := &rpcSmartRouterStartOptions{
		rpcEndpoints:        []*routersession.RPCEndpoint{endpoint},
		strategy:            provideroptimizer.StrategyBalanced,
		cmdFlags:            common.ConsumerCmdFlags{StaticSpecPaths: []string{"../../specs/ethereum.json"}},
		staticProvidersList: primaries,
		backupProvidersList: backups,
	}
	require.NoError(t, rpsr.CreateSmartRouterEndpoint(ctx, endpoint, make(chan error, 1), &common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{},
		map[string]*sync.Mutex{"ETH1": {}}, options, "test-router", logs, nil, metricsManager, nil))
	server := rpsr.rpcServers[endpoint.Key()]
	require.NotNil(t, server)
	return server
}

// TestCrossValidationAll_TheCapacityGuardCountsTheBackups is MAG-2783's max-participants: all through the
// real boot: two primaries and one backup resolve "all" to 3, and the request-time capacity guard, which
// runs before selection, must count the backup too. It counted the primaries alone, so every header-less
// request under the policy was refused with insufficient-capacity before the backup top-up could run.
func TestCrossValidationAll_TheCapacityGuardCountsTheBackups(t *testing.T) {
	p1, p2, b1 := fleetTestUpstream(t), fleetTestUpstream(t), fleetTestUpstream(t)
	server := bootParticipantsTestEndpoint(t, "all",
		[]*routersession.RPCStaticProviderEndpoint{fleetTestPrimary("sim-1", "", p1.URL), fleetTestPrimary("sim-2", "", p2.URL)},
		[]*routersession.RPCStaticProviderEndpoint{fleetTestPrimary("backup-1", "", b1.URL)})
	ctx := context.Background()

	params, applies := server.crossValidationResolver.Resolve("ETH1", "jsonrpc", "eth_getBalance", common.CrossValidationParams{}, false)
	require.True(t, applies)
	require.Equal(t, 3, params.MaxParticipants, "all = 2 primaries + 1 backup")
	require.True(t, params.IncludeBackups)

	reason, err := server.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &params, "", nil)
	require.NoError(t, err, "every configured upstream is up, so the guard must let the request through")
	require.Empty(t, reason)

	// The control: the same count without the backup tier (a numeric max-participants: 3) is still refused,
	// because only "all" draws on backups.
	numeric := params
	numeric.IncludeBackups = false
	reason, err = server.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, &numeric, "", nil)
	require.Error(t, err)
	require.Equal(t, common.CrossValidationReasonInsufficientCapacity, reason)
}

// countingBalanceUpstream is fleetTestUpstream that also counts the eth_getBalance calls it answers.
func countingBalanceUpstream(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	inner := fleetTestUpstream(t)
	var balanceCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"eth_getBalance"`) {
			balanceCalls.Add(1)
		}
		resp, err := http.Post(inner.URL, "application/json", bytes.NewReader(body)) //nolint:noctx // test-only forwarder
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv, &balanceCalls
}

// TestCrossValidationAll_ARequestReachesTheBackup sends a real header-less eth_getBalance through the
// booted endpoint: under max-participants: all it is cross-validated by both primaries and the backup.
func TestCrossValidationAll_ARequestReachesTheBackup(t *testing.T) {
	p1, p1Calls := countingBalanceUpstream(t)
	p2, p2Calls := countingBalanceUpstream(t)
	b1, b1Calls := countingBalanceUpstream(t)
	server := bootParticipantsTestEndpoint(t, "all",
		[]*routersession.RPCStaticProviderEndpoint{fleetTestPrimary("sim-1", "", p1.URL), fleetTestPrimary("sim-2", "", p2.URL)},
		[]*routersession.RPCStaticProviderEndpoint{fleetTestPrimary("backup-1", "", b1.URL)})

	result, err := server.SendRelay(context.Background(), "", `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x00000000000000000000000000000000000000c1","latest"]}`,
		http.MethodPost, "", "", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	// The reply can come back as soon as two answers agree, so the third call may still be on its way.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, [3]int64{1, 1, 1}, [3]int64{p1Calls.Load(), p2Calls.Load(), b1Calls.Load()},
			"[sim-1, sim-2, backup-1] eth_getBalance calls: all = every configured upstream, the backup included")
	}, 5*time.Second, 10*time.Millisecond)
	var participants string
	for _, md := range result.Reply.GetMetadata() {
		if md.Name == common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME {
			participants = md.Value
		}
	}
	require.Contains(t, participants, "backup-1", "the backup is reported as a participant")
}

// TestCrossValidationShortfall_AllCountsTheBackups keeps the startup prediction in step with the guard
// above: a max-participants: all policy met by primaries plus backups is not reported as a shortfall, and
// on an endpoint with no primary it is the one policy that can still be met.
func TestCrossValidationShortfall_AllCountsTheBackups(t *testing.T) {
	parser := fleetTestParser(t)
	all := fleetTestPolicy("eth_getBalance", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Participants: participantsAll}})
	numeric := fleetTestPolicy("eth_getCode", CrossValidationPolicy{Enabled: true, MaxParticipants: Bound{Floor: new(2)}})
	twoPrimaries := map[string][]string{common.DefaultProviderGroup: {"sim-1", "sim-2"}}

	t.Run("two primaries and one backup meet all = 3", func(t *testing.T) {
		resolver := fleetTestResolver(t, all)
		resolver.SetParticipantCounts(2, 3)
		requiredProviders, requiredGroups := crossValidationShortfall(resolver, "ETH1", "jsonrpc", groupSizesOf(twoPrimaries))
		require.Equal(t, [2]int{0, 0}, [2]int{requiredProviders, requiredGroups}, "[requiredProviders, requiredGroups]")
		advisory, err := validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", twoPrimaries, twoPrimaries)
		require.NoError(t, err)
		require.Nil(t, advisory, "no ATTENTION line for a policy the fleet meets")
	})
	t.Run("a backup-only endpoint can meet all, and still never a numeric policy", func(t *testing.T) {
		resolver := fleetTestResolver(t, all)
		resolver.SetParticipantCounts(0, 2)
		advisory, err := validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", map[string][]string{}, map[string][]string{})
		require.NoError(t, err)
		require.Nil(t, advisory, "max-participants: all fans out to the backups")

		resolver = fleetTestResolver(t, all, numeric)
		resolver.SetParticipantCounts(0, 2)
		advisory, err = validateCrossValidationFleet(resolver, parser, "ETH1", "jsonrpc", map[string][]string{}, map[string][]string{})
		require.NoError(t, err)
		require.NotNil(t, advisory, "the numeric policy never draws on backups")
		require.True(t, advisory.configuredShort)
	})
}

// balanceUpstream is fleetTestUpstream that answers eth_getBalance with the given result.
func balanceUpstream(t *testing.T, balance string) *httptest.Server {
	t.Helper()
	inner := fleetTestUpstream(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"eth_getBalance"`) {
			var req struct {
				ID json.RawMessage `json:"id"`
			}
			_ = json.Unmarshal(body, &req)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": balance})
			return
		}
		resp, err := http.Post(inner.URL, "application/json", bytes.NewReader(body)) //nolint:noctx // test-only forwarder
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestCrossValidationAll_ABackupCountsAsItsGroupInTheQuorum pins what CROSS-VALIDATION.md says about a
// backup that max-participants: all folds in: when the quorum counts groups for min-groups, its answer
// counts under its own group-label, or under the implicit "default" group when it has none, the same
// rule as for a primary. Primaries sim-1 (group a) and sim-2 (group b) and one backup; sim-1 and the
// backup answer 0x1, sim-2 answers 0x2, under min-groups 2 and agreement-threshold 2. Only one primary
// group agrees with the backup, so the backup's group decides the quorum.
func TestCrossValidationAll_ABackupCountsAsItsGroupInTheQuorum(t *testing.T) {
	for _, tc := range []struct {
		name        string
		backupGroup string
		wantStatus  string
	}{
		{"an unlabeled backup counts as the default group", "", "success"},
		{"a backup labeled c counts as group c", "c", "success"},
		{"a backup labeled a adds no group to sim-1's", "a", "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := bootParticipantsTestEndpoint(t, "all",
				[]*routersession.RPCStaticProviderEndpoint{
					fleetTestPrimary("sim-1", "a", balanceUpstream(t, "0x1").URL),
					fleetTestPrimary("sim-2", "b", balanceUpstream(t, "0x2").URL),
				},
				[]*routersession.RPCStaticProviderEndpoint{fleetTestPrimary("backup-1", tc.backupGroup, balanceUpstream(t, "0x1").URL)},
				map[string]any{"min-groups": 2})

			result, _ := server.SendRelay(context.Background(), "", `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x00000000000000000000000000000000000000c1","latest"]}`,
				http.MethodPost, "", "", nil, nil)
			require.NotNil(t, result)
			headers := map[string]string{}
			for _, md := range result.Reply.GetMetadata() {
				headers[md.Name] = md.Value
			}
			require.Equal(t, tc.wantStatus, headers[common.CROSS_VALIDATION_STATUS_HEADER_NAME], "headers: %v", headers)
			if tc.wantStatus == "success" {
				require.Equal(t, "backup-1,sim-1", headers[common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER])
			} else {
				require.Equal(t, common.CrossValidationReasonDiversityUnmet, headers[common.CROSS_VALIDATION_FAILURE_REASON_HEADER])
			}
		})
	}
}
