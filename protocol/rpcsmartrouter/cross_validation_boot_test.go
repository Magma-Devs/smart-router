package rpcsmartrouter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestBootConfigBarrier pins the contract the boot-time cross-validation refusal rests on (MAG-3604):
// no endpoint proceeds past its own configuration check until every endpoint has arrived, and one
// endpoint's failure is what every other endpoint sees.
func TestBootConfigBarrier(t *testing.T) {
	t.Run("every endpoint passes -> wait returns nil for all", func(t *testing.T) {
		b := newBootConfigBarrier(3)
		var wg sync.WaitGroup
		results := make([]error, 3)
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				b.arrive(nil)
				results[i] = b.wait()
			}(i)
		}
		wg.Wait()
		for i, r := range results {
			require.NoError(t, r, "endpoint %d", i)
		}
	})

	t.Run("one failure releases the others and every waiter sees it", func(t *testing.T) {
		b := newBootConfigBarrier(3)
		boom := errors.New("policy names a method the spec does not serve")
		var wg sync.WaitGroup
		results := make([]error, 3)
		verdicts := []error{nil, boom, nil}
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				b.arrive(verdicts[i])
				results[i] = b.wait()
			}(i)
		}
		wg.Wait()
		for i, r := range results {
			require.ErrorIs(t, r, boom, "endpoint %d must see the sibling's failure", i)
		}
	})

	t.Run("an endpoint that returns before its check still releases the others", func(t *testing.T) {
		b := newBootConfigBarrier(2)
		done := make(chan error, 1)
		go func() {
			b.arrive(nil) // this endpoint's config passed
			done <- b.wait()
		}()
		// The second endpoint aborts before its check (e.g. spec load failed) and arrives with a failure,
		// exactly as CreateSmartRouterEndpoint's deferred arrival does. Without that arrival the first
		// endpoint would block here forever.
		b.arrive(errBootAbortedBeforeConfigCheck)
		select {
		case got := <-done:
			require.ErrorIs(t, got, errBootAbortedBeforeConfigCheck)
		case <-time.After(5 * time.Second):
			t.Fatal("wait() did not return; the early-return endpoint failed to release the barrier")
		}
	})
}

// TestCreateSmartRouterEndpoint_RefusesUnservedMethodBeforeDialing drives the real boot path with a policy
// naming a method the ETH1 spec does not serve, and a provider URL that is not a reachable node. The method
// guard runs right after the spec loads, before any provider is dialed or any listener bound, so the error
// is the policy refusal — not a dial or verification failure — which is what keeps a refused config from
// coming up as a half-started router (MAG-3604).
func TestCreateSmartRouterEndpoint_RefusesUnservedMethodBeforeDialing(t *testing.T) {
	rand.InitRandomSeed()
	prevPolicies := viper.Get(common.CrossValidationConfigName)
	viper.Set(common.CrossValidationConfigName, map[string]any{"policies": []any{map[string]any{
		"chain-id": "ETH1", "api-interface": "jsonrpc", "method": "eth_thisMethodDoesNotExist",
		"enabled": true, "agreement-threshold": 2, "max-participants": 2,
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
		rpcEndpoints: []*lavasession.RPCEndpoint{endpoint},
		strategy:     provideroptimizer.StrategyBalanced,
		cmdFlags:     common.ConsumerCmdFlags{StaticSpecPaths: []string{"../../specs/ethereum.json"}},
		// blackhole:1 would fail loudly if dialed; the guard fires before we get there, so it never is.
		staticProvidersList: []*lavasession.RPCStaticProviderEndpoint{{
			Name: "sim-1", ChainID: "ETH1", ApiInterface: "jsonrpc",
			NodeUrls: []common.NodeUrl{{Url: "http://127.0.0.1:1"}},
		}},
		// bootBarrier is nil: called directly, the endpoint runs its check inline and waits for nobody.
	}
	err = rpsr.CreateSmartRouterEndpoint(ctx, endpoint, make(chan error, 1), &common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{},
		map[string]*sync.Mutex{"ETH1": {}}, options, "test-router", logs, nil, metricsManager, nil)
	require.ErrorContains(t, err, "does not serve under that name", "the refusal is the method guard, before any provider dial")
	require.ErrorContains(t, err, "eth_thisMethodDoesNotExist")
	require.Empty(t, rpsr.sessionManagers, "no session manager was built: the endpoint refused before registering providers")
}
