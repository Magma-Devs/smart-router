package chainlib

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// countingChainRouter fails every send with a fixed error and counts attempts per api
// name, so the tests below can see exactly how many requests Validate's retry loops
// spend against a refusing upstream.
type countingChainRouter struct {
	mu    sync.Mutex
	calls map[string]int
	err   error
}

func (r *countingChainRouter) SendNodeMsg(ctx context.Context, ch chan interface{}, chainMessage ChainMessageForSend, extensions []string) (*RelayReplyWrapper, string, *rpcclient.ClientSubscription, common.NodeUrl, string, error) {
	r.mu.Lock()
	r.calls[chainMessage.GetApi().Name]++
	r.mu.Unlock()
	return nil, "", nil, common.NodeUrl{}, "", r.err
}

func (r *countingChainRouter) ExtensionsSupported(internalPath string, extensions []string) bool {
	return true
}

func (r *countingChainRouter) maxAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	maxCount := 0
	for _, c := range r.calls {
		if c > maxCount {
			maxCount = c
		}
	}
	return maxCount
}

func newEthValidateFetcher(t *testing.T, router ChainRouter) *ChainFetcher {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "ETH1")
	require.NoError(t, err)
	cp, err := NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	cp.SetSpec(spec)
	return &ChainFetcher{
		endpoint: &lavasession.RPCProviderEndpoint{
			ChainID:      "ETH1",
			ApiInterface: spectypes.APIInterfaceJsonRPC,
			NodeUrls:     []common.NodeUrl{{Url: "http://node:8545"}},
		},
		chainRouter: router,
		chainParser: cp,
	}
}

// A rate-limited attempt ends the retry budget: retrying back-to-back inside the same
// limiter window cannot succeed and only adds to the load that produced the 429.
func TestValidate_RateLimitStopsTheRetryBurst(t *testing.T) {
	router := &countingChainRouter{calls: map[string]int{}, err: rateLimited429("60")}
	cf := newEthValidateFetcher(t, router)

	_ = cf.Validate(context.Background())
	require.Positive(t, len(router.calls), "validation must have sent something")
	require.Equal(t, 1, router.maxAttempts(), "a rate-limited attempt must not be retried back-to-back; got %v", router.calls)
}

// Any other failure keeps the full budget — the zero-delay retries exist for a node
// that is still booting, and that behaviour must not change.
func TestValidate_OtherFailuresKeepThreeAttempts(t *testing.T) {
	router := &countingChainRouter{calls: map[string]int{}, err: errors.New("connection refused")}
	cf := newEthValidateFetcher(t, router)

	_ = cf.Validate(context.Background())
	require.Equal(t, 3, router.maxAttempts(), "a plain failure keeps the 3-attempt startup budget; got %v", router.calls)
}
