package chainlib

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// erringChainRouter fails every SendNodeMsg with a fixed error, standing in for the
// chain proxy layer so the tests below exercise only ChainFetcher's wrapping.
type erringChainRouter struct{ err error }

func (r erringChainRouter) SendNodeMsg(ctx context.Context, ch chan interface{}, chainMessage ChainMessageForSend, extensions []string) (*RelayReplyWrapper, string, *rpcclient.ClientSubscription, common.NodeUrl, string, error) {
	return nil, "", nil, common.NodeUrl{}, "", r.err
}

func (r erringChainRouter) ExtensionsSupported(internalPath string, extensions []string) bool {
	return true
}

func newEthChainFetcherWithRouterErr(t *testing.T, routerErr error) *ChainFetcher {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "ETH1")
	require.NoError(t, err)
	cp, err := NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	cp.SetSpec(spec)
	return &ChainFetcher{
		endpoint:    &lavasession.RPCProviderEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
		chainRouter: erringChainRouter{err: routerErr},
		chainParser: cp,
	}
}

// rateLimited429 builds the error shape the HTTP-family proxies produce for an upstream
// 429: HandleStatusError's sentinel enriched by WithRetryAfter, wrapped once by the
// proxy's LavaFormatWarning.
func rateLimited429(retryAfterSeconds string) error {
	h := http.Header{}
	if retryAfterSeconds != "" {
		h.Set("Retry-After", retryAfterSeconds)
	}
	return utils.LavaFormatWarning("Received invalid status code", common.WithRetryAfter(common.StatusCodeError429, h, time.Now()))
}

// The latest-block fetch used to pass the send error as a LavaFormatDebug attribute,
// which returns a plain fmt.Errorf with no Unwrap — severing errors.Is and RetryAfterFrom
// for every consumer (startup verification, epoch re-verification, the ChainTracker).
func TestChainFetcher_FetchLatestBlockNum_PreservesRateLimitCause(t *testing.T) {
	cf := newEthChainFetcherWithRouterErr(t, rateLimited429("120"))

	_, err := cf.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	d, ok := common.RetryAfterFrom(err)
	require.True(t, ok, "Retry-After must survive the fetcher's wrapping")
	require.Equal(t, 2*time.Minute, d)
}

// Same sever existed on the block-hash path (fork detection, tracker init).
func TestChainFetcher_FetchBlockHashByNum_PreservesRateLimitCause(t *testing.T) {
	cf := newEthChainFetcherWithRouterErr(t, rateLimited429("30"))

	_, err := cf.FetchBlockHashByNum(context.Background(), 1)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	d, ok := common.RetryAfterFrom(err)
	require.True(t, ok)
	require.Equal(t, 30*time.Second, d)
}

// A plain transport failure must not read as a rate limit through the same wrapping.
func TestChainFetcher_FetchLatestBlockNum_PlainErrorIsNotRateLimit(t *testing.T) {
	cf := newEthChainFetcherWithRouterErr(t, errors.New("dial tcp 1.2.3.4:443: connect: connection refused"))

	_, err := cf.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok)
}
