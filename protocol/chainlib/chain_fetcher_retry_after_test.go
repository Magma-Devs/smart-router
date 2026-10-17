package chainlib

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// erringChainRouter fails every SendNodeMsg with a fixed error, standing in for the
// chain proxy layer so the tests below exercise only ChainFetcher's wrapping.
type erringChainRouter struct{ err error }

func (r erringChainRouter) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend, extensions []string) (*RelayReplyWrapper, common.NodeUrl, string, error) {
	return nil, common.NodeUrl{}, "", r.err
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

// validate aborts a url whose head probe fails; ValidateCollect used to carry on and verify every
// latest-distance check against NOT_APPLICABLE (2^64-1 once cast), reporting the url green to
// `smartrouter health`. The failed probe is now a failed fetch-latest-block row, each check that
// needs the head is failed as "latest block unknown", and the rest still run (MAG-4278).
func TestValidateCollect_FailedHeadProbeFailsTheURL(t *testing.T) {
	cf := newEthChainFetcherWithRouterErr(t, errors.New("node down"))
	cf.endpoint.NodeUrls = []common.NodeUrl{{Url: "http://127.0.0.1:1"}}
	verifications, err := cf.chainParser.GetVerifications(nil, "", cf.endpoint.ApiInterface)
	require.NoError(t, err)

	results := cf.ValidateCollect(context.Background())
	require.Len(t, results, 1)
	row := results[0]
	require.Equal(t, spectypes.NOT_APPLICABLE, row.LatestBlock)
	require.Len(t, row.Verifications, 1+len(verifications), "the head row, then one row per verification")
	require.Equal(t, "fetch-latest-block", row.Verifications[0].Name)
	require.False(t, row.Verifications[0].Ok)
	require.Contains(t, row.Verifications[0].Error, "node down")

	headDependent, headFree := 0, 0
	for i, verification := range verifications {
		got := row.Verifications[i+1]
		require.Equal(t, verification.Name, got.Name)
		require.False(t, got.Ok, "every relay fails, so no verification passes: %s", got.Name)
		if verificationNeedsHead(verification) {
			headDependent++
			require.Contains(t, got.Error, "latest block unknown", got.Name)
			require.Contains(t, got.Error, "node down", "the head error is carried: %s", got.Name)
		} else {
			headFree++
			require.NotContains(t, got.Error, "latest block unknown", "a check that needs no head is run: %s", got.Name)
		}
	}
	require.Positive(t, headDependent, "ETH1 has a latest-distance check, or this test proves nothing")
	require.Positive(t, headFree, "ETH1 has a check that needs no head, or this test proves nothing")
}

// headOnlyRefusingRouter answers every relay except the head probe, the way a node behind a proxy
// that refuses or rate-limits eth_blockNumber does, and counts the methods it is sent.
type headOnlyRefusingRouter struct {
	mu   sync.Mutex
	sent map[string]int
}

func (r *headOnlyRefusingRouter) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend, extensions []string) (*RelayReplyWrapper, common.NodeUrl, string, error) {
	method := chainMessage.GetApi().Name
	r.mu.Lock()
	r.sent[method]++
	r.mu.Unlock()
	if method == "eth_blockNumber" {
		return nil, common.NodeUrl{}, "", errors.New("head probe refused")
	}
	return &RelayReplyWrapper{StatusCode: http.StatusOK, RelayReply: &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}}, common.NodeUrl{}, "", nil
}

func (r *headOnlyRefusingRouter) ExtensionsSupported(internalPath string, extensions []string) bool {
	return true
}

// A node that answers everything but the head probe keeps the checks it passes in the health
// report: chain-id needs no head and is reported ok, the latest-distance check (pruning) is failed
// as "latest block unknown" without being sent, and the url still does not pass (MAG-4278).
func TestValidateCollect_HeadFailureStillReportsHeadIndependentChecks(t *testing.T) {
	router := &headOnlyRefusingRouter{sent: map[string]int{}}
	cf := newEthChainFetcherWithRouterErr(t, nil)
	cf.chainRouter = router
	cf.endpoint.NodeUrls = []common.NodeUrl{{Url: "http://127.0.0.1:1"}}

	results := cf.ValidateCollect(context.Background())
	require.Len(t, results, 1)
	row := results[0]
	require.Equal(t, spectypes.NOT_APPLICABLE, row.LatestBlock, "no head was measured")

	byName := map[string]VerificationResult{}
	allOk := true
	for _, v := range row.Verifications {
		byName[v.Name] = v
		allOk = allOk && v.Ok
	}
	require.False(t, allOk, "a url whose head probe failed does not pass")
	require.False(t, byName["fetch-latest-block"].Ok)
	require.Contains(t, byName["fetch-latest-block"].Error, "head probe refused")

	chainID, reported := byName["chain-id"]
	require.True(t, reported, "chain-id needs no head; it must still be reported")
	require.True(t, chainID.Ok, chainID.Error)

	pruning, reported := byName["pruning"]
	require.True(t, reported, "a check that needs the head is reported failed, not dropped")
	require.False(t, pruning.Ok)
	require.Contains(t, pruning.Error, "latest block unknown")
	require.Contains(t, pruning.Error, "head probe refused", "the head error says why")

	router.mu.Lock()
	defer router.mu.Unlock()
	require.Positive(t, router.sent["eth_chainId"], "the head-independent check was sent")
	require.Zero(t, router.sent["eth_getBlockByNumber"], "the latest-distance check is not sent against an unknown head")
}
