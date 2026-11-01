package chainlib

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// replyingChainRouter answers every SendNodeMsg with a fixed 200 body, standing in for an
// upstream that put its refusal inside the reply rather than on the status line — what a
// gateway does on an open WebSocket connection, where there is no status line at all.
type replyingChainRouter struct{ body string }

func (r replyingChainRouter) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend, extensions []string) (*RelayReplyWrapper, common.NodeUrl, string, error) {
	return &RelayReplyWrapper{StatusCode: 200, RelayReply: &pairingtypes.RelayReply{Data: []byte(r.body)}}, common.NodeUrl{Url: "ws://node.example"}, "ETH1", nil
}

func (r replyingChainRouter) ExtensionsSupported(internalPath string, extensions []string) bool {
	return true
}

func newEthChainFetcherWithReply(t *testing.T, body string) *ChainFetcher {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "ETH1")
	require.NoError(t, err)
	cp, err := NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	cp.SetSpec(spec)
	return &ChainFetcher{
		endpoint:    &lavasession.RPCProviderEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
		chainRouter: replyingChainRouter{body: body},
		chainParser: cp,
	}
}

const rateLimitBody = `{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"Too Many Requests"}}`

// A JSON-RPC error body with code 429 used to reach the ChainTracker as an untyped "failed to
// parse" error, so no hold-off and no backoff ever applied to it. The head fetch must type it
// as the rate limit it is (MAG-4165); a body carries no Retry-After, so none is claimed.
func TestChainFetcher_FetchLatestBlockNum_TypesRateLimitBody(t *testing.T) {
	cf := newEthChainFetcherWithReply(t, rateLimitBody)

	_, err := cf.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok, "a body names no Retry-After")
}

// The block-hash path (fork detection, tracker init) meets the same bodies.
func TestChainFetcher_FetchBlockHashByNum_TypesRateLimitBody(t *testing.T) {
	cf := newEthChainFetcherWithReply(t, rateLimitBody)

	_, err := cf.FetchBlockHashByNum(context.Background(), 100)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
}

// Any other error body keeps its existing shape: a failed fetch, not a rate limit.
func TestChainFetcher_FetchLatestBlockNum_OtherErrorBodyIsNotRateLimited(t *testing.T) {
	cf := newEthChainFetcherWithReply(t, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_blockNumber does not exist/is not available"}}`)

	_, err := cf.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, common.StatusCodeError429)
}
