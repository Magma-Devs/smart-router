package endpointstate

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// fixedReplyConnection answers every request with one 200 body, standing in for an upstream
// that put its refusal inside the reply rather than on the status line — what a gateway does on
// an open WebSocket connection, where there is no status line at all.
type fixedReplyConnection struct {
	url  string
	body string
}

func (c *fixedReplyConnection) SendRequest(context.Context, []byte, map[string]string) (*lavasession.DirectRPCResponse, error) {
	return &lavasession.DirectRPCResponse{Data: []byte(c.body), StatusCode: 200}, nil
}

func (c *fixedReplyConnection) GetProtocol() lavasession.DirectRPCProtocol {
	return lavasession.DirectRPCProtocolWS
}

func (c *fixedReplyConnection) Close() error { return nil }

func (c *fixedReplyConnection) GetURL() string { return c.url }

func (c *fixedReplyConnection) GetNodeUrl() *common.NodeUrl { return nil }

const rateLimitReplyBody = `{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"Too Many Requests"}}`

func newReplyingETHPoller(t *testing.T, body string) *EndpointPoller {
	t.Helper()
	const url = "ws://eth-ep:8546"
	return NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		&fixedReplyConnection{url: url, body: body},
		newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		"ETH1",
		spectypes.APIInterfaceJsonRPC,
	)
}

// The per-endpoint poller is the fetcher behind every per-url ChainTracker, and it sends over its
// own connection: chainlib.ChainFetcher's typing never runs for it. A JSON-RPC 429 body reached
// the tracker from here as an untyped "failed to parse response", so no hold-off ever applied and
// the start loop retried into the limit (MAG-4165). The head fetch must type the body as the rate
// limit it is; a body carries no Retry-After, so none is claimed.
func TestEndpointPoller_FetchLatestBlockNum_TypesRateLimitBody(t *testing.T) {
	poller := newReplyingETHPoller(t, rateLimitReplyBody)

	_, err := poller.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok, "a body names no Retry-After")
}

// The block-hash path (fork detection, tracker init) meets the same bodies.
func TestEndpointPoller_FetchBlockHashByNum_TypesRateLimitBody(t *testing.T) {
	poller := newReplyingETHPoller(t, rateLimitReplyBody)

	_, err := poller.FetchBlockHashByNum(context.Background(), 100)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
}

// Any other error body keeps its existing shape: a failed fetch, not a rate limit.
func TestEndpointPoller_FetchLatestBlockNum_OtherErrorBodyIsNotRateLimited(t *testing.T) {
	poller := newReplyingETHPoller(t, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_blockNumber does not exist/is not available"}}`)

	_, err := poller.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, common.StatusCodeError429)
}
