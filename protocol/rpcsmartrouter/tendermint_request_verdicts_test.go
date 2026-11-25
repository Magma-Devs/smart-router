package rpcsmartrouter

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// MAG-5760 end to end, through the real Tendermint parser (the COSMOSHUB spec), sender and session
// manager, with three providers that all answer alike.

// threeCometBFTNodes starts three nodes that answer every request with body under HTTP 200, and
// returns their URLs and the requests they got between them.
func threeCometBFTNodes(t *testing.T, body string) ([]string, *atomic.Int64) {
	t.Helper()
	hits := &atomic.Int64{}
	return []string{
		cometBFTNode(t, http.StatusOK, body, hits),
		cometBFTNode(t, http.StatusOK, body, hits),
		cometBFTNode(t, http.StatusOK, body, hits),
	}, hits
}

// A request no node can serve as asked is the caller's mistake: the first node's answer goes back,
// no other node is asked, and no endpoint is blamed. A range or a page a node reads from what it
// holds can differ elsewhere: other nodes are asked, and none is blamed.
func TestCometBFTRequestErrorsBlameNoEndpoint(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct {
		name, request, data string
		oneNodeAsked        bool
	}{
		{"block at height 0", `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"0"}}`, "height must be greater than 0, but got 0", true},
		{"validators at height -1", `{"jsonrpc":"2.0","id":1,"method":"validators","params":{"height":"-1"}}`, "height must be greater than 0, but got -1", true},
		{"validators, page 0", `{"jsonrpc":"2.0","id":1,"method":"validators","params":{"page":"0"}}`, "page should be within [1, 1] range, given 0", true},
		{"tx_search, a query that does not parse", `{"jsonrpc":"2.0","id":1,"method":"tx_search","params":{"query":"tx.height=="}}`, "offset 11: got TOp, wanted TNumber", true},
		{"blockchain, min above max", `{"jsonrpc":"2.0","id":1,"method":"blockchain","params":{"minHeight":"500","maxHeight":"400"}}`, "min height 500 can't be greater than max height 400", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			benchAfter(t, 1) // one strike would disable an endpoint
			spy := &scoringSpyOptimizer{
				ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "COSMOSHUB"),
			}
			reply := cometBFTInternalError(tc.data)
			nodes, hits := threeCometBFTNodes(t, reply)
			result, endpoints, _ := sendToCometBFTNodes(t, "COSMOSHUB", tc.request, spy, nodes...)

			require.NotNil(t, result)
			require.NotNil(t, result.Reply)
			require.Equal(t, reply, string(result.Reply.Data), "the client gets the node's body unchanged")
			if tc.oneNodeAsked {
				require.Equal(t, int64(1), hits.Load(), "the caller's mistake ends at the first node")
			}
			for _, endpoint := range endpoints {
				require.Zero(t, endpoint.ConnectionRefusals, "the endpoint's refusal counter")
				require.True(t, endpoint.IsEnabled())
			}
			time.Sleep(100 * time.Millisecond) // failure samples reach the optimizer asynchronously
			require.Zero(t, spy.failures.Load(), "OnSessionFailure must not be called")
		})
	}
}

// A failed abci_query's log is the application's text, not the node's. Whatever it says, no
// provider is held off and no endpoint takes a strike for it.
func TestAppQueryLogHoldsNoProviderOffAndBlamesNoEndpoint(t *testing.T) {
	rand.InitRandomSeed()
	for _, text := range []string{"rate limit", "too many requests", "all attempts exhausted", "node is syncing"} {
		t.Run(text, func(t *testing.T) {
			benchAfter(t, 1)
			optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "COSMOSHUB")
			reply := `{"jsonrpc":"2.0","id":1,"result":{"response":{"code":6,"log":"unknown query: [app ` + text + `]: unknown request","info":"","index":"0","key":null,"value":null,"proofOps":null,"height":"100","codespace":"sdk"}}}`
			nodes, hits := threeCometBFTNodes(t, reply)
			request := `{"jsonrpc":"2.0","id":1,"method":"abci_query","params":{"path":"/app/` + text + `","data":"","height":"0","prove":false}}`
			_, endpoints, server := sendToCometBFTNodes(t, "COSMOSHUB", request, optimizer, nodes...)

			require.Positive(t, hits.Load())
			require.Empty(t, server.sessionManager.ProviderRoutingSnapshot().HeldOff, "no provider is held off for the application's words")
			for _, endpoint := range endpoints {
				require.Zero(t, endpoint.ConnectionRefusals, "no endpoint takes a strike for the application's words")
				require.True(t, endpoint.IsEnabled())
			}
		})
	}
}
