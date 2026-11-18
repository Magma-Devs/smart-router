package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlibtest"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// TestJSONRPCRelay_ARevertThatMentionsARateLimitHoldsNoProviderOff (MAG-5385) sends an eth_call
// through SendParsedRelay to two upstreams that both revert it with a reason that mentions a rate
// limit. The reason is the contract's words, not the provider's: the caller gets the revert, and
// neither provider is held off as if it had rate-limited us.
func TestJSONRPCRelay_ARevertThatMentionsARateLimitHoldsNoProviderOff(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlibtest.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	call := []byte(`{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x0000000000000000000000000000000000000001","data":"0x"},"latest"],"id":1}`)
	for _, reason := range []string{"rate limit exceeded", "Too Many Requests"} {
		t.Run(reason, func(t *testing.T) {
			revert := refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted: ` + reason + `","data":"0x08c379a0"}}`}
			require.Equal(t, common.RouterErrorChainExecutionReverted.Code, classifyLikeTheJSONRPCSender(t, chainParser, revert).Code,
				"a revert classifies as one, whatever its reason says")

			_, server := newRefusalTestServer(t, chainParser, logs, revert, revert)
			result, sendErr := sendRefusalTestRequestBody(t, ctx, chainParser, server, call)
			require.NoError(t, sendErr, "a node's answer is relayed to the caller")
			require.NotNil(t, result.Reply)
			require.JSONEq(t, revert.body, string(result.Reply.Data), "the caller gets the revert as the node sent it")
			require.Empty(t, server.sessionManager.ProviderRoutingSnapshot().HeldOff,
				"a revert's reason is the contract's words: no provider may be held off for it")
		})
	}
}
