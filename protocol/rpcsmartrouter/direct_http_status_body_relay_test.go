package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-4448, through the whole relay with two providers: the first answers a 5xx, and the second
// would answer the read. A 5xx around the caller's own mistake is the answer every provider gives,
// so the second is never asked, whether the status is the node's or a gateway's. A node's own limit,
// and a gateway's error page, still send the relay on to the second, which answers.
func TestSendRelay_A5xxAroundTheCallersMistakeIsNotAskedOfThePeer(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	invalidParams := `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument 0: hex string has length 3"}}`
	revert := `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted","data":"0x"}}`
	peer := refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`}
	cases := []struct {
		name      string
		first     refusalReply
		peerAsked bool
	}{
		{"the caller's mistake inside a node's 503", refusalReply{http.StatusServiceUnavailable, invalidParams}, false},
		{"the caller's mistake inside a node's 500", refusalReply{http.StatusInternalServerError, invalidParams}, false},
		{"the caller's mistake inside a gateway's 502", refusalReply{http.StatusBadGateway, invalidParams}, false},
		{"a revert under code 3 inside a node's 503", refusalReply{http.StatusServiceUnavailable, revert}, false},
		{"a gateway's error page", refusalReply{http.StatusBadGateway, `<html>Bad Gateway</html>`}, true},
		{"a node's own log response cap inside a 503", refusalReply{http.StatusServiceUnavailable, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"response is too big"}}`}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hits, server := newRefusalTestServer(t, chainParser, logs, tc.first, peer)

			result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)

			if !tc.peerAsked {
				require.Equal(t, [2]int64{1, 0}, loadRefusalHits(hits), "the peer is not asked the same mistake")
				require.Error(t, sendErr, "the relay ends on the node's reply")
				require.NotNil(t, result.GetClientFacing(), "the client gets the node's reply")
				require.Equal(t, tc.first.status, result.GetClientFacing().UpstreamStatus)
				require.JSONEq(t, tc.first.body, string(result.GetClientFacing().UpstreamBody))
				return
			}
			require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits), "the relay fails over to the peer")
			require.NoError(t, sendErr)
			require.Contains(t, string(result.GetReply().GetData()), "0x10", "the peer's answer reaches the caller")
		})
	}
}
