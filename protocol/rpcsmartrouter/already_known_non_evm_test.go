package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// TestNonEVMRead_AlreadyKnownWordingGoesToThePeer_MAG5489 sends a Solana read to an upstream that
// answers it inside an HTTP 200 with a -32000 that says "already known", and a peer that serves
// it. "already known" is the EVM node software's duplicate answer, so on Solana it is no duplicate:
// the read goes on to the peer, and the first upstream is counted against. Read as a duplicate it
// was non-retryable and not the node's fault, so the read ended on the first upstream.
func TestNonEVMRead_AlreadyKnownWordingGoesToThePeer_MAG5489(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "SOLANA", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	read := []byte(`{"jsonrpc":"2.0","method":"getBalance","params":["11111111111111111111111111111111"],"id":1}`)
	for _, message := range []string{"already known", "AlreadyKnown", "known transaction: 0xabc"} {
		t.Run(message, func(t *testing.T) {
			refuser := refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"` + message + `"}}`}
			hits, server := newRefusalTestServerForChain(t, "SOLANA", chainParser, logs, refuser, refusalReply{http.StatusOK, refusalPeerAnswer})

			result, sendErr := sendRefusalTestRequestBody(t, ctx, chainParser, server, read)
			require.NoError(t, sendErr)
			require.JSONEq(t, refusalPeerAnswer, string(result.Reply.Data), "the read must reach the peer, not end on the first upstream")
			require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits))
		})
	}
}
