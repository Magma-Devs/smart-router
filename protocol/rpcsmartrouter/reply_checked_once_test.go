package rpcsmartrouter

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// relayThroughResultsManager sends one JSON-RPC relay through relayInnerDirect against an upstream
// that answers HTTP 200 with body, hands the result to a results manager as the relay goroutine
// does, and returns the result, each text the request's CheckResponseError was asked about, and
// the manager's verdict.
func relayThroughResultsManager(t *testing.T, body string, check func(data []byte) (bool, string)) (*common.RelayResult, []string, error) {
	t.Helper()
	h := newDirectRelayHarness(t, http.StatusOK, body)
	var asked []string
	msg := &mockChainMessage{
		requestData: []byte(`{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[1]}`),
		checkResponseError: func(data []byte, _ int) (bool, string) {
			asked = append(asked, string(data))
			return check(data)
		},
	}
	relayResult := &common.RelayResult{}
	_, err := h.rpcss.relayInnerDirect(
		context.Background(), h.session, relayResult, 5*time.Second, 30*time.Second,
		msg, msg.requestData, func() bool { return false }, nil, false,
	)
	require.NoError(t, err)

	rm := relaycore.NewResultsManager(1, "SOLANA")
	protocolMessage := chainlib.NewProtocolMessage(msg, nil, &pairingtypes.RelayPrivateData{}, "dapp", "127.0.0.1")
	nodeErr := rm.SetResponse(&relaycore.RelayResponse{RelayResult: *relayResult}, protocolMessage)
	return relayResult, asked, nodeErr
}

// A clean reply is read for a node error once on its way from the upstream to the results
// manager: by the sender, whose answer the results manager takes.
func TestRelayInnerDirect_ACleanReplyIsCheckedOnce(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"result":{"blockHeight":1,"transactions":[]}}`
	relayResult, asked, nodeErr := relayThroughResultsManager(t, body, func([]byte) (bool, string) { return false, "" })

	require.NoError(t, nodeErr)
	require.False(t, relayResult.IsNodeError)
	require.True(t, relayResult.ReplyCheckedClean(), "the sender's record did not reach the relay result")
	require.Equal(t, []string{body}, asked, "the reply was read for an error more than once")
}

// A node error is read again by the results manager, on the reply as it is after redaction: the
// text it files and logs is the redacted one, never the sender's.
func TestRelayInnerDirect_ANodeErrorIsReadAgainAfterRedaction(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rejected by https://node.example.com/v2/KEY123SECRET"}}`
	relayResult, asked, nodeErr := relayThroughResultsManager(t, body, func(data []byte) (bool, string) {
		return true, string(data) // the reply's own text, as a real check quotes the node's message
	})

	require.True(t, relayResult.IsNodeError)
	require.False(t, relayResult.ReplyCheckedClean())
	require.Len(t, asked, 2, "the sender and the results manager each read a node error")
	require.Contains(t, asked[0], "KEY123SECRET", "the sender reads the reply as the node sent it")
	require.NotContains(t, asked[1], "KEY123SECRET", "the results manager reads the redacted reply")
	require.Error(t, nodeErr)
	require.False(t, strings.Contains(nodeErr.Error(), "KEY123SECRET"), "the filed node error carries the key: %s", nodeErr)
}

// A node error with nothing to redact keeps its bytes, and is still read by the results manager:
// only a clean verdict is recorded, so a node error is never filed as a success.
func TestRelayInnerDirect_ANodeErrorWithNothingToRedactIsStillFiledAsOne(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"Block not available for slot 1"}}`
	relayResult, asked, nodeErr := relayThroughResultsManager(t, body, func([]byte) (bool, string) {
		return true, "Block not available for slot 1"
	})

	require.True(t, relayResult.IsNodeError)
	require.False(t, relayResult.ReplyCheckedClean())
	require.Equal(t, []string{body, body}, asked)
	require.EqualError(t, nodeErr, "Block not available for slot 1")
}
