package relaycore

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// countingChainMessage is a JSON-RPC request that counts how often its reply is read for an error,
// and answers what it is told to.
type countingChainMessage struct {
	grpcChainMessage
	checks   *int
	hasError bool
}

func (m countingChainMessage) CheckResponseError([]byte, int) (bool, string) {
	*m.checks++
	if m.hasError {
		return true, "the node refused"
	}
	return false, ""
}

func (countingChainMessage) GetApiCollection() *spectypes.ApiCollection {
	return &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{ApiInterface: "jsonrpc"}}
}

func (countingChainMessage) GetApi() *spectypes.Api { return &spectypes.Api{Name: "getBlock"} }

func countingProtocolMessage(checks *int, hasError bool) chainlib.ProtocolMessage {
	return chainlib.NewProtocolMessage(countingChainMessage{checks: checks, hasError: hasError}, nil, &pairingtypes.RelayPrivateData{}, "dapp", "127.0.0.1")
}

func blockReply() *pairingtypes.RelayReply {
	return &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":{"blockHeight":1}}`)}
}

// A reply the sender read and found clean is filed as a success without being read again.
func TestResultsManager_AReplyCheckedCleanIsNotReadAgain(t *testing.T) {
	checks := 0
	rm := NewResultsManager(1, "SOLANA")
	result := common.RelayResult{Reply: blockReply(), StatusCode: 200}
	result.MarkReplyCheckedClean()

	// hasError is true: were the reply read here, it would be filed as a node error.
	require.NoError(t, rm.SetResponse(&RelayResponse{RelayResult: result}, countingProtocolMessage(&checks, true)))
	require.Zero(t, checks, "the reply was read a second time")
	success, nodeErrors, _, protocolErrors := rm.GetResults()
	require.Equal(t, []int{1, 0, 0}, []int{success, nodeErrors, protocolErrors})
}

// A reply nobody recorded as checked is read, once, and its verdict decides.
func TestResultsManager_AnUncheckedReplyIsRead(t *testing.T) {
	for _, hasError := range []bool{false, true} {
		checks := 0
		rm := NewResultsManager(1, "SOLANA")
		nodeErr := rm.SetResponse(&RelayResponse{RelayResult: common.RelayResult{Reply: blockReply(), StatusCode: 200}}, countingProtocolMessage(&checks, hasError))
		require.Equal(t, 1, checks)
		success, nodeErrors, _, _ := rm.GetResults()
		if hasError {
			require.EqualError(t, nodeErr, "the node refused")
			require.Equal(t, []int{0, 1}, []int{success, nodeErrors})
		} else {
			require.NoError(t, nodeErr)
			require.Equal(t, []int{1, 0}, []int{success, nodeErrors})
		}
	}
}

// The record names the bytes that were checked. A reply replaced after the check, or filed under
// another status code, is read where it lands.
func TestResultsManager_AReplyChangedAfterItsCheckIsRead(t *testing.T) {
	t.Run("replaced reply", func(t *testing.T) {
		checks := 0
		rm := NewResultsManager(1, "SOLANA")
		result := common.RelayResult{Reply: blockReply(), StatusCode: 200}
		result.MarkReplyCheckedClean()
		result.Reply.Data = []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"[redacted]"}}`)
		require.Error(t, rm.SetResponse(&RelayResponse{RelayResult: result}, countingProtocolMessage(&checks, true)))
		require.Equal(t, 1, checks)
	})
	t.Run("another status code", func(t *testing.T) {
		checks := 0
		rm := NewResultsManager(1, "SOLANA")
		result := common.RelayResult{Reply: blockReply(), StatusCode: 200}
		result.MarkReplyCheckedClean()
		result.StatusCode = 500
		require.Error(t, rm.SetResponse(&RelayResponse{RelayResult: result}, countingProtocolMessage(&checks, true)))
		require.Equal(t, 1, checks)
	})
}
