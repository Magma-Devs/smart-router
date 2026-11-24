package relaycore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// suiWriteChainMessage is grpcChainMessage carrying a gRPC request parsed for the Sui spec.
type suiWriteChainMessage struct {
	grpcChainMessage
	request *rpcInterfaceMessages.GrpcMessage
}

func (m suiWriteChainMessage) GetRPCMessage() rpcInterfaceMessages.GenericMessage { return m.request }

// The policy's IsChainListWrite is set for Sui's gRPC write and nothing else (MAG-5186), so the
// rate-limit carve-out is off for that write only.
func TestIsChainListWrite(t *testing.T) {
	parsed := func(path string, family common.ChainFamily) chainlib.ProtocolMessage {
		request := &rpcInterfaceMessages.GrpcMessage{Path: path}
		request.SetChainFamily(family)
		return chainlib.NewProtocolMessage(suiWriteChainMessage{request: request}, nil, nil, "dapp", "127.0.0.1")
	}
	require.True(t, isChainListWrite(parsed("sui.rpc.v2.TransactionExecutionService/ExecuteTransaction", common.ChainFamilySui)))
	require.False(t, isChainListWrite(parsed("sui.rpc.v2.TransactionExecutionService/SimulateTransaction", common.ChainFamilySui)), "a Sui read")
	require.False(t, isChainListWrite(parsed("cosmos.tx.v1beta1.Service/BroadcastTx", common.ChainFamilyCosmosSDK)), "a write on a chain with no gRPC list")
	require.False(t, isChainListWrite(grpcProtocolMessage()))
}
