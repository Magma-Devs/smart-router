package relaycore

// A node reply is classified as the node's answer: the results manager must not run its text
// through the transport table (MAG-4242 item 5 added "unexpected eof" and "no such host" to it).

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

type restReplyChainMessage struct{ grpcChainMessage }

func (restReplyChainMessage) CheckResponseError(data []byte, httpStatusCode int) (bool, string) {
	return rpcInterfaceMessages.RestMessage{}.CheckResponseError(data, httpStatusCode)
}

func (restReplyChainMessage) GetApiCollection() *spectypes.ApiCollection {
	return &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{ApiInterface: "rest"}}
}

func (restReplyChainMessage) GetApi() *spectypes.Api {
	return &spectypes.Api{Name: "/cosmos/tx/v1beta1/simulate"}
}

func TestResultsManager_NodeReplyTextIsNotATransportFault(t *testing.T) {
	rm := NewResultsManager(1, "COSMOSHUB")
	pm := chainlib.NewProtocolMessage(restReplyChainMessage{}, nil, &pairingtypes.RelayPrivateData{ApiUrl: "/cosmos/tx/v1beta1/simulate"}, "dapp", "127.0.0.1")
	nodeErr := rm.SetResponse(&RelayResponse{RelayResult: common.RelayResult{
		Reply:       &pairingtypes.RelayReply{Data: []byte(`{"code":3,"message":"unexpected EOF","details":[]}`)},
		StatusCode:  400,
		IsNodeError: true,
	}}, pm)
	require.Error(t, nodeErr)
	best := rm.GetBestNodeErrorMessageForUser()
	require.Equal(t, common.CategoryExternal, best.LavaError.Category, "a node's answer is classified as the node's answer, not as a transport failure")
}
