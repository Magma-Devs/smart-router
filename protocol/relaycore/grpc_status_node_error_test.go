package relaycore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// ---------------------------------------------------------------------------
// MAG-2549 — the downstream half.
//
// direct_rpc_relay.sendGRPCRelay now stamps the gRPC status onto
// RelayResult.StatusCode. This file pins WHY that mattered: results_manager
// .setValidResponse re-runs CheckResponseError against that field, and
// GrpcMessage.CheckResponseError decides purely from it. Left at its 0 default
// the router filed a node error as a SUCCESS — returned to the client as a good
// answer, counted toward the required-successes quorum, and eligible for caching.
// ---------------------------------------------------------------------------

// grpcChainMessage is a minimal ChainMessage on the gRPC interface. Only the
// three methods setValidResponse actually consults carry behaviour:
// CheckResponseError (delegated to the production GrpcMessage), GetApiCollection
// (selects TransportGRPC) and GetApi (the subscribe-tag check). The rest are
// inert stubs.
type grpcChainMessage struct{}

func (grpcChainMessage) CheckResponseError(data []byte, httpStatusCode int) (bool, string) {
	// The production implementation, deliberately not re-implemented: it is the
	// component whose contract ("non-OK status means node error") the fix relies on.
	return rpcInterfaceMessages.GrpcMessage{}.CheckResponseError(data, httpStatusCode)
}

func (grpcChainMessage) GetApiCollection() *spectypes.ApiCollection {
	return &spectypes.ApiCollection{
		CollectionData: spectypes.CollectionData{ApiInterface: "grpc"},
	}
}

func (grpcChainMessage) GetApi() *spectypes.Api {
	return &spectypes.Api{Name: "cosmos.bank.v1beta1.Query/Balance"}
}

func (grpcChainMessage) GetRPCMessage() rpcInterfaceMessages.GenericMessage {
	return &rpcInterfaceMessages.GrpcMessage{Path: "cosmos.bank.v1beta1.Query/Balance"}
}

func (grpcChainMessage) SubscriptionIdExtractor(*rpcclient.JsonrpcMessage) string { return "" }
func (grpcChainMessage) RequestedBlock() (int64, int64)                           { return 0, 0 }
func (grpcChainMessage) UpdateLatestBlockInMessage(int64, bool) bool              { return false }
func (grpcChainMessage) AppendHeader([]pairingtypes.Metadata)                     {}
func (grpcChainMessage) GetExtensions() []*spectypes.Extension                    { return nil }
func (grpcChainMessage) OverrideExtensions([]string, *extensionslib.ExtensionParser) {
}
func (grpcChainMessage) DisableErrorHandling()                          {}
func (grpcChainMessage) TimeoutOverride(...time.Duration) time.Duration { return 0 }
func (grpcChainMessage) GetForceCacheRefresh() bool                     { return false }
func (grpcChainMessage) SetForceCacheRefresh(bool) bool                 { return false }
func (grpcChainMessage) GetRawRequestHash() ([]byte, error)             { return nil, nil }
func (grpcChainMessage) GetRequestedBlocksHashes() []string             { return nil }
func (grpcChainMessage) UpdateEarliestInMessage(int64) bool             { return false }
func (grpcChainMessage) SetExtension(*spectypes.Extension)              {}
func (grpcChainMessage) GetUsedDefaultValue() bool                      { return false }
func (grpcChainMessage) GetParseDirective() *spectypes.ParseDirective   { return nil }
func (grpcChainMessage) IsBatch() bool                                  { return false }

func grpcProtocolMessage() chainlib.ProtocolMessage {
	return chainlib.NewProtocolMessage(
		grpcChainMessage{},
		nil,
		&pairingtypes.RelayPrivateData{ApiUrl: "cosmos.bank.v1beta1.Query/Balance"},
		"dapp",
		"127.0.0.1",
	)
}

// TestResultsManager_GRPCStatusResponseIsANodeError drives the real
// ResultsManager.SetResponse with the RelayResult shape sendGRPCRelay now
// produces, and asserts it is filed as a node error rather than a success.
//
// The StatusCode=0 subtest is the regression itself: it reproduces the pre-fix
// RelayResult and shows the identical error body landing in successResults.
func TestResultsManager_GRPCStatusResponseIsANodeError(t *testing.T) {
	errorBody := []byte(`{"error_message":"invalid address: decoding bech32 failed","error_code":3}`)

	t.Run("status code carried through is filed as a node error", func(t *testing.T) {
		rm := NewResultsManager(1, "LAVA")

		nodeErr := rm.SetResponse(&RelayResponse{
			RelayResult: common.RelayResult{
				Reply:       &pairingtypes.RelayReply{Data: errorBody},
				StatusCode:  int(codes.InvalidArgument),
				IsNodeError: true,
				Finalized:   true,
			},
		}, grpcProtocolMessage())

		require.Error(t, nodeErr,
			"setValidResponse returns non-nil exactly when it parsed a node error")

		success, nodeErrors, _, protocolErrors := rm.GetResults()
		require.Equal(t, 0, success,
			"a gRPC INVALID_ARGUMENT must never count as a successful relay")
		require.Equal(t, 1, nodeErrors)
		require.Equal(t, 0, protocolErrors,
			"this is a node error, not a protocol error — the relay itself completed")
	})

	// The pre-fix shape. Kept as an executable statement of the bug rather than a
	// comment: if someone drops StatusCode from the RelayResult again, the test
	// above goes red and this one explains what changed.
	t.Run("REGRESSION: a zero status code is filed as a success", func(t *testing.T) {
		rm := NewResultsManager(1, "LAVA")

		nodeErr := rm.SetResponse(&RelayResponse{
			RelayResult: common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: errorBody},
				StatusCode: 0, // what this arm left behind before MAG-2549
				Finalized:  true,
			},
		}, grpcProtocolMessage())

		require.NoError(t, nodeErr)
		success, nodeErrors, _, _ := rm.GetResults()
		require.Equal(t, 1, success,
			"documents the defect: GrpcMessage.CheckResponseError reads the status code alone, "+
				"so a zero made an error body indistinguishable from a good answer")
		require.Equal(t, 0, nodeErrors)
	})
}

// TestGrpcCheckResponseError_DependsOnlyOnStatusCode isolates the contract the
// test above depends on, so a change to GrpcMessage is attributed to GrpcMessage
// rather than surfacing as a confusing failure in the results manager.
func TestGrpcCheckResponseError_DependsOnlyOnStatusCode(t *testing.T) {
	body := []byte(`{"error_message":"invalid address","error_code":3}`)

	hasError, message := rpcInterfaceMessages.GrpcMessage{}.CheckResponseError(body, int(codes.InvalidArgument))
	require.True(t, hasError, "any non-OK gRPC status is a node error")
	require.Empty(t, message,
		"and it yields NO message — which is why sendGRPCRelay must fall back to the node's "+
			"own status message before classifying")

	hasError, _ = rpcInterfaceMessages.GrpcMessage{}.CheckResponseError(body, 0)
	require.False(t, hasError,
		"the same error body with a zero status reads as success — the MAG-2549 defect")
}
