package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// Sui's ExecuteTransaction messages as the protobuf runtime marshals them from Sui's own definitions
// (MystenLabs/sui-rust-sdk, crates/sui-rpc/vendored/proto/sui/rpc/v2, commit d1fa927, through
// dynamicpb), so the field numbers under test are Sui's, not this package's. The transaction is
// suiGRPCSnapshotBcs, a TransactionData from Sui's own test snapshot
// (crates/sui-indexer-alt-e2e-tests/tests/graphql/transactions/bcs.snap), whose digest Sui reports
// as suiGRPCSnapshotDigest in the same snapshot.
const (
	suiGRPCSnapshotBcs    = "AAACAAgqAAAAAAAAAAAgp7AycDh4qnTDEmk1eJ/R1NfhEdWRGwkkfWljBhwxK1oCAgABAQAAAQECAAABAQD8zJpCG7sTwaZqGqmPCtdQKe3pSFd3nGkVtE+UBouSHgHhFa5/+bVpRPr+9v0BXVEKL9ljttQuyKYmZAnA6kD7NgMAAAAAAAAAIEzrkQF+I9IJ4x0c30+YdFP63Y1T+vAeA9YRM4e19EVk/MyaQhu7E8GmahqpjwrXUCnt6UhXd5xpFbRPlAaLkh7oAwAAAAAAAADyBSoBAAAAAA=="
	suiGRPCSnapshotDigest = "gHrD7i4MD93GsSzM3ZYibg2CQpBCmZFnxGdANrXFYMP"

	// The request: the transaction's BCS, one signature and read_mask "effects".
	suiGRPCExecuteRequest = "0af0010aed010a0f5472616e73616374696f6e4461746112d90100000200082a000000000000000020a7b032703878aa74c3126935789fd1d4d7e111d5911b09247d6963061c312b5a020200010100000101020000010100fccc9a421bbb13c1a66a1aa98f0ad75029ede94857779c6915b44f94068b921e01e115ae7ff9b56944fafef6fd015d510a2fd963b6d42ec8a6266409c0ea40fb360300000000000000204ceb91017e23d209e31d1cdf4f987453fadd8d53faf01e03d6113387b5f44564fccc9a421bbb13c1a66a1aa98f0ad75029ede94857779c6915b44f94068b921ee80300000000000000f2052a010000000012050a031201001a090a0765666665637473"
	// A request whose transaction is given field by field, with no BCS.
	suiGRPCExecuteRequestNoBcs = "0a0718012a03307831"

	// Sui's default read mask, effects: the effects digest, status success, epoch, version and the
	// transaction's digest.
	suiGRPCExecutedSuccess = "0a652263122c385862586e4c7170463956546e4771627a424751596b35754d324a677a775879454e68714344686d4c52705a18022202080128073a2b674872443769344d4439334773537a4d335a596962673243517042436d5a466e784764414e725846594d50"
	// The effects of a transaction that ran and failed: InsufficientCoinBalance in command 0.
	suiGRPCExecutedFailure = "0a5f225d222e0800122a0a24496e73756666696369656e74436f696e42616c616e636520696e20636f6d6d616e642030100018083a2b674872443769344d4439334773537a4d335a596962673243517042436d5a466e784764414e725846594d50"
	// read_mask "digest": the transaction's digest alone.
	suiGRPCExecutedDigestOnly = "0a2d0a2b674872443769344d4439334773537a4d335a596962673243517042436d5a466e784764414e725846594d50"
	// read_mask "checkpoint": neither the digest nor the effects.
	suiGRPCExecutedCheckpointOnly = "0a0330b960"
)

func suiGRPCExecuteMessage(t *testing.T, body []byte) chainlib.ProtocolMessage {
	t.Helper()
	grpcMessage := &rpcInterfaceMessages.GrpcMessage{Path: suiGRPCExecuteMethod, Msg: body}
	grpcMessage.SetChainFamily(common.ChainFamilySui)
	return chainlib.NewProtocolMessage(&mockChainMessage{apiInterface: spectypes.APIInterfaceGrpc, api: &spectypes.Api{Name: suiGRPCExecuteMethod}, rpcMessage: grpcMessage}, nil, nil, "test-dapp", "127.0.0.1")
}

// TestRelayEnd_SuiGRPCExecuteReplies (MAG-5186): the transaction's digest, status and error read
// from each read mask's reply, and from the request when the reply holds no digest.
func TestRelayEnd_SuiGRPCExecuteReplies(t *testing.T) {
	for _, tc := range []struct {
		name, reply             string
		hash, status, errorText string
	}{
		{"the default mask: the effects", suiGRPCExecutedSuccess, suiGRPCSnapshotDigest, relayEndTxSuccess, ""},
		{"a transaction that ran and failed", suiGRPCExecutedFailure, suiGRPCSnapshotDigest, relayEndTxFailure, "InsufficientCoinBalance in command 0"},
		{"the digest alone", suiGRPCExecutedDigestOnly, suiGRPCSnapshotDigest, "", ""},
		{"neither", suiGRPCExecutedCheckpointOnly, "", "", ""},
		{"an empty transaction", "0a00", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hash, status, errorText := suiGRPCExecution(relayEndHexBytes(t, tc.reply))
			require.Equal(t, tc.hash, hash)
			require.Equal(t, tc.status, status)
			require.Equal(t, tc.errorText, errorText)
		})
	}
	hash, status, _ := suiGRPCExecution([]byte(`{"error_message":"system is overloaded","error_code":14}`))
	require.Empty(t, hash, "a node error's reply is the router's JSON, which does not decode")
	require.Empty(t, status)

	t.Run("the digest from the request", func(t *testing.T) {
		require.Equal(t, suiGRPCSnapshotDigest, suiGRPCRequestDigest(suiGRPCExecuteMessage(t, relayEndHexBytes(t, suiGRPCExecuteRequest))))
		jsonRequest := `{"transaction":{"bcs":{"name":"TransactionData","value":"` + suiGRPCSnapshotBcs + `"}},"signatures":[{"bcs":{"value":"AA=="}}],"readMask":"effects"}`
		require.Equal(t, suiGRPCSnapshotDigest, suiGRPCRequestDigest(suiGRPCExecuteMessage(t, []byte(jsonRequest))), "a JSON request")
		require.Empty(t, suiGRPCRequestDigest(suiGRPCExecuteMessage(t, relayEndHexBytes(t, suiGRPCExecuteRequestNoBcs))), "no BCS, no digest")
		require.Empty(t, suiGRPCRequestDigest(suiGRPCExecuteMessage(t, []byte(`{"transaction":{"bcs":{"value":"not base64!"}}}`))))
	})
}

// suiWriteRelay drives sendGRPCRelay for a Sui ExecuteTransaction against an endpoint that answers
// with the status under test, built the way handleGRPCError builds it.
func suiWriteRelay(t *testing.T, path string, code codes.Code, nodeMessage string, md map[string][]string) *common.RelayResult {
	t.Helper()
	body, err := json.Marshal(routersession.GRPCNodeErrorResponse{ErrorMessage: nodeMessage, ErrorCode: uint32(code)})
	require.NoError(t, err)
	sender := &DirectRPCRelaySender{
		directConnection: &fakeDirectConn{
			resp: &routersession.DirectRPCResponse{Data: body, StatusCode: int(code), Metadata: md},
			err:  routersession.NewGRPCStatusError(uint32(code), nodeMessage, nil, md),
		},
		endpointName: "grpc-endpoint",
		chainFamily:  common.ChainFamilySui,
	}
	grpcMessage := &rpcInterfaceMessages.GrpcMessage{Path: path}
	grpcMessage.SetChainFamily(common.ChainFamilySui)
	result, relayErr := sender.sendGRPCRelay(context.Background(), &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{apiInterface: "grpc", rpcMessage: grpcMessage}}, time.Second)
	require.NoError(t, relayErr)
	require.NotNil(t, result)
	return result
}

// The sender files a Sui send by Sui's rows (MAG-5186): RESOURCE_EXHAUSTED is a throttle whether or
// not the node names a wait (G-W8, decided 18 Nov), and a lock conflict is the caller's (G-W4). Both
// keep the endpoint's health, as the availability gate reads it. A Sui read keeps the status table.
func TestSendGRPCRelay_SuiWriteRows(t *testing.T) {
	t.Run("G-W8 with no wait: the default hold-off", func(t *testing.T) {
		result := suiWriteRelay(t, suiGRPCExecuteMethod, codes.ResourceExhausted, "Validators overloaded", nil)
		assert.True(t, result.IsRateLimited)
		assert.False(t, result.IsNodeAtFault)
		assert.Zero(t, result.RetryAfter, "no wait named: the hold-off uses its default")
		assert.False(t, shouldFailSessionForResult(nil, result))
	})
	t.Run("G-W8 with the wait the node names", func(t *testing.T) {
		result := suiWriteRelay(t, suiGRPCExecuteMethod, codes.ResourceExhausted, "Validators overloaded", map[string][]string{"grpc-retry-pushback-ms": {"1500"}})
		assert.True(t, result.IsRateLimited)
		assert.Equal(t, 1500*time.Millisecond, result.RetryAfter)
	})
	t.Run("G-W4 a lock conflict", func(t *testing.T) {
		result := suiWriteRelay(t, suiGRPCExecuteMethod, codes.FailedPrecondition, "Failed to sign transaction by a quorum of validators because of locked objects. Conflicting Transactions:\n{}", nil)
		assert.True(t, result.IsNonRetryable)
		assert.False(t, result.IsNodeAtFault)
		assert.False(t, shouldFailSessionForResult(nil, result), "on main this was UNKNOWN_ERROR and scored")
	})
	t.Run("a Sui read's RESOURCE_EXHAUSTED is no throttle", func(t *testing.T) {
		result := suiWriteRelay(t, "sui.rpc.v2.NameService/LookupName", codes.ResourceExhausted, "name has expired", nil)
		assert.False(t, result.IsRateLimited, "the status table decides a read, as on main")
	})
}

// Sui sends through the listener (MAG-5186): the client gets the node's reply and status unchanged,
// the node gets each send once, and relay_end records each copy's outcome by Sui's rows and the
// transaction's digest, from the reply or, when the reply has none, from the request.
func TestRelayEnd_SuiGRPCWrites(t *testing.T) {
	withFreshRelayHoldoff(t)
	read := captureRelayEnds(t)
	var (
		mu       sync.Mutex
		answer   func() ([]byte, error)
		received [][]byte
	)
	node := startStalledReflectionNodeAnswering(t, func(_ string, body []byte) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, body)
		return answer()
	})
	client := startRelayEndGRPCRouterFor(t, "SUI", fmt.Sprintf(`{"name": %q, "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 1}}`, suiGRPCExecuteMethod), node)

	request := relayEndHexBytes(t, suiGRPCExecuteRequest)
	// The throttle runs last: it holds the one provider off.
	for _, tc := range []struct {
		name     string
		reply    string
		nodeErr  error
		wantCode codes.Code
	}{
		{name: "sui_grpc_execute_accepted", reply: suiGRPCExecutedSuccess},
		{name: "sui_grpc_execute_ran_and_failed", reply: suiGRPCExecutedFailure},
		{name: "sui_grpc_execute_reply_without_digest", reply: suiGRPCExecutedCheckpointOnly},
		{name: "sui_grpc_execute_refused_before_submission", nodeErr: status.Error(codes.InvalidArgument, "invalid transaction: missing field `kind`"), wantCode: codes.InvalidArgument},
		{name: "sui_grpc_execute_lock_conflict", nodeErr: status.Error(codes.FailedPrecondition, "Failed to sign transaction by a quorum of validators because of locked objects. Conflicting Transactions:\n{}"), wantCode: codes.FailedPrecondition},
		{name: "sui_grpc_execute_no_finality", nodeErr: status.Error(codes.Unavailable, "timed-out before finality could be reached"), wantCode: codes.Unavailable},
		{name: "sui_grpc_execute_throttled", nodeErr: status.Error(codes.ResourceExhausted, "Validators overloaded"), wantCode: codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reply []byte
			if tc.reply != "" {
				reply = relayEndHexBytes(t, tc.reply)
			}
			mu.Lock()
			answer, received = func() ([]byte, error) { return reply, tc.nodeErr }, nil
			mu.Unlock()
			seen := len(read())

			callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer callCancel()
			var got []byte
			err := client.Invoke(callCtx, "/"+suiGRPCExecuteMethod, request, &got, grpc.ForceCodec(stalledReflectionCodec{}))
			if tc.nodeErr != nil {
				require.Equal(t, tc.wantCode, status.Code(err), "the node's status reached the client")
				require.Equal(t, status.Convert(tc.nodeErr).Message(), status.Convert(err).Message(), "and its message, as the node wrote it")
			} else {
				require.NoError(t, err)
				require.Equal(t, reply, got, "the node's reply reached the client as it sent it")
			}
			mu.Lock()
			require.Equal(t, [][]byte{request}, received, "the node got the send once, byte for byte")
			mu.Unlock()

			events := newRelayEndsSince(read, seen)
			require.Len(t, events, 1, "one relay_end per gRPC call")
			event := normalizeRelayEnd(t, events[0], false)
			event["GUID"] = "<minted by the listener>"
			requireRelayEndGolden(t, tc.name, event)
		})
	}
}

// Sui's gRPC SimulateTransaction is never served from the cache (MAG-5186), whatever the spec's
// deterministic flag says, and only on gRPC: the names are full method paths.
func TestCacheExclusionReasonRefusesSuiGRPCSimulate(t *testing.T) {
	message := func(name, apiInterface string) chainlib.ProtocolMessage {
		return chainlib.NewProtocolMessage(&mockChainMessage{
			apiInterface: apiInterface,
			api:          &spectypes.Api{Name: name, Category: spectypes.SpecCategory{Deterministic: true}},
			rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: name},
		}, nil, nil, "test-dapp", "127.0.0.1")
	}
	simulate := message("sui.rpc.v2.TransactionExecutionService/SimulateTransaction", spectypes.APIInterfaceGrpc)
	require.Equal(t, "never-cache", cacheExclusionReason(simulate))
	require.Contains(t, cacheExclusionWhy(simulate), "SimulateTransaction runs a transaction against the answering provider's own state")
	require.Empty(t, cacheExclusionReason(message("sui.rpc.v2.LedgerService/GetCheckpoint", spectypes.APIInterfaceGrpc)), "a read that is the same everywhere stays cacheable")
	require.Empty(t, cacheExclusionReason(message("sui.rpc.v2.TransactionExecutionService/SimulateTransaction", spectypes.APIInterfaceRest)), "read for gRPC only")
}
