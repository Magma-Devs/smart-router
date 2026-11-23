package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainlibtest/fixture"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// MAG-5323: on three write paths a node's refusal of a transaction arrives as an ordinary success,
// and a stateful broadcast ended on it whenever the refusing node answered first.
const (
	refusingNode  = "refuser@test"
	acceptingNode = "accepter@test"

	cosmosTxHash = "5C504ED432CB51138BCF09AA5E8A410DD4A1E204EF84BFED1BE16DFBA1B22060"

	// CometBFT 0.38's ResultBroadcastTx and ResultBroadcastTxCommit, as a Cosmos SDK node answers.
	tendermintSyncAccepted  = `{"jsonrpc":"2.0","id":1,"result":{"code":0,"data":"","log":"[]","codespace":"","hash":"` + cosmosTxHash + `"}}`
	tendermintSyncSequence  = `{"jsonrpc":"2.0","id":1,"result":{"code":32,"data":"","log":"account sequence mismatch, expected 7, got 6: incorrect account sequence","codespace":"sdk","hash":"` + cosmosTxHash + `"}}`
	tendermintCommitApplied = `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":0,"data":null,"log":"[]","info":"","gas_wanted":"200000","gas_used":"61234","events":[],"codespace":""},"tx_result":{"code":0,"data":"EiYKJC9jb3Ntb3MuYmFuay52MWJldGExLk1zZ1NlbmRSZXNwb25zZQ==","log":"","info":"","gas_wanted":"200000","gas_used":"80321","events":[],"codespace":""},"hash":"` + cosmosTxHash + `","height":"1234"}}`
	tendermintCommitRanOut  = `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":0,"data":null,"log":"[]","info":"","gas_wanted":"200000","gas_used":"61234","events":[],"codespace":""},"tx_result":{"code":11,"data":null,"log":"out of gas in location: WriteFlat; gasWanted: 200000, gasUsed: 201234: out of gas","info":"","gas_wanted":"200000","gas_used":"201234","events":[],"codespace":"sdk"},"hash":"` + cosmosTxHash + `","height":"1234"}}`
	tendermintCommitRefused = `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":32,"data":null,"log":"account sequence mismatch, expected 7, got 6: incorrect account sequence","info":"","gas_wanted":"0","gas_used":"0","events":[],"codespace":"sdk"},"tx_result":{"code":0,"data":null,"log":"","info":"","gas_wanted":"0","gas_used":"0","events":[],"codespace":""},"hash":"` + cosmosTxHash + `","height":"0"}}`

	// java-tron's answers: /wallet/broadcasttransaction prints the Return message (bytes, so hex),
	// leaving out result:false and code:SUCCESS as protobuf defaults.
	tronAccepted  = `{"result":true,"txid":"77ddfa7093cc5f745c0d3a54abb89ef070f983343c05e0f89e5a52f3e5401299"}`
	tronDuplicate = `{"code":"DUP_TRANSACTION_ERROR","txid":"77ddfa7093cc5f745c0d3a54abb89ef070f983343c05e0f89e5a52f3e5401299"}`
	tronSigError  = `{"code":"SIGERROR","txid":"77ddfa7093cc5f745c0d3a54abb89ef070f983343c05e0f89e5a52f3e5401299","message":"56616c6964617465207369676e6174757265206572726f723a206d69737320736967206f7220636f6e7472616374"}`
)

// cosmosBroadcastTxReply is a BroadcastTxResponse on the wire: tx_response (1), a TxResponse whose
// height is 1, txhash 2, codespace 3, code 4 and raw_log 6. Protobuf leaves zeros off the wire.
func cosmosBroadcastTxReply(height int64, codespace string, code uint32, rawLog string) []byte {
	var txResponse []byte
	if height != 0 {
		txResponse = protowire.AppendTag(txResponse, 1, protowire.VarintType)
		txResponse = protowire.AppendVarint(txResponse, uint64(height))
	}
	txResponse = protowire.AppendTag(txResponse, 2, protowire.BytesType)
	txResponse = protowire.AppendString(txResponse, cosmosTxHash)
	if codespace != "" {
		txResponse = protowire.AppendTag(txResponse, 3, protowire.BytesType)
		txResponse = protowire.AppendString(txResponse, codespace)
	}
	if code != 0 {
		txResponse = protowire.AppendTag(txResponse, 4, protowire.VarintType)
		txResponse = protowire.AppendVarint(txResponse, uint64(code))
	}
	if rawLog != "" {
		txResponse = protowire.AppendTag(txResponse, 6, protowire.BytesType)
		txResponse = protowire.AppendString(txResponse, rawLog)
	}
	reply := protowire.AppendTag(nil, 1, protowire.BytesType)
	return protowire.AppendBytes(reply, txResponse)
}

// newStatefulBroadcastProcessor builds a relay processor for one broadcast fanned out to the refusing
// and the accepting node, parsed by the chain's own parser so each reply goes through its classifier.
func newStatefulBroadcastProcessor(t *testing.T, spec spectypes.Spec, apiInterface, url, body, connectionType string) *RelayProcessor {
	t.Helper()
	ctx := context.Background()
	chainParser, err := chainlib.NewChainParser(apiInterface)
	require.NoError(t, err)
	chainParser.SetSpec(spec)
	chainMsg, err := chainParser.ParseMsg(url, []byte(body), connectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "127.0.0.1")
	usedProviders := routersession.NewUsedProviders(nil)
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateful))

	require.NoError(t, usedProviders.TryLockSelection(ctx))
	usedProviders.AddUsed(routersession.ConsumerSessionsMap{refusingNode: &routersession.SessionInfo{}, acceptingNode: &routersession.SessionInfo{}}, nil)
	return relayProcessor
}

// A broadcast of each kind, the refusal a node answers it with, and the acceptance another sends.
type broadcastRefusalCase struct {
	name                                string
	spec                                spectypes.Spec
	apiInterface, url, body, connection string
	refusal, acceptance                 []byte
}

func broadcastRefusalCases() []broadcastRefusalCase {
	tendermint := fixture.CreateMockCosmosBroadcastSpec("COSMOSBCAST", spectypes.APIInterfaceTendermintRPC)
	grpcSpec := fixture.CreateMockCosmosBroadcastSpec("COSMOSBCASTG", spectypes.APIInterfaceGrpc)
	tron := fixture.CreateMockTronSpec("TRONBCAST")
	const grpcBroadcast = "cosmos.tx.v1beta1.Service/BroadcastTx"
	grpcRequest := string([]byte{0x0a, 0x02, 0x01, 0x02, 0x10, 0x02}) // tx_bytes, mode SYNC
	return []broadcastRefusalCase{
		{
			name: "broadcast_tx_sync, code 32", spec: tendermint, apiInterface: spectypes.APIInterfaceTendermintRPC,
			body:    `{"jsonrpc":"2.0","method":"broadcast_tx_sync","params":{"tx":"CpIBCo8B"},"id":1}`,
			refusal: []byte(tendermintSyncSequence), acceptance: []byte(tendermintSyncAccepted),
		},
		{
			name: "broadcast_tx_commit, check_tx code 32", spec: tendermint, apiInterface: spectypes.APIInterfaceTendermintRPC,
			body:    `{"jsonrpc":"2.0","method":"broadcast_tx_commit","params":{"tx":"CpIBCo8B"},"id":1}`,
			refusal: []byte(tendermintCommitRefused), acceptance: []byte(tendermintCommitApplied),
		},
		{
			name: "BroadcastTx, code 19", spec: grpcSpec, apiInterface: spectypes.APIInterfaceGrpc, url: grpcBroadcast, body: grpcRequest,
			refusal: cosmosBroadcastTxReply(0, "sdk", 19, ""), acceptance: cosmosBroadcastTxReply(0, "", 0, "[]"),
		},
		{
			name: "BroadcastTx, code 32", spec: grpcSpec, apiInterface: spectypes.APIInterfaceGrpc, url: grpcBroadcast, body: grpcRequest,
			refusal:    cosmosBroadcastTxReply(0, "sdk", 32, "account sequence mismatch, expected 7, got 6: incorrect account sequence"),
			acceptance: cosmosBroadcastTxReply(0, "", 0, "[]"),
		},
		{
			name: "broadcasttransaction, DUP_TRANSACTION_ERROR", spec: tron, apiInterface: spectypes.APIInterfaceRest,
			url: "/wallet/broadcasttransaction", body: `{"txID":"77dd","raw_data":{},"signature":["00"]}`, connection: http.MethodPost,
			refusal: []byte(tronDuplicate), acceptance: []byte(tronAccepted),
		},
		{
			name: "broadcasttransaction, SIGERROR", spec: tron, apiInterface: spectypes.APIInterfaceRest,
			url: "/wallet/broadcasttransaction", body: `{"txID":"77dd","raw_data":{},"signature":["00"]}`, connection: http.MethodPost,
			refusal: []byte(tronSigError), acceptance: []byte(tronAccepted),
		},
		{
			name: "broadcasthex, SIGERROR", spec: tron, apiInterface: spectypes.APIInterfaceRest,
			url: "/wallet/broadcasthex", body: `{"transaction":"0a8a010a02"}`, connection: http.MethodPost,
			refusal:    []byte(`{"result":false,"code":"SIGERROR","message":"Validate signature error: miss sig or contract","txid":"77dd"}`),
			acceptance: []byte(`{"result":true,"code":"SUCCESS","message":"","txid":"77dd"}`),
		},
	}
}

func (tc broadcastRefusalCase) processor(t *testing.T) *RelayProcessor {
	return newStatefulBroadcastProcessor(t, tc.spec, tc.apiInterface, tc.url, tc.body, tc.connection)
}

// TestStatefulBroadcastWaitsThroughARefusal is MAG-5323 at the relay processor: the refusing node
// answers first, the accepting node after. The caller must get the acceptance.
func TestStatefulBroadcastWaitsThroughARefusal(t *testing.T) {
	for _, tc := range broadcastRefusalCases() {
		t.Run(tc.name, func(t *testing.T) {
			relayProcessor := tc.processor(t)

			deliverReply(relayProcessor, refusingNode, tc.refusal)
			requireStillWaiting(t, relayProcessor, 1)

			deliverReply(relayProcessor, acceptingNode, tc.acceptance)
			answerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, relayProcessor.WaitForResults(answerCtx))
			hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
			require.True(t, hasResults)

			returnedResult, err := relayProcessor.ProcessingResult()
			require.NoError(t, err)
			require.Equal(t, tc.acceptance, returnedResult.Reply.Data)
			require.Equal(t, acceptingNode, returnedResult.ProviderInfo.ProviderAddress)
		})
	}
}

// TestStatefulBroadcastEveryNodeRefusesReturnsTheFirstRefusal: a client re-sends a transaction, and
// every node refuses it. The caller gets the first refusal as the node sent it.
func TestStatefulBroadcastEveryNodeRefusesReturnsTheFirstRefusal(t *testing.T) {
	for _, tc := range broadcastRefusalCases() {
		t.Run(tc.name, func(t *testing.T) {
			relayProcessor := tc.processor(t)

			deliverReply(relayProcessor, refusingNode, tc.refusal)
			deliverReply(relayProcessor, acceptingNode, tc.refusal)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, relayProcessor.WaitForResults(ctx), "both answered, nothing left in flight")
			successes, nodeErrors, _, _ := relayProcessor.GetResults()
			require.Equal(t, 0, successes, "two refusals are not a success")
			require.Equal(t, 2, nodeErrors)

			returnedResult, err := relayProcessor.ProcessingResult()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, returnedResult.StatusCode)
			require.Equal(t, tc.refusal, returnedResult.Reply.Data)
			require.Equal(t, refusingNode, returnedResult.ProviderInfo.ProviderAddress)
		})
	}
}

// TestStatefulBroadcastTxCommitThatRanIsTheAnswer: broadcast_tx_commit's tx_result is the
// transaction's execution in a block. A non-zero code there means the transaction ran and failed,
// which is its outcome, so the first such reply ends the broadcast as the answer.
func TestStatefulBroadcastTxCommitThatRanIsTheAnswer(t *testing.T) {
	tc := broadcastRefusalCases()[1]
	relayProcessor := tc.processor(t)

	deliverReply(relayProcessor, refusingNode, []byte(tendermintCommitRanOut))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx), "a transaction that ran is the answer")
	successes, nodeErrors, _, _ := relayProcessor.GetResults()
	require.Equal(t, 1, successes)
	require.Zero(t, nodeErrors)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, tendermintCommitRanOut, string(returnedResult.Reply.Data))
}

// deliverReply hands the processor a completed attempt answered with HTTP 200, or gRPC OK, the
// status each of these nodes answers a refusal with.
func deliverReply(relayProcessor *RelayProcessor, provider string, body []byte) {
	deliverJsonrpcReply(relayProcessor, provider, string(body))
}
