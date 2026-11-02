package rpcsmartrouter

import (
	"cmp"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// relay_end's transaction fields: the id a submitted transaction has on its chain (tx_hash), and its
// execution result (tx_status, tx_error) when the submit's reply reports one. docs/RELAY-END-EVENT.md
// lists, per chain, which method each is read from.

// tx_status values.
const (
	relayEndTxSuccess = "success"
	relayEndTxFailure = "failure"
)

// relayEndTxReplyLimit bounds the reply relay_end reads for a transaction: a submit's answer is small.
const relayEndTxReplyLimit = 64 << 10

// The submit methods whose reply, or request, names the transaction.
const (
	solanaSendMethod           = "sendTransaction"
	suiJSONRPCExecuteMethod    = "sui_executeTransactionBlock"
	suiGraphQLExecuteRootField = "executeTransaction"
)

// cosmosTxSubmitMethods are the Cosmos submits whose reply carries the transaction's hash, by api
// name without the leading slash. The Tendermint RPC's other write, broadcast_evidence, answers with
// the evidence's hash, which names no transaction.
var cosmosTxSubmitMethods = map[string]struct{}{
	"broadcast_tx_sync":     {},
	"broadcast_tx_async":    {},
	"broadcast_tx_commit":   {},
	"cosmos/tx/v1beta1/txs": {},
}

// readTransaction fills the transaction fields from the reply the client got, nil when it got none or
// one over relayEndTxReplyLimit, and for an EVM submit from the signed transaction it sends. A batch
// is not read: its reply answers several requests.
func (e *relayEnd) readTransaction(protocolMessage chainlib.ProtocolMessage, reply []byte) {
	if e.isBatch || protocolMessage == nil {
		return
	}
	api := protocolMessage.GetApi()
	if api == nil {
		return
	}
	if len(reply) > relayEndTxReplyLimit {
		reply = nil
	}
	switch e.apiInterface {
	case spectypes.APIInterfaceJsonRPC:
		switch api.Name {
		case rywWriteMethod:
			e.txHash = evmTxHash(protocolMessage, reply)
		case solanaSendMethod:
			e.txHash = solanaTxSignature(reply)
		case suiJSONRPCExecuteMethod:
			e.txHash, e.txStatus, e.txError = suiJSONRPCExecution(reply)
		}
	case spectypes.APIInterfaceGraphQL:
		// One root field: a mutation that selects several has no single transaction to name.
		if api.Name == suiGraphQLExecuteRootField {
			e.txHash, e.txStatus, e.txError = suiGraphQLExecution(reply)
		}
	case spectypes.APIInterfaceTendermintRPC, spectypes.APIInterfaceRest:
		// A Cosmos node answers a refused transaction with a normal reply, so the outcome stays ok and
		// the reply's code says whether the node took it. Read on a write only: REST names the
		// transaction query by events with the submit's path. gRPC's reply is protobuf and is not read.
		if len(reply) == 0 || !e.stateful {
			return
		}
		tx, hash, found := cosmosTxOutcome(reply)
		if found {
			e.txCode, e.txCodespace = tx.Code, tx.Codespace
			if *tx.Code != 0 {
				e.txLog = relayEndText(cmp.Or(tx.Log, tx.RawLog))
			}
		}
		if _, submit := cosmosTxSubmitMethods[strings.TrimPrefix(api.Name, "/")]; submit && isCosmosTxHash(hash) {
			e.txHash = hash
		}
	}
}

// evmTxHash is an eth_sendRawTransaction's hash: the one its reply returns, or, when the reply
// returns none (a node's refusal, or no reply at all), the one computed from the signed transaction
// in its params, so a refused write or one whose outcome is unknown can still be looked up.
func evmTxHash(protocolMessage chainlib.ProtocolMessage, reply []byte) string {
	if hash := rywReplyTxHash(reply); hash != "" {
		if _, err := hexutil.Decode(hash); err == nil {
			return hash
		}
	}
	rawHex, ok := firstStringParam(protocolMessage)
	if !ok {
		return ""
	}
	raw, err := hexutil.Decode(rawHex)
	if err != nil {
		return ""
	}
	_, hash, ok := evmTxSenderAndHash(raw)
	if !ok {
		return ""
	}
	return hash
}

// solanaTxSignature is a sendTransaction's signature, the transaction's id: its reply's result, a
// 64-byte value in base58. Stellar's sendTransaction, which answers with an object, names none.
func solanaTxSignature(reply []byte) string {
	var answer struct {
		Result string `json:"result"`
	}
	if json.Unmarshal(reply, &answer) != nil || !isBase58OfSize(answer.Result, 64) {
		return ""
	}
	return answer.Result
}

// suiJSONRPCExecution reads a sui_executeTransactionBlock reply: result.digest, and the execution
// status in result.effects, which the reply carries when the client asked for showEffects.
func suiJSONRPCExecution(reply []byte) (hash, status, errorText string) {
	var answer struct {
		Result *struct {
			Digest  string `json:"digest"`
			Effects *struct {
				Status *struct {
					Status string `json:"status"`
					Error  string `json:"error"`
				} `json:"status"`
			} `json:"effects"`
		} `json:"result"`
	}
	if json.Unmarshal(reply, &answer) != nil || answer.Result == nil {
		return "", "", ""
	}
	if isBase58OfSize(answer.Result.Digest, 32) {
		hash = answer.Result.Digest
	}
	if effects := answer.Result.Effects; effects != nil && effects.Status != nil {
		switch effects.Status.Status {
		case "success":
			status = relayEndTxSuccess
		case "failure":
			status, errorText = relayEndTxFailure, relayEndText(effects.Status.Error)
		}
	}
	return hash, status, errorText
}

// suiGraphQLExecution reads an executeTransaction mutation's reply: data's one entry, under the root
// field's name or the client's alias for it, and in it effects, of which the reply carries only what
// the client selected. effects.digest is the transaction's digest, as is effects.transaction.digest.
func suiGraphQLExecution(reply []byte) (hash, status, errorText string) {
	var answer struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if json.Unmarshal(reply, &answer) != nil || len(answer.Data) != 1 {
		return "", "", ""
	}
	var execution struct {
		Effects *struct {
			Status         string `json:"status"`
			Digest         string `json:"digest"`
			ExecutionError *struct {
				Message string `json:"message"`
			} `json:"executionError"`
			Transaction *struct {
				Digest string `json:"digest"`
			} `json:"transaction"`
		} `json:"effects"`
	}
	for _, field := range answer.Data {
		if json.Unmarshal(field, &execution) != nil {
			return "", "", ""
		}
	}
	effects := execution.Effects
	if effects == nil {
		return "", "", ""
	}
	digest := effects.Digest
	if effects.Transaction != nil {
		digest = cmp.Or(effects.Transaction.Digest, digest)
	}
	if isBase58OfSize(digest, 32) {
		hash = digest
	}
	switch effects.Status {
	case "SUCCESS":
		status = relayEndTxSuccess
	case "FAILURE":
		status = relayEndTxFailure
		if effects.ExecutionError != nil {
			errorText = relayEndText(effects.ExecutionError.Message)
		}
	}
	return hash, status, errorText
}

// isCosmosTxHash reports whether text is a CometBFT transaction hash: a SHA-256, 32 bytes in hex, in
// either case.
func isCosmosTxHash(text string) bool {
	if len(text) != 64 {
		return false
	}
	_, err := hex.DecodeString(text)
	return err == nil
}

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// isBase58OfSize reports whether text is size bytes in base58, Bitcoin's alphabet, which Solana and
// Sui write ids in.
func isBase58OfSize(text string, size int) bool {
	// A base58 digit carries more than 5 bits, so size bytes never take more than 2*size of them.
	if text == "" || len(text) > 2*size {
		return false
	}
	leadingZeros := len(text) - len(strings.TrimLeft(text, "1"))
	value, radix, digit := new(big.Int), big.NewInt(58), new(big.Int)
	for i := 0; i < len(text); i++ {
		index := strings.IndexByte(base58Alphabet, text[i])
		if index < 0 {
			return false
		}
		value.Mul(value, radix).Add(value, digit.SetInt64(int64(index)))
	}
	return leadingZeros+len(value.Bytes()) == size
}
