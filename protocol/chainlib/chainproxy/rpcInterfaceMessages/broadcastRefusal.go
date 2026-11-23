package rpcInterfaceMessages

import (
	"encoding/hex"
	"strings"
	"unicode/utf8"

	"github.com/goccy/go-json"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// A node's refusal of a broadcast transaction, inside an ordinary reply (MAG-5323).
//
// Three write paths answer a refused transaction with HTTP 200 or gRPC OK and say so in the body:
// Tendermint RPC's broadcast_tx_sync and broadcast_tx_commit, Cosmos gRPC's BroadcastTx, and Tron's
// /wallet/broadcasttransaction and /wallet/broadcasthex. Read as a success, a refusal ended a stateful
// broadcast whenever it arrived first, and it does arrive first when a node that already has the
// transaction (by gossip, or from an earlier send) answers before the node that takes it: the client
// was told its transaction was refused while another node accepted it. As a node error, the broadcast
// waits for its siblings, and when every node refuses, the first refusal still reaches the client
// unchanged. XRPL's submit has the same rule (CheckXRPLResponseError).
//
// Each check reads one request's reply only, chosen by the method or path, never by the chain: the
// same shape elsewhere is the answer the caller asked for (a transaction's recorded outcome, a
// simulation's verdict). The message starts with what the node said; the registry's refusal rows
// (common.CosmosTxRefusalMessage, common.TronTxRefusalMessage) decide what it means.

// CosmosGRPCBroadcastMethod is Cosmos SDK's gRPC transaction broadcast.
const CosmosGRPCBroadcastMethod = "cosmos.tx.v1beta1.Service/BroadcastTx"

// cosmosABCIResult is the part of an ABCI result that says whether the node took the transaction.
type cosmosABCIResult struct {
	Code      uint32 `json:"code"`
	Codespace string `json:"codespace"`
	Log       string `json:"log"`
}

// checkTendermintBroadcast reads a Tendermint RPC broadcast's result for the node's refusal of the
// transaction.
//
//   - broadcast_tx_sync: result.code is CheckTx's. Non-zero is a refusal.
//   - broadcast_tx_commit: result.check_tx.code is CheckTx's. Non-zero is a refusal. A non-zero
//     result.tx_result.code (deliver_tx before CometBFT 0.38) is not: the transaction is in a block
//     and ran, and that failure is its outcome, the same from every node.
//
// broadcast_tx_async answers before CheckTx, so its code is always 0, and every other method's
// result is left to the abci_query rule.
func checkTendermintBroadcast(method string, resultBytes []byte) (refused bool, errorMessage string) {
	var checkTx *cosmosABCIResult
	switch strings.TrimPrefix(method, "/") {
	case "broadcast_tx_sync":
		if err := json.Unmarshal(resultBytes, &checkTx); err != nil {
			return false, ""
		}
	case "broadcast_tx_commit":
		var result struct {
			CheckTx *cosmosABCIResult `json:"check_tx"`
		}
		if err := json.Unmarshal(resultBytes, &result); err != nil {
			return false, ""
		}
		checkTx = result.CheckTx
	default:
		return false, ""
	}
	if checkTx == nil || checkTx.Code == 0 {
		return false, ""
	}
	return true, common.CosmosTxRefusalMessage(checkTx.Codespace, checkTx.Code, checkTx.Log)
}

// checkGRPCBroadcast reads a Cosmos BroadcastTx reply for the node's refusal of the transaction:
// tx_response.code non-zero at height 0. The reply is read from the wire, so it needs neither a
// descriptor nor the node's reflection service, and a binary request to a node without reflection
// is read like any other.
//
// The height tells the two broadcast modes apart. In SYNC and ASYNC mode a reply never has one, and
// its code is CheckTx's, or the SDK's conversion of a CometBFT refusal (tx already in mempool,
// mempool is full, tx too large). BLOCK mode, served before Cosmos SDK v0.47, answers once the
// transaction is in a block, with the block's height and the execution's code: the transaction ran,
// and a failure there is its outcome.
func checkGRPCBroadcast(method string, reply []byte) (refused bool, errorMessage string) {
	if strings.TrimPrefix(method, "/") != CosmosGRPCBroadcastMethod {
		return false, ""
	}
	tx, found := ReadCosmosBroadcastTx(reply)
	if !found || tx.Code == 0 || tx.Height != 0 {
		return false, ""
	}
	return true, common.CosmosTxRefusalMessage(tx.Codespace, tx.Code, tx.RawLog)
}

// CosmosBroadcastTx is what a BroadcastTx reply says about the transaction.
type CosmosBroadcastTx struct {
	Height                  int64
	Hash, Codespace, RawLog string
	Code                    uint32
}

// ReadCosmosBroadcastTx reads a BroadcastTxResponse: its tx_response is field 1, a TxResponse whose
// height is field 1, txhash 2, codespace 3, code 4 and raw_log 6, as in cosmos-sdk's own proto
// files. Protobuf leaves a 0 off the wire, so a tx_response with no code names 0. found is false
// for a reply that does not decode as the message or carries no tx_response; a node error's reply
// is the router's JSON, which does not decode.
func ReadCosmosBroadcastTx(reply []byte) (tx CosmosBroadcastTx, found bool) {
	var txResponse []byte
	if !ProtoWireFields(reply, func(num protowire.Number, typ protowire.Type, _ uint64, value []byte) {
		if num == 1 && typ == protowire.BytesType {
			txResponse, found = value, true
		}
	}) || !found {
		return CosmosBroadcastTx{}, false
	}
	decoded := ProtoWireFields(txResponse, func(num protowire.Number, typ protowire.Type, varint uint64, value []byte) {
		switch {
		case num == 1 && typ == protowire.VarintType:
			tx.Height = int64(varint)
		case num == 2 && typ == protowire.BytesType:
			tx.Hash = string(value)
		case num == 3 && typ == protowire.BytesType:
			tx.Codespace = string(value)
		case num == 4 && typ == protowire.VarintType:
			tx.Code = uint32(varint)
		case num == 6 && typ == protowire.BytesType:
			tx.RawLog = string(value)
		}
	})
	if !decoded {
		return CosmosBroadcastTx{}, false
	}
	return tx, true
}

// ProtoWireFields calls field for each top-level field of a protobuf message, in wire order, with
// its value: a varint's number, or a length-delimited field's bytes. Fields of any other wire type
// are skipped. false when the message does not decode.
func ProtoWireFields(message []byte, field func(num protowire.Number, typ protowire.Type, varint uint64, value []byte)) bool {
	for len(message) > 0 {
		num, typ, n := protowire.ConsumeTag(message)
		if n < 0 {
			return false
		}
		message = message[n:]
		switch typ {
		case protowire.VarintType:
			varint, n := protowire.ConsumeVarint(message)
			if n < 0 {
				return false
			}
			field(num, typ, varint, nil)
			message = message[n:]
		case protowire.BytesType:
			value, n := protowire.ConsumeBytes(message)
			if n < 0 {
				return false
			}
			field(num, typ, 0, value)
			message = message[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, message)
			if n < 0 {
				return false
			}
			message = message[n:]
		}
	}
	return true
}

// tronBroadcastPaths are Tron's transaction broadcasts. /walletsolidity serves reads only.
var tronBroadcastPaths = map[string]struct{}{
	"/wallet/broadcasttransaction": {},
	"/wallet/broadcasthex":         {},
}

// checkTronBroadcast reads a Tron broadcast's reply for the node's refusal of the transaction.
// java-tron answers every broadcast with HTTP 200 and its Return message: result true when the node
// took the transaction; otherwise result false, or absent (/wallet/broadcasttransaction leaves the
// protobuf default out), beside a response code and a message. /wallet/broadcasttransaction writes
// the message's bytes in hex, /wallet/broadcasthex as text; either way it is read as text.
//
// A reply of any other shape keeps the old rule, among them {"Error": ...}, the servlet's answer to
// a request it could not process.
func checkTronBroadcast(path string, data []byte) (refused bool, errorMessage string) {
	if _, broadcast := tronBroadcastPaths[path]; !broadcast {
		return false, ""
	}
	var reply struct {
		Result  *bool  `json:"result"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return false, ""
	}
	if (reply.Result != nil && *reply.Result) || reply.Code == "" || reply.Code == "SUCCESS" {
		return false, ""
	}
	return true, common.TronTxRefusalMessage(reply.Code, tronMessageText(reply.Message))
}

// tronMessageText is a Return message as text: decoded when it is hex of UTF-8 text, as is otherwise.
func tronMessageText(message string) string {
	if decoded, err := hex.DecodeString(message); err == nil && utf8.Valid(decoded) {
		return string(decoded)
	}
	return message
}
