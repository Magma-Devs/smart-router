package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// A write that no copy served ends with a verdict across the copies it was sent to, and on JSON-RPC
// and Tendermint RPC the client is told it (MAG-5232; Part I R-L; Part IV sections 4 and 5). Each
// copy's failure is read for what it proves about the transaction:
//
//   - refused: the copy was answered 401, 402, 403 or 429, a refusal by status;
//   - not sent: transport proof that no byte of it left the router (a refused connection, a DNS,
//     TLS or network failure before the request was written);
//   - may have run: anything else. A timeout, a cut connection, any other status (a 5xx above all,
//     often from a gateway that had already passed the transaction to a node), or a 2xx that is not
//     JSON-RPC (an HTML page, an empty body, {}).
//
// One copy that may have run makes the outcome unknown, whatever the others answered, and the client
// gets the router's OUTCOME_UNKNOWN reply rather than a node's 5xx, which reads as "failed" and gets a
// new transaction signed. Otherwise a refusal makes it rejected and the client gets the best-ranked
// node reply (relaycore.failureKind), and with neither it was not sent.
//
// A copy that answered with a 2xx JSON-RPC reply ("nonce too low", "already known", "insufficient
// funds") is out of this until the chain's write rows ship: its reply is forwarded as today with no
// Lava-Write-Outcome, and beside a copy that may have run main's handling stays
// (unclearWriteOutcomeFor).

// The Lava-Write-Outcome values (Part I R-L point 4: accepted > unknown > rejected > not_sent).
const (
	writeOutcomeAccepted = "accepted"
	writeOutcomeUnknown  = "unknown"
	writeOutcomeRejected = "rejected"
	writeOutcomeNotSent  = "not_sent"
)

// statesWriteOutcome reports whether protocolMessage is a write whose verdict this file states: a
// write on JSON-RPC or Tendermint RPC. GraphQL states its own (MAG-5122), and REST's replies are
// MAG-5256's.
func statesWriteOutcome(protocolMessage chainlib.ProtocolMessage) bool {
	if chainlib.GetStateful(protocolMessage) != common.CONSISTENCY_SELECT_ALL_PROVIDERS {
		return false
	}
	collection := protocolMessage.GetApiCollection()
	if collection == nil {
		return false
	}
	switch collection.CollectionData.ApiInterface {
	case spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC:
		return true
	default:
		return false
	}
}

// writeVerdict is what a write that no copy served ends with.
type writeVerdict struct {
	outcome string // writeOutcomeUnknown, writeOutcomeRejected or writeOutcomeNotSent
	// unclear is why the outcome is unknown, for the router's log and relay_end; nil otherwise.
	unclear *unclearWriteError
}

// errUnclearWriteErrorAnswer: every copy came back, at least one with an answer that does not prove
// the node refused the transaction: a status other than 401, 402, 403 and 429, or a 2xx that is not
// JSON-RPC.
var errUnclearWriteErrorAnswer = &unclearWriteError{reason: "a node answered with an error that does not prove the transaction was refused"}

// copyEvidence is what one copy's failure proves about the write.
type copyEvidence int

const (
	copyMayHaveRun copyEvidence = iota
	copyRefused
	copyNotSent
)

// writeVerdictFor is the verdict of a JSON-RPC or Tendermint RPC write that no copy served. ok is
// false when there is none to state: not such a write, a copy served it, or a copy answered with a
// 2xx JSON-RPC reply and the outcome is not unknown (that reply is forwarded as today).
func writeVerdictFor(protocolMessage chainlib.ProtocolMessage, relayProcessor *relaycore.RelayProcessor) (verdict writeVerdict, ok bool) {
	if !statesWriteOutcome(protocolMessage) || relayProcessor == nil {
		return writeVerdict{}, false
	}
	successes, nodeErrors, protocolErrors := relayProcessor.GetResultsDetail()
	if len(successes) > 0 {
		return writeVerdict{}, false
	}
	for _, nodeError := range nodeErrors {
		if answeredInJSONRPC(nodeError) {
			if unclear := unclearWriteOutcomeFor(protocolMessage, relayProcessor); unclear != nil {
				return writeVerdict{outcome: writeOutcomeUnknown, unclear: unclear}, true
			}
			return writeVerdict{}, false
		}
	}

	// A copy that never came back was dispatched and is silent: it may hold the transaction.
	mayHaveRun := len(nodeErrors)+len(protocolErrors) < relayProcessor.GetUsedProviders().SessionsDispatched()
	refused := false
	note := func(evidence copyEvidence) {
		switch evidence {
		case copyMayHaveRun:
			mayHaveRun = true
		case copyRefused:
			refused = true
		}
	}
	for _, nodeError := range nodeErrors {
		note(nodeErrorEvidence(nodeError))
	}
	for _, protocolError := range protocolErrors {
		note(protocolErrorEvidence(protocolError))
	}

	switch {
	case mayHaveRun:
		unclear := unclearWriteOutcomeFor(protocolMessage, relayProcessor)
		if unclear == nil {
			unclear = errUnclearWriteErrorAnswer
		}
		return writeVerdict{outcome: writeOutcomeUnknown, unclear: unclear}, true
	case refused:
		return writeVerdict{outcome: writeOutcomeRejected}, true
	default:
		// Every copy failed before a byte left the router, or none was dispatched at all.
		return writeVerdict{outcome: writeOutcomeNotSent}, true
	}
}

// answeredInJSONRPC reports whether a node error is a 2xx JSON-RPC reply: the node's own answer.
func answeredInJSONRPC(nodeError relaycore.RelayError) bool {
	if nodeError.Response == nil {
		return false
	}
	result := nodeError.Response.RelayResult
	return result.StatusCode >= http.StatusOK && result.StatusCode < http.StatusMultipleChoices &&
		rpcInterfaceMessages.IsJsonrpcReply(result.Reply.GetData())
}

// nodeErrorEvidence reads a node error that is not a 2xx JSON-RPC reply (answeredInJSONRPC).
func nodeErrorEvidence(nodeError relaycore.RelayError) copyEvidence {
	if nodeError.Response == nil {
		return copyMayHaveRun
	}
	return statusEvidence(nodeError.Response.RelayResult.StatusCode)
}

// protocolErrorEvidence reads a copy that failed without a node error.
func protocolErrorEvidence(protocolError relaycore.RelayError) copyEvidence {
	var httpErr *routersession.HTTPStatusError
	if errors.As(protocolError.Err, &httpErr) {
		return statusEvidence(httpErr.StatusCode)
	}
	if errors.Is(protocolError.Err, common.StatusCodeError429) {
		return copyRefused
	}
	switch protocolError.RouterError {
	case common.RouterErrorConnectionRefused, common.RouterErrorDNSFailure, common.RouterErrorTLSMismatch, common.RouterErrorNetworkUnreachable:
		// Connect-phase failures: the request was never written (MayHaveReachedNode is false).
		return copyNotSent
	default:
		return copyMayHaveRun
	}
}

// statusEvidence reads a copy's HTTP status: 401, 402, 403 and 429 refuse it; any other status,
// a 2xx that is not JSON-RPC included, does not prove the node did not run it.
func statusEvidence(status int) copyEvidence {
	switch status {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests:
		return copyRefused
	default:
		return copyMayHaveRun
	}
}

// unknownWriteMessage is Part I's sentence for a write whose outcome is unknown: it names the hash
// when the router computed it, and otherwise tells the client how to get it.
func unknownWriteMessage(txHash string) string {
	const status = "Status unknown - this transaction may have been broadcast. "
	if txHash == "" {
		return status + "Look up this transaction by the hash of your signed bytes before signing anything new."
	}
	return status + "Look up " + txHash + " before signing anything new."
}

// notSentWriteReply is the router's code and sentence for a write that was not sent: cause is the
// error the relay ended with. One plain sentence, naming no provider and no transport.
func notSentWriteReply(cause error, dispatched int) (chainlib.RouterReplyCode, string) {
	switch {
	case errors.Is(cause, routersession.SelectedProviderUnavailableError):
		return chainlib.RouterReplyProviderUnavailable, "The selected provider cannot be asked - this transaction was not sent."
	case dispatched == 0 && errors.Is(cause, routersession.PairingListEmptyError):
		return chainlib.RouterReplyNoProviders, "No provider can serve this request - this transaction was not sent."
	default:
		return chainlib.RouterReplyNoProviderAnswer, "No provider answered - this transaction was not sent."
	}
}

// writeOutcomeReply is the router's own reply to a write whose verdict is unknown or not sent (Part
// IV sections 4 and 5): HTTP 200, a JSON-RPC error object with the router's code, the caller's id,
// one sentence, and error.data with router_error, write_outcome, tx_hash and guid. It is carried as
// an upstream's answer is (ClientFacing), the one reply the listener forwards as is, so masked mode
// keeps the sentence and the fields (MAG-5061 item 5): they are the advice the client acts on.
//
// base is the result the reply is built on, nil for none: its reply headers are kept, except the
// node-error flag, which a router-made reply does not carry. base is not changed: it can alias a
// stored result. The caller adds Lava-Write-Outcome and Lava-Tx-Hash (stampWriteOutcome, with the
// txHash returned here) once the request's own headers are on: a result with no reply is how
// appendHeadersToRelayResult knows no upstream answered.
func (rpcss *RPCSmartRouterServer) writeOutcomeReply(guid string, protocolMessage chainlib.ProtocolMessage, base *common.RelayResult, verdict writeVerdict, cause error, dispatched int) (reply *common.RelayResult, txHash string) {
	txHash = rpcss.writeTxHash(protocolMessage)
	code, message := chainlib.RouterReplyOutcomeUnknown, unknownWriteMessage(txHash)
	if verdict.outcome == writeOutcomeNotSent {
		code, message = notSentWriteReply(cause, dispatched)
	}
	var requestBody []byte
	if relayData := protocolMessage.RelayPrivateData(); relayData != nil {
		requestBody = relayData.Data
	}

	stamped := common.RelayResult{}
	if base != nil {
		stamped = *base
	}
	stamped.StatusCode = http.StatusOK
	stamped.IsNodeError = false
	stamped.ClientFacing = &common.ClientFacingError{
		UpstreamStatus: http.StatusOK,
		UpstreamBody: chainlib.ConvertToJsonRpcRouterError(requestBody, code, message, map[string]string{
			"write_outcome": verdict.outcome,
			"tx_hash":       txHash,
			"guid":          guid,
		}),
		UpstreamContentType: common.JSONContentType,
	}
	stamped.Reply = nil
	if baseReply := base.GetReply(); baseReply != nil {
		stamped.Reply = &pairingtypes.RelayReply{}
		for _, md := range baseReply.GetMetadata() {
			if !strings.EqualFold(md.Name, common.IDENTIFIED_NODE_ERROR_HEADER) {
				stamped.Reply.Metadata = append(stamped.Reply.Metadata, md)
			}
		}
	}
	return &stamped, txHash
}

// stampWriteOutcome adds Lava-Write-Outcome and, when the router computed it, Lava-Tx-Hash to
// result's reply (MAG-5232 Expected 6). Lava-Guid is on every reply already.
func stampWriteOutcome(result *common.RelayResult, outcome, txHash string) {
	if result == nil {
		return
	}
	ensureWriteOutcomeHeader(result, outcome)
	if txHash == "" {
		return
	}
	for _, md := range result.Reply.Metadata {
		if md.Name == common.TX_HASH_HEADER_NAME {
			return
		}
	}
	result.Reply.Metadata = append(result.Reply.Metadata, pairingtypes.Metadata{Name: common.TX_HASH_HEADER_NAME, Value: txHash})
}

// requestGUID is the request's guid as Lava-Guid carries it, "" when ctx has none.
func requestGUID(ctx context.Context) string {
	if guid, found := utils.GetUniqueIdentifier(ctx); found && guid != 0 {
		return strconv.FormatUint(guid, 10)
	}
	return ""
}

// evmRawTxMethods are the EVM submits whose first parameter is the signed transaction itself, so its
// hash is keccak-256 of those bytes: eth_sendRawTransaction and the variants chains add beside it
// (a synchronous one, a conditional one, a private one, MegaETH's realtime one, Filecoin's EVM one).
var evmRawTxMethods = map[string]struct{}{
	rywWriteMethod:                      {},
	"eth_sendRawTransactionSync":        {},
	"eth_sendRawTransactionConditional": {},
	"eth_sendPrivateRawTransaction":     {},
	"realtime_sendRawTransaction":       {},
	"Filecoin.EthSendRawTransaction":    {},
}

// writeTxHash is the transaction's hash, computed from the request with the chain's formula (Part I
// R-L, MAG-5232 Expected 7), or "" when the chain has none or the request carries no single
// transaction (a batch):
//
//   - EVM, evmRawTxMethods: keccak-256 of the raw transaction, 0x-hex, as the node reports it;
//   - Solana, sendTransaction: the transaction's first signature, in base58.
//
// Every other chain has no formula here yet, and its reply carries the no-hash sentence.
func (rpcss *RPCSmartRouterServer) writeTxHash(protocolMessage chainlib.ProtocolMessage) string {
	if protocolMessage.IsBatch() {
		return ""
	}
	api := protocolMessage.GetApi()
	if api == nil {
		return ""
	}
	_, evmRawTx := evmRawTxMethods[api.Name]
	switch {
	case evmRawTx:
		return evmRawTxHash(protocolMessage)
	case api.Name == solanaSendMethod && rpcss.directRelayChainFamily() == common.ChainFamilySolana:
		return solanaRequestSignature(protocolMessage)
	default:
		return ""
	}
}

// evmRawTxHash is keccak-256 of the raw transaction eth_sendRawTransaction sends, "" when its first
// parameter is not hex.
func evmRawTxHash(protocolMessage chainlib.ProtocolMessage) string {
	rawHex, ok := firstStringParam(protocolMessage)
	if !ok {
		return ""
	}
	raw, err := hexutil.Decode(rawHex)
	if err != nil || len(raw) == 0 {
		return ""
	}
	return hexutil.Encode(crypto.Keccak256(raw))
}

// solanaSignatureSize is the size of an ed25519 signature, the first of which is a Solana
// transaction's id.
const solanaSignatureSize = 64

// solanaRequestSignature is the first signature of the transaction sendTransaction sends, in base58:
// the transaction's id. The transaction is params[0], encoded as params[1].encoding says, base58 by
// default; on the wire it starts with its signature count as a compact-u16, then the signatures.
func solanaRequestSignature(protocolMessage chainlib.ProtocolMessage) string {
	encoded, ok := firstStringParam(protocolMessage)
	if !ok {
		return ""
	}
	var raw []byte
	var err error
	switch solanaTxEncoding(protocolMessage) {
	case "base64":
		raw, err = base64.StdEncoding.DecodeString(encoded)
	case "base58":
		raw, err = base58Decode(encoded)
	default:
		return ""
	}
	if err != nil {
		return ""
	}
	count, size, ok := decodeCompactU16(raw)
	if !ok || count == 0 || len(raw) < size+solanaSignatureSize {
		return ""
	}
	return base58Encode(raw[size : size+solanaSignatureSize])
}

// solanaTxEncoding is the encoding sendTransaction's config names, base58 when it names none.
func solanaTxEncoding(protocolMessage chainlib.ProtocolMessage) string {
	params, ok := protocolMessage.GetRPCMessage().GetParams().([]interface{})
	if !ok || len(params) < 2 {
		return "base58"
	}
	config, ok := params[1].(map[string]interface{})
	if !ok {
		return "base58"
	}
	encoding, ok := config["encoding"].(string)
	if !ok || encoding == "" {
		return "base58"
	}
	return encoding
}

// decodeCompactU16 reads Solana's compact-u16 at the start of data: up to three bytes, seven bits
// each, low bits first. size is how many bytes it took.
func decodeCompactU16(data []byte) (value, size int, ok bool) {
	for i := 0; i < 3 && i < len(data); i++ {
		value |= int(data[i]&0x7f) << (7 * i)
		if data[i]&0x80 == 0 {
			return value, i + 1, true
		}
	}
	return 0, 0, false
}

// base58Encode writes data in base58, Bitcoin's alphabet, which Solana writes ids in.
func base58Encode(data []byte) string {
	leadingZeros := len(data) - len(bytes.TrimLeft(data, "\x00"))
	value, radix, digit := new(big.Int).SetBytes(data), big.NewInt(58), new(big.Int)
	var out []byte
	for value.Sign() > 0 {
		value.DivMod(value, radix, digit)
		out = append(out, base58Alphabet[digit.Int64()])
	}
	for i := 0; i < leadingZeros; i++ {
		out = append(out, base58Alphabet[0])
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

// errNotBase58 is base58Decode's error for a character outside the alphabet.
var errNotBase58 = errors.New("not base58")

// base58Decode reads text in base58, Bitcoin's alphabet.
func base58Decode(text string) ([]byte, error) {
	leadingZeros := len(text) - len(strings.TrimLeft(text, base58Alphabet[:1]))
	value, radix, digit := new(big.Int), big.NewInt(58), new(big.Int)
	for i := 0; i < len(text); i++ {
		index := strings.IndexByte(base58Alphabet, text[i])
		if index < 0 {
			return nil, errNotBase58
		}
		value.Mul(value, radix).Add(value, digit.SetInt64(int64(index)))
	}
	return append(make([]byte, leadingZeros), value.Bytes()...), nil
}

// errWriteNotSent is a not-sent write's error when the relay ended with none of its own.
var errWriteNotSent = errors.New("no copy of the write left the router")

// writeOutcomeReplyError is the error a write whose reply the router makes returns beside it: why
// the outcome is unknown, or the relay's own error for a write that was not sent.
func writeOutcomeReplyError(verdict writeVerdict, cause error) error {
	if verdict.unclear != nil {
		return verdict.unclear
	}
	if cause != nil {
		return cause
	}
	return errWriteNotSent
}
