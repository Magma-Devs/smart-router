package rpcsmartrouter

import (
	"encoding/json"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// Read-your-writes pins (MAG-4032).
//
// A write is broadcast to every upstream and the caller is answered with the first acceptance.
// The caller's next read goes to one upstream picked by weight, and when that pick has not seen
// the write yet the caller reads the state from before its own transaction. For an EVM wallet
// the damage case is the pending nonce: eth_getTransactionCount(addr, "pending") hands back a
// nonce the wallet already used, it signs a second transaction with it, and one of the two is
// dropped or replaced. A lookup of the hash just returned answers "not found" the same way.
//
// So after a served eth_sendRawTransaction the router remembers, for a short window, which
// upstream accepted it, under the two things a follow-up read names: the sender and the
// transaction hash. The sender's pending-nonce reads and the hash's lookups go to that upstream
// while the window lasts.
//
// Keyed by what the write names, not by who sent it. The router cannot tell one wallet from
// another behind the same caller — a custody backend submits for thousands of wallets from a
// handful of addresses — and pinning by caller would send wallet A's nonce read to the upstream
// that accepted wallet B's write.
//
// Best effort throughout. The pin is soft: an upstream that cannot take the read right now gives
// way to ordinary selection rather than failing it. It is honoured on the first attempt only, it
// is pod-local, and a hedge is free to answer from elsewhere. The window only has to outlast the
// time the other upstreams take to see the transaction, by gossip or in a block.

// Method names. The hash lookups are the transaction-shaped subset of evmByHashMethods: a block
// hash names no transaction this router could have submitted.
const (
	rywWriteMethod      = "eth_sendRawTransaction"
	rywNonceReadMethod  = "eth_getTransactionCount"
	rywSenderKeyPrefix  = "sender:"
	rywTxHashKeyPrefix  = "tx:"
	rywTxHashHexLength  = 2 + 64
	rywAddressHexLength = 2 + 40
)

var rywHashLookupMethods = map[string]struct{}{
	"eth_getTransactionByHash":    {},
	"eth_getRawTransactionByHash": {},
	"eth_getTransactionReceipt":   {},
}

// rywMaxPins bounds the table whatever the write rate. At the default window it allows thousands
// of writes a second sustained, far beyond any tenant's; past it a new pin is dropped rather than
// an old one evicted, so a flood costs the flood its pins and nobody else theirs.
const rywMaxPins = 100_000

type rywPin struct {
	provider string
	expires  time.Time
}

// readYourWrites is the pin table of one listener. A nil *readYourWrites is the feature turned
// off, and every method is a no-op on it. Safe for concurrent use.
type readYourWrites struct {
	window time.Duration
	now    func() time.Time

	lock      sync.Mutex
	pins      map[string]rywPin
	nextSweep time.Time
}

// newReadYourWrites returns nil — off — for a window of zero or less.
func newReadYourWrites(window time.Duration) *readYourWrites {
	if window <= 0 {
		return nil
	}
	return &readYourWrites{
		window: window,
		now:    time.Now,
		pins:   make(map[string]rywPin),
	}
}

// recordWrite pins what a served eth_sendRawTransaction names to the upstream that accepted it.
// replyData is the reply the caller is about to receive; its result is the transaction hash.
func (r *readYourWrites) recordWrite(protocolMessage chainlib.ProtocolMessage, provider string, replyData []byte) {
	if r == nil || provider == "" || protocolMessage.GetApi().GetName() != rywWriteMethod {
		return
	}
	// The hash the upstream returned is the one the caller will look up. The computed one is the
	// same for every type go-ethereum knows, and covers a reply that is not the usual JSON; a
	// duplicate key costs nothing.
	keys := make([]string, 0, 3)
	if hash := rywReplyTxHash(replyData); hash != "" {
		keys = append(keys, rywTxHashKeyPrefix+hash)
	}
	if rawHex, ok := firstStringParam(protocolMessage); ok {
		if raw, err := hexutil.Decode(rawHex); err == nil {
			if sender, hash, ok := evmTxSenderAndHash(raw); ok {
				keys = append(keys, rywSenderKeyPrefix+sender, rywTxHashKeyPrefix+hash)
			}
		}
	}
	r.pin(keys, provider)
}

// pinFor returns the upstream this read should go to, or "" when it names no pinned write.
func (r *readYourWrites) pinFor(protocolMessage chainlib.ProtocolMessage) string {
	if r == nil {
		return ""
	}
	key := rywReadKey(protocolMessage)
	if key == "" {
		return ""
	}
	now := r.now()
	r.lock.Lock()
	defer r.lock.Unlock()
	pin, ok := r.pins[key]
	if !ok {
		return ""
	}
	if !now.Before(pin.expires) {
		delete(r.pins, key)
		return ""
	}
	return pin.provider
}

func (r *readYourWrites) pin(keys []string, provider string) {
	if len(keys) == 0 {
		return
	}
	now := r.now()
	r.lock.Lock()
	defer r.lock.Unlock()
	// Sweeping once per window keeps the table at about two windows of writes. Reads delete the
	// expired pins they meet, but a key that is never read again is only ever removed here.
	if !now.Before(r.nextSweep) {
		for key, pin := range r.pins {
			if !now.Before(pin.expires) {
				delete(r.pins, key)
			}
		}
		r.nextSweep = now.Add(r.window)
	}
	for _, key := range keys {
		if _, exists := r.pins[key]; !exists && len(r.pins) >= rywMaxPins {
			continue
		}
		r.pins[key] = rywPin{provider: provider, expires: now.Add(r.window)}
	}
}

// rywReadKey names the write a read depends on: the sender for a pending-nonce read, the hash for
// a transaction lookup. A nonce read at any other tag is left alone — "latest" moves with blocks,
// which every upstream sees, and the consistency gate already covers an upstream behind on those.
func rywReadKey(protocolMessage chainlib.ProtocolMessage) string {
	apiName := protocolMessage.GetApi().GetName()
	if apiName == rywNonceReadMethod {
		if requestedBlock, _ := protocolMessage.RequestedBlock(); requestedBlock != spectypes.PENDING_BLOCK {
			return ""
		}
		address, ok := firstStringParam(protocolMessage)
		if !ok || len(address) != rywAddressHexLength {
			return ""
		}
		return rywSenderKeyPrefix + strings.ToLower(address)
	}
	if _, ok := rywHashLookupMethods[apiName]; !ok {
		return ""
	}
	hash, ok := firstStringParam(protocolMessage)
	if !ok || len(hash) != rywTxHashHexLength {
		return ""
	}
	return rywTxHashKeyPrefix + strings.ToLower(hash)
}

// firstStringParam returns a JSON-RPC request's first positional parameter when it is a string.
// A batch, a named-parameter object or a non-string first parameter returns false.
func firstStringParam(protocolMessage chainlib.ProtocolMessage) (string, bool) {
	rpcMessage := protocolMessage.GetRPCMessage()
	if rpcMessage == nil {
		return "", false
	}
	params, ok := rpcMessage.GetParams().([]interface{})
	if !ok || len(params) == 0 {
		return "", false
	}
	value, ok := params[0].(string)
	return value, ok && value != ""
}

// rywReplyTxHash reads the transaction hash out of an eth_sendRawTransaction reply, lowercased, or
// returns "" when the reply carries none.
func rywReplyTxHash(replyData []byte) string {
	var reply struct {
		Result string `json:"result"`
	}
	if json.Unmarshal(replyData, &reply) != nil || len(reply.Result) != rywTxHashHexLength || !strings.HasPrefix(reply.Result, "0x") {
		return ""
	}
	return strings.ToLower(reply.Result)
}

// evmTxSenderAndHash recovers the sender of a signed EVM transaction and computes its hash, both
// as lowercase 0x-hex, without knowing the transaction's type.
//
// Every signed envelope carries its signature as the last three list items and signs the same
// list without them:
//
//	legacy, EIP-155:          keccak(rlp([fields..., chainId, 0, 0]))   v = chainId*2 + 35 + recid
//	legacy, no chain id:      keccak(rlp([fields...]))                  v = 27 + recid
//	typed (EIP-2718):         keccak(type || rlp([fields...]))          yParity = recid
//
// Read that way rather than through go-ethereum's typed decoder on purpose: the decoder knows only
// Ethereum's own types and brings the KZG stack in for blob transactions, while this rule covers
// those types and every chain type built the same way — Celo's fee-currency transactions (0x7b)
// among them — without a list of them. A type built differently yields no sender, or a wrong one;
// the only thing at stake is a pin on an address nobody reads.
func evmTxSenderAndHash(raw []byte) (sender string, hash string, ok bool) {
	// The raw transaction is caller input. Whatever it holds may cost this write its pin, never
	// the process.
	defer func() {
		if recover() != nil {
			sender, hash, ok = "", "", false
		}
	}()
	if len(raw) == 0 {
		return "", "", false
	}
	if raw[0] >= 0xc0 {
		return legacyTxSenderAndHash(raw)
	}
	if raw[0] > 0x7f {
		return "", "", false
	}
	return typedTxSenderAndHash(raw[0], raw[1:])
}

func legacyTxSenderAndHash(raw []byte) (string, string, bool) {
	var items []rlp.RawValue
	if err := rlp.DecodeBytes(raw, &items); err != nil || len(items) != 9 {
		return "", "", false
	}
	v, r, s, ok := decodeSignatureValues(items[6:])
	if !ok {
		return "", "", false
	}
	var payload []byte
	var recoveryID *big.Int
	var err error
	switch {
	case v.Cmp(big.NewInt(27)) == 0 || v.Cmp(big.NewInt(28)) == 0:
		recoveryID = new(big.Int).Sub(v, big.NewInt(27))
		payload, err = rlp.EncodeToBytes(items[:6])
	case v.Cmp(big.NewInt(35)) >= 0:
		chainID := new(big.Int).Sub(v, big.NewInt(35))
		recoveryID = new(big.Int).Mod(chainID, big.NewInt(2))
		chainID.Rsh(chainID, 1)
		payload, err = rlp.EncodeToBytes([]interface{}{items[0], items[1], items[2], items[3], items[4], items[5], chainID, uint(0), uint(0)})
	default:
		return "", "", false
	}
	if err != nil {
		return "", "", false
	}
	sender, ok := recoverSender(crypto.Keccak256(payload), recoveryID, r, s)
	if !ok {
		return "", "", false
	}
	return sender, hexutil.Encode(crypto.Keccak256(raw)), true
}

func typedTxSenderAndHash(txType byte, body []byte) (string, string, bool) {
	var items []rlp.RawValue
	if err := rlp.DecodeBytes(body, &items); err != nil || len(items) == 0 {
		return "", "", false
	}
	// A blob transaction is submitted in its network form, [transaction, blobs, commitments,
	// proofs]: the transaction is the first item, a list where every other type has an integer.
	signedList := body
	if len(items[0]) > 0 && items[0][0] >= 0xc0 {
		signedList = items[0]
		items = nil
		if err := rlp.DecodeBytes(signedList, &items); err != nil {
			return "", "", false
		}
	}
	if len(items) < 4 {
		return "", "", false
	}
	yParity, r, s, ok := decodeSignatureValues(items[len(items)-3:])
	if !ok {
		return "", "", false
	}
	unsigned, err := rlp.EncodeToBytes(items[:len(items)-3])
	if err != nil {
		return "", "", false
	}
	sender, ok := recoverSender(crypto.Keccak256([]byte{txType}, unsigned), yParity, r, s)
	if !ok {
		return "", "", false
	}
	return sender, hexutil.Encode(crypto.Keccak256([]byte{txType}, signedList)), true
}

func decodeSignatureValues(items []rlp.RawValue) (v, r, s *big.Int, ok bool) {
	v, r, s = new(big.Int), new(big.Int), new(big.Int)
	for i, target := range []*big.Int{v, r, s} {
		if err := rlp.DecodeBytes(items[i], target); err != nil {
			return nil, nil, nil, false
		}
	}
	return v, r, s, true
}

func recoverSender(signingHash []byte, recoveryID, r, s *big.Int) (string, bool) {
	if !recoveryID.IsUint64() || recoveryID.Uint64() > 1 || !crypto.ValidateSignatureValues(byte(recoveryID.Uint64()), r, s, true) {
		return "", false
	}
	signature := make([]byte, crypto.SignatureLength)
	r.FillBytes(signature[0:32])
	s.FillBytes(signature[32:64])
	signature[64] = byte(recoveryID.Uint64())
	publicKey, err := crypto.SigToPub(signingHash, signature)
	if err != nil {
		return "", false
	}
	return strings.ToLower(crypto.PubkeyToAddress(*publicKey).Hex()), true
}
