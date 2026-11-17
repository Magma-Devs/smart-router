package rpcInterfaceMessages

import (
	"encoding/base64"
	"math/big"

	"github.com/ethereum/go-ethereum/crypto/blake2b"
)

// suiTransactionDataBcsArgument is executeTransaction's argument that carries the transaction: the
// BCS bytes of its TransactionData, in base64.
const suiTransactionDataBcsArgument = "transactionDataBcs"

// suiTransactionDigestPrefix opens the bytes Sui hashes into a transaction's digest: the type's
// name and "::" (sui-types, crypto.rs: impl Signable for any BcsSignable type writes
// `{name}::` and then the value's BCS bytes).
const suiTransactionDigestPrefix = "TransactionData::"

// suiTransactionDigest is the digest of the transaction a Sui executeTransaction request carries
// (MAG-4909, Sui data 3): Base58(Blake2b-256("TransactionData::" || BCS bytes of the transaction
// data)), Sui's TransactionData::digest. It is "" when the request names no single
// executeTransaction, or its transaction bytes are not a base64 text: a caller then gets the unknown
// reply that says how to find the transaction without it.
func suiTransactionDigest(request *GraphQLMessage) string {
	var encoded string
	found := 0
	for _, rootField := range request.RootFields {
		if rootField.Name != suiExecuteTransactionField {
			continue
		}
		found++
		encoded, _ = rootField.Args[suiTransactionDataBcsArgument].(string)
	}
	if found != 1 || encoded == "" {
		return ""
	}
	transactionData, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return ""
	}
	return suiDigestOfTransactionData(transactionData)
}

// suiDigestOfTransactionData is Sui's TransactionData::digest over a transaction's BCS bytes.
func suiDigestOfTransactionData(transactionData []byte) string {
	hashed := make([]byte, 0, len(suiTransactionDigestPrefix)+len(transactionData))
	hashed = append(hashed, suiTransactionDigestPrefix...)
	hashed = append(hashed, transactionData...)
	digest := blake2b.Sum256(hashed)
	return base58Encode(digest[:])
}

// base58Alphabet is Bitcoin's, which Sui writes its digests in.
const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// base58Encode writes data in base58: the bytes read as one big-endian number, written in base 58,
// with one leading '1' for each leading zero byte.
func base58Encode(data []byte) string {
	number := new(big.Int).SetBytes(data)
	radix := big.NewInt(int64(len(base58Alphabet)))
	remainder := new(big.Int)
	var reversed []byte
	for number.Sign() > 0 {
		number.QuoRem(number, radix, remainder)
		reversed = append(reversed, base58Alphabet[remainder.Int64()])
	}
	for _, b := range data {
		if b != 0 {
			break
		}
		reversed = append(reversed, base58Alphabet[0])
	}
	encoded := make([]byte, len(reversed))
	for i, c := range reversed {
		encoded[len(reversed)-1-i] = c
	}
	return string(encoded)
}
