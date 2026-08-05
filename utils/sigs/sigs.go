// Signable is an interface for objects that should be signed. For example, relay requests
// are signed by the provider so it can prove that it's their relay.
//
// To create an object that satisfies the Signable interface, use relay_exchange.go as a reference
//
// A Signable object can use the Sign() to be signed, ExtractSignerAddress() to get the object
// that signed it, and RecoverPubKey() to get the public key that corresponds to the object's
// private key

package sigs

import (
	"encoding/binary"

	tendermintcrypto "github.com/cometbft/cometbft/crypto"
	//nolint:staticcheck // needed for Bitcoin-style address derivation
)

// EncodeUint64 encodes a uint64 value to a byte array
func EncodeUint64(val uint64) []byte {
	encodedVal := make([]byte, 8)
	binary.LittleEndian.PutUint64(encodedVal, val)
	return encodedVal
}

// HashMsg hashes msgData using SHA-256
func HashMsg(msgData []byte) []byte {
	return tendermintcrypto.Sha256(msgData)
}
