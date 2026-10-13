// Package sigs holds the encoding and hashing helpers used when building the
// payloads that relay messages are signed over.
//
// The Signable interface and the signing/recovery helpers that used to live
// here were removed once nothing on the smart-router path signed or verified
// relays locally.

package sigs

import (
	"crypto/sha256"
	"encoding/binary"
)

// EncodeUint64 encodes a uint64 value to a byte array
func EncodeUint64(val uint64) []byte {
	encodedVal := make([]byte, 8)
	binary.LittleEndian.PutUint64(encodedVal, val)
	return encodedVal
}

// HashMsg hashes msgData using SHA-256
func HashMsg(msgData []byte) []byte {
	sum := sha256.Sum256(msgData)
	return sum[:]
}
