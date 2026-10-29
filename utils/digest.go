package utils

import (
	"crypto/sha256"
	"encoding/hex"
)

// Digest is the first n bytes of s's sha256, in hex (2n characters). It is how a value that
// must not leave the process — a node url carrying an api key, a client-chosen session id —
// is named instead: every pod derives the same digest from the same string, which is all the
// fleet, the metrics and the logs need to agree on, and the plaintext is not recoverable from
// it. n must be between 0 and 32.
func Digest(s string, n int) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:n])
}
