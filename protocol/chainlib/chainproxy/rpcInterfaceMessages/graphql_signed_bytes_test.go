package rpcInterfaceMessages

import (
	"encoding/base64"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// suiSnapshotTransaction is a TransactionData in BCS, base64, from Sui's own test snapshot
// (crates/sui-indexer-alt-e2e-tests/tests/graphql/transactions/bcs.snap).
const suiSnapshotTransaction = "AAACAAgqAAAAAAAAAAAgp7AycDh4qnTDEmk1eJ/R1NfhEdWRGwkkfWljBhwxK1oCAgABAQAAAQECAAABAQD8zJpCG7sTwaZqGqmPCtdQKe3pSFd3nGkVtE+UBouSHgHhFa5/+bVpRPr+9v0BXVEKL9ljttQuyKYmZAnA6kD7NgMAAAAAAAAAIEzrkQF+I9IJ4x0c30+YdFP63Y1T+vAeA9YRM4e19EVk/MyaQhu7E8GmahqpjwrXUCnt6UhXd5xpFbRPlAaLkh7oAwAAAAAAAADyBSoBAAAAAA=="

// suiSignature is a signature-sized value: a flag byte, an Ed25519 signature and its key, 97 bytes.
func suiSignature() string {
	signature := make([]byte, 97)
	for i := range signature {
		signature[i] = byte(i*7 + 3)
	}
	return base64.StdEncoding.EncodeToString(signature)
}

// requireNoPieceOf fails when text holds any 16 characters in a row of secret.
func requireNoPieceOf(t *testing.T, text, secret string) {
	t.Helper()
	for start := 0; start+16 <= len(secret); start++ {
		require.NotContains(t, text, secret[start:start+16], "a piece of the signed bytes reached the text")
	}
}

// TestRedactGraphQLSignedBytes is MAG-5122 test 8's rule (Part I R-L logs; MAG-4909 Sui data 4): a
// write's signed bytes, in base64, are replaced with [REDACTED:<n>], and the ids a reader needs stay.
func TestRedactGraphQLSignedBytes(t *testing.T) {
	signature := suiSignature()
	require.Len(t, signature, 132)
	const (
		address = "0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf"
		digest  = "gHrD7i4MD93GsSzM3ZYibg2CQpBCmZFnxGdANrXFYMP"
		query   = `mutation Submit($tx: Base64!, $sigs: [Base64!]!) { executeTransaction(transactionDataBcs: $tx, signatures: $sigs) { effects { status } } }`
	)
	body := `{"query":"` + query + `","variables":{"tx":"` + suiSnapshotTransaction + `","sigs":["` + signature + `"],"owner":"` + address + `","digest":"` + digest + `"}}`

	redacted := RedactGraphQLSignedBytes(body)
	requireNoPieceOf(t, redacted, suiSnapshotTransaction)
	requireNoPieceOf(t, redacted, signature)
	require.Contains(t, redacted, `"tx":"[REDACTED:`+strconv.Itoa(len(suiSnapshotTransaction))+`]"`)
	require.Contains(t, redacted, `"sigs":["[REDACTED:132]"]`)
	require.Contains(t, redacted, query, "the operation stays readable")
	require.Contains(t, redacted, address, "an address or object id stays")
	require.Contains(t, redacted, digest, "a digest stays")

	t.Run("bytes inline in the query", func(t *testing.T) {
		inline := `{"query":"mutation { executeTransaction(transactionDataBcs: \"` + suiSnapshotTransaction + `\", signatures: [\"` + signature + `\"]) { digest } }"}`
		redacted := RedactGraphQLSignedBytes(inline)
		requireNoPieceOf(t, redacted, suiSnapshotTransaction)
		requireNoPieceOf(t, redacted, signature)
		require.Contains(t, redacted, "executeTransaction(transactionDataBcs:")
	})
	t.Run("JSON-escaped slashes do not split the bytes", func(t *testing.T) {
		escaped := strings.ReplaceAll(suiSnapshotTransaction, "/", `\/`)
		require.Equal(t, `"[REDACTED:`+strconv.Itoa(len(escaped))+`]"`, RedactGraphQLSignedBytes(`"`+escaped+`"`))
	})
	t.Run("a transaction signed in 0x-hex", func(t *testing.T) {
		signedHex := "0x" + strings.Repeat("0a1b2c3d", 20)
		require.Equal(t, `"[REDACTED:162]"`, RedactGraphQLSignedBytes(`"`+signedHex+`"`))
	})
	t.Run("letters alone are a word", func(t *testing.T) {
		word := strings.Repeat("a", 200)
		require.Equal(t, word, RedactGraphQLSignedBytes(word))
	})
	t.Run("short text", func(t *testing.T) {
		require.Equal(t, `{"sigs":["AA=="]}`, RedactGraphQLSignedBytes(`{"sigs":["AA=="]}`))
	})
}
