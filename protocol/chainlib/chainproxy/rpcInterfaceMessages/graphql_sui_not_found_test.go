package rpcInterfaceMessages

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestSuiTransactionNotFound pins R23 (MAG-4909; MAG-5121 P6): a Sui transaction looked up by its
// digest and answered null is "not found": a data-scope node error while the read is in flight
// (another provider is asked, nobody blamed), never any other null.
func TestSuiTransactionNotFound(t *testing.T) {
	const (
		lookup        = `{"query":"{ transaction(digest: \"x\") { digest } }"}`
		lookupAliased = `{"query":"{ tx: transaction(digest: \"x\") { digest } }"}`
		effects       = `{"query":"{ transactionEffects(digest: \"x\") { status } }"}`
		withScalar    = `{"query":"{ chainIdentifier transaction(digest: \"x\") { digest } }"}`
		twoLookups    = `{"query":"{ a: transaction(digest: \"x\") { digest } b: transaction(digest: \"y\") { digest } }"}`
	)
	tests := []struct {
		name         string
		request      string
		status       int
		body         string
		wantNotFound bool
		wantEffect   graphQLEffect
	}{
		{"a null transaction is not found", lookup, 200, `{"data":{"transaction":null}}`, true, effectElsewhere},
		{"a null under the caller's alias is not found", lookupAliased, 200, `{"data":{"tx":null}}`, true, effectElsewhere},
		{"a null transactionEffects is not found", effects, 200, `{"data":{"transactionEffects":null}}`, true, effectElsewhere},
		{"a null beside a scalar root field is not found", withScalar, 200, `{"data":{"chainIdentifier":"35834a8a","transaction":null}}`, true, effectElsewhere},
		{"one of two lookups null is not found", twoLookups, 200, `{"data":{"a":{"digest":"x"},"b":null}}`, true, effectElsewhere},
		{"a status of 0 reads as 2xx", lookup, 0, `{"data":{"transaction":null}}`, true, effectElsewhere},

		{"a transaction that was found is the answer", lookup, 200, `{"data":{"transaction":{"digest":"x"}}}`, false, effectAnswer},
		{"a found transaction beside a scalar is the answer", withScalar, 200, `{"data":{"chainIdentifier":"35834a8a","transaction":{"digest":"x"}}}`, false, effectAnswer},
		{"a null on another field is the answer", suiCheckpointRead, 200, `{"data":{"checkpoint":null}}`, false, effectAnswer},
		{"a null under the alias of another field is the answer", `{"query":"{ transaction: checkpoint(sequenceNumber: 1) { digest } }"}`, 200, `{"data":{"transaction":null}}`, false, effectAnswer},
		{"data null with no errors stays a provider fault", lookup, 200, `{"data":null}`, false, effectFault},
		{
			"an error beside the null is read by the error rows", lookup, 200,
			`{"data":{"transaction":null},"errors":[{"message":"boom","path":["transaction"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, false, effectFault,
		},
		{"a null under a 5xx is the status's", lookup, 503, `{"data":{"transaction":null}}`, false, effectFault},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			read := parsedSuiRequest(t, test.request)
			require.Equal(t, test.wantNotFound, read.ReplyIsNotFound([]byte(test.body), test.status))
			require.Equal(t, test.wantEffect, readEffect(t, read, test.body, test.status))
			if test.wantNotFound {
				// The entry the relay applies (IsDataScope, so OnSessionDataNotHeld), not the shared
				// rows' reading of the status: a read verdict naming none is filed by its kind.
				classified := read.OwnClassification([]byte(test.body), test.status)
				require.Equal(t, common.RouterErrorNodeDataNotHeld, classified, "filed as data this provider does not hold, got %v", classified)
			}
		})
	}
}

// TestSuiTransactionNotFoundOnlyOnSuiReads: the row is Sui's, and reads only.
func TestSuiTransactionNotFoundOnlyOnSuiReads(t *testing.T) {
	const body = `{"data":{"transaction":null}}`
	other, err := ParseGraphQLMsg([]byte(`{"query":"{ transaction(digest: \"x\") { digest } }"}`))
	require.NoError(t, err)
	other.SetChainFamily(common.ChainFamilyUnknown)
	require.False(t, other.ReplyIsNotFound([]byte(body), http.StatusOK), "a chain without rows reads a null as the answer")
	hasError, _ := other.CheckResponseError([]byte(body), http.StatusOK)
	require.False(t, hasError)

	write := parsedSuiRequest(t, suiExecuteWrite)
	require.False(t, write.ReplyIsNotFound([]byte(`{"data":{"executeTransaction":null}}`), http.StatusOK))
}

// TestSuiTransactionNotFoundDoesNotCopyTheAnswer: a found transaction is walked, not kept.
func TestSuiTransactionNotFoundDoesNotCopyTheAnswer(t *testing.T) {
	read := parsedSuiRequest(t, `{"query":"{ transaction(digest: \"x\") { effects { objectChanges { nodes { address } } } } }"}`)
	body := largeSuiTransaction(2 << 20)
	require.False(t, read.ReplyIsNotFound(body, http.StatusOK))
	allocated := testing.AllocsPerRun(5, func() { _, _ = suiTransactionNotFound(read, body) })
	require.Less(t, allocated, float64(64), "reading whether a transaction is null must not allocate per member of the answer")
}

// largeSuiTransaction is a found transaction of about size bytes.
func largeSuiTransaction(size int) []byte {
	body := []byte(`{"data":{"transaction":{"effects":{"objectChanges":{"nodes":[`)
	node := []byte(`{"address":"0x0000000000000000000000000000000000000000000000000000000000000002"},`)
	for len(body) < size {
		body = append(body, node...)
	}
	body = append(body[:len(body)-1], []byte(`]}}}}}`)...)
	return body
}
