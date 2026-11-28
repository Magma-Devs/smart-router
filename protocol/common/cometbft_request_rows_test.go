package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-5760: CometBFT answers a request it cannot serve as asked with -32603 "Internal error" and its
// reason in data (rpc/core, the same sentences in v0.37, v0.38 and v1.0). The rows read who the
// reason is about. A request no node can serve is the caller's: it ends at the first node, and no
// endpoint is blamed. A range a node clamps to its own base and height, and a page count from its
// own index, can differ on another node: it is tried there, and this one is not blamed either.
func TestCometBFTRequestRows(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		want       *RouterError
	}{
		{"height zero", "height must be greater than 0, but got 0", RouterErrorUserInvalidParams},
		{"negative height", "height must be greater than 0, but got -1", RouterErrorUserInvalidParams},
		{"negative blockchain bounds", "heights must be non-negative", RouterErrorUserInvalidParams},
		{"page zero", "page should be within [1, 3] range, given 0", RouterErrorUserInvalidParams},
		{"negative page", "page should be within [1, 3] range, given -2", RouterErrorUserInvalidParams},
		{"bad order_by (v0.37, v0.38)", "expected order_by to be either `asc` or `desc` or empty", RouterErrorUserInvalidParams},
		{"bad order_by (v1)", "invalid order_by: expected `asc`, `desc` or ``, but got up", RouterErrorUserInvalidParams},
		{"query too long", "maximum query length exceeded", RouterErrorUserInvalidParams},
		{"query syntax, tx_search", "offset 9: got TNumber, wanted TTag", RouterErrorUserInvalidParams},
		{"query syntax, subscribe", "failed to parse query: offset 3: unexpected operator =", RouterErrorUserInvalidParams},
		{"query character", "invalid input ! at offset 4", RouterErrorUserInvalidParams},
		{"query that does not compile", "compile tm.event > 'x': invalid op/arg combination (>, TString)", RouterErrorUserInvalidParams},
		{"no evidence", "no evidence was provided", RouterErrorUserInvalidParams},
		{"evidence that does not validate", "evidence.ValidateBasic failed: empty vote A", RouterErrorUserInvalidParams},
		{"evidence the node finds invalid", "failed to add evidence: Invalid evidence: don't have header #5. Evidence: DuplicateVoteEvidence{}", RouterErrorUserInvalidParams},
		{"blockchain range outside this node's", "min height 500 can't be greater than max height 400", RouterErrorChainDataNotAvailable},
		{"page past this node's index", "page should be within [1, 3] range, given 7", RouterErrorChainDataNotAvailable},
		// Controls: the node's own failures keep NODE_INTERNAL_ERROR.
		{"evidence the node could not store", "failed to add evidence: can't add evidence to pending list: leveldb: closed", RouterErrorNodeInternalError},
		{"no data", "", RouterErrorNodeInternalError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := JSONRPCErrorText(JSONRPCInternalErrorCode, "Internal error", tc.data)
			for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyUnknown, ChainFamilyEVM} {
				require.Equal(t, tc.want, ClassifyError(nil, family, TransportJsonRPC, JSONRPCInternalErrorCode, text), "family %d", family)
			}
			if tc.want == RouterErrorNodeInternalError {
				return
			}
			// A GET URI request's 500, read on the sender's second pass, and a batch element.
			require.Equal(t, tc.want, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, 500, "HTTP 500: "+text))
			require.Equal(t, tc.want, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, 200, "tx (AB) not found,-,"+text))
			require.False(t, tc.want.EndpointAtFault(), "no endpoint is blamed")
		})
	}
}

// The caller's verdicts end the relay at the first node; the node's range does not.
func TestCometBFTRequestRows_WhoIsAsked(t *testing.T) {
	require.False(t, RouterErrorUserInvalidParams.Retryable)
	require.True(t, RouterErrorChainDataNotAvailable.Retryable)
	require.True(t, RouterErrorChainDataNotAvailable.SubCategory.IsDataScope())
}

// The rows read CometBFT's sentence where its reader writes it, at the start of the data: a revert
// or another element whose text quotes one keeps its own verdict.
func TestCometBFTRequestRows_ReadTheSentenceWhereCometBFTWritesIt(t *testing.T) {
	quoted := JSONRPCErrorText(JSONRPCInternalErrorCode, "Internal error", "abci query: height must be greater than 0, but got 0")
	require.Equal(t, RouterErrorNodeInternalError, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, JSONRPCInternalErrorCode, quoted))
	revert := "execution reverted: Internal error: height must be greater than 0, but got 0"
	require.Equal(t, RouterErrorChainExecutionReverted, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, 500, "HTTP 500: "+revert))
}

// Two more requests every node answers the same under -32603: a tx_search whose tx.hash is
// not hex (CometBFT's kv indexer decodes it before it searches), and a genesis_chunked chunk past the
// last one. Both are the caller's, as a single reply, under the URI GET's 500 and in a batch.
func TestCometBFTRequestRows_AHashThatIsNotHexAndAChunkPastTheLast(t *testing.T) {
	for _, data := range []string{
		"error during searching for a hash in the query: encoding/hex: invalid byte: U+007A 'z'",
		"error during searching for a hash in the query: encoding/hex: odd length hex string",
		"there are 4 chunks, 9 is invalid",
		"there are 4 chunks, -1 is invalid",
	} {
		text := "Internal error: " + data
		for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyUnknown, ChainFamilyEVM} {
			require.Same(t, RouterErrorUserInvalidParams, ClassifyError(nil, family, TransportJsonRPC, JSONRPCInternalErrorCode, text), "%q", data)
			require.Same(t, RouterErrorUserInvalidParams, ClassifyError(nil, family, TransportJsonRPC, JSONRPCInternalErrorCode, "HTTP 500: "+text), "%q under a 500", data)
			require.Same(t, RouterErrorUserInvalidParams, ClassifyError(nil, family, TransportJsonRPC, 200, "Internal error: odd,-,"+text), "%q in a batch", data)
		}
	}
	// The indexer failing to read what it found is the node's own fault, as before.
	require.Same(t, RouterErrorNodeInternalError, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, JSONRPCInternalErrorCode, "Internal error: error while retrieving the result: leveldb: closed"))
}
