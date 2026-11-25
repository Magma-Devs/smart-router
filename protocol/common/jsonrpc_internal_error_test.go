package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONRPCErrorText(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int
		message string
		data    any
		want    string
	}{
		{"internal error with a data string", -32603, "Internal error", "tx already exists in cache", "Internal error: tx already exists in cache"},
		{"internal error without data", -32603, "Internal error", nil, "Internal error"},
		{"internal error with an empty data string", -32603, "Internal error", "", "Internal error"},
		{"internal error with object data", -32603, "Internal error", map[string]any{"reason": "x"}, "Internal error"},
		{"internal error with numeric data", -32603, "Internal error", float64(7), "Internal error"},
		{"a revert keeps its message alone", 3, "execution reverted", "0x08c379a00000000000000000000000000000000000000000000000000000000000000020", "execution reverted"},
		{"invalid params keeps its message alone", -32602, "Invalid params", "error converting json params to arguments: x", "Invalid params"},
		{"server error keeps its message alone", -32000, "Server error", "subscription was canceled", "Server error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, JSONRPCErrorText(tc.code, tc.message, tc.data))
		})
	}
}

// cometBFTInternalErrorCases are CometBFT's -32603 answers by the data it sends (cometbft v0.38.21
// mempool/errors.go, mempool/mempool.go, rpc/core/mempool.go, env.go, tx.go), each with the verdict
// MAG-5325 gives it.
var cometBFTInternalErrorCases = []struct {
	name string
	data string
	want *RouterError
}{
	{"broadcast_tx_sync, a duplicate", "tx already exists in cache", RouterErrorChainTxAlreadyKnown},
	{"broadcast_tx_commit, a duplicate", "error on broadcastTxCommit: tx already exists in cache", RouterErrorChainTxAlreadyKnown},
	{"mempool full", "mempool is full: number of txs 5000 (max: 5000), total txs bytes 1048576 (max: 1073741824)", RouterErrorChainMempoolFull},
	{"broadcast_tx_commit, mempool full", "error on broadcastTxCommit: mempool is full: number of txs 5000 (max: 5000), total txs bytes 429 (max: 1073741824)", RouterErrorChainMempoolFull},
	{"mempool rechecking", "mempool is still rechecking after a new committed block, so it is considered as full", RouterErrorChainMempoolFull},
	{"tx too large", "Tx too large. Max size is 1048576, but got 2000000", RouterErrorChainTxTooLarge},
	{"tx pre check, too big for a block", "tx pre check: tx size is too big: 2000000, max: 1048576", RouterErrorChainTxTooLarge},
	{"broadcast_tx_commit, tx pre check", "error on broadcastTxCommit: tx pre check: tx size is too big: 2000000, max: 1048576", RouterErrorChainTxTooLarge},
	{"commit timeout", "timed out waiting for tx to be included in a block", RouterErrorNodeAborted},
	{"tx not found", "tx (ABCDEF0123456789) not found", RouterErrorChainTxNotFound},
	{"pruned height", "height 5 is not available, lowest height is 1000", RouterErrorChainStatePruned},
	{"future height", "height 9999999 must be less than or equal to the current blockchain height 100", RouterErrorChainBlockNotFound},
	{"unrecognised data", "failed to load state at height 429", RouterErrorNodeInternalError},
	{"the node went away during a commit", "txSub was canceled (reason: CometBFT exited)", RouterErrorNodeInternalError},
}

// The CometBFT rows apply on every chain that serves Tendermint RPC, by the reason CometBFT writes,
// whatever the chain's family: COSMOSHUB is Cosmos SDK, SEI and DYDX have no family, AGR and CANTO
// are EVM. The reply reaches them as the reader writes it: inside a 2xx, behind the direct-RPC
// sender's "HTTP <status>: " prefix (a 4xx, or CometBFT's 500 for a GET), and as a batch element,
// whose code is the batch's HTTP status.
func TestCometBFTInternalErrorRows(t *testing.T) {
	families := []struct {
		chains string
		family ChainFamily
	}{
		{"COSMOSHUB", ChainFamilyCosmosSDK},
		{"SEI, DYDX", ChainFamilyUnknown},
		{"AGR, CANTO", ChainFamilyEVM},
	}
	for _, family := range families {
		for _, tc := range cometBFTInternalErrorCases {
			text := JSONRPCErrorText(-32603, "Internal error", tc.data)
			readings := []struct {
				name    string
				code    int
				message string
			}{
				{"single reply", -32603, text},
				{"behind a status", -32603, "HTTP 500: " + text},
				// A batch's elements' messages joined, classified with the batch's status.
				{"batch, first element", 200, text + ",-,Invalid params"},
				{"batch, later element", 200, "Invalid params,-," + text},
			}
			for _, reading := range readings {
				t.Run(family.chains+"/"+tc.name+"/"+reading.name, func(t *testing.T) {
					want := tc.want
					if want == RouterErrorNodeInternalError && reading.code == 200 {
						// A batch's internal error with data no row reads keeps main's batch verdict.
						want = RouterErrorUnknown
					}
					got := ClassifyError(nil, family.family, TransportJsonRPC, reading.code, reading.message)
					require.Same(t, want, got, "message %q", reading.message)
					if tc.want != RouterErrorNodeInternalError {
						require.False(t, got.EndpointAtFault(), "%s is not the endpoint failing", got.Name)
					}
				})
			}
		}
	}
}

// -32603 without data, or with no string data, is the internal error it always was.
func TestCometBFTInternalErrorWithoutDataStaysInternal(t *testing.T) {
	for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyUnknown, ChainFamilyEVM} {
		for _, data := range []any{nil, "", map[string]any{"log": "tx already exists in cache"}} {
			got := ClassifyError(nil, family, TransportJsonRPC, -32603, JSONRPCErrorText(-32603, "Internal error", data))
			require.Same(t, RouterErrorNodeInternalError, got, "family %d, data %v", family, data)
			require.True(t, got.EndpointAtFault())
		}
	}
}

// The batch row reads only a batch: a single reply under another code whose message a node itself
// starts with "Internal error: " keeps the verdict its code row gives it.
func TestBatchElementInternalErrorLeavesOtherCodes(t *testing.T) {
	for _, tc := range []struct {
		code int
		want *RouterError
	}{
		{-32001, RouterErrorNodeResourceNotFound},
		{-32005, RouterErrorNodeLimitExceeded},
		{-32602, RouterErrorUserInvalidParams},
	} {
		message := "Internal error: limit reached"
		require.Same(t, tc.want, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, tc.code, message), "code %d", tc.code)
	}
	// The control: the same text as a batch element, classified with the batch's status, keeps main's
	// batch verdict, and its data reaches no row after the batch row.
	require.Same(t, RouterErrorUnknown, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 200, "Internal error: limit reached"))
}

// A batch's text joins its elements' messages, and an element may echo what the caller sent, so the
// batch row's words can come from anyone. It blames no endpoint and holds none off: a batch's
// internal error with data no row reads is UNKNOWN_ERROR, as main reads a batch's internal error.
func TestBatchElementInternalErrorBlamesNoEndpoint(t *testing.T) {
	for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyUnknown, ChainFamilyEVM} {
		for _, text := range []string{
			JSONRPCErrorText(-32603, "Internal error", "failed to load state at height 429"),
			"Invalid params,-," + JSONRPCErrorText(-32603, "Internal error", "rate limit exceeded"),
			JSONRPCErrorText(-32603, "Internal error", "all attempts exhausted") + ",-,Invalid params",
		} {
			got := ClassifyError(nil, family, TransportJsonRPC, 200, text)
			require.Same(t, RouterErrorUnknown, got, "family %d, text %q", family, text)
			require.False(t, got.EndpointAtFault(), "family %d, text %q", family, text)
			require.False(t, got.IsRateLimited(), "family %d, text %q", family, text)
		}
	}
}

// The direct-RPC sender classifies a 4xx's JSON-RPC body a second time by its status, to escalate a
// status that says the request itself was refused (classifyJSONRPCNodeError). An internal error's
// data must not take that pass from the status's row.
func TestCometBFTInternalErrorLeavesTheStatusPass(t *testing.T) {
	message := "HTTP 413: " + JSONRPCErrorText(-32603, "Internal error", "failed to read request body")
	require.Same(t, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 413, "HTTP 413: Internal error"),
		ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 413, message))
	require.False(t, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 413, message).Retryable)
}

// The CometBFT rows and the batch row run before the revert row, so they read only a text a
// CometBFT internal error can be: under -32603, or under the HTTP status a batch is classified
// with. A revert's reason is the contract's own text and may hold CometBFT's words or a batch's
// separator; under every code a revert arrives with, alone or as a batch's text, it stays a revert,
// not retried and not the endpoint's fault, as on main.
func TestCometBFTRowsLeaveARevertAlone(t *testing.T) {
	reasons := []string{"Internal error: limit reached", "x,-,Internal error: y"}
	for _, tc := range cometBFTInternalErrorCases {
		reasons = append(reasons, JSONRPCErrorText(-32603, "Internal error", tc.data), "x,-,"+JSONRPCErrorText(-32603, "Internal error", tc.data))
	}
	for _, family := range []ChainFamily{ChainFamilyEVM, ChainFamilyUnknown, ChainFamilyCosmosSDK} {
		for _, reason := range reasons {
			// Geth's 3, the generic -32000, none at all, and a batch's status.
			for _, code := range []int{3, -32000, 0, 200} {
				got := ClassifyError(nil, family, TransportJsonRPC, code, "execution reverted: "+reason)
				require.Same(t, RouterErrorChainExecutionReverted, got, "family %d, code %d, reason %q", family, code, reason)
			}
		}
	}
}

// Under any code other than -32603 or an HTTP status, the text is another reply's, and its code and
// the rows after the CometBFT rows decide it.
func TestCometBFTRowsReadOnlyTheirOwnCodes(t *testing.T) {
	text := JSONRPCErrorText(-32603, "Internal error", "tx already exists in cache")
	for _, code := range []int{3, 40, 0} {
		require.Same(t, RouterErrorUnknown, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, code, text), "code %d", code)
		require.Same(t, RouterErrorUnknown, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, code, "Invalid params,-,Internal error: y"), "code %d", code)
	}
	// The codes the readers classify a CometBFT text with keep the PR's verdicts.
	for _, code := range []int{-32603, 200, 500} {
		require.Same(t, RouterErrorChainTxAlreadyKnown, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, code, text), "code %d", code)
	}
	// Read where the readers write it: not in the middle of an element's text.
	require.Same(t, RouterErrorUnknown, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 200, "unknown query: "+text))
}
