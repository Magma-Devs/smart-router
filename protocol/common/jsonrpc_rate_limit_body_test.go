package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A gateway that refuses a request for rate on an open WebSocket connection, or inside a 200,
// has no HTTP status to say so with: it mirrors the 429 into the JSON-RPC error body. The
// JSON-RPC table must read that body as the rate limit it is (MAG-4165) — the same verdict an
// HTTP 429 gets: retryable, rate-limited, not the endpoint's fault.
func TestJSONRPC_RateLimitBodyIsRateLimited(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		message string
	}{
		{"mirrored HTTP code with its reason phrase", 429, "Too Many Requests"},
		{"mirrored HTTP code with a vendor message", 429, "Your app has exceeded its capacity"},
		{"reason phrase under a generic server code", -32000, "Too many requests, please slow down"},
		{"reason phrase, mixed case, no code", 0, "TOO MANY REQUESTS"},
		{"rate limit text, pre-existing row", -32000, "rate limit exceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, tc.code, tc.message)
			require.Same(t, LavaErrorNodeRateLimited, got)
			require.True(t, got.SubCategory.IsRateLimit())
			require.True(t, got.Retryable)
		})
	}
}

// The new rows must not widen into verdicts that already exist: a request-shape limit stays a
// non-retryable limit, Solana's -32005 stays its health signal, a standard code still wins over
// the message, and a -32000 body with neither the code nor the phrase keeps its generic
// server-error verdict. A chain family with no table of its own reads the same generic row.
func TestJSONRPC_RateLimitBodyRowsStayNarrow(t *testing.T) {
	require.Same(t, LavaErrorNodeLimitExceeded, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32005, "Limit exceeded"))
	require.Same(t, LavaErrorNodeSolanaUnhealthy, ClassifyError(nil, ChainFamilySolana, TransportJsonRPC, -32005, "Node is behind by 100 slots"))
	require.Same(t, LavaErrorNodeInternalError, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32603, "Too Many Requests"))
	require.Same(t, LavaErrorNodeServerError, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32000, "request limit reached"))
	require.Same(t, LavaErrorNodeRateLimited, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 429, "Too Many Requests"))
}
