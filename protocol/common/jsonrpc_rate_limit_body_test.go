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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, tc.code, tc.message)
			require.Same(t, RouterErrorNodeRateLimited, got)
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
	require.Same(t, RouterErrorNodeLimitExceeded, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32005, "Limit exceeded"))
	require.Same(t, RouterErrorNodeSolanaUnhealthy, ClassifyError(nil, ChainFamilySolana, TransportJsonRPC, -32005, "Node is behind by 100 slots"))
	require.Same(t, RouterErrorNodeInternalError, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32603, "Too Many Requests"))
	require.Same(t, RouterErrorNodeServerError, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32000, "request limit reached"))
	require.Same(t, RouterErrorNodeRateLimited, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 429, "Too Many Requests"))
}

// "rate limit" and "enhance_your_calm" in a JSON-RPC error are no rate limit on any chain: wording
// decides only through a chain row (rulebook Part IV 3.3), and a rate limit is an HTTP 429 or a
// chain's own rate-limit or quota row. Read on every chain, a write whose every copy said so was
// sent again (MAG-5487). Under -32000 the error is the -32000 row's; with no code it is unknown.
func TestJSONRPC_RateLimitWordingIsNoRateLimit(t *testing.T) {
	families := []ChainFamily{ChainFamilyEVM, ChainFamilySolana, ChainFamilyBitcoin, ChainFamilyUnknown}
	messages := []string{"rate limit exceeded", "Rate limit reached for requests", "enhance_your_calm"}
	want := map[int]*RouterError{-32000: RouterErrorNodeServerError, 0: RouterErrorUnknown}
	for _, family := range families {
		for code, verdict := range want {
			for _, message := range messages {
				got := ClassifyError(nil, family, TransportJsonRPC, code, message)
				require.Same(t, verdict, got, "family %d, code %d, %q", family, code, message)
				require.False(t, got.SubCategory.IsRateLimit(), "family %d, code %d, %q", family, code, message)
			}
		}
	}
}

// What still decides a rate limit on JSON-RPC: a chain's own row (XRPL tooBusy) and the HTTP status.
func TestJSONRPC_RateLimitStillDecidedByChainRowAndStatus(t *testing.T) {
	require.Same(t, RouterErrorNodeRateLimited, ClassifyError(nil, ChainFamilyXRP, TransportJsonRPC, 0, "tooBusy"))
	require.Same(t, RouterErrorNodeRateLimited, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, 0, "HTTP 429: rate limit exceeded"))
}
