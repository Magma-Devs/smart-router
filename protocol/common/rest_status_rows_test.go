package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRESTStatusRowsAreRESTOnly pins two things about the 400/403/410/422 rows: on REST they
// classify (the status is the node's answer), and on JSON-RPC and gRPC they do not exist — a bare
// 400 or 403 there is a gateway in front of the node, and the classifier's "unknown ⇒ retry
// elsewhere" default must keep applying. The JSON-RPC sender prefixes "HTTP <status>: " to the
// message of a non-2xx reply, which is exactly the string a shared HTTPStatusContains row would
// have matched; the assertion below uses that string.
func TestRESTStatusRowsAreRESTOnly(t *testing.T) {
	cases := []struct {
		status int
		want   *LavaError
	}{
		{400, LavaErrorUserInvalidRequest},
		{403, LavaErrorNodeUnauthorized},
		{410, LavaErrorChainStatePruned},
		{422, LavaErrorUserInvalidParams},
	}
	for _, tc := range cases {
		rest := ClassifyError(nil, ChainFamilyUnknown, TransportREST, tc.status, "HTTP 400: whatever the body said")
		require.Same(t, tc.want, rest, "REST %d must classify by its status row", tc.status)
		require.False(t, tc.want.EndpointAtFault(), "REST %d is never the endpoint's fault", tc.status)

		jsonrpc := ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, tc.status, "HTTP "+itoa(tc.status)+": malformed JSON-RPC response: body is not valid JSON")
		require.Same(t, LavaErrorUnknown, jsonrpc, "JSON-RPC %d must stay unknown (retry elsewhere)", tc.status)

		grpc := ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, tc.status, "HTTP "+itoa(tc.status)+": unavailable")
		require.Same(t, LavaErrorUnknown, grpc, "gRPC %d must stay unknown", tc.status)
	}
}

func itoa(n int) string {
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}
