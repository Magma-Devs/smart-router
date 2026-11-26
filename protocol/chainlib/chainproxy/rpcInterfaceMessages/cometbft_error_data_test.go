package rpcInterfaceMessages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// CometBFT answers most failures as -32603 "Internal error" with the reason in data, and the reader
// used to drop data, so every one of them read as "Internal error" (MAG-5325). The reader's text is
// what both the direct-RPC sender and the results manager classify.
func TestCheckResponseError_InternalErrorCarriesItsData(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "a data string",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"tx already exists in cache"}}`,
			want: "Internal error: tx already exists in cache",
		},
		{
			name: "no data",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}`,
			want: "Internal error",
		},
		{
			name: "an empty data string",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":""}}`,
			want: "Internal error",
		},
		{
			name: "object data",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":{"log":"tx already exists in cache"}}}`,
			want: "Internal error",
		},
		{
			name: "data null",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":null}}`,
			want: "Internal error",
		},
		{
			name: "a code written as a string",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":"-32603","message":"Internal error","data":"tx already exists in cache"}}`,
			want: "Internal error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reader := range []struct {
				name  string
				check func([]byte, int) (bool, string)
			}{
				{"tendermintrpc", TendermintrpcMessage{}.CheckResponseError},
				{"jsonrpc", JsonrpcMessage{}.CheckResponseError},
			} {
				hasError, message := reader.check([]byte(tc.body), 200)
				require.True(t, hasError, reader.name)
				require.Equal(t, tc.want, message, reader.name)
			}
		})
	}
}

// Only an internal error takes its data. Geth sends a revert's ABI-encoded reason as data beside code
// 3, and Invalid params' data is CometBFT's own detail; each keeps its message alone, so its text and
// its verdict are what they were.
func TestCheckResponseError_OtherCodesKeepTheirMessageAlone(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "geth revert",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted","data":"0x08c379a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000047465737400000000000000000000000000000000000000000000000000000000"}}`,
			want: "execution reverted",
		},
		{
			name: "CometBFT invalid params",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Invalid params","data":"error converting json params to arguments: invalid character"}}`,
			want: "Invalid params",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hasError, message := JsonrpcMessage{}.CheckResponseError([]byte(tc.body), 200)
			require.True(t, hasError)
			require.Equal(t, tc.want, message)
		})
	}
}

// A batch element is read by the single reply's rule, so its data reaches the batch's text too.
func TestCheckResponseErrorForTendermintBatch_InternalErrorCarriesItsData(t *testing.T) {
	body := `[{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"tx (ABCDEF) not found"}}]`
	hasError, message := CheckResponseErrorForTendermintBatch([]byte(body), 200)
	require.True(t, hasError)
	require.Equal(t, "Internal error: tx (ABCDEF) not found", message)
}
