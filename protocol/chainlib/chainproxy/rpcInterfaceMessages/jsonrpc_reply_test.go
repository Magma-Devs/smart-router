package rpcInterfaceMessages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A JSON-RPC node answers in JSON-RPC: an object carrying "result" or "error", or a batch of them.
// Anything else is not the node's answer (MAG-5072).
func TestIsJsonrpcReply(t *testing.T) {
	cases := map[string]struct {
		body string
		want bool
	}{
		"error object":                   {`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"no"}}`, true},
		"result":                         {`{"jsonrpc":"2.0","id":1,"result":"0x1"}`, true},
		"null result":                    {`{"jsonrpc":"2.0","id":1,"result":null}`, true},
		"error as a string":              {`{"id":1,"error":"bad request"}`, true},
		"application error in result":    {`{"jsonrpc":"2.0","id":1,"result":{"response":{"code":18,"log":"invalid height"}}}`, true},
		"batch":                          {`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"x"}}]`, true},
		"html page":                      {`<html><body>403 Forbidden</body></html>`, false},
		"empty":                          {``, false},
		"plain text":                     {`Forbidden`, false},
		"empty object":                   {`{}`, false},
		"object without result or error": {`{"message":"Missing Authentication Token"}`, false},
		"empty batch":                    {`[]`, false},
		"batch with a non-reply":         {`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"status":"error"}]`, false},
		"array of numbers":               {`[1,2,3]`, false},
		"truncated":                      {`{"jsonrpc":"2.0","id":1,"error":{"code":`, false},
	}
	for name, tc := range cases {
		require.Equal(t, tc.want, IsJsonrpcReply([]byte(tc.body)), name)
	}
}
