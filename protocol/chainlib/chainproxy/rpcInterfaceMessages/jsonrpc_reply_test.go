package rpcInterfaceMessages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A JSON-RPC node answers in JSON-RPC: an object whose "error" is an error object or that carries
// a "result", or a batch of them. Anything else is not the node's answer (MAG-5072).
func TestIsJsonrpcReply(t *testing.T) {
	cases := map[string]struct {
		body string
		want bool
	}{
		"error object":                      {`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"no"}}`, true},
		"empty error object":                {`{"jsonrpc":"2.0","id":1,"error":{}}`, true},
		"result":                            {`{"jsonrpc":"2.0","id":1,"result":"0x1"}`, true},
		"null result":                       {`{"jsonrpc":"2.0","id":1,"result":null}`, true},
		"null error beside a result":        {`{"jsonrpc":"2.0","id":1,"error":null,"result":"0x1"}`, true},
		"JSON-RPC 1.0 error":                {`{"result":null,"error":{"code":-5,"message":"Block not found"},"id":1}`, true},
		"application error in result":       {`{"jsonrpc":"2.0","id":1,"result":{"response":{"code":18,"log":"invalid height"}}}`, true},
		"XRPL error in result":              {`{"result":{"error":"lgrNotFound","error_code":21,"status":"error"}}`, true},
		"leading whitespace":                {" \n{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32000,\"message\":\"x\"}}", true},
		"batch":                             {`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"x"}}]`, true},
		"batch after leading whitespace":    {" \n[{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}]", true},
		"error as a string":                 {`{"id":1,"error":"bad request"}`, false},
		"gateway refusal":                   {`{"error":"invalid api key"}`, false},
		"error as a string beside a result": {`{"error":"invalid api key","result":"0x1"}`, false},
		"error as a number":                 {`{"id":1,"error":-32000}`, false},
		"null error alone":                  {`{"jsonrpc":"2.0","id":1,"error":null}`, false},
		"html page":                         {`<html><body>403 Forbidden</body></html>`, false},
		"empty":                             {``, false},
		"plain text":                        {`Forbidden`, false},
		"null":                              {`null`, false},
		"empty object":                      {`{}`, false},
		"object without result or error":    {`{"message":"Missing Authentication Token"}`, false},
		"empty batch":                       {`[]`, false},
		"batch with a non-reply":            {`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"status":"error"}]`, false},
		"batch with a string error":         {`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"id":2,"error":"invalid api key"}]`, false},
		"array of numbers":                  {`[1,2,3]`, false},
		"truncated":                         {`{"jsonrpc":"2.0","id":1,"error":{"code":`, false},
		"truncated batch":                   {`[{"jsonrpc":"2.0","id":1,"result":"0x1"}`, false},
	}
	for name, tc := range cases {
		require.Equal(t, tc.want, IsJsonrpcReply([]byte(tc.body)), name)
	}
}
