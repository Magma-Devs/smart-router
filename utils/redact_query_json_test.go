package utils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// A url at the end of a JSON string whose query ends in "=" is followed by the string's closing
// quote. That quote does not open a quoted query value, so the url ends there, the keys after it
// survive and a redacted JSON body is still JSON.
func TestRedactSecrets_AQueryEndingInEqualsInsideJSONKeepsTheJSON(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{
			`{"url":"https://host.example.com/p?a=","b":"c"}`,
			`{"url":"https://host.example.com/[redacted]","b":"c"}`,
		},
		{
			`{"u":"https://host.example.com/p?sig=abc==","next":"v","x":1}`,
			`{"u":"https://host.example.com/[redacted]","next":"v","x":1}`,
		},
		{
			`{"a":{"u":"https://host.example.com/p?x="},"b":"c"}`,
			`{"a":{"u":"https://host.example.com/[redacted]"},"b":"c"}`,
		},
		{
			`["https://host.example.com/p?x=","y"]`,
			`["https://host.example.com/[redacted]","y"]`,
		},
		{
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"see https://docs.example.com/err?code=","data":"keep-me"}}`,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"see https://docs.example.com/[redacted]","data":"keep-me"}}`,
		},
		{
			`{"m":"dial node.example.com:443/v2?k=","n":"keep-me"}`,
			`{"m":"dial node.example.com:443/[redacted]","n":"keep-me"}`,
		},
	} {
		require.True(t, json.Valid([]byte(tc.in)), tc.in)
		got := RedactSecrets(tc.in)
		require.Equal(t, tc.want, got)
		require.True(t, json.Valid([]byte(got)), got)
	}
}
