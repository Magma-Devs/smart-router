package rpcInterfaceMessages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The reply half of MAG-5123's cache rule. The router-level test (rpcsmartrouter
// TestGraphQLCacheKeepsOnlyCleanAnswers) shows each kind kept out of the cache end to end; this
// one pins which replies are which, the edges included.
func TestGraphQLReplyCacheExclusion(t *testing.T) {
	cases := []struct {
		name  string
		reply string
		// cached is true when the reply may be written.
		cached bool
	}{
		{name: "a clean answer", reply: `{"data":{"epoch":{"epochId":"912"}}}`, cached: true},
		{name: "a clean answer with an empty errors list", reply: `{"data":{"epoch":{"epochId":"912"}},"errors":[]}`, cached: true},
		{name: "a clean answer with a null errors member", reply: `{"data":{"epoch":{"epochId":"912"}},"errors":null}`, cached: true},
		{name: "a null below the root is part of an answer", reply: `{"data":{"epoch":{"endTimestamp":null}}}`, cached: true},
		{name: "an empty list below the root is part of an answer", reply: `{"data":{"checkpoints":{"nodes":[]}}}`, cached: true},
		{name: "a root list with elements", reply: `{"data":{"multiGetTransactions":[{"digest":"a"}]}}`, cached: true},
		{name: "a root false is an answer", reply: `{"data":{"verifySignature":{"success":false}}}`, cached: true},

		{name: "an error", reply: `{"errors":[{"message":"Unknown field"}]}`},
		{name: "a partial reply", reply: `{"data":{"epoch":{"epochId":"912"}},"errors":[{"message":"timed out","path":["epoch","referenceGasPrice"]}]}`},
		{name: "a null root field", reply: `{"data":{"transaction":null}}`},
		{name: "a null root field beside an answer", reply: `{"data":{"epoch":{"epochId":"912"},"transaction":null}}`},
		{name: "an aliased null root field", reply: `{"data":{"tx":null}}`},
		{name: "an empty root list", reply: `{"data":{"multiGetTransactions":[]}}`},
		{name: "an empty root list with spaces", reply: `{"data":{"multiGetTransactions":[ ]}}`},
		{name: "a null data", reply: `{"data":null}`},
		{name: "no data", reply: `{}`},
		{name: "not JSON", reply: `<html>bad gateway</html>`},
		{name: "a data that is not an object", reply: `{"data":"ok"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			why := GraphQLReplyCacheExclusion([]byte(tc.reply))
			if tc.cached {
				require.Empty(t, why)
			} else {
				require.NotEmpty(t, why)
			}
		})
	}
}
