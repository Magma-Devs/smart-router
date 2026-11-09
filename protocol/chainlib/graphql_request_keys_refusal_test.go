package chainlib

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGraphQLListener_KeysTheNodeWouldReadDifferentlyAreRefusedBeforeAnyProvider is MAG-4908 at
// the listener: the ticket's three reproduction bodies, each decoded by the router as a read and by
// a serde-based node as the mutation, and a document that holds a mutation beside a query. Each is
// answered 400 with a GraphQL error document the caller can read, and no provider is asked. The
// control, a read with exact keys, is served.
func TestGraphQLListener_KeysTheNodeWouldReadDifferentlyAreRefusedBeforeAnyProvider(t *testing.T) {
	relay := &parsingRelay{}
	post := serveGraphQLListener(t, relay)

	const mutation = `mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { effects { status } } }`
	const both = `mutation M { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } } query Q { chainIdentifier }`

	for _, tc := range []struct{ name, body, wantMessage string }{
		{
			name:        "query and QUERY carrying different documents",
			body:        `{"query":"` + mutation + `","QUERY":"{ chainIdentifier }"}`,
			wantMessage: `"QUERY" differs from "query" only by letter case`,
		},
		{
			name:        "operationName and OPERATIONNAME naming different operations",
			body:        `{"query":"` + both + `","operationName":"M","OPERATIONNAME":"Q"}`,
			wantMessage: `"OPERATIONNAME" differs from "operationName" only by letter case`,
		},
		{
			name:        "query twice",
			body:        `{"query":"` + mutation + `","query":"{ chainIdentifier }"}`,
			wantMessage: `repeats the "query" member`,
		},
		{
			// The owner's ruling of 2026-11-09: a document that holds a mutation beside another
			// operation is refused, even with exact keys and operationName naming the query.
			name:        "a mutation beside a query, operationName naming the query",
			body:        `{"query":"` + both + `","operationName":"Q"}`,
			wantMessage: `holds a mutation beside another operation`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			relayed := relay.relayed
			status, body := post(tc.body)
			require.Equal(t, graphQLRefusalStatus, status, "body: %s", body)
			require.Contains(t, requireGraphQLError(t, body), tc.wantMessage)
			require.Equal(t, relayed, relay.relayed, "a refused request reaches no provider")
		})
	}

	t.Run("a request with exact keys is served", func(t *testing.T) {
		relayed := relay.relayed
		status, body := post(`{"query":"query Q { chainIdentifier }","operationName":"Q","variables":{},"extensions":{}}`)
		require.Equal(t, http.StatusOK, status, "body: %s", body)
		require.Equal(t, relayed+1, relay.relayed, "the control must reach a provider")
	})
}
