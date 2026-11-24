package chainlib

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// noAnswerRelay is a relay that ends with no provider answer, the way the router's relay path ends
// one: the status the router chose, the client-facing verdict with the transport cause, and the
// reply headers the relay put on, its own Router-Guid among them.
func noAnswerRelay(status int, cf common.ClientFacingError) *recordingRelaySender {
	return &recordingRelaySender{send: answeringWith(&common.RelayResult{
		StatusCode:   status,
		ClientFacing: &cf,
		Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{{Name: common.GUID_HEADER_NAME, Value: "a-relay-guid"}}},
	}, errors.New(cf.NoAnswerMessage()))}
}

// TestGraphQLListener_NoProviderAnswerIsTheRoutersGraphQLError is MAG-5119's no-answer reply through
// the listener (the GraphQL chapter's cases T9 and T10): a read no provider answered gets the status
// the router chose, 502 when a provider refused the connection and 504 when every provider timed
// out, and one GraphQL error with the code NO_PROVIDER_ANSWER, whose guid equals Router-Guid, with no
// data key and no word of a provider's url, address or connection error.
//
// It used to be one JSON object with the key "error", and on a refused connection its text named
// the provider's url and address and said "connection refused".
func TestGraphQLListener_NoProviderAnswerIsTheRoutersGraphQLError(t *testing.T) {
	refused := common.ClientFacingError{
		Cause:           `http request failed: Post "https://sui.node.example.com/graphql/APIKEY123": dial tcp 192.0.2.7:443: connect: connection refused`,
		RouterErrorCode: 1002,
		RouterErrorName: "PROTOCOL_CONNECTION_REFUSED",
	}
	timedOut := common.ClientFacingError{TimedOut: true, Timeout: 30 * time.Second, RouterErrorCode: 1007, RouterErrorName: "PROTOCOL_CONTEXT_DEADLINE"}
	for _, tc := range []struct {
		name   string
		relay  *recordingRelaySender
		status int
	}{
		{"a provider refused the connection (T9)", noAnswerRelay(http.StatusBadGateway, refused), http.StatusBadGateway},
		{"every provider timed out (T10)", noAnswerRelay(http.StatusGatewayTimeout, timedOut), http.StatusGatewayTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			send := sendToGraphQLListener(t, tc.relay)
			reply := send(http.MethodPost, "/", `{"query":"{ chainIdentifier }"}`)

			require.Equal(t, tc.status, reply.status, "body: %s", reply.body)
			require.Equal(t, "application/json; charset=utf-8", reply.header.Get("Content-Type"))
			entry := requireGraphQLErrorEntry(t, reply.body)
			require.Equal(t, graphQLNoProviderAnswerCode, entry.Extensions["code"])
			require.Equal(t, entry.Extensions["guid"], reply.header.Get(common.GUID_HEADER_NAME), "Router-Guid equals extensions.guid")
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(reply.body), &raw))
			for _, leak := range []string{"sui.node.example.com", "APIKEY123", "192.0.2.7", "connection refused", "dial tcp"} {
				require.NotContains(t, reply.body, leak, "the reply names no provider and no transport error")
			}
		})
	}
}
