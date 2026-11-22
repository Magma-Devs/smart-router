package rpcsmartrouter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// TestGraphQLDirectRelay drives a GraphQL request through the direct-RPC sender, which is the
// path every live relay takes. Without a graphql case in sendForInterface the interface parsed
// requests but could not send one.
func TestGraphQLDirectRelay(t *testing.T) {
	ctx := context.Background()

	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(spectypes.Spec{
		Index:            "SUI",
		Enabled:          true,
		AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{
					Name:         "chainIdentifier",
					Enabled:      true,
					ComputeUnits: 10,
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
				{
					Name:         "executeTransaction",
					Enabled:      true,
					ComputeUnits: 100,
					Category:     spectypes.SpecCategory{Stateful: 1},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
			},
		}},
	})

	const (
		readBody     = `{"query":"{ chainIdentifier }"}`
		mutationBody = `{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`
	)

	tests := []struct {
		name            string
		request         string
		status          int
		reply           string
		wantNodeError   bool
		wantNonRetrying bool
		wantRateLimited bool
		wantAtFault     bool
		wantCapability  bool
		// wantTransportFailure: the sender returns an error and no reply.
		wantTransportFailure bool
	}{
		{
			name:   "a success is passed through",
			status: http.StatusOK,
			reply:  `{"data":{"chainIdentifier":"35834a8a"}}`,
		},
		{
			// When unsure, retry without blame (MAG-5121).
			name:           "an error with no code is unknown: retried, not blamed",
			status:         http.StatusOK,
			reply:          `{"data":null,"errors":[{"message":"request timed out"}]}`,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			name:            "a validation failure is the caller's: not retried, not blamed",
			status:          http.StatusOK,
			reply:           `{"data":null,"errors":[{"message":"Unknown field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
		},
		{
			name:            "a parse failure is the caller's",
			status:          http.StatusOK,
			reply:           `{"data":null,"errors":[{"message":"recursion limit exceeded.","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
		},
		{
			// Read as no code, not as an unreadable reply blamed as a bad gateway (MAG-5121 test 13).
			name:           "a numeric code is no code",
			status:         http.StatusOK,
			reply:          `{"data":null,"errors":[{"message":"bad","extensions":{"code":400}}]}`,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			// A limit inside the on-chain data, the same on every provider: Sui's R16 (MAG-4909), the
			// caller's. Not a vendor quota, so no pause.
			name:            "RESOURCE_EXHAUSTED with a path is the caller's on Sui, not a rate limit",
			status:          http.StatusOK,
			reply:           `{"data":null,"errors":[{"message":"Display output too large","path":["object"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
		},
		{
			name:          "a server-fault code inside a 200 is the node's",
			status:        http.StatusOK,
			reply:         `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`,
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			// No code under a 400: the status table, where an unnamed 4xx is tried on another
			// provider without blame (MAG-5121 Change 7). It used to be the caller's.
			name:           "a codeless error under a 400 is tried on another provider",
			status:         http.StatusBadRequest,
			reply:          `{"errors":[{"message":"bad request"}]}`,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			// Sui's 429 arrives inside a 200; it must reach the rate-limit hold-off.
			name:            "RESOURCE_EXHAUSTED inside a 200 is a rate limit",
			status:          http.StatusOK,
			reply:           `{"data":null,"errors":[{"message":"slow down","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`,
			wantNodeError:   true,
			wantRateLimited: true,
		},
		{
			// Sui answers a query timeout inside an ordinary response. Its limit is an operator
			// setting (query_timeout_ms), so it is Sui's R20 (MAG-4909): retried elsewhere, not blamed.
			name:           "REQUEST_TIMEOUT inside a 200 is this operator's limit on Sui",
			status:         http.StatusOK,
			reply:          `{"data":null,"errors":[{"message":"Query timed out after 40.00s","extensions":{"code":"REQUEST_TIMEOUT"}}]}`,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			// This operator lacks the store the query needs: retried elsewhere, not scored.
			name:           "FEATURE_UNAVAILABLE inside a 200 is a node capability",
			status:         http.StatusOK,
			reply:          `{"data":null,"errors":[{"message":"not available","extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			// A refused write must not end the broadcast as a success. Sui's W9 (MAG-4909): the
			// network refused this copy, which is the caller's, not the provider's. The reply is Sui's
			// own (sui-indexer-alt-e2e-tests, execute_transaction_grpc_error.snap).
			name:            "a refused mutation is a node error filed as the caller's",
			request:         mutationBody,
			status:          http.StatusOK,
			reply:           `{"data":null,"errors":[{"message":"Invalid argument: Invalid user signature: Signature is not valid: Fail to verify user sig Invalid signature was given to the function","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
		},
		{
			// Sui's W11: a codeless error on a write tells nothing, so the copy may have run. Filed
			// as unknown: not blamed. A write is never re-sent, whatever the registry says.
			name:           "a codeless error on a mutation is unknown",
			request:        mutationBody,
			status:         http.StatusOK,
			reply:          `{"data":null,"errors":[{"message":"Invalid user signature"}]}`,
			wantNodeError:  true,
			wantCapability: true,
		},
		// A 2xx carrying neither data nor errors is not a GraphQL response (MAG-4369): a gateway
		// answered, not the node. Retried on another provider and blamed on the endpoint, as a 502.
		{
			name:          "an empty object inside a 200 is not the node's answer",
			status:        http.StatusOK,
			reply:         `{}`,
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			name:          "null data with no errors inside a 200 is not the node's answer",
			status:        http.StatusOK,
			reply:         `{"data":null}`,
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			name:          "a gateway's key rejection inside a 200 is not the node's answer",
			status:        http.StatusOK,
			reply:         `{"error":"Invalid API key"}`,
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			name:          "a JSON-RPC error envelope inside a 200 is not the node's answer",
			status:        http.StatusOK,
			reply:         `{"jsonrpc":"2.0","id":1,"error":{"code":-32007,"message":"50/second request limit reached"}}`,
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			name:          "an empty 200 is not the node's answer",
			status:        http.StatusOK,
			reply:         ``,
			wantNodeError: true,
			wantAtFault:   true,
		},
		// A page that is not JSON at all is a whole reply from whatever answered (MAG-5121): on a
		// read it is kept as this provider's reply, blamed as a bad gateway.
		{
			name:          "an HTML page inside a 200 is kept, and blamed",
			status:        http.StatusOK,
			reply:         "<html><head><title>502 Bad Gateway</title></head><body>nginx</body></html>",
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			name:          "a text page inside a 200 is kept, and blamed",
			status:        http.StatusOK,
			reply:         "upstream connect error or disconnect/reset before headers",
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			// The page's words are not read as the provider's own: a quota page is still a gateway.
			name:          "an HTML quota page inside a 200 is a bad gateway, not a pause",
			status:        http.StatusOK,
			reply:         "\n  <html><body>Too Many Requests</body></html>",
			wantNodeError: true,
			wantAtFault:   true,
		},
		// A body that starts like JSON and does not parse was cut on the way: a dropped
		// connection, so the sender fails and no reply is kept.
		{
			name:                 "JSON cut off inside a 200 is a dropped connection",
			status:               http.StatusOK,
			reply:                `{"data":{"chainIdentifier":"358`,
			wantTransportFailure: true,
		},
		{
			name:                 "a JSON list cut off after whitespace is a dropped connection",
			status:               http.StatusOK,
			reply:                "\r\n [{\"data\":",
			wantTransportFailure: true,
		},
		{
			// Only a 2xx page is new: under an error status a page was always kept and judged by
			// its status, and stays so.
			name:          "an HTML page under a 503 is kept, and blamed under its status",
			status:        http.StatusServiceUnavailable,
			reply:         "<html><head><title>503 Service Temporarily Unavailable</title></head><body>nginx</body></html>",
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			// A write keeps the transport failure, so the broadcast knows it may have run (MAG-5122).
			name:                 "an HTML page answering a mutation is still a transport failure",
			request:              mutationBody,
			status:               http.StatusOK,
			reply:                "<html><body>502 Bad Gateway</body></html>",
			wantTransportFailure: true,
		},
		{
			// On a write this is what ended the broadcast at 0 s while the other provider landed
			// the transaction (Fuel Sepolia, 2026-10-15).
			name:          "a gateway's key rejection of a mutation is not the node's answer",
			request:       mutationBody,
			status:        http.StatusOK,
			reply:         `{"error":"Invalid API key"}`,
			wantNodeError: true,
			wantAtFault:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestBody := test.request
			if requestBody == "" {
				requestBody = readBody
			}
			var gotMethod, gotPath, gotBody string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				gotMethod, gotPath, gotBody = r.Method, r.URL.Path, string(body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.reply))
			}))
			defer server.Close()

			directConn, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL + "/graphql"}, 1, "")
			require.NoError(t, err)
			sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "graphql-upstream"}

			chainMessage, err := chainParser.ParseMsg("", []byte(requestBody), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)

			result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
			if test.wantTransportFailure {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)

			require.Equal(t, http.MethodPost, gotMethod)
			require.Equal(t, "/graphql", gotPath, "every operation goes to the endpoint's one URL")
			require.Equal(t, requestBody, gotBody, "the document is forwarded as the caller wrote it")
			require.Equal(t, test.status, result.StatusCode)
			require.Equal(t, test.reply, string(result.Reply.Data))
			require.Equal(t, test.wantNodeError, result.IsNodeError)
			require.Equal(t, test.wantNonRetrying, result.IsNonRetryable)
			require.Equal(t, test.wantRateLimited, result.IsRateLimited)
			require.Equal(t, test.wantAtFault, result.IsNodeAtFault)
			require.Equal(t, test.wantCapability, result.IsNodeCapability)
			require.Equal(t, !test.wantNodeError, relayProvesEndpointHealthy(result),
				"only the node's own answer may reset the endpoint's failure count")
		})
	}
}
