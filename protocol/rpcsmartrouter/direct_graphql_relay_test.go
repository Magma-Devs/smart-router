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
	"github.com/magma-Devs/smart-router/protocol/lavasession"
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
	}{
		{
			name:   "a success is passed through",
			status: http.StatusOK,
			reply:  `{"data":{"chainIdentifier":"35834a8a"}}`,
		},
		{
			name:   "an error with no code is the caller's",
			status: http.StatusOK,
			reply:  `{"data":null,"errors":[{"message":"Expected input type \"UInt53\", found \"abc\"."}]}`,
		},
		{
			name:          "a server-fault code inside a 200 is the node's",
			status:        http.StatusOK,
			reply:         `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`,
			wantNodeError: true,
			wantAtFault:   true,
		},
		{
			name:            "a 400 is a node error the registry files as the caller's",
			status:          http.StatusBadRequest,
			reply:           `{"errors":[{"message":"bad request"}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
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
			// Sui answers a query timeout inside an ordinary response: retried, and the endpoint's.
			name:          "REQUEST_TIMEOUT inside a 200 is the node's",
			status:        http.StatusOK,
			reply:         `{"data":null,"errors":[{"message":"Request timed out","extensions":{"code":"REQUEST_TIMEOUT"}}]}`,
			wantNodeError: true,
			wantAtFault:   true,
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
			// A rejected write must not end the broadcast as a success, and is the caller's.
			name:            "a rejected mutation is a node error filed as the caller's",
			request:         mutationBody,
			status:          http.StatusOK,
			reply:           `{"data":null,"errors":[{"message":"Invalid user signature"}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
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

			directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL + "/graphql"}, 1, "")
			require.NoError(t, err)
			sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "graphql-upstream"}

			chainMessage, err := chainParser.ParseMsg("", []byte(requestBody), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)

			result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
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
		})
	}
}
