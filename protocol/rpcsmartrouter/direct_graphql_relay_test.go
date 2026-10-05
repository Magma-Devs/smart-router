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
			Apis: []*spectypes.Api{{
				Name:         "chainIdentifier",
				Enabled:      true,
				ComputeUnits: 10,
				BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
			}},
		}},
	})

	const requestBody = `{"query":"{ chainIdentifier }"}`

	tests := []struct {
		name            string
		status          int
		reply           string
		wantNodeError   bool
		wantNonRetrying bool
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
		},
		{
			name:            "a 400 is a node error the registry files as the caller's",
			status:          http.StatusBadRequest,
			reply:           `{"errors":[{"message":"bad request"}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
		})
	}
}
