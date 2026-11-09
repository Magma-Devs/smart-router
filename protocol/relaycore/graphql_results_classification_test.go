package relaycore

// The results manager classifies a node error a second time, after the sender, for its log line,
// the smartrouter_errors_total label and the best-error choice. A GraphQL node answers its faults
// inside an HTTP 200, so classifying by the status filed every one of them as UNKNOWN_ERROR at
// DEBUG, while the sender, which reads the error's code, had already retried it correctly
// (MAG-4375). These tests run real GraphQL messages through SetResponse.

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	graphQLResultsCheckpointRead = `{"query":"{ checkpoint(sequenceNumber: 1000) { digest } }"}`
	graphQLResultsSubmit         = `{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { effects { status } } }"}`
	nodeErrorReplyLogLine        = "received node error reply from provider"
)

// graphQLResultsProtocolMessage parses body through the real GraphQL chain parser, with a Sui-shaped
// spec holding one read and the write surface.
func graphQLResultsProtocolMessage(t *testing.T, body string) chainlib.ProtocolMessage {
	t.Helper()
	emptyBlockParsing := spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY}
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
				{Name: "checkpoint", Enabled: true, ComputeUnits: 20, Category: spectypes.SpecCategory{Deterministic: true}, BlockParsing: emptyBlockParsing},
				{Name: "executeTransaction", Enabled: true, ComputeUnits: 100, Category: spectypes.SpecCategory{Stateful: 1}, BlockParsing: emptyBlockParsing},
			},
		}},
	})
	chainMessage, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	return chainlib.NewProtocolMessage(chainMessage, nil, &pairingtypes.RelayPrivateData{}, "dapp", "127.0.0.1")
}

func graphQLErrorReply(message, code string) string {
	if code == "" {
		return `{"data":null,"errors":[{"message":"` + message + `"}]}`
	}
	return `{"data":null,"errors":[{"message":"` + message + `","extensions":{"code":"` + code + `"}}]}`
}

// graphQLNodeErrorResponse is a reply the GraphQL sender returned as a node error: a 200 carrying
// the node's errors document.
func graphQLNodeErrorResponse(provider, reply string) *RelayResponse {
	return &RelayResponse{RelayResult: common.RelayResult{
		Reply:        &pairingtypes.RelayReply{Data: []byte(reply)},
		StatusCode:   http.StatusOK,
		IsNodeError:  true,
		ProviderInfo: common.ProviderInfo{ProviderAddress: provider},
	}}
}

// captureNodeErrorReplies runs fn with the debug ring and the error-metric callback both recording,
// and returns the level of every "received node error reply from provider" line and every
// smartrouter_errors_total label fn emitted. Both sinks are process-wide and restored on cleanup.
func captureNodeErrorReplies(t *testing.T, fn func()) (levels, metricNames []string) {
	t.Helper()
	var metricMu sync.Mutex
	common.SetErrorMetricsCallback(func(_ uint32, errorName string, _ string, _ bool, _ string) {
		metricMu.Lock()
		defer metricMu.Unlock()
		metricNames = append(metricNames, errorName)
	})
	t.Cleanup(func() { common.SetErrorMetricsCallback(nil) })
	utils.EnableDebugLogBuffer(10000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	utils.ClearDebugLogBuffer()

	fn()

	for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 10000) {
		record := map[string]any{}
		if json.Unmarshal(line, &record) != nil || record["message"] != nodeErrorReplyLogLine {
			continue
		}
		level, _ := record["level"].(string)
		levels = append(levels, level)
	}
	return levels, metricNames
}

// TestResultsManager_GraphQLInBodyFaultIsClassifiedByItsCode: each GraphQL fault code lands on the
// registry row the sender's ClassificationStatus picks for it, so the log level, the error metric's
// label and the error the best-error choice ranks all say what the node said.
func TestResultsManager_GraphQLInBodyFaultIsClassifiedByItsCode(t *testing.T) {
	tests := []struct {
		name        string
		request     string
		reply       string
		wantName    string
		wantAtFault bool
		wantLevel   string
	}{
		{
			name:        "INTERNAL_SERVER_ERROR is the node's internal error, logged at ERROR",
			request:     graphQLResultsCheckpointRead,
			reply:       graphQLErrorReply("boom", "INTERNAL_SERVER_ERROR"),
			wantName:    common.LavaErrorNodeInternalError.Name,
			wantAtFault: true,
			wantLevel:   "error",
		},
		{
			name:        "REQUEST_TIMEOUT is a gateway timeout, logged at ERROR",
			request:     graphQLResultsCheckpointRead,
			reply:       graphQLErrorReply("Request timed out", "REQUEST_TIMEOUT"),
			wantName:    common.LavaErrorNodeGatewayTimeout.Name,
			wantAtFault: true,
			wantLevel:   "error",
		},
		{
			name:      "RESOURCE_EXHAUSTED is a rate limit: busy, not broken",
			request:   graphQLResultsCheckpointRead,
			reply:     graphQLErrorReply("slow down", "RESOURCE_EXHAUSTED"),
			wantName:  common.LavaErrorNodeRateLimited.Name,
			wantLevel: "debug",
		},
		{
			name:      "FEATURE_UNAVAILABLE is a capability this node does not offer",
			request:   graphQLResultsCheckpointRead,
			reply:     graphQLErrorReply("not available", "FEATURE_UNAVAILABLE"),
			wantName:  common.LavaErrorNodeAccessDenied.Name,
			wantLevel: "debug",
		},
		{
			// CheckResponseError's mutation rule: any error on a write is a node error, and
			// ClassificationStatus files a code-less one as the caller's 400.
			name:      "a code-less error on a mutation is the caller's invalid request",
			request:   graphQLResultsSubmit,
			reply:     graphQLErrorReply("Invalid user signature", ""),
			wantName:  common.LavaErrorUserInvalidRequest.Name,
			wantLevel: "debug",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			protocolMessage := graphQLResultsProtocolMessage(t, test.request)
			resultsManager := NewResultsManager(1, "SUI")

			levels, metricNames := captureNodeErrorReplies(t, func() {
				require.Error(t, resultsManager.SetResponse(graphQLNodeErrorResponse("provider-a", test.reply), protocolMessage))
			})

			best := resultsManager.GetBestNodeErrorMessageForUser()
			require.NotNil(t, best.LavaError)
			// assert, not require: a wrong classification shows in all four at once.
			assert.Equal(t, test.wantName, best.LavaError.Name, "classified by the 200's status instead of the error's code")
			assert.Equal(t, test.wantAtFault, best.LavaError.EndpointAtFault())
			assert.Equal(t, []string{test.wantName}, metricNames, "the smartrouter_errors_total label")
			assert.Equal(t, []string{test.wantLevel}, levels, "the level of the node-error line")

			// The sender's verdict on the same reply (sendGraphQLRelay: ClassificationStatus, then
			// the registry on the REST transport), which drove the retry: the two must agree.
			hasError, message := protocolMessage.CheckResponseError([]byte(test.reply), http.StatusOK)
			require.True(t, hasError)
			statusClassifier, ok := protocolMessage.GetRPCMessage().(replyStatusClassifier)
			require.True(t, ok, "a GraphQL message classifies its own replies")
			senderStatus := statusClassifier.ClassificationStatus([]byte(test.reply), http.StatusOK)
			sender := common.ClassifyNodeErrorForRetry(common.GetChainFamilyOrDefault("SUI"), common.TransportREST, senderStatus, message)
			assert.Equal(t, sender.IsNodeAtFault, best.LavaError.EndpointAtFault(), "the results manager blames the endpoint differently from the sender")
			assert.Equal(t, sender.IsRateLimited, best.LavaError.SubCategory.IsRateLimit())
			assert.Equal(t, sender.IsNonRetryable, !best.LavaError.Retryable)
		})
	}
}

// TestResultsManager_GraphQLBestErrorKeepsItsChoice pins what the reclassification does and does not
// change for the client. The best-error choice ranks by agreement first and by category second, and
// every row a GraphQL fault lands on is External, as UNKNOWN_ERROR was: the choice, and with it the
// body a failed relay returns, is the same, and only the label on the chosen error is now the
// node's.
func TestResultsManager_GraphQLBestErrorKeepsItsChoice(t *testing.T) {
	protocolMessage := graphQLResultsProtocolMessage(t, graphQLResultsCheckpointRead)
	internal := graphQLErrorReply("boom", "INTERNAL_SERVER_ERROR")
	rateLimited := graphQLErrorReply("slow down", "RESOURCE_EXHAUSTED")

	resultsManager := NewResultsManager(1, "SUI")
	for provider, reply := range map[string]string{"provider-a": internal, "provider-b": rateLimited, "provider-c": internal} {
		require.Error(t, resultsManager.SetResponse(graphQLNodeErrorResponse(provider, reply), protocolMessage))
	}

	best := resultsManager.GetBestNodeErrorMessageForUser()
	require.NotNil(t, best.Response)
	require.Equal(t, internal, string(best.Response.RelayResult.Reply.Data), "the two providers that agree are the answer")
	require.Equal(t, common.LavaErrorNodeInternalError.Name, best.LavaError.Name)

	require.Equal(t, common.CategoryExternal, common.LavaErrorUnknown.Category,
		"the old label was External too, so the category preference cannot have reordered the candidates")
	for _, code := range []string{"INTERNAL_SERVER_ERROR", "REQUEST_TIMEOUT", "RESOURCE_EXHAUSTED", "FEATURE_UNAVAILABLE"} {
		resultsManager := NewResultsManager(1, "SUI")
		require.Error(t, resultsManager.SetResponse(graphQLNodeErrorResponse("provider-a", graphQLErrorReply("m", code)), protocolMessage))
		require.Equal(t, common.CategoryExternal, resultsManager.GetBestNodeErrorMessageForUser().LavaError.Category, code)
	}
}
