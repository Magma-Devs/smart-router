package relaycore

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

const suiNullTransaction = `{"data":{"transaction":null}}`

// suiLookupProtocolMessage is a Sui transaction looked up by its digest (MAG-4909 R23), parsed
// through the real GraphQL chain parser.
func suiLookupProtocolMessage(t *testing.T) chainlib.ProtocolMessage {
	t.Helper()
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
				Name: "transaction", Enabled: true, ComputeUnits: 10,
				BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
			}},
		}},
	})
	chainMessage, err := chainParser.ParseMsg("", []byte(`{"query":"{ transaction(digest: \"x\") { digest } }"}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	return chainlib.NewProtocolMessage(chainMessage, nil, &pairingtypes.RelayPrivateData{}, "dapp", "127.0.0.1")
}

// suiNullTransactionResponse is a null transaction as sendGraphQLRelay hands it over: a node error
// marked as a not-found answer.
func suiNullTransactionResponse() *RelayResponse {
	response := graphQLNodeErrorResponse("provider-a", suiNullTransaction)
	response.RelayResult.IsNotFoundAnswer = true
	return response
}

// TestResultsManager_NotFoundAnswerUnderCrossValidation (MAG-5121 P6, MAG-4909 R23): a null Sui
// transaction is a node error for a read that asks one provider at a time, so that another
// provider is asked, and an answer under cross-validation, whose providers agree on the null as on
// any other reply. Filed as a node error there, a transaction no provider holds failed the quorum
// with HTTP 500 where it used to be the null.
func TestResultsManager_NotFoundAnswerUnderCrossValidation(t *testing.T) {
	protocolMessage := suiLookupProtocolMessage(t)
	hasError, _ := protocolMessage.CheckResponseError([]byte(suiNullTransaction), http.StatusOK)
	require.True(t, hasError, "setup: the null is a not-found verdict, so the exemption below is what files it")

	t.Run("one provider at a time: a node error", func(t *testing.T) {
		resultsManager := newResultsManager(1, "SUI", false)
		require.Error(t, resultsManager.SetResponse(suiNullTransactionResponse(), protocolMessage))
		successes, nodeErrors, _ := resultsManager.GetResultsData()
		require.Empty(t, successes)
		require.Len(t, nodeErrors, 1)
	})

	t.Run("cross-validation: an answer", func(t *testing.T) {
		resultsManager := newResultsManager(1, "SUI", true)
		require.NoError(t, resultsManager.SetResponse(suiNullTransactionResponse(), protocolMessage))
		successes, nodeErrors, _ := resultsManager.GetResultsData()
		require.Empty(t, nodeErrors)
		require.Len(t, successes, 1)
		require.False(t, successes[0].IsNodeError, "served without a node-error header")
		require.True(t, successes[0].IsNotFoundAnswer, "and still never cached")
	})

	t.Run("cross-validation: any other error stays a node error", func(t *testing.T) {
		resultsManager := newResultsManager(1, "SUI", true)
		fault := graphQLNodeErrorResponse("provider-a", graphQLErrorReply("boom", "INTERNAL_SERVER_ERROR"))
		require.Error(t, resultsManager.SetResponse(fault, protocolMessage))
		successes, _, _ := resultsManager.GetResultsData()
		require.Empty(t, successes)
	})
}

// TestClassifyStragglerResponse_NotFoundAnswer: a late null agrees with a null consensus, as it
// would have counted at quorum time; a late error is still a node error.
func TestClassifyStragglerResponse_NotFoundAnswer(t *testing.T) {
	protocolMessage := suiLookupProtocolMessage(t)
	consensus := responseContentHash([]byte(suiNullTransaction))

	outcome, hash := classifyStragglerResponse(suiNullTransactionResponse(), consensus, protocolMessage)
	require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, outcome)
	require.Equal(t, consensus, hash)

	fault := graphQLNodeErrorResponse("provider-b", graphQLErrorReply("boom", "INTERNAL_SERVER_ERROR"))
	outcome, _ = classifyStragglerResponse(fault, consensus, protocolMessage)
	require.Equal(t, common.CrossValidationStragglerOutcomeNodeError, outcome)
}

// TestResultsManager_NotFoundTally pins what the policy reads to ask one more provider after a
// not-found answer (MAG-4909 R23): how many arrived, and how many results of any kind came after
// the first.
func TestResultsManager_NotFoundTally(t *testing.T) {
	protocolMessage := suiLookupProtocolMessage(t)
	fault := func() *RelayResponse {
		return graphQLNodeErrorResponse("provider-b", graphQLErrorReply("boom", "INTERNAL_SERVER_ERROR"))
	}
	unreachable := func() *RelayResponse {
		return &RelayResponse{Err: errors.New("connection refused"), RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "provider-c"}}}
	}
	for _, test := range []struct {
		name                       string
		responses                  []func() *RelayResponse
		wantAnswers, wantAfterNull int
	}{
		{"none", []func() *RelayResponse{fault, fault}, 0, 0},
		{"null first", []func() *RelayResponse{suiNullTransactionResponse}, 1, 0},
		{"a fault, then null: nothing after it yet", []func() *RelayResponse{fault, suiNullTransactionResponse}, 1, 0},
		{"null, then null", []func() *RelayResponse{suiNullTransactionResponse, suiNullTransactionResponse}, 2, 1},
		{"null, then a fault", []func() *RelayResponse{suiNullTransactionResponse, fault}, 1, 1},
		{"null, then a provider that could not be reached", []func() *RelayResponse{suiNullTransactionResponse, unreachable}, 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			resultsManager := newResultsManager(1, "SUI", false)
			for _, response := range test.responses {
				_ = resultsManager.SetResponse(response(), protocolMessage)
			}
			answers, afterNull := resultsManager.NotFoundAnswers()
			require.Equal(t, test.wantAnswers, answers)
			require.Equal(t, test.wantAfterNull, afterNull)
		})
	}
}
