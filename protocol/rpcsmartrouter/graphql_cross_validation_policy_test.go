package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// TestGraphQLMultiRootCrossValidationPolicy pins that a GraphQL request selecting several root
// fields is governed by each root's policy. Its combined api name ("a&b") matches no policy, so
// without the per-root lookup bundling an operation with another escaped a forbid-caller-cv and an
// enabled policy alike.
func TestGraphQLMultiRootCrossValidationPolicy(t *testing.T) {
	ctx := context.Background()
	const (
		chainID      = "SUI"
		apiInterface = spectypes.APIInterfaceGraphQL
	)
	emptyBlockParsing := spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY}

	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(spectypes.Spec{
		Index:            chainID,
		Enabled:          true,
		AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: apiInterface, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{Name: "chainIdentifier", Enabled: true, ComputeUnits: 10, Category: spectypes.SpecCategory{Deterministic: true}, BlockParsing: emptyBlockParsing},
				{Name: "checkpoint", Enabled: true, ComputeUnits: 20, Category: spectypes.SpecCategory{Deterministic: true}, BlockParsing: emptyBlockParsing},
				{Name: "executeTransaction", Enabled: true, ComputeUnits: 100, Category: spectypes.SpecCategory{Stateful: 1}, BlockParsing: emptyBlockParsing},
			},
		}},
	})

	callerCVHeaders := map[string]string{
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}
	protocolMessage := func(body string, headers map[string]string) chainlib.ProtocolMessage {
		chainMessage, perr := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{})
		require.NoError(t, perr)
		return chainlib.NewProtocolMessage(chainMessage, headers, nil, "dapp", "1.2.3.4")
	}
	resolverWith := func(entries ...CrossValidationPolicyEntry) *CrossValidationPolicyResolver {
		resolver, rerr := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: entries})
		require.NoError(t, rerr)
		return resolver
	}
	stateMachine := func(pm chainlib.ProtocolMessage, resolver *CrossValidationPolicyResolver) RelayStateMachine {
		sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil), &SmartRouterRelaySenderMock{retValue: nil}, pm, nil, false, resolver, chainID, apiInterface)
		require.NoError(t, smErr)
		return sm
	}

	t.Run("a forbid on one root holds when the write is selected twice", func(t *testing.T) {
		resolver := resolverWith(CrossValidationPolicyEntry{ChainID: chainID, ApiInterface: apiInterface, Method: "executeTransaction", CrossValidationPolicy: CrossValidationPolicy{ForbidCallerCV: true}})
		pm := protocolMessage(`{"query":"mutation { a: executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } b: executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`, callerCVHeaders)
		require.Equal(t, "executeTransaction&executeTransaction", pm.GetApi().GetName())

		sm := stateMachine(pm, resolver)
		require.Equal(t, relaycore.Stateful, sm.GetSelection(), "caller headers must not fan a forbidden write out for cross-validation")
		require.Nil(t, sm.GetCrossValidationParams())
	})

	t.Run("a forbid on one root holds when bundled with a read", func(t *testing.T) {
		resolver := resolverWith(CrossValidationPolicyEntry{ChainID: chainID, ApiInterface: apiInterface, Method: "checkpoint", CrossValidationPolicy: CrossValidationPolicy{ForbidCallerCV: true}})
		sm := stateMachine(protocolMessage(`{"query":"{ checkpoint { digest } chainIdentifier }"}`, callerCVHeaders), resolver)
		require.NotEqual(t, relaycore.CrossValidation, sm.GetSelection())
	})

	t.Run("an enabled policy on one root applies to the bundle", func(t *testing.T) {
		resolver := resolverWith(CrossValidationPolicyEntry{ChainID: chainID, ApiInterface: apiInterface, Method: "checkpoint", CrossValidationPolicy: CrossValidationPolicy{
			Enabled: true, MaxParticipants: Bound{Floor: new(3)}, AgreementThreshold: Bound{Floor: new(3)},
		}})
		sm := stateMachine(protocolMessage(`{"query":"{ chainIdentifier checkpoint { digest } }"}`, nil), resolver)
		require.Equal(t, relaycore.CrossValidation, sm.GetSelection())
		require.Equal(t, 3, sm.GetCrossValidationParams().AgreementThreshold)
	})

	t.Run("the strictest of two enabled policies wins, whatever the order", func(t *testing.T) {
		resolver := resolverWith(
			CrossValidationPolicyEntry{ChainID: chainID, ApiInterface: apiInterface, Method: "chainIdentifier", CrossValidationPolicy: CrossValidationPolicy{
				Enabled: true, MaxParticipants: Bound{Floor: new(3)}, AgreementThreshold: Bound{Floor: new(2)},
			}},
			CrossValidationPolicyEntry{ChainID: chainID, ApiInterface: apiInterface, Method: "checkpoint", CrossValidationPolicy: CrossValidationPolicy{
				Enabled: true, MaxParticipants: Bound{Floor: new(4)}, AgreementThreshold: Bound{Floor: new(4)},
			}},
		)
		for _, body := range []string{
			`{"query":"{ chainIdentifier checkpoint { digest } }"}`,
			`{"query":"{ checkpoint { digest } chainIdentifier }"}`,
		} {
			sm := stateMachine(protocolMessage(body, nil), resolver)
			require.Equal(t, relaycore.CrossValidation, sm.GetSelection())
			require.Equal(t, 4, sm.GetCrossValidationParams().AgreementThreshold, body)
		}
	})
}
