package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3746: the router's readiness health check crafts a latest-block request and sends it through
// the ordinary relay path, taking ONE session. It used to build that request with the per-method
// cross-validation resolver, so an operator policy on the latest-block method applied to the health
// check too — and one session against an agreement threshold of two is refused before anything is
// sent. Every health check failed for as long as the policy stood: /readyz answered 503, and under
// the published chart's readiness probe the pod never became Ready, so a router whose providers were
// all healthy received no traffic at all.
//
// Both interfaces the report measured are covered, and LAVA/rest is the one it reproduced on first.
// The method per interface is whatever the spec tags FUNCTION_TAG_GET_BLOCKNUM, which is what
// craftRelay asks for.
type healthCheckCVCase struct {
	name         string
	specID       string
	apiInterface string
	specAPI      string
	// latestBlockMethod is the method the policy is written for; url/body/connectionType are how
	// that interface expresses a request for it.
	latestBlockMethod string
	url               string
	body              []byte
	connectionType    string
}

func healthCheckCVCases() []healthCheckCVCase {
	return []healthCheckCVCase{
		{
			// The report's primary, measured reproduction.
			name: "LAVA rest", specID: "LAVA", apiInterface: "rest", specAPI: spectypes.APIInterfaceRest,
			latestBlockMethod: "/cosmos/base/tendermint/v1beta1/blocks/latest",
			url:               "/cosmos/base/tendermint/v1beta1/blocks/latest", connectionType: http.MethodGet,
		},
		{
			name: "ETH1 jsonrpc", specID: "ETH1", apiInterface: "jsonrpc", specAPI: spectypes.APIInterfaceJsonRPC,
			latestBlockMethod: "eth_blockNumber",
			body:              []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), connectionType: http.MethodPost,
		},
	}
}

// theReportsPolicy is the policy block from the report, including the min-groups floor the first
// version of this test dropped: 3 participants, 2 must agree, across 2 independent groups.
func theReportsPolicy(threshold int) CrossValidationPolicy {
	return CrossValidationPolicy{
		Enabled:            true,
		MaxParticipants:    Bound{Floor: new(3), Cap: new(3)},
		AgreementThreshold: Bound{Floor: new(threshold), Cap: new(3)},
		MinGroups:          Bound{Floor: new(2)},
	}
}

func TestInternalRelayIgnoresCrossValidationPolicy(t *testing.T) {
	for _, tc := range healthCheckCVCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, tc.specID, tc.specAPI, handler, nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)

			resolverFor := func(t *testing.T, threshold int) *CrossValidationPolicyResolver {
				t.Helper()
				r, rerr := NewCrossValidationPolicyResolver(CrossValidationConfig{
					Policies: []CrossValidationPolicyEntry{{
						ChainID: tc.specID, ApiInterface: tc.apiInterface, Method: tc.latestBlockMethod,
						CrossValidationPolicy: theReportsPolicy(threshold),
					}},
				})
				require.NoError(t, rerr)
				require.True(t, r.HasPolicies(), "premise: the policy this case is about actually loaded")
				return r
			}
			// headers is nil for the health check as craftRelay builds it, and non-nil for the case
			// that proves the seal does not depend on that.
			message := func(t *testing.T, headers map[string]string) chainlib.ProtocolMessage {
				t.Helper()
				cm, perr := chainParser.ParseMsg(tc.url, tc.body, tc.connectionType, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
				require.NoError(t, perr)
				require.Equal(t, tc.latestBlockMethod, cm.GetApi().GetName(),
					"premise: this request must resolve to the method the policy names")
				return chainlib.NewProtocolMessage(cm, headers, nil, initRelaysDappId, initRelaysSmartRouterIp)
			}
			serverWith := func(r *CrossValidationPolicyResolver) *RPCSmartRouterServer {
				return &RPCSmartRouterServer{
					listenEndpoint:          &lavasession.RPCEndpoint{ChainID: tc.specID, ApiInterface: tc.apiInterface},
					crossValidationResolver: r,
				}
			}

			t.Run("the health check is not cross-validated", func(t *testing.T) {
				server := serverWith(resolverFor(t, 2))
				sm, smErr := server.internalRelayStateMachine(ctx, lavasession.NewUsedProviders(nil), message(t, nil))
				require.NoError(t, smErr)
				require.Equal(t, relaycore.Stateless, sm.GetSelection(),
					"a health check must take one provider's latest block, not require several to agree on it")
				require.Nil(t, sm.GetCrossValidationParams(),
					"with no params there is no threshold for the session count to be refused against")
			})

			// The seal, as distinct from the decision. Dropping the resolver alone leaves the machine
			// falling through to its caller-header branch, so the fix would rest on craftRelay
			// happening to parse with nil metadata. Give the crafted relay headers and it must STILL
			// route as an ordinary relay — otherwise a future craftRelay that carries any directive
			// header brings MAG-3746 back with the suite green.
			t.Run("still not cross-validated when the crafted relay carries the headers", func(t *testing.T) {
				server := serverWith(resolverFor(t, 2))
				withHeaders := message(t, map[string]string{
					common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
					common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
				})
				sm, smErr := server.internalRelayStateMachine(ctx, lavasession.NewUsedProviders(nil), withHeaders)
				require.NoError(t, smErr)
				require.Equal(t, relaycore.Stateless, sm.GetSelection(),
					"the internal path must forbid cross-validation outright, not merely lack a resolver")
				require.Nil(t, sm.GetCrossValidationParams())
			})

			// Every threshold an operator could write, including the one that used to slip through.
			// This is the artifact behind the claim that threshold 1 was an accidental workaround:
			// the refusal is len(sessions) < AgreementThreshold, and a health check holds one session.
			t.Run("no threshold can refuse the health check any more", func(t *testing.T) {
				for _, threshold := range []int{1, 2, 3} {
					server := serverWith(resolverFor(t, threshold))
					sm, smErr := server.internalRelayStateMachine(ctx, lavasession.NewUsedProviders(nil), message(t, nil))
					require.NoError(t, smErr)
					require.Equal(t, relaycore.Stateless, sm.GetSelection(),
						"threshold %d must not reach the health check", threshold)
				}
			})

			// The control, and the half that must NOT change: the same policy on the same method
			// still cross-validates a CLIENT request, with the operator's own numbers. Without this
			// the cases above would pass equally against a resolver that had quietly stopped working,
			// or a policy that never loaded.
			t.Run("control: a client request is still cross-validated", func(t *testing.T) {
				resolver := resolverFor(t, 2)
				clientSM, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil),
					serverWith(resolver), message(t, nil), nil, false, resolver, tc.specID, tc.apiInterface)
				require.NoError(t, smErr)
				require.Equal(t, relaycore.CrossValidation, clientSM.GetSelection(),
					"the operator's policy must still govern what a caller is actually given")
				params := clientSM.GetCrossValidationParams()
				require.NotNil(t, params)
				require.Equal(t, 2, params.AgreementThreshold, "with the threshold the operator configured")
				require.Equal(t, 3, params.MaxParticipants)
				require.Equal(t, 2, params.MinGroups, "and the min-groups floor the report's policy carries")
			})
		})
	}
}
