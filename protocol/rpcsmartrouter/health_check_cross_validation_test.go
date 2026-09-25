package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3746: the router's readiness health check crafts a latest-block request and sends it through
// the ordinary relay path, taking ONE session. It used to build that request with the per-method
// cross-validation resolver, so an operator policy on the latest-block method applied to the health
// check too — and one session against an agreement threshold of two is refused before anything is
// sent. Every health check failed, for ever: /readyz answered 503, and under the published chart's
// readiness probe the pod never became Ready, so a router whose providers were all healthy received
// no traffic at all.
//
// The policy here is the one from the report, on eth_blockNumber, which is what craftRelay asks for
// on an EVM chain (FUNCTION_TAG_GET_BLOCKNUM).
func TestInternalRelayIgnoresCrossValidationPolicy(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	const specId, apiInterface = "ETH1", "jsonrpc"
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	// The threshold from the report: 3 participants, 2 must agree, on the latest-block method.
	resolver, err := NewCrossValidationPolicyResolver(CrossValidationConfig{
		Policies: []CrossValidationPolicyEntry{{
			ChainID: specId, ApiInterface: apiInterface, Method: "eth_blockNumber",
			CrossValidationPolicy: CrossValidationPolicy{
				Enabled:            true,
				MaxParticipants:    Bound{Floor: new(3), Cap: new(3)},
				AgreementThreshold: Bound{Floor: new(2), Cap: new(3)},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, resolver.HasPolicies(), "premise: the policy this test is about is actually loaded")

	server := &RPCSmartRouterServer{
		listenEndpoint:          &lavasession.RPCEndpoint{ChainID: specId, ApiInterface: apiInterface},
		crossValidationResolver: resolver,
	}
	// The health check's own request: no caller headers, since the router crafts it (see
	// sendCraftedRelays, which passes nil metadata).
	latestBlockMessage := func(t *testing.T) chainlib.ProtocolMessage {
		t.Helper()
		cm, perr := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, perr)
		return chainlib.NewProtocolMessage(cm, nil, nil, initRelaysDappId, initRelaysSmartRouterIp)
	}

	usedProviders := lavasession.NewUsedProviders(nil)
	sm, err := server.internalRelayStateMachine(ctx, usedProviders, latestBlockMessage(t))
	require.NoError(t, err)
	require.Equal(t, relaycore.Stateless, sm.GetSelection(),
		"a health check must take one provider's latest block, not require several to agree on it")
	require.Nil(t, sm.GetCrossValidationParams(),
		"with no params there is no threshold for the session count to be refused against")

	// The control, and the half that must NOT change: the same policy on the same method still
	// cross-validates a CLIENT request. Without this the test would pass just as well against a
	// resolver that had quietly stopped working, or a policy that never loaded.
	clientSM, err := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil),
		server, latestBlockMessage(t), nil, false, resolver, specId, apiInterface)
	require.NoError(t, err)
	require.Equal(t, relaycore.CrossValidation, clientSM.GetSelection(),
		"the operator's policy must still govern what a caller is actually given")
	require.NotNil(t, clientSM.GetCrossValidationParams())
	require.Equal(t, 2, clientSM.GetCrossValidationParams().AgreementThreshold,
		"and with the threshold the operator configured")
}
