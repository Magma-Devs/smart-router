package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// newCapacityTestServer builds an RPCSmartRouterServer whose session manager holds the given
// group-labeled providers, so the request-time cross-validation capacity gate can be exercised against a
// real ConsumerSessionManager. validAddresses is populated synchronously by UpdateAllProviders (probing is
// deferred to a background goroutine), so dummy endpoint addresses are sufficient.
func newCapacityTestServer(t *testing.T, groupByAddr map[string]string) *RPCSmartRouterServer {
	t.Helper()
	rand.InitRandomSeed()
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, 0, 1, nil, "dontcare")
	csm := lavasession.NewConsumerSessionManager(
		&lavasession.RPCEndpoint{NetworkAddress: "stub", ChainID: "LAVA", ApiInterface: "rest"},
		optimizer, nil, nil, "lava@test", lavasession.NewActiveSubscriptionProvidersStorage())

	pairingList := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(groupByAddr))
	var i uint64
	for addr, group := range groupByAddr {
		pairingList[i] = &lavasession.ConsumerSessionsWithProvider{
			PublicLavaAddress: addr,
			Endpoints:         []*lavasession.Endpoint{{NetworkAddress: "127.0.0.1:0", Enabled: true, Connections: []*lavasession.EndpointConnection{}}},
			Sessions:          map[int64]*lavasession.SingleConsumerSession{},
			MaxComputeUnits:   200,
			PairingEpoch:      1,
			GroupLabel:        group,
		}
		i++
	}
	require.NoError(t, csm.UpdateAllProviders(1, pairingList, nil))
	return &RPCSmartRouterServer{
		sessionManager: csm,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"},
	}
}

// TestValidateCrossValidationCapacity_Reasons checks the request-time fail-fast classifier maps each
// unsatisfiable policy to the correct structured reason (distinct from the quorum-time reasons), and
// returns no error when the candidate set can satisfy the policy. Two primary providers span two groups.
func TestValidateCrossValidationCapacity_Reasons(t *testing.T) {
	srv := newCapacityTestServer(t, map[string]string{"lava@p0": "g1", "lava@p1": "g2"})
	ctx := context.Background()

	t.Run("min-groups exceeds candidate groups -> insufficient-groups", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 2, MinGroups: 3}
		reason, err := srv.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, params, "", nil)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)
	})

	t.Run("max-participants exceeds candidate endpoints -> insufficient-capacity", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 1}
		reason, err := srv.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, params, "", nil)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonInsufficientCapacity, reason)
	})

	t.Run("satisfiable policy -> no error, no reason", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 2, MinGroups: 2}
		reason, err := srv.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, params, "", nil)
		require.NoError(t, err)
		require.Empty(t, reason)
	})

	t.Run("non-cross-validation selection is never gated", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 5, MinGroups: 9}
		reason, err := srv.validateCrossValidationCapacity(ctx, relaycore.Stateless, params, "", nil)
		require.NoError(t, err)
		require.Empty(t, reason)
	})
}

// TestCrossValidationFailFastResult verifies the minimal result returned on a request-time fail-fast
// carries the cross-validation status and the structured reason as response-header metadata — this is the
// payload the interface listeners write onto the HTTP error response so the client can distinguish a
// capacity/diversity failure from a generic upstream error.
func TestCrossValidationFailFastResult(t *testing.T) {
	res := crossValidationFailFastResult(common.CrossValidationReasonInsufficientGroups)
	require.NotNil(t, res)
	require.Equal(t, http.StatusInternalServerError, res.StatusCode)
	require.Equal(t, common.CrossValidationReasonInsufficientGroups, res.CrossValidationFailureReason)
	require.NotNil(t, res.Reply)

	headers := map[string]string{}
	for _, m := range res.Reply.Metadata {
		headers[m.Name] = m.Value
	}
	require.Equal(t, "failed", headers[common.CROSS_VALIDATION_STATUS_HEADER_NAME])
	require.Equal(t, common.CrossValidationReasonInsufficientGroups, headers[common.CROSS_VALIDATION_FAILURE_REASON_HEADER])
}
