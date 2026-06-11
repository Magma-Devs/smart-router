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

// TestValidateCrossValidationCapacity_PerGroup covers the 2.3 runtime per-group capacity guards in
// validateCrossValidationCapacity: the self-consistency check (max >= minGroups*threshold, catching
// caller-induced impossible effective params) and the adequately-staffed-group check (>= minGroups
// candidate groups each with >= threshold providers after addon/extension filtering).
func TestValidateCrossValidationCapacity_PerGroup(t *testing.T) {
	ctx := context.Background()

	t.Run("caller-induced impossible: minGroups 2 * threshold 3 > maxParticipants 4 -> insufficient-capacity", func(t *testing.T) {
		// Fleet has 4 providers across 2 groups, so the provider/group checks pass and the self-consistency
		// check is what fires. This is the case config-time Validate cannot catch (the caller raised the threshold).
		srv := newCapacityTestServer(t, map[string]string{"lava@a0": "A", "lava@a1": "A", "lava@b0": "B", "lava@b1": "B"})
		params := &common.CrossValidationParams{AgreementThreshold: 3, MaxParticipants: 4, MinGroups: 2, PerGroupQuorum: true}
		reason, err := srv.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, params, "", nil)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonInsufficientCapacity, reason)
	})

	t.Run("two groups but only one has >= threshold providers -> insufficient-groups", func(t *testing.T) {
		// A=2, B=1, C=1: 4 candidates across 3 groups (passes provider/group/self-consistency checks for
		// minGroups 2, threshold 2, max 4), but only group A can reach an internal quorum of 2.
		srv := newCapacityTestServer(t, map[string]string{"lava@a0": "A", "lava@a1": "A", "lava@b0": "B", "lava@c0": "C"})
		params := &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 4, MinGroups: 2, PerGroupQuorum: true}
		reason, err := srv.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, params, "", nil)
		require.Error(t, err)
		require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)
	})

	t.Run("two adequately-staffed groups -> ok", func(t *testing.T) {
		srv := newCapacityTestServer(t, map[string]string{"lava@a0": "A", "lava@a1": "A", "lava@b0": "B", "lava@b1": "B"})
		params := &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 4, MinGroups: 2, PerGroupQuorum: true}
		reason, err := srv.validateCrossValidationCapacity(ctx, relaycore.CrossValidation, params, "", nil)
		require.NoError(t, err)
		require.Empty(t, reason)
	})
}

// TestCrossValidationGroupShortfall covers the pure decision used by the post-consistency-filter guard:
// given per-group survivor counts, can the request still satisfy its group requirement. Per-group mode
// requires MinGroups groups that EACH still have >= threshold survivors; MinGroups mode requires only that
// many distinct groups.
func TestCrossValidationGroupShortfall(t *testing.T) {
	t.Run("per-group: a surviving group dropped below threshold -> insufficient-groups", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MinGroups: 2, PerGroupQuorum: true}
		qualifying, reason := crossValidationGroupShortfall(map[string]int{"A": 2, "B": 1}, params)
		require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)
		require.Equal(t, 1, qualifying)
	})
	t.Run("per-group: both groups still adequate -> ok", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MinGroups: 2, PerGroupQuorum: true}
		qualifying, reason := crossValidationGroupShortfall(map[string]int{"A": 2, "B": 2}, params)
		require.Empty(t, reason)
		require.Equal(t, 2, qualifying)
	})
	t.Run("min-groups diversity: distinct groups suffice regardless of per-group count", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MinGroups: 2}
		_, reason := crossValidationGroupShortfall(map[string]int{"A": 1, "B": 1}, params)
		require.Empty(t, reason, "MinGroups mode only needs distinct groups, not threshold per group")
	})
	t.Run("min-groups diversity: too few distinct groups -> insufficient-groups", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MinGroups: 2}
		_, reason := crossValidationGroupShortfall(map[string]int{"A": 5}, params)
		require.Equal(t, common.CrossValidationReasonInsufficientGroups, reason)
	})
	t.Run("min-groups <= 1 is never a shortfall", func(t *testing.T) {
		params := &common.CrossValidationParams{AgreementThreshold: 2, MinGroups: 1, PerGroupQuorum: true}
		_, reason := crossValidationGroupShortfall(map[string]int{"A": 1}, params)
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

// TestPreferStructuralFailureReason covers the precedence fix: when an earlier batch produced a non-quorum
// result (so HasResults() was true and the fail-fast early-return was skipped) but a later batch hit a
// structural capacity/diversity guard, the client must see the structural reason — strictly more actionable
// than the final-eval "providers disagreed" reason — not the two reason channels disagreeing.
func TestPreferStructuralFailureReason(t *testing.T) {
	t.Run("structural fail-fast reason overrides the final-eval reason on failure", func(t *testing.T) {
		res := &common.RelayResult{CrossValidationFailureReason: common.CrossValidationReasonNoAgreement}
		preferStructuralFailureReason(res, common.CrossValidationReasonInsufficientGroups)
		require.Equal(t, common.CrossValidationReasonInsufficientGroups, res.CrossValidationFailureReason)
	})
	t.Run("no fail-fast reason -> result reason unchanged", func(t *testing.T) {
		res := &common.RelayResult{CrossValidationFailureReason: common.CrossValidationReasonDiversityUnmet}
		preferStructuralFailureReason(res, "")
		require.Equal(t, common.CrossValidationReasonDiversityUnmet, res.CrossValidationFailureReason)
	})
	t.Run("nil result -> no panic", func(t *testing.T) {
		require.NotPanics(t, func() { preferStructuralFailureReason(nil, common.CrossValidationReasonInsufficientGroups) })
	})
}
