package lavasession

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// The in-process counts behind GET /debug/sticky-claims (MAG-3860). They are the resolutions
// smartrouter_csm_sticky_claims_total counts, readable where a test can reach them.

// TestStickyClaimCounts_FollowRealResolutions drives two pods through one registry: the pod that
// claims a session first, the same pod answering it from memory, and a second pod taking the first
// pod's claim, which is the one outcome that proves a session crossed pods.
func TestStickyClaimCounts_FollowRealResolutions(t *testing.T) {
	store := newFakeSharedSticky()
	podA := stickyCSM(t, store)
	podB := stickyCSM(t, store)

	resolvedProvider(t, podA, "session-1") // podA makes the fleet claim
	resolvedProvider(t, podA, "session-1") // and then answers it from memory
	resolvedProvider(t, podB, "session-1") // podB takes podA's claim

	sharedA, a := podA.StickyClaimCounts()
	sharedB, b := podB.StickyClaimCounts()
	require.True(t, sharedA)
	require.True(t, sharedB)
	require.Equal(t, uint64(1), a[stickyOutcomeClaimed])
	require.Equal(t, uint64(1), a[stickyOutcomeLocalHit])
	require.Zero(t, a[stickyOutcomeAdopted])
	require.Equal(t, uint64(1), b[stickyOutcomeAdopted], "the cross-pod signal")
	require.Zero(t, b[stickyOutcomeClaimed])

	store.err = errors.New("cache backend unreachable")
	_, err := podB.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-2", "")
	require.ErrorIs(t, err, ErrStickyUnavailable)
	_, b = podB.StickyClaimCounts()
	require.Equal(t, uint64(1), b[stickyOutcomeError])
}

// TestStickyClaimCounts_WithoutARegistryTheFeatureReadsOff is what lets a test tell "off" from
// "never fired": pod-local stickiness resolves no claim, so the counts stay zero and shared is false.
func TestStickyClaimCounts_WithoutARegistryTheFeatureReadsOff(t *testing.T) {
	csm := stickyCSM(t, nil)
	resolvedProvider(t, csm, "session-1")
	resolvedProvider(t, csm, "session-1")

	shared, counts := csm.StickyClaimCounts()
	require.False(t, shared)
	require.Len(t, counts, len(stickyOutcomes))
	for outcome, count := range counts {
		require.Zero(t, count, outcome)
	}
}

// TestStickyClaimCounts_EveryOutcomeHasASlot pins the keys to the metric's outcome label values,
// which is what a test reading the route matches on, and that each outcome counts in its own slot.
func TestStickyClaimCounts_EveryOutcomeHasASlot(t *testing.T) {
	csm := CreateConsumerSessionManager()
	for i, outcome := range stickyOutcomes {
		for n := 0; n <= i; n++ {
			csm.recordStickyOutcome(outcome)
		}
	}
	_, counts := csm.StickyClaimCounts()
	require.Equal(t, map[string]uint64{
		"local_hit":    1,
		"adopted":      2,
		"claimed":      3,
		"lost_race":    4,
		"error":        5,
		"no_candidate": 6,
		"invalidated":  7,
	}, counts)
}
