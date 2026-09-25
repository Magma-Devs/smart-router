package lavasession

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// The in-process counts behind GET /debug/sticky-claims (MAG-3860). They are the resolutions
// smartrouter_csm_sticky_claims_total counts, readable where a test can reach them. The keys are the
// metric's outcome label values, which is what a test reading the route matches on.

// TestStickyClaimCounts_FollowRealResolutions drives two pods through one registry: the pod that
// claims a session first, the same pod answering it from memory, and a second pod taking the first
// pod's claim, which is how a session that crossed pods shows up.
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
	require.Equal(t, uint64(1), a["claimed"])
	require.Equal(t, uint64(1), a["local_hit"])
	require.Zero(t, a["adopted"])
	require.Equal(t, uint64(1), b["adopted"], "podB took podA's claim")
	require.Zero(t, b["claimed"])

	store.err = errors.New("cache backend unreachable")
	_, err := podB.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-2", "")
	require.ErrorIs(t, err, ErrStickyUnavailable)
	_, b = podB.StickyClaimCounts()
	require.Equal(t, uint64(1), b["error"])
}

// TestStickyClaimCounts_APodReadingBackItsOwnClaimCountsAdopted pins why adopted alone does not prove a
// session crossed pods. Dropping a local pin leaves the fleet claim in place on purpose, so the same
// pod's next request reads its own claim back from the registry, and that counts as adopted too. What
// tells the two apart is invalidated moving on the same pod.
func TestStickyClaimCounts_APodReadingBackItsOwnClaimCountsAdopted(t *testing.T) {
	pod := stickyCSM(t, newFakeSharedSticky())
	claimed := resolvedProvider(t, pod, "session-1")
	pod.invalidateStickyPin(StickyLocalKey(StickyServiceScope("", nil), "session-1"))
	require.Equal(t, claimed, resolvedProvider(t, pod, "session-1"), "the fleet claim survived the dropped pin")

	_, counts := pod.StickyClaimCounts()
	require.Equal(t, uint64(1), counts["claimed"])
	require.Equal(t, uint64(1), counts["invalidated"])
	require.Equal(t, uint64(1), counts["adopted"], "one pod, reading back its own claim")
}

// TestStickyClaimCounts_WithoutARegistryTheFeatureReadsOff is what lets a test tell "off" from
// "never fired": pod-local stickiness resolves no claim, so the counts stay zero and shared is false.
func TestStickyClaimCounts_WithoutARegistryTheFeatureReadsOff(t *testing.T) {
	csm := stickyCSM(t, nil)
	resolvedProvider(t, csm, "session-1")
	resolvedProvider(t, csm, "session-1")

	shared, counts := csm.StickyClaimCounts()
	require.False(t, shared)
	require.Len(t, counts, int(numStickyOutcomes))
	for outcome, count := range counts {
		require.Zero(t, count, outcome)
	}
}

// TestStickyClaimCounts_EveryOutcomeHasASlot records each declared outcome a different number of times
// and reads every count back under its own label. It ranges over the declared outcomes, so one added
// without a label, or sharing another's, fails here, as does one the route's documentation does not
// list yet.
func TestStickyClaimCounts_EveryOutcomeHasASlot(t *testing.T) {
	csm := CreateConsumerSessionManager()
	for outcome := range numStickyOutcomes {
		require.NotEmpty(t, stickyOutcomes[outcome], "outcome %d has no label", outcome)
		for n := stickyOutcome(0); n <= outcome; n++ {
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
