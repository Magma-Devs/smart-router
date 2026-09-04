package lavasession

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// fakeSharedSticky is one fleet-wide claim registry shared by several session managers, which
// is what a cache backend is to several router pods.
type fakeSharedSticky struct {
	mu      sync.Mutex
	claims  map[string]StickySession
	err     error
	fetches int
	writes  int
}

func newFakeSharedSticky() *fakeSharedSticky {
	return &fakeSharedSticky{claims: map[string]StickySession{}}
}

func (f *fakeSharedSticky) Fetch(_ context.Context, chainID, apiInterface, stickyID string) (string, uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	if f.err != nil {
		return "", 0, false, f.err
	}
	claim, ok := f.claims[chainID+"|"+apiInterface+"|"+stickyID]
	return claim.Provider, claim.Epoch, ok, nil
}

func (f *fakeSharedSticky) PublishIfAbsent(_ context.Context, chainID, apiInterface, stickyID, provider string, epoch uint64, _ time.Duration) (string, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	if f.err != nil {
		return "", 0, f.err
	}
	key := chainID + "|" + apiInterface + "|" + stickyID
	if existing, ok := f.claims[key]; ok {
		return existing.Provider, existing.Epoch, nil
	}
	f.claims[key] = StickySession{Provider: provider, Epoch: epoch}
	return provider, epoch, nil
}

func stickyCSM(t *testing.T, store SharedStickyStore) *ConsumerSessionManager {
	t.Helper()
	csm := CreateConsumerSessionManager()
	csm.SetSharedStickyStore(store, 15*time.Minute)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	return csm
}

func resolvedProvider(t *testing.T, csm *ConsumerSessionManager, stickyID string) string {
	t.Helper()
	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, stickyID, "")
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	for provider := range sessions {
		return provider
	}
	return ""
}

// The requirement. Two session managers are two router pods; one registry is the cache backend.
// Without the shared claim each pod runs its own optimizer and they disagree.
func TestSharedSticky_TwoPodsResolveOneUpstream(t *testing.T) {
	store := newFakeSharedSticky()
	podA := stickyCSM(t, store)
	podB := stickyCSM(t, store)

	chosenByA := resolvedProvider(t, podA, "session-1")
	chosenByB := resolvedProvider(t, podB, "session-1")
	require.Equal(t, chosenByA, chosenByB, "both pods must route one session to one upstream")

	// And it holds across repeated requests to either pod.
	for i := 0; i < 5; i++ {
		require.Equal(t, chosenByA, resolvedProvider(t, podA, "session-1"))
		require.Equal(t, chosenByA, resolvedProvider(t, podB, "session-1"))
	}
}

// CONTROL for the test above. Two pods that share no registry must actually disagree, or the
// test proves nothing — both session managers would simply be picking the same upstream on
// their own. Measured at the time of writing: they disagreed on all eight ids.
//
// Keep this alongside the test it guards. If a future change makes independent pods converge
// (a deterministic optimizer seed, say), this fails loudly instead of leaving the requirement
// test quietly passing for the wrong reason.
func TestSharedSticky_ControlPodsDisagreeWithoutARegistry(t *testing.T) {
	disagreements := 0
	ids := []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}
	for _, id := range ids {
		podA := stickyCSM(t, nil)
		podB := stickyCSM(t, nil)
		if resolvedProvider(t, podA, id) != resolvedProvider(t, podB, id) {
			disagreements++
		}
	}
	require.Positive(t, disagreements,
		"unshared pods never disagreed, so TestSharedSticky_TwoPodsResolveOneUpstream proves nothing")
}

// Distinct sessions must still spread over the pool — the customer relies on this alongside
// affinity, so pinning everything to one upstream would be a regression, not a fix.
func TestSharedSticky_DistinctSessionsStillSpread(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)

	seen := map[string]struct{}{}
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		seen[resolvedProvider(t, csm, id)] = struct{}{}
	}
	require.Greater(t, len(seen), 1, "different sticky ids must not collapse onto one upstream")
}

// Fail closed. An unreachable registry means this pod cannot know the fleet's claim, and
// serving anyway is the silent split the feature removes.
func TestSharedSticky_UnreachableRegistryFailsTheRequest(t *testing.T) {
	store := newFakeSharedSticky()
	store.err = errors.New("cache backend unreachable")
	csm := stickyCSM(t, store)

	_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "")
	require.ErrorIs(t, err, ErrStickyUnavailable)
}

// Traffic without a sticky id is untouched by an unreachable registry: only sticky requests
// carry the guarantee, so only they may fail for it.
func TestSharedSticky_UnreachableRegistryDoesNotAffectPlainTraffic(t *testing.T) {
	store := newFakeSharedSticky()
	store.err = errors.New("cache backend unreachable")
	csm := stickyCSM(t, store)

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	require.Len(t, sessions, 1)
}

// A confirmed claim answers from memory. Without this the registry would be consulted on every
// relay, which is the cost the read-through design exists to avoid.
func TestSharedSticky_ConfirmedClaimCostsNoRoundTrip(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)

	resolvedProvider(t, csm, "session-1")
	fetchesAfterFirst, writesAfterFirst := store.fetches, store.writes

	for i := 0; i < 10; i++ {
		resolvedProvider(t, csm, "session-1")
	}
	require.Equal(t, fetchesAfterFirst, store.fetches, "a confirmed claim must not re-read the registry")
	require.Equal(t, writesAfterFirst, store.writes)
}

// A pod that loses the race adopts the winner rather than keeping its own pick, so the two pods
// converge inside the request that raced instead of disagreeing until the claim expires.
func TestSharedSticky_LoserOfARaceAdoptsTheWinner(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)

	// A peer claimed this session first, naming an upstream this pod also knows.
	store.claims["stub|stub|"+StickyIDDigest("session-1")] = StickySession{Provider: "provider1", Epoch: firstEpochHeight}

	require.Equal(t, "provider1", resolvedProvider(t, csm, "session-1"))
}

// An unconfirmed pin is this pod's private opinion — the pre-existing single-pod behaviour, and
// what the subscription managers create. It must never be routed on as though the fleet agreed.
func TestSharedSticky_UnconfirmedLocalPinIsNotTrusted(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)

	csm.stickySessions.Set("session-1", &StickySession{Provider: "provider1", Epoch: firstEpochHeight})
	resolvedProvider(t, csm, "session-1")
	require.Positive(t, store.fetches, "an unconfirmed pin must still consult the fleet registry")
}

// With no registry wired the router keeps exactly the behaviour it had before this feature.
func TestSharedSticky_NilStoreKeepsPodLocalBehaviour(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	first := resolvedProvider(t, csm, "session-1")
	require.Equal(t, first, resolvedProvider(t, csm, "session-1"), "pod-local stickiness still applies")
}

func TestStickyIDDigest_HidesThePlaintextAndIsStable(t *testing.T) {
	id := "customer-user-42"
	digest := StickyIDDigest(id)
	require.NotContains(t, digest, id, "the raw session id must not travel to the registry")
	require.Equal(t, digest, StickyIDDigest(id), "every pod must derive the same key")
	require.NotEqual(t, digest, StickyIDDigest("customer-user-43"))
}
