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

func (f *fakeSharedSticky) Fetch(_ context.Context, chainID, apiInterface, service, stickyID string) (string, uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	if f.err != nil {
		return "", 0, false, f.err
	}
	claim, ok := f.claims[chainID+"|"+apiInterface+"|"+service+"|"+stickyID]
	return claim.Provider, claim.Epoch, ok, nil
}

func (f *fakeSharedSticky) PublishIfAbsent(_ context.Context, chainID, apiInterface, service, stickyID, provider string, epoch uint64, _ time.Duration) (string, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	if f.err != nil {
		return "", 0, f.err
	}
	key := chainID + "|" + apiInterface + "|" + service + "|" + stickyID
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
	store.claims["stub|stub|base|"+StickyIDDigest("session-1")] = StickySession{Provider: "provider1", Epoch: firstEpochHeight}

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

// --- regressions found in review of the first cut ---------------------------------------

// lava-select-provider names an upstream explicitly, which is the more specific ask, and it has
// always taken priority — getValidProviderAddresses handles it and returns before the sticky
// block. The first cut of cross-pod stickiness overwrote it unconditionally, inverting that
// order on any deployment running --shared-state.
func TestSharedSticky_SelectProviderBeatsAStickyClaim(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)

	// A live fleet claim for this session names provider1.
	store.claims["stub|stub|base|"+StickyIDDigest("session-1")] = StickySession{Provider: "provider1", Epoch: firstEpochHeight}

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "provider3")
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	for provider := range sessions {
		require.Equal(t, "provider3", provider, "the explicitly named provider must win over the fleet claim")
	}
}

// Selection is filtered by add-on and extension, so an upstream serving the base collection may
// be unable to serve an archive call. A single claim spanning both would pin such a session to
// an upstream that cannot answer half of it — and since a resolved claim is enforced as a hard
// pin, that half would fail for as long as the claim lived.
func TestSharedSticky_ClaimsAreScopedPerServiceClass(t *testing.T) {
	require.NotEqual(t, StickyServiceScope("", nil), StickyServiceScope("", []string{"archive"}),
		"an archive request must not share a claim with a plain one")
	require.NotEqual(t, StickyServiceScope("", nil), StickyServiceScope("debug", nil))

	// Extension order must not change the key, or two pods handed the same set in a different
	// order would claim separately and split the session.
	require.Equal(t, StickyServiceScope("debug", []string{"archive", "trace"}),
		StickyServiceScope("debug", []string{"trace", "archive"}))

	// The pod-local table is scoped the same way, or the wedge just moves from the registry
	// into local memory.
	require.NotEqual(t, StickyLocalKey("base", "s1"), StickyLocalKey("base+archive", "s1"))
	require.NotEqual(t, StickyLocalKey("base", "s1"), StickyLocalKey("base", "s2"))
}

// A claim outliving the upstream it names must not wedge the session. The resolved claim is a
// hard pin, so the request fails — but the local copy has to be dropped, or the fast path
// replays that same dead decision on every later request until it ages out an epoch or two on.
func TestSharedSticky_UnusableClaimIsDroppedSoTheNextRequestRecovers(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)

	pinned := resolvedProvider(t, csm, "session-1")
	localKey := StickyLocalKey(StickyServiceScope("", nil), "session-1")
	_, cached := csm.stickySessions.Get(localKey)
	require.True(t, cached, "the resolved claim should be remembered locally")

	// The pinned upstream stops being selectable on this pod.
	csm.lock.Lock()
	remaining := make([]string, 0, len(csm.validAddresses))
	for _, address := range csm.validAddresses {
		if address != pinned {
			remaining = append(remaining, address)
		}
	}
	csm.validAddresses = remaining
	// getValidAddresses answers from the per-addon cache when it is warm, so trimming the base
	// list alone changes nothing until that cache is dropped too.
	csm.addonAddresses = nil
	csm.lock.Unlock()

	_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "")
	require.Error(t, err, "a hard pin on an unusable upstream must fail rather than reroute silently")

	_, stillCached := csm.stickySessions.Get(localKey)
	require.False(t, stillCached, "the dead claim must be dropped, or every later request replays it")
}

// The guarantee has to survive a claimed upstream that is BUSY, not just one that is gone.
//
// Every earlier test here made an upstream unusable by removing it from the valid set — the
// failure shape of a drained or unhealthy node. There is a second, much more ordinary shape:
// the upstream is valid, healthy and serving, but cannot take THIS relay because its compute
// units for the epoch are spent or it is at its session cap. That check happens later, in the
// refill loop, and the refill used to blank the pin — so the request was quietly served by a
// different upstream with no error and no metric.
//
// For this feature that is the exact split it exists to remove, arriving under ordinary load
// rather than during an incident, and invisible when it does.
func TestSharedSticky_BusyClaimedUpstreamFailsRatherThanRerouting(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)

	pinned := resolvedProvider(t, csm, "session-1")

	// Spend the pinned upstream's compute units for this epoch. It stays a valid address —
	// not blocked, still serving the collection — it simply cannot take this relay.
	csm.lock.RLock()
	cswp := csm.pairing[pinned]
	csm.lock.RUnlock()
	cswp.Lock.Lock()
	cswp.UsedComputeUnits = cswp.MaxComputeUnits
	cswp.Lock.Unlock()

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil),
		servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "")

	if err == nil {
		served := ""
		for provider := range sessions {
			served = provider
		}
		require.Failf(t, "the claim was silently abandoned",
			"the fleet claim named %s, but %s served the request with no error — a busy upstream must fail the request, not reroute it",
			pinned, served)
	}
	require.ErrorIs(t, err, SelectedProviderAlreadyFailedError)

	// The claim is deliberately KEPT. A busy upstream is not a bad one: the fleet's answer is
	// still correct and the next request should use it. Dropping it would spend a registry
	// round trip to be handed the very same claim back.
	//
	// This is the distinction between the two sentinels. `Unavailable` — blocked, unhealthy, or
	// missing the add-on — means the claim names something that cannot serve this class at all,
	// and THAT one drops the local pin (see the deferred invalidation in GetSessions).
	// `AlreadyFailed` means healthy but busy this instant, and leaves it alone.
	pin, held := csm.stickySessions.Get(StickyLocalKey(StickyServiceScope("", nil), "session-1"))
	require.True(t, held, "a busy upstream must not cost the session its claim")
	require.Equal(t, pinned, pin.Provider)
}
