package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const obsKey = "obs:ETH1:jsonrpc:ep1"

func TestEndpointObservation_RoundTripAndMiss(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	_, _, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err, "nothing published is a miss, not an error")
	require.False(t, found)

	applied, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 500, PodID: "host/abcd"}, 5*time.Second)
	require.NoError(t, err)
	require.True(t, applied)

	obs, age, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, core.EndpointObservation{Block: 500, PodID: "host/abcd"}, obs)
	require.Less(t, age, time.Second)
}

// The same rule the cache server's in-memory store applies: a lower block from a slower peer
// is dropped while the entry is live, an equal block replaces it and refreshes the stamp, a
// higher block always wins.
func TestEndpointObservation_BlockMonotonicWhileLive(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	applied, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 100, PodID: "pod-a"}, time.Second)
	require.NoError(t, err)
	require.True(t, applied)

	applied, err = store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 99, PodID: "pod-b"}, time.Second)
	require.NoError(t, err)
	require.False(t, applied, "a lower block from a slower peer is dropped while the entry is live")
	obs, _, _, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.Equal(t, core.EndpointObservation{Block: 100, PodID: "pod-a"}, obs)

	applied, err = store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 100, PodID: "pod-b"}, time.Second)
	require.NoError(t, err)
	require.True(t, applied, "an equal block replaces the entry")
	obs, _, _, err = store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.Equal(t, "pod-b", obs.PodID)

	applied, err = store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 101, PodID: "pod-a"}, time.Second)
	require.NoError(t, err)
	require.True(t, applied, "a higher block always wins")
}

// Age is measured by the backend, not the pod: the stamp is taken by TIME inside the script and
// compared against TIME on read (one server here; with read-addresses or Cluster the two TIMEs
// can come from different servers — see the store's comment). miniredis owns that clock (SetTime moves what TIME answers,
// FastForward ages the TTLs), so both the age and the expiry are exercised without a sleep.
func TestEndpointObservation_AgeIsOnTheStoreClockAndExpires(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mr.SetTime(base)

	_, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 100, PodID: "pod-a"}, 2*time.Second)
	require.NoError(t, err)

	mr.SetTime(base.Add(1500 * time.Millisecond))
	_, age, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1500*time.Millisecond, age, "age is the backend's clock, not the reader's")

	mr.FastForward(2500 * time.Millisecond)
	_, _, found, err = store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.False(t, found, "an expired entry is a miss")

	// Once expired, a lower block (reorg / fresh restart) is accepted again.
	applied, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 50, PodID: "pod-b"}, time.Second)
	require.NoError(t, err)
	require.True(t, applied)
}

// A foreign writer on a shared backend can leave anything under the key. Corruption must read
// as a miss and must not fence the next publish — the same rule the int64 tip follows.
func TestEndpointObservation_CorruptValueReadsAsMissAndIsOverwritten(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()
	require.NoError(t, mr.Set("sr:"+obsKey, "garbage"))

	_, _, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.False(t, found)

	applied, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 7, PodID: "pod-a"}, time.Second)
	require.NoError(t, err)
	require.True(t, applied, "a corrupt value falls through to the write")
	obs, _, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 7, obs.Block)
}

// The script's fence and the read's decoder must agree on what a valid value is. A foreign value
// with a numeric head but no valid stamp reads as a miss, so it must not fence the next publish
// either — written without a TTL, it would otherwise refuse every lower block for good.
func TestEndpointObservation_ValueTheReadRejectsDoesNotFence(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()
	require.NoError(t, mr.Set("sr:"+obsKey, "999999|not-a-stamp|pod-x"))

	_, _, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.False(t, found, "the read decodes no stamp, so it is a miss")

	applied, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 7, PodID: "pod-a"}, time.Second)
	require.NoError(t, err)
	require.True(t, applied, "a value the read treats as a miss must not fence the write")
	obs, _, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 7, obs.Block)
}

// The pod id is the one free-form field and goes last, so any character in it survives.
func TestEndpointObservation_PodIDWithSeparators(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	podID := "host-1/ab|cd:ef"

	_, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 1, PodID: podID}, time.Second)
	require.NoError(t, err)
	obs, _, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, podID, obs.PodID)
}

// Purge is prefix-scoped and covers observations like every other key the store writes.
func TestEndpointObservation_PurgeDropsIt(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	_, err := store.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 1, PodID: "pod-a"}, time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.Purge(ctx))
	_, _, found, err := store.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.False(t, found)
}

// Observations are shared across key prefixes (core.EndpointObservationKey, MAG-3521): two
// routers on different keyspaces polling the same upstream borrow each other's poll, as on the
// gRPC cache server. A purge clears them for every router, as that server's FlushCache does.
func TestEndpointObservation_SharedAcrossKeyPrefixes(t *testing.T) {
	storeA, mr := newTestStore(t)
	clientB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	storeB, err := NewWithClient(clientB, "other-router")
	require.NoError(t, err)
	t.Cleanup(func() { _ = storeB.Close() })
	ctx := context.Background()

	_, err = storeA.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 42, PodID: "pod-a"}, time.Minute)
	require.NoError(t, err)
	require.True(t, mr.Exists(obsKey), "the observation is written under the unscoped key")
	require.False(t, mr.Exists("sr:"+obsKey), "the observation must not be scoped to the writer's prefix")

	obs, _, found, err := storeB.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.True(t, found, "a router on another key prefix reads the same observation")
	require.Equal(t, int64(42), obs.Block)

	// A lower block from the other router is fenced, so the shared value stays monotonic.
	applied, err := storeB.PublishEndpointObservation(ctx, obsKey, core.EndpointObservation{Block: 41, PodID: "pod-b"}, time.Minute)
	require.NoError(t, err)
	require.False(t, applied)

	// Either router's purge clears the shared observations, and only its own prefixed keys.
	require.NoError(t, mr.Set("sr:own", "x"))
	require.NoError(t, storeB.Purge(ctx))
	_, _, found, err = storeA.GetEndpointObservation(ctx, obsKey)
	require.NoError(t, err)
	require.False(t, found)
	require.True(t, mr.Exists("sr:own"), "a purge leaves another router's prefixed keys alone")
}
