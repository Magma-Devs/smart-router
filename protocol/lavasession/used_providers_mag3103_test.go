package lavasession

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-3103: GetErroredProviders handed the caller the live erroredProviders map. The lock is
// released when the function returns, so the caller iterated shared state while RemoveUsed and
// ReleaseFromLatestBatch, still running for the slower providers of the same relay, wrote to
// it. Go treats concurrent map iteration and write as a fatal runtime throw, not a panic, so
// the whole router process ended. The header that reaches this path, lava-debug-relay, is
// accepted from any caller.

// The contract, pinned without any scheduling luck: what the caller holds is a snapshot.
func TestGetErroredProvidersReturnsASnapshot(t *testing.T) {
	failure := errors.New("provider failed")
	key := NewRouterKey(nil)

	t.Run("a failure recorded after the call does not appear in the map already handed out", func(t *testing.T) {
		up := NewUsedProviders(nil)
		up.RemoveUsed("lava@first", key, failure)

		snapshot := up.GetErroredProviders(key)
		require.Equal(t, map[string]struct{}{"lava@first": {}}, snapshot)

		// A straggler reports its failure after the header builder has taken its copy.
		up.RemoveUsed("lava@straggler", key, failure)
		require.NotContains(t, snapshot, "lava@straggler",
			"GetErroredProviders returned the live map: a later write showed up in a map already handed out (MAG-3103)")

		// The store itself must still have both, so the next reader sees the straggler.
		require.Len(t, up.GetErroredProviders(key), 2)
	})

	t.Run("mutating the returned map does not reach the store", func(t *testing.T) {
		up := NewUsedProviders(nil)
		up.RemoveUsed("lava@first", key, failure)

		snapshot := up.GetErroredProviders(key)
		snapshot["lava@injected"] = struct{}{}
		delete(snapshot, "lava@first")

		require.Equal(t, map[string]struct{}{"lava@first": {}}, up.GetErroredProviders(key),
			"a caller's edits to the returned map must not alter the store")
	})

	t.Run("a key with no failures yields an empty non-nil map", func(t *testing.T) {
		up := NewUsedProviders(nil)
		snapshot := up.GetErroredProviders(NewRouterKey([]string{"archive"}))
		require.NotNil(t, snapshot)
		require.Empty(t, snapshot)
	})
}

// The failure the ticket describes, as the ticket asks for it: several goroutines iterate the
// result while others record failures through both write paths. Run with -race to see the old
// contract fail deterministically; without it, the old code dies with "fatal error: concurrent
// map iteration and map write" on most runs, which is the production symptom.
func TestGetErroredProvidersIsSafeWhileFailuresAreRecorded(t *testing.T) {
	up := NewUsedProviders(nil)
	key := NewRouterKey(nil)
	failure := errors.New("provider failed")

	const (
		writers           = 8
		failuresPerWriter = 250
		readers           = 4
	)

	var writersWG sync.WaitGroup
	writersWG.Add(writers)
	writersDone := make(chan struct{})

	// Readers start first so they are already iterating when the first failure lands.
	var readersWG sync.WaitGroup
	readersWG.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer readersWG.Done()
			for {
				errored := up.GetErroredProviders(key)
				// Iterating is the operation the runtime aborts the process for. Mirror the
				// header builder, which also sizes an array once and then fills it by index.
				names := make([]string, len(errored))
				idx := 0
				for provider := range errored {
					names[idx] = provider
					idx++
				}
				select {
				case <-writersDone:
					return
				default:
				}
			}
		}()
	}

	for w := 0; w < writers; w++ {
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < failuresPerWriter; i++ {
				provider := fmt.Sprintf("lava@w%d-p%d", w, i)
				if i%2 == 0 {
					// The response path: a provider answered with a protocol error.
					up.RemoveUsed(provider, key, failure)
				} else {
					// The pre-dispatch path: a provider was dropped after AddUsed by a filter.
					up.AddUsed(ConsumerSessionsMap{provider: &SessionInfo{}}, nil)
					up.ReleaseFromLatestBatch(provider, key, failure)
				}
			}
		}(w)
	}
	writersWG.Wait()
	close(writersDone)
	readersWG.Wait()

	require.Len(t, up.GetErroredProviders(key), writers*failuresPerWriter,
		"every failure recorded on either write path must be in the store once the writers finish")
}
