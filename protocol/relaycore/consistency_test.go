package relaycore

import (
	"strconv"
	"sync"
	"testing"
	"time"

	common "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

func setupConsistency() Consistency {
	return NewConsistency("test")
}

func TestSetGet(t *testing.T) {
	consistency, ok := setupConsistency().(*ConsistencyImpl)
	require.True(t, ok, "setupConsistency should return *ConsistencyImpl")
	const BLOCKVALUE = int64(5)
	for i := 0; i < 100; i++ {
		consistency.SetLatestBlock(strconv.Itoa(i), BLOCKVALUE)
	}
	time.Sleep(4 * time.Millisecond)
	for i := 0; i < 100; i++ {
		block, found := consistency.GetLatestBlock(strconv.Itoa(i))
		require.Equal(t, BLOCKVALUE, block)
		require.True(t, found)
	}
}

func TestBasic(t *testing.T) {
	consistency := setupConsistency()

	dappid := "/1245/"
	ip := "1.1.1.1:443"

	dappid_other := "/77777/"
	ip_other := "2.1.1.1:443"

	userDataOne := common.UserData{DappId: dappid, ConsumerIp: ip}
	userDataOther := common.UserData{DappId: dappid_other, ConsumerIp: ip_other}

	for i := 1; i < 100; i++ {
		consistency.SetSeenBlock(int64(i), userDataOne)
		time.Sleep(4 * time.Millisecond) // need to let each set finish
	}
	consistency.SetSeenBlock(5, userDataOther)
	time.Sleep(4 * time.Millisecond)
	// try to set older values and discard them
	consistency.SetSeenBlock(3, userDataOther)
	time.Sleep(4 * time.Millisecond)
	consistency.SetSeenBlock(3, userDataOne)
	time.Sleep(4 * time.Millisecond)
	block, found := consistency.GetSeenBlock(userDataOne)
	require.True(t, found)
	require.Equal(t, int64(99), block)
	block, found = consistency.GetSeenBlock(userDataOther)
	require.True(t, found)
	require.Equal(t, int64(5), block)
}

// TestResetState_FlushesCorruptionUnderConcurrentWrites mirrors the production
// MAG-1878 shape: an injector continuously writes a corrupted seenBlock value
// up to the moment /debug/reset fires, then stops; the test then asserts the
// corruption did not survive the reset.
//
// Without the RWMutex serialization in ResetState/SetSeenBlockFromKey, the
// injector's last write — which is still in ristretto's setBuf when Clear
// runs — can commit *after* Clear's store-clear step but *before* Clear
// returns, leaving the cache holding the corruption value despite the reset
// completing successfully. ristretto's own Clear doc warns it "is not an
// atomic operation (but that shouldn't be a problem as it's assumed that
// Set/Get calls won't be occurring until after this)" — the lock makes that
// precondition hold.
//
// Iteration amplifies the probability of catching the race across goroutine-
// scheduling variance. With the fix the test is deterministic (0 leaks);
// without it, some iterations leak corruption.
func TestResetState_FlushesCorruptionUnderConcurrentWrites(t *testing.T) {
	const (
		corruption   = int64(1_780_000_000_000)
		key          = "victim"
		warmup       = 2 * time.Millisecond
		iterations   = 50
	)

	consistency, ok := setupConsistency().(*ConsistencyImpl)
	require.True(t, ok, "setupConsistency should return *ConsistencyImpl")

	leaks := 0
	for iter := 0; iter < iterations; iter++ {
		// Start each iteration from a quiescent, empty cache so a previous
		// iteration's leak can't masquerade as a fresh one.
		consistency.ResetState()
		consistency.cache.Wait()

		var wg sync.WaitGroup
		injectorStop := make(chan struct{})

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-injectorStop:
					return
				default:
					consistency.SetSeenBlockFromKey(corruption, key)
				}
			}
		}()

		// Let the injector saturate setBuf with corrupted writes.
		time.Sleep(warmup)

		// Stop the injector and fire reset back-to-back so the injector's
		// last writes are still in flight (in setBuf) when Clear runs.
		close(injectorStop)
		consistency.ResetState()

		wg.Wait()
		// Drain anything still in setBuf so a late commit surfaces as a leak.
		consistency.cache.Wait()

		if block, found := consistency.GetLatestBlock(key); found && block == corruption {
			leaks++
		}
	}
	require.Equalf(t, 0, leaks,
		"%d/%d iterations leaked corruption past ResetState — write/Clear serialization is broken",
		leaks, iterations)
}
