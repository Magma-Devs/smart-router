package endpointstate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newObsMonitor builds a real EndpointMonitor for observation tests. No trackers are
// created, so no poll goroutines spin — we exercise the observation APIs directly.
func newObsMonitor(t *testing.T) *EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 12 * time.Second,
		BlocksToSave:     10,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

func TestEndpointMonitor_GetObservation_AbsentEndpoint(t *testing.T) {
	m := newObsMonitor(t)
	_, ok := m.GetObservation("http://nope:8545")
	require.False(t, ok, "no observation should exist for an unseen endpoint")
}

func TestEndpointMonitor_RecordPollObservation_Success(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	at := time.Unix(1000, 0)

	m.RecordPollObservation(url, 100, 25*time.Millisecond, nil, at)

	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(100), o.LatestBlock)
	require.Equal(t, at, o.ObservedAt)
	require.Equal(t, ObservationSourcePoll, o.Source)
	require.Equal(t, at, o.LastPollAttempt)
	require.Equal(t, at, o.LastSuccessfulPoll)
	require.Equal(t, 25*time.Millisecond, o.LastPollLatency)
	require.Empty(t, o.LastPollError)
	require.Equal(t, 0, o.ConsecutivePollFailures)
}

// The headline gap A closes: a healthy endpoint on a slow chain polls successfully every
// interval but only advances its block every avgBlockTime. LastSuccessfulPoll must move
// forward on every successful poll, even when the block is unchanged.
func TestEndpointMonitor_RecordPollObservation_SuccessSameBlockAdvancesLiveness(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	t0 := time.Unix(1000, 0)
	t1 := t0.Add(12 * time.Second)

	m.RecordPollObservation(url, 100, 10*time.Millisecond, nil, t0)
	m.RecordPollObservation(url, 100, 20*time.Millisecond, nil, t1) // same block, later poll

	o, _ := m.GetObservation(url)
	require.Equal(t, int64(100), o.LatestBlock, "block unchanged")
	require.Equal(t, t1, o.LastSuccessfulPoll, "liveness advances even when block is unchanged")
	require.Equal(t, t1, o.ObservedAt)
	require.Equal(t, 20*time.Millisecond, o.LastPollLatency)
}

func TestEndpointMonitor_RecordPollObservation_FailureThenRecovery(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	t0 := time.Unix(1000, 0)

	// One success, then two failures.
	m.RecordPollObservation(url, 100, 10*time.Millisecond, nil, t0)
	m.RecordPollObservation(url, 0, 0, errors.New("dial timeout"), t0.Add(1*time.Second))
	m.RecordPollObservation(url, 0, 0, errors.New("dial timeout"), t0.Add(2*time.Second))

	o, _ := m.GetObservation(url)
	require.Equal(t, t0.Add(2*time.Second), o.LastPollAttempt, "attempt stamps on failure")
	require.Equal(t, t0, o.LastSuccessfulPoll, "last-success is NOT touched by failures")
	require.Equal(t, int64(100), o.LatestBlock, "block triple is NOT touched by failures")
	require.Equal(t, ObservationSourcePoll, o.Source)
	require.Equal(t, "dial timeout", o.LastPollError)
	require.Equal(t, 2, o.ConsecutivePollFailures)

	// Recovery resets the failure counter and clears the error.
	m.RecordPollObservation(url, 101, 15*time.Millisecond, nil, t0.Add(3*time.Second))
	o, _ = m.GetObservation(url)
	require.Equal(t, 0, o.ConsecutivePollFailures)
	require.Empty(t, o.LastPollError)
	require.Equal(t, int64(101), o.LatestBlock)
	require.Equal(t, t0.Add(3*time.Second), o.LastSuccessfulPoll)
}

// A poll that reaches upstream but parses no block (err == nil, block <= 0) is a failure
// for liveness purposes.
func TestEndpointMonitor_RecordPollObservation_ParseFailIsFailure(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	at := time.Unix(1000, 0)

	m.RecordPollObservation(url, 0, 5*time.Millisecond, nil, at) // no error, but no block parsed

	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, at, o.LastPollAttempt)
	require.True(t, o.LastSuccessfulPoll.IsZero(), "parse-fail is not a successful poll")
	require.Equal(t, 1, o.ConsecutivePollFailures)
	require.NotEmpty(t, o.LastPollError)
	require.Equal(t, int64(0), o.LatestBlock)
}

func TestEndpointMonitor_RecordRelayObservation(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	at := time.Unix(2000, 0)

	m.RecordRelayObservation(url, 555, at)

	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(555), o.LatestBlock)
	require.Equal(t, at, o.ObservedAt)
	require.Equal(t, ObservationSourceRelay, o.Source)
	// Relay observations must NOT touch the poll-health fields.
	require.True(t, o.LastPollAttempt.IsZero())
	require.True(t, o.LastSuccessfulPoll.IsZero())
	require.Equal(t, 0, o.ConsecutivePollFailures)
}

func TestEndpointMonitor_RecordRelayObservation_IgnoresNonPositiveBlock(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	m.RecordRelayObservation(url, 0, time.Unix(2000, 0))
	_, ok := m.GetObservation(url)
	require.False(t, ok, "a non-positive relay block records nothing")
}

// The block triple is monotonic in ObservedAt: a stale observation (older timestamp)
// from either source must not move it backward, while poll-health still updates.
func TestEndpointMonitor_Observation_MonotonicObservedAt(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	tNew := time.Unix(3000, 0)
	tOld := tNew.Add(-1 * time.Minute)

	m.RecordRelayObservation(url, 900, tNew) // newest block via relay

	// A stale poll arrives late: it must NOT overwrite the newer relay block triple,
	// but it MUST still update poll-health (it was a real, successful poll attempt).
	m.RecordPollObservation(url, 880, 30*time.Millisecond, nil, tOld)

	o, _ := m.GetObservation(url)
	require.Equal(t, int64(900), o.LatestBlock, "stale poll must not move the block backward")
	require.Equal(t, tNew, o.ObservedAt)
	require.Equal(t, ObservationSourceRelay, o.Source)
	require.Equal(t, tOld, o.LastSuccessfulPoll, "poll-health still records the (stale) successful poll")
	require.Equal(t, 30*time.Millisecond, o.LastPollLatency)

	// A stale relay is fully ignored.
	m.RecordRelayObservation(url, 870, tOld)
	o, _ = m.GetObservation(url)
	require.Equal(t, int64(900), o.LatestBlock)
	require.Equal(t, ObservationSourceRelay, o.Source)
}

func TestEndpointMonitor_RemoveTracker_DropsObservation(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	m.RecordPollObservation(url, 100, 10*time.Millisecond, nil, time.Unix(1000, 0))
	_, ok := m.GetObservation(url)
	require.True(t, ok)

	m.RemoveTracker(url)

	_, ok = m.GetObservation(url)
	require.False(t, ok, "RemoveTracker should drop the endpoint's observation record")
}

// Concurrent poll + relay writers and snapshot readers must be race-free and never
// expose a half-updated record. Run with -race to exercise the locking.
func TestEndpointMonitor_GetObservation_ConcurrentSnapshot(t *testing.T) {
	m := newObsMonitor(t)
	url := "http://ep:8545"
	base := time.Unix(1000, 0)

	const iters = 500
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			m.RecordPollObservation(url, int64(1000+i), time.Duration(i)*time.Microsecond, nil, base.Add(time.Duration(i)*time.Millisecond))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			m.RecordRelayObservation(url, int64(2000+i), base.Add(time.Duration(i)*time.Millisecond))
		}
	}()
	var torn int32
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			// Snapshot must be internally consistent: any recorded block is > 0
			// (a torn/half-updated read would surface a zero block alongside ok=true).
			// Don't call require from a non-test goroutine — flag and assert after.
			if o, ok := m.GetObservation(url); ok && o.LatestBlock <= 0 {
				atomic.AddInt32(&torn, 1)
			}
		}
	}()

	wg.Wait()
	require.Zero(t, atomic.LoadInt32(&torn), "GetObservation must never return a torn/half-updated snapshot")
	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.NotZero(t, o.LatestBlock)
}
