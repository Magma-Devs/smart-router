package endpointstate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// OnPollResult feeds rpc_endpoint_latest_block's per-url heads (MAG-4204). OnNewBlock cannot:
// it fires only for a strictly higher block, so it never reports the tracker's first head or a
// url that keeps answering the same block — which is what a stuck url looks like.

type pollResult struct {
	url      string
	accepted int64
	failures int
}

type pollResultSink struct {
	mu      sync.Mutex
	results []pollResult
}

func (s *pollResultSink) record(url string, accepted int64, failures int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, pollResult{url, accepted, failures})
}

func (s *pollResultSink) snapshot() []pollResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pollResult(nil), s.results...)
}

func newPollResultMonitor(t *testing.T, sink *pollResultSink) *EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		OnPollResult:     sink.record,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

func TestOnPollResult_ReportsEveryAnsweredPollAndTheFailureStreak(t *testing.T) {
	const url = "http://poll-result-hook:8545"
	sink := &pollResultSink{}
	m := newPollResultMonitor(t, sink)
	gen := m.registerGenForTest(url)
	now := time.Now()
	at := func(ms int) time.Time { return now.Add(time.Duration(ms) * time.Millisecond) }

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(0))                 // first head
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(1))                 // the same block again
	m.recordPollObservation(url, gen, 0, 0, errors.New("503"), at(2))                    // a failed poll
	m.recordPollObservation(url, gen, 0, 0, errors.New("timeout"), at(3))                // another
	m.recordPollObservation(url, gen, 0, 0, nil, at(4))                                  // answered without a block
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(5))                 // answers again, same block
	m.recordPollObservation(url, gen, 90, time.Millisecond, nil, at(6))                  // below a fresh tip: not accepted
	m.recordPollObservation(url, gen, 101, time.Millisecond, nil, at(7))                 // a new head
	m.recordPollObservation(url, gen, 102, time.Millisecond, nil, at(6).Add(-time.Hour)) // an older attempt: dropped
	m.recordPollObservation(url, gen+1, 103, time.Millisecond, nil, at(8))               // a replaced tracker: dropped

	require.Equal(t, []pollResult{
		{url, 100, 0},
		{url, 100, 0},
		{url, 0, 1},
		{url, 0, 2},
		{url, 0, 3},
		{url, 100, 0},
		{url, 0, 0},
		{url, 101, 0},
	}, sink.snapshot())

	m.Stop()
	m.recordPollObservation(url, gen, 104, time.Millisecond, nil, at(9))
	require.Len(t, sink.snapshot(), 8, "nothing is reported after Stop")
}

// A relay observation is not a poll: the harvest path reports its own provider-scoped head,
// and a url-wide report from here would move every provider sharing the url.
func TestOnPollResult_RelayObservationIsNotReported(t *testing.T) {
	const url = "http://poll-result-relay:8545"
	sink := &pollResultSink{}
	m := newPollResultMonitor(t, sink)
	gen := m.registerGenForTest(url)

	require.True(t, m.RecordRelayObservation(url, gen, 500, time.Now()))
	require.Empty(t, sink.snapshot())
}
