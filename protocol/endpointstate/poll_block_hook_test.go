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

// OnPollBlock feeds rpc_endpoint_latest_block's per-url heads (MAG-4204). OnNewBlock cannot: it
// fires only for a strictly higher block, so it never reports the tracker's first head or a url
// that keeps answering the same block — which is what a stuck url looks like.

type polledBlock struct {
	url   string
	block int64
}

type pollBlockSink struct {
	mu     sync.Mutex
	blocks []polledBlock
}

func (s *pollBlockSink) record(url string, block int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks = append(s.blocks, polledBlock{url, block})
}

func (s *pollBlockSink) snapshot() []polledBlock {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]polledBlock(nil), s.blocks...)
}

func newPollBlockMonitor(t *testing.T, sink *pollBlockSink) *EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		OnPollBlock:      sink.record,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

func TestOnPollBlock_ReportsEveryAnsweredPoll(t *testing.T) {
	const url = "http://poll-block-hook:8545"
	sink := &pollBlockSink{}
	m := newPollBlockMonitor(t, sink)
	gen := m.registerGenForTest(url)
	now := time.Now()
	at := func(ms int) time.Time { return now.Add(time.Duration(ms) * time.Millisecond) }

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(0))                 // first head
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(1))                 // the same block again
	m.recordPollObservation(url, gen, 0, 0, errors.New("503"), at(2))                    // a failed poll
	m.recordPollObservation(url, gen, 0, 0, nil, at(3))                                  // answered without a block
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(4))                 // answers again, same block
	m.recordPollObservation(url, gen, 90, time.Millisecond, nil, at(5))                  // below a fresh tip: not accepted
	m.recordPollObservation(url, gen, 101, time.Millisecond, nil, at(6))                 // a new head
	m.recordPollObservation(url, gen, 102, time.Millisecond, nil, at(5).Add(-time.Hour)) // an older attempt: dropped
	m.recordPollObservation(url, gen+1, 103, time.Millisecond, nil, at(7))               // a replaced tracker: dropped

	require.Equal(t, []polledBlock{{url, 100}, {url, 100}, {url, 100}, {url, 101}}, sink.snapshot())

	m.Stop()
	m.recordPollObservation(url, gen, 104, time.Millisecond, nil, at(8))
	require.Len(t, sink.snapshot(), 4, "nothing is reported after Stop")
}

// A relay observation is not a poll: the harvest path reports its own provider-scoped head,
// and a url-wide report from here would move every provider sharing the url.
func TestOnPollBlock_RelayObservationIsNotReported(t *testing.T) {
	const url = "http://poll-block-relay:8545"
	sink := &pollBlockSink{}
	m := newPollBlockMonitor(t, sink)
	gen := m.registerGenForTest(url)

	require.True(t, m.RecordRelayObservation(url, gen, 500, time.Now()))
	require.Empty(t, sink.snapshot())
}

// OnPollFailure tells the router how long an endpoint has given no answer, so a url that stopped
// answering can leave rpc_endpoint_url_latest_block: it is down, not stuck. An answer is a
// successful poll or an accepted relay observation; a url that never answered reports 0.

type pollFailure struct {
	url       string
	silentFor time.Duration
}

type pollFailureSink struct {
	mu       sync.Mutex
	failures []pollFailure
}

func (s *pollFailureSink) record(url string, silentFor time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, pollFailure{url, silentFor})
}

func (s *pollFailureSink) snapshot() []pollFailure {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pollFailure(nil), s.failures...)
}

func TestOnPollFailure_ReportsHowLongTheEndpointHasGivenNoAnswer(t *testing.T) {
	const url = "http://poll-failure-hook:8545"
	sink := &pollFailureSink{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		OnPollFailure:    sink.record,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	gen := m.registerGenForTest(url)
	t0 := time.Now()
	at := t0.Add

	m.recordPollObservation(url, gen, 0, 0, errors.New("dial"), at(0))                        // never answered yet
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(time.Second))            // answers: no failure
	m.recordPollObservation(url, gen, 0, 0, errors.New("503"), at(31*time.Second))            // 30s after the answer
	m.recordPollObservation(url, gen, 0, 0, nil, at(3*time.Minute+time.Second))               // answered without a block
	require.True(t, m.RecordRelayObservation(url, gen, 100, at(4*time.Minute)))               // a relay answers
	m.recordPollObservation(url, gen, 0, 0, errors.New("503"), at(5*time.Minute))             // 1m after the relay
	m.recordPollObservation(url, gen+1, 0, 0, errors.New("503"), at(6*time.Minute))           // a replaced tracker
	m.recordPollObservation(url, gen, 0, 0, errors.New("503"), at(4*time.Minute+time.Second)) // an older attempt

	require.Equal(t, []pollFailure{
		{url, 0},
		{url, 30 * time.Second},
		{url, 3 * time.Minute},
		{url, time.Minute},
	}, sink.snapshot())
}
