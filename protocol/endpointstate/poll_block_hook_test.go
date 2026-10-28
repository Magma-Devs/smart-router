package endpointstate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// OnPollBlock feeds rpc_endpoint_url_latest_block (MAG-4204). OnNewBlock cannot: it
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
	m.recordPollObservation(url, gen, 90, time.Millisecond, nil, at(5))                  // below a fresh tip: still an answer
	m.recordPollObservation(url, gen, 101, time.Millisecond, nil, at(6))                 // a new head
	m.recordPollObservation(url, gen, 102, time.Millisecond, nil, at(5).Add(-time.Hour)) // an older attempt: dropped
	m.recordPollObservation(url, gen+1, 103, time.Millisecond, nil, at(7))               // a replaced tracker: dropped

	// The block 90 row is MAG-4347. The tip store drops a block below a still-fresh stored
	// tip, and the hook used to report only what the store accepted — so a url that answered
	// every poll wrote nothing to its own series, and the answered-until time the stuck alerts
	// read went stale. The block reported is the one the url ANSWERED, not the stored tip, so
	// a url stuck on an old block still reads as stuck.
	require.Equal(t, []polledBlock{{url, 100}, {url, 100}, {url, 100}, {url, 90}, {url, 101}}, sink.snapshot())

	m.Stop()
	m.recordPollObservation(url, gen, 104, time.Millisecond, nil, at(8))
	require.Len(t, sink.snapshot(), 5, "nothing is reported after Stop")
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

// PollInterval is the tracker cadence the router sizes a url's answer timeout from: the block time
// over the poll divisor, capped at one minute as the tracker caps it (MAG-4249).
// WorstCaseGapBetweenPolls is 5 of them: the traffic gate skips up to 4 polls in a row, and the
// fifth polls.
func TestPollInterval_IsTheBlockTimeOverTheDivisorCappedAtAMinute(t *testing.T) {
	for _, tc := range []struct {
		blockTime time.Duration
		divisor   float64
		want      time.Duration
	}{
		{12 * time.Second, 0, 6 * time.Second}, // the default divisor, 2
		{12 * time.Second, 0.5, 24 * time.Second},
		{12 * time.Second, 4, 3 * time.Second},
		{150 * time.Second, 0, time.Minute},   // 75s
		{10 * time.Minute, 0, time.Minute},    // 5m
		{10 * time.Minute, 0.25, time.Minute}, // 40m
	} {
		ctx, cancel := context.WithCancel(context.Background())
		m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
			ChainID:             "ETH1",
			ApiInterface:        spectypes.APIInterfaceJsonRPC,
			AverageBlockTime:    tc.blockTime,
			BlocksToSave:        1,
			PollIntervalDivisor: tc.divisor,
		})
		require.Equal(t, tc.want, m.PollInterval(), "block time %v, divisor %v", tc.blockTime, tc.divisor)
		require.Equal(t, 5*tc.want, m.WorstCaseGapBetweenPolls(), "block time %v, divisor %v", tc.blockTime, tc.divisor)
		m.Stop()
		cancel()
	}
	var nilMonitor *EndpointMonitor
	require.Zero(t, nilMonitor.PollInterval())
	require.Zero(t, nilMonitor.WorstCaseGapBetweenPolls())
}

// The answered-until window has to outlast the longest gap between two answered polls of a url that
// answers every poll (MAG-4249), and the traffic gate makes that gap longer than one poll interval.
// While a relay or another pod's poll keeps the tip fresh, the gate skips up to 4 polls in a row, and
// neither writes every series of the url on this pod: another pod's poll writes nothing here, and a
// relay writes only the provider it went through. So the polls alone have to answer in time.
//
// This runs a real tracker with the gate held open, first by relays and then by another pod's poll,
// and then lets the gate close, the way a burst of relays ends. Every gap between two answers
// OnPollBlock reports stays inside WorstCaseGapBetweenPolls plus one poll interval, which is what the
// router's window is made of. At least one gap is longer than 2 poll intervals, so the gate really
// skipped polls in a row, and the two-interval window MAG-4204 shipped with would have lapsed.
//
// The tracker polls every 200ms here. The window scales with that interval, and
// TestEndpointMonitor_PollDivisor_ReachesLiveTracker shows PollInterval is the interval a live
// tracker waits at block times up to 10 minutes.
func TestWorstCaseGapBetweenPolls_BoundsTheGapsUnderTheTrafficGate(t *testing.T) {
	const block = 256 // mockDirectRPCConnection answers eth_blockNumber with 0x100
	for _, source := range []string{metrics.TrackerGateSkipSourceRelay, metrics.TrackerGateSkipSourcePeer} {
		t.Run(source, func(t *testing.T) {
			ensureRandSeeded()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			url := "http://eth-gate-gap-" + source + ":8545"
			var mu sync.Mutex
			var answers []time.Time
			var skips atomic.Int32
			peers := &fakePeerStore{}
			cfg := EndpointChainTrackerConfig{
				ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
				ChainID:          "ETH1",
				ApiInterface:     spectypes.APIInterfaceJsonRPC,
				AverageBlockTime: 400 * time.Millisecond, // polls every 200ms; a fresh tip holds the gate for 400ms
				BlocksToSave:     1,
				OnPollBlock: func(string, int64) {
					mu.Lock()
					defer mu.Unlock()
					answers = append(answers, time.Now())
				},
				OnGateSkip: func(_ string, skipSource string) {
					if skipSource == source {
						skips.Add(1)
					}
				},
			}
			if source == metrics.TrackerGateSkipSourcePeer {
				peers.set(block, "other-pod/abcd", 0) // the same block as this pod's polls, always fresh
				cfg.PeerObservations = peers
			}
			m := NewEndpointMonitor(ctx, cfg)
			t.Cleanup(m.Stop)
			_, err := m.GetOrCreateTracker("", &lavasession.Endpoint{NetworkAddress: url, Enabled: true}, &mockDirectRPCConnection{url: url})
			require.NoError(t, err)
			gen, ok := m.ObservationGeneration(url)
			require.True(t, ok)

			// Hold the gate open for a little over two rounds of 5 polls.
			heldOpen := 11 * m.PollInterval()
			if source == metrics.TrackerGateSkipSourceRelay {
				stop := make(chan struct{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					ticker := time.NewTicker(m.PollInterval() / 4)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							m.RecordRelayObservation(url, gen, block, time.Now())
						}
					}
				}()
				time.Sleep(heldOpen)
				close(stop)
				<-done
			} else {
				time.Sleep(heldOpen)
				peers.set(block, "other-pod/abcd", time.Hour) // too old to borrow: the gate closes
			}
			// The gate closes once the tip is a block time old, and the tracker polls every interval.
			time.Sleep(4 * m.PollInterval())

			require.Positive(t, skips.Load(), "the gate skipped polls on %s observations", source)
			mu.Lock()
			defer mu.Unlock()
			require.GreaterOrEqual(t, len(answers), 4, "init, the gate's forced polls and the polls after it")
			var longest time.Duration
			for i := 1; i < len(answers); i++ {
				longest = max(longest, answers[i].Sub(answers[i-1]))
			}
			t.Logf("%d answers, longest gap %v, poll interval %v", len(answers), longest, m.PollInterval())
			require.Greater(t, longest, 2*m.PollInterval(), "the gate skipped polls in a row")
			require.LessOrEqual(t, longest, m.WorstCaseGapBetweenPolls()+m.PollInterval(),
				"a url that answers every poll answers again before its answer stops counting")
		})
	}
}
