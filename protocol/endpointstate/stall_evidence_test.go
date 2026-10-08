package endpointstate

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MAG-3986 — stall evidence. Each endpoint counts consecutive tracker cycles in which it answered
// without going above the highest block it has shown. These tests pin every row of the counting table in the plan, the relay
// de-duplication (H9), the threshold rounding (D20, H10), and the repeat flag on the tip hook.

var stallBase = time.Unix(1_800_000_000, 0)

func obsOf(t *testing.T, m *EndpointMonitor, url string) EndpointObservation {
	t.Helper()
	o, ok := m.GetObservation(url)
	require.True(t, ok)
	return o
}

func TestStallEvidence_PollRows(t *testing.T) {
	m := newObsMonitor(t)
	const url = "https://stall-poll.example"
	gen := registerGen(m, url)
	at := stallBase
	poll := func(block int64, err error) {
		at = at.Add(time.Second)
		m.recordPollObservation(url, gen, block, time.Millisecond, err, at)
	}

	poll(100, nil) // first answer: sets the highest, counts nothing (D18)
	o := obsOf(t, m, url)
	require.Equal(t, int64(100), o.HighestBlockSeen)
	require.Equal(t, 0, o.SameBlockCycles)

	poll(100, nil) // same block: +1
	poll(100, nil) // same block: +1
	require.Equal(t, 2, obsOf(t, m, url).SameBlockCycles)

	poll(0, errors.New("timeout")) // failed poll: unchanged (D16)
	poll(0, nil)                   // no block parsed: unchanged
	require.Equal(t, 2, obsOf(t, m, url).SameBlockCycles)

	poll(99, nil) // a lower block is not movement: +1, highest kept
	o = obsOf(t, m, url)
	require.Equal(t, 3, o.SameBlockCycles)
	require.Equal(t, int64(99), o.LastAnsweredBlock)
	require.Equal(t, int64(100), o.HighestBlockSeen)

	poll(101, nil) // higher: reset
	o = obsOf(t, m, url)
	require.Equal(t, 0, o.SameBlockCycles)
	require.Equal(t, int64(101), o.HighestBlockSeen)
}

// A load balancer that fails over to a backend frozen BELOW what the url already reported (the
// Chainstack half of the Plasma incident) must still count: stuck means no answer above the
// highest block the url has shown.
func TestStallEvidence_FrozenBelowHighestStillCounts(t *testing.T) {
	m := newObsMonitor(t)
	const url = "https://stall-below.example"
	gen := registerGen(m, url)
	m.recordPollObservation(url, gen, 1003, time.Millisecond, nil, stallBase)
	for i := 1; i <= 25; i++ {
		m.recordPollObservation(url, gen, 1000, time.Millisecond, nil, stallBase.Add(time.Duration(i)*time.Second))
	}
	o := obsOf(t, m, url)
	require.Equal(t, int64(1003), o.HighestBlockSeen)
	require.Equal(t, int64(1000), o.LastAnsweredBlock)
	require.Equal(t, 25, o.SameBlockCycles, "every answer below the highest is a cycle without movement")
}

// A frozen Solana node answers two frozen slots: relays report context.slot at the client's
// commitment (confirmed), the poll the finalized one. Alternating between them is not movement.
// Comparing each answer with the previous one reset the counter on every switch, so a busy frozen
// node peaked at 4 cycles and was never marked.
func TestStallEvidence_FrozenAlternatingSlotsStillCount(t *testing.T) {
	freshness := time.Hour
	m := newGatedMonitor(t, freshness)
	const url = "https://stall-solana.example"
	gen := m.registerGenForTest(url)
	const confirmed, finalized = int64(5032), int64(5000)

	at := stallBase
	m.recordPollObservation(url, gen, finalized, time.Millisecond, nil, at)
	for cycle := 1; cycle <= 25; cycle++ {
		at = at.Add(time.Second)
		if cycle%5 == 0 { // the traffic gate forces a real poll every 5th cycle
			m.recordPollObservation(url, gen, finalized, time.Millisecond, nil, at)
			continue
		}
		require.True(t, m.RecordRelayObservation(url, gen, confirmed, at))
		tip, ok := m.freshRelayTip(url, at)
		require.True(t, ok)
		m.countRelayCoveredCycle(url, gen, tip)
	}
	o := obsOf(t, m, url)
	require.Equal(t, confirmed, o.HighestBlockSeen)
	require.Equal(t, 25, o.SameBlockCycles, "every cycle counts: polls below the highest slot and relay-covered cycles alike")
}

func TestStallEvidence_RelayOnlyAdvances(t *testing.T) {
	m := newObsMonitor(t)
	const url = "https://stall-relay.example"
	gen := registerGen(m, url)

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, stallBase)
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, stallBase.Add(time.Second))
	require.Equal(t, 1, obsOf(t, m, url).SameBlockCycles)

	// A relay repeating the block never counts by itself: per-request rate must not inflate it.
	for i := 0; i < 5; i++ {
		m.RecordRelayObservation(url, gen, 100, stallBase.Add(time.Duration(2+i)*time.Second))
	}
	require.Equal(t, 1, obsOf(t, m, url).SameBlockCycles)

	// A relay with a higher block resets, like a poll.
	m.RecordRelayObservation(url, gen, 105, stallBase.Add(10*time.Second))
	o := obsOf(t, m, url)
	require.Equal(t, 0, o.SameBlockCycles)
	require.Equal(t, int64(105), o.HighestBlockSeen)
}

// A peer pod's observation is another pod's view (D14): it feeds the tip store but never this pod's
// stall evidence.
func TestStallEvidence_PeerNeverCounts(t *testing.T) {
	m := newObsMonitor(t)
	const url = "https://stall-peer.example"
	gen := registerGen(m, url)
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, stallBase)

	m.recordPeerObservation(url, gen, 100, stallBase.Add(time.Second))
	m.recordPeerObservation(url, gen, 500, stallBase.Add(2*time.Second))
	o := obsOf(t, m, url)
	require.Equal(t, 0, o.SameBlockCycles)
	require.Equal(t, int64(100), o.HighestBlockSeen, "a peer's higher block is not this pod's evidence of movement")
}

// A cycle the traffic gate skipped because this pod's fresh relay covered it counts once per NEW
// relay answer (H9): one relay spanning two poll cycles is one piece of evidence.
func TestStallEvidence_RelayCoveredCycleCountsOncePerRelay(t *testing.T) {
	freshness := time.Hour // keep the relay tip fresh for the whole test
	m := newGatedMonitor(t, freshness)
	const url = "https://stall-gate.example"
	gen := m.registerGenForTest(url)

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, stallBase)
	require.True(t, m.RecordRelayObservation(url, gen, 100, stallBase.Add(time.Second)))

	now := stallBase.Add(2 * time.Second)
	tip, ok := m.freshRelayTip(url, now)
	require.True(t, ok)
	m.countRelayCoveredCycle(url, gen, tip)
	m.countRelayCoveredCycle(url, gen, tip) // same relay answer, second skipped cycle
	require.Equal(t, 1, obsOf(t, m, url).SameBlockCycles)

	require.True(t, m.RecordRelayObservation(url, gen, 100, stallBase.Add(3*time.Second)))
	tip, ok = m.freshRelayTip(url, stallBase.Add(4*time.Second))
	require.True(t, ok)
	m.countRelayCoveredCycle(url, gen, tip)
	require.Equal(t, 2, obsOf(t, m, url).SameBlockCycles)

	// A stale generation (a replaced tracker) never counts.
	m.countRelayCoveredCycle(url, gen+99, tip)
	require.Equal(t, 2, obsOf(t, m, url).SameBlockCycles)
}

func TestStallEvidence_ResetStallCountersKeepsHighest(t *testing.T) {
	m := newObsMonitor(t)
	const url = "https://stall-reset.example"
	gen := registerGen(m, url)
	for i := 0; i < 4; i++ {
		m.recordPollObservation(url, gen, 100, time.Millisecond, nil, stallBase.Add(time.Duration(i)*time.Second))
	}
	require.Equal(t, 3, obsOf(t, m, url).SameBlockCycles)

	m.ResetStallCounters()
	o := obsOf(t, m, url)
	require.Equal(t, 0, o.SameBlockCycles)
	require.Equal(t, int64(100), o.HighestBlockSeen)
}

func TestStallCycleThreshold(t *testing.T) {
	require.Equal(t, 20, StallCycleThresholdFor(0), "0 means the default divisor of 2")
	require.Equal(t, 20, StallCycleThresholdFor(2))
	require.Equal(t, 3, StallCycleThresholdFor(0.25), "2.5 rounds up to 3, and the floor is 3")
	require.Equal(t, 5, StallCycleThresholdFor(0.5))
	require.Equal(t, 80, StallCycleThresholdFor(8))

	require.Equal(t, 20, newObsMonitor(t).StallCycleThreshold(), "a monitor with no divisor uses the default")
}

type repeatCall struct {
	url    string
	block  int64
	repeat bool
}

type repeatSink struct {
	mu    sync.Mutex
	calls []repeatCall
}

func (s *repeatSink) record(url string, block int64, repeat bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, repeatCall{url, block, repeat})
}

func (s *repeatSink) last() repeatCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[len(s.calls)-1]
}

// The tip hook carries the url and whether the block repeats that url's stored tip — what the
// router needs to refuse a repeat vote from a head-stalled endpoint.
func TestOnTipObservation_CarriesURLAndRepeat(t *testing.T) {
	sink := &repeatSink{}
	m := newTipMonitor(t, nil)
	m.onTipObservation = sink.record
	const url = "https://stall-hook.example"
	gen := registerGen(m, url)

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, stallBase)
	require.Equal(t, repeatCall{url, 100, false}, sink.last(), "first block is not a repeat")

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, stallBase.Add(time.Second))
	require.Equal(t, repeatCall{url, 100, true}, sink.last(), "same block again is a repeat")

	m.RecordRelayObservation(url, gen, 100, stallBase.Add(2*time.Second))
	require.Equal(t, repeatCall{url, 100, true}, sink.last(), "a relay repeating it is a repeat too")

	m.recordPeerObservation(url, gen, 100, stallBase.Add(3*time.Second))
	require.Equal(t, repeatCall{url, 100, true}, sink.last(), "and so is a peer pod's repeat")

	m.RecordRelayObservation(url, gen, 101, stallBase.Add(4*time.Second))
	require.Equal(t, repeatCall{url, 101, false}, sink.last(), "a higher block is an advance")
}
