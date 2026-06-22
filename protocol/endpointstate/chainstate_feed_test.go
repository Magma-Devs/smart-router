package endpointstate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Phase 2 (MAG-2160 / Topic C): every accepted positive poll/relay block feeds the per-chain
// tip via the OnTipObservation hook, fired AFTER obsMu is released. These tests pin which
// observations fire the hook (accepted + positive only) and that SnapshotObservations returns
// the full record set for the consensus recompute pull.

// tipSink records the blocks delivered to OnTipObservation (the hook is called off the
// observation lock, possibly from the poll goroutine, so guard it).
type tipSink struct {
	mu     sync.Mutex
	blocks []int64
}

func (s *tipSink) record(block int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks = append(s.blocks, block)
}

func (s *tipSink) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.blocks...)
}

func newTipMonitor(t *testing.T, sink *tipSink) *EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		OnTipObservation: sink.record,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

func TestOnTipObservation_FiresForAcceptedPositiveBlocksOnly(t *testing.T) {
	const url = "http://ep:8545"
	sink := &tipSink{}
	m := newTipMonitor(t, sink)
	gen := m.registerGenForTest(url)
	now := time.Now()

	// A successful poll feeds the tip.
	m.recordPollObservation(url, gen, 100, 5*time.Millisecond, nil, now)
	require.Equal(t, []int64{100}, sink.snapshot())

	// A failed poll (no block) must NOT feed the tip.
	m.recordPollObservation(url, gen, 0, 0, errors.New("boom"), now.Add(time.Millisecond))
	require.Equal(t, []int64{100}, sink.snapshot(), "a failed poll feeds no tip")

	// A relay observation feeds the tip.
	m.RecordRelayObservation(url, gen, 200, now.Add(2*time.Millisecond))
	require.Equal(t, []int64{100, 200}, sink.snapshot())

	// A stale-generation relay is rejected by the gate → no tip feed.
	m.RecordRelayObservation(url, gen+1, 999, now.Add(3*time.Millisecond))
	require.Equal(t, []int64{100, 200}, sink.snapshot(), "a rejected (stale-gen) relay feeds no tip")

	// A non-positive relay block is ignored before the lock → no tip feed.
	m.RecordRelayObservation(url, gen, 0, now.Add(4*time.Millisecond))
	require.Equal(t, []int64{100, 200}, sink.snapshot())
}

// A stale poll (older than the last attempt) is dropped wholesale and must not feed the tip.
func TestOnTipObservation_StalePollDoesNotFeed(t *testing.T) {
	const url = "http://ep:8545"
	sink := &tipSink{}
	m := newTipMonitor(t, sink)
	gen := m.registerGenForTest(url)
	now := time.Now()

	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, now)
	require.Equal(t, []int64{100}, sink.snapshot())

	// An older poll attempt is dropped before the block triple updates → no feed.
	m.recordPollObservation(url, gen, 90, time.Millisecond, nil, now.Add(-time.Second))
	require.Equal(t, []int64{100}, sink.snapshot(), "a stale poll must not feed the tip")
}

func TestSnapshotObservations_ReturnsAllRecords(t *testing.T) {
	sink := &tipSink{}
	m := newTipMonitor(t, sink)
	now := time.Now()

	for _, u := range []string{"http://a", "http://b", "http://c"} {
		gen := m.registerGenForTest(u)
		m.RecordRelayObservation(u, gen, 1000, now)
	}

	snap := m.SnapshotObservations()
	require.Len(t, snap, 3)
	for _, u := range []string{"http://a", "http://b", "http://c"} {
		o, ok := snap[u]
		require.True(t, ok)
		require.Equal(t, int64(1000), o.LatestBlock)
		require.Equal(t, ObservationSourceRelay, o.Source)
	}

	// The returned map is a copy — mutating it must not affect the monitor.
	delete(snap, "http://a")
	require.Len(t, m.SnapshotObservations(), 3, "SnapshotObservations must return a copy")
}

// TestEndToEnd_ObservationsFeedChainStateTipAndConsensus is the Topic-C verification at the
// integration boundary: relay observations recorded through the real monitor feed a real
// ChainState (cheap monotonic tip via the hook), and a recompute over SnapshotObservations
// establishes the strict-majority baseline + rejects an anti-lie outlier. Uses
// registerGenForTest so there are no background-poll goroutines — fully deterministic and
// race-free. This is the production data path minus the rpcss recompute-loop wrapper.
func TestEndToEnd_ObservationsFeedChainStateTipAndConsensus(t *testing.T) {
	cs := chainstate.New("ETH1", chainstate.Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  time.Minute,
		TTL:              time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		OnTipObservation: func(block int64) { cs.SetLatestBlock(block) },
	})
	t.Cleanup(m.Stop)

	now := time.Now()
	// Three endpoints agree on ~1000 → each relay feeds the monotonic tip.
	for _, u := range []string{"http://a", "http://b", "http://c"} {
		gen := m.registerGenForTest(u)
		m.RecordRelayObservation(u, gen, 1000, now)
	}
	block, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1000), block, "agreeing relay observations feed the monotonic tip")

	// Recompute over the snapshot establishes the strict-majority baseline.
	recompute := func() {
		snap := m.SnapshotObservations()
		obs := make([]chainstate.BlockObservation, 0, len(snap))
		for url, o := range snap {
			if o.LatestBlock <= 0 {
				continue
			}
			obs = append(obs, chainstate.BlockObservation{URL: url, Block: o.LatestBlock, ObservedAt: o.ObservedAt})
		}
		cs.Recompute(obs)
	}
	recompute()
	base, hasBase := cs.HasConsensusBaseline()
	require.True(t, hasBase, "3 agreeing endpoints establish a consensus baseline")
	require.Equal(t, int64(1000), base)

	// Anti-lie: a 4th endpoint lies far ahead. With a baseline in force, the outlier write is
	// rejected and the tip does not jump.
	genLiar := m.registerGenForTest("http://liar")
	m.RecordRelayObservation("http://liar", genLiar, 5_000_000, now)
	tip, _ := cs.GetLatestBlock()
	require.Less(t, tip, int64(2000), "an anti-lie outlier must not poison the tip")
}
