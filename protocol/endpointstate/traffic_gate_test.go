package endpointstate

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The traffic gate (MAG-2159 Topic B) skips a per-endpoint poll cycle when a fresh relay-
// harvested tip already covers the endpoint. The gate itself lives on the ChainTracker
// (above the generic/SVM wrapper split — see chaintracker.fetchAllPreviousBlocksIfNecessary
// and chain_tracker_gate_test.go); the EndpointMonitor's contribution is the freshRelayTip
// predicate and wiring it into the tracker config. These tests pin that predicate's load-
// bearing invariants and the end-to-end SOLANA suppression the wiring must deliver.

// countingDirectRPCConnection wraps the shared mock and counts upstream SendRequest calls
// (optionally per request body), so a test can assert the gate suppressed real round-trips.
type countingDirectRPCConnection struct {
	mockDirectRPCConnection
	sends        atomic.Int32
	matchSubstr  string
	matchedSends atomic.Int32
}

func (c *countingDirectRPCConnection) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	c.sends.Add(1)
	if c.matchSubstr != "" && bytes.Contains(data, []byte(c.matchSubstr)) {
		c.matchedSends.Add(1)
	}
	return c.mockDirectRPCConnection.SendRequest(ctx, data, headers)
}

// newGatedMonitor builds a monitor with a deterministic freshness window for gate tests.
func newGatedMonitor(t *testing.T, freshness time.Duration) *EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: freshness, // relayGateFreshness defaults to avgBlockTime
		BlocksToSave:     1,
	})
	require.NotNil(t, m)
	require.Equal(t, freshness, m.relayGateFreshness, "freshness defaults to averageBlockTime")
	t.Cleanup(m.Stop)
	return m
}

// registerGen makes the monitor accept observation writes for a URL (the production path
// stamps a generation in GetOrCreateTracker; here we register one directly).
func (m *EndpointMonitor) registerGenForTest(url string) uint64 {
	m.obsMu.Lock()
	defer m.obsMu.Unlock()
	m.nextObsGen++
	m.generations[url] = m.nextObsGen
	return m.nextObsGen
}

// TestFreshRelayTip_OnlyRelaySourceSuppressesPoll is the anti-circularity invariant: a
// fresh POLL-sourced observation must NOT suppress the next poll (otherwise one poll
// throttles all polls), while a fresh RELAY-sourced one does.
func TestFreshRelayTip_OnlyRelaySourceSuppressesPoll(t *testing.T) {
	const url = "http://ep:8545"
	now := time.Now()

	t.Run("fresh relay observation suppresses the poll", func(t *testing.T) {
		m := newGatedMonitor(t, 400*time.Millisecond)
		gen := m.registerGenForTest(url)
		m.RecordRelayObservation(url, gen, 1000, now)

		tip, ok := m.freshRelayTip(url, now)
		require.True(t, ok, "a fresh relay tip suppresses the poll")
		require.Equal(t, int64(1000), tip)
	})

	t.Run("fresh poll observation does NOT suppress the poll", func(t *testing.T) {
		m := newGatedMonitor(t, 400*time.Millisecond)
		gen := m.registerGenForTest(url)
		// A successful poll sets Source = Poll.
		m.recordPollObservation(url, gen, 1000, 5*time.Millisecond, nil, now)

		o, ok := m.GetObservation(url)
		require.True(t, ok)
		require.Equal(t, ObservationSourcePoll, o.Source)

		_, ok = m.freshRelayTip(url, now)
		require.False(t, ok, "a poll-sourced observation must never suppress the next poll")
	})

	t.Run("unknown endpoint does not suppress", func(t *testing.T) {
		m := newGatedMonitor(t, 400*time.Millisecond)
		_, ok := m.freshRelayTip("http://nope:8545", now)
		require.False(t, ok)
	})
}

// TestFreshRelayTip_FreshnessBoundary pins the staleness window: a relay tip exactly at
// the threshold still suppresses; one past it falls through to a real poll.
func TestFreshRelayTip_FreshnessBoundary(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	m := newGatedMonitor(t, freshness)
	gen := m.registerGenForTest(url)

	observedAt := time.Now()
	m.RecordRelayObservation(url, gen, 1000, observedAt)

	// Exactly at the window (age == freshness): still fresh (the gate rejects only age > window).
	_, ok := m.freshRelayTip(url, observedAt.Add(freshness))
	require.True(t, ok, "a tip exactly at the freshness window still suppresses")

	// One nanosecond past the window: stale, poll runs.
	_, ok = m.freshRelayTip(url, observedAt.Add(freshness+time.Nanosecond))
	require.False(t, ok, "a tip past the freshness window must fall through to a real poll")
}

// TestEndpointMonitor_SolanaTrafficGate_SuppressesUpstreamPoll is the F1 end-to-end proof on
// the path that motivated the finding: a real SOLANA tracker built by GetOrCreateTracker polls
// the upstream via getLatestBlockhash (CustomMessage), which NEVER goes through
// EndpointPoller.FetchLatestBlockNum — so the old per-poller gate could not suppress it. With
// continuously-fresh relay observations the tracker must skip most of those polls. This also
// guards the monitor→ChainTracker wiring: if GetOrCreateTracker failed to set RelayTipFresh,
// every steady tick would poll and the upper bound below would be exceeded.
func TestEndpointMonitor_SolanaTrafficGate_SuppressesUpstreamPoll(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const url = "https://solana-ep:443/"
	conn := &countingDirectRPCConnection{
		mockDirectRPCConnection: mockDirectRPCConnection{
			url: url,
			responses: map[string][]byte{
				svmLatestBlockRequest: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":250000000},"value":{"blockhash":"solhash"}}}`),
			},
		},
		matchSubstr: "getLatestBlockhash", // count only the latest-block poll upstream calls
	}

	// avgBlockTime 100ms => flat poll 50ms, relay-freshness window 100ms.
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "SOLANA", spectypes.APIInterfaceJsonRPC),
		ChainID:          "SOLANA",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	_, err := m.GetOrCreateTracker(ep, conn)
	require.NoError(t, err)
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)

	// Keep a fresh relay observation in place for the whole run so the gate stays engaged.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				m.RecordRelayObservation(url, gen, 250000000, time.Now())
			}
		}
	}()

	// Run across many flat-poll ticks (~600ms / 50ms ≈ 12 ticks). With the gate engaged the
	// tracker forces only ~1 real poll per (defaultMaxRelaySkipsBeforePoll+1)=5 ticks, plus a
	// couple of init polls. Without the gate wired it would poll on every tick (≥10).
	time.Sleep(600 * time.Millisecond)
	close(stop)
	time.Sleep(20 * time.Millisecond)

	polls := conn.matchedSends.Load()
	require.LessOrEqual(t, polls, int32(6),
		"a relay-gated SOLANA tracker must suppress most getLatestBlockhash polls; got %d", polls)
	require.GreaterOrEqual(t, polls, int32(1),
		"the tracker must still poll at least once (init / bounded verification); got %d", polls)
}
