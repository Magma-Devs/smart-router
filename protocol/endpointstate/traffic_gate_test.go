package endpointstate

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The traffic gate (MAG-2159 Topic B / Pass 2) lets a per-endpoint poll borrow a fresh
// relay-harvested tip instead of calling upstream. These tests pin its load-bearing
// invariants: only a RELAY observation suppresses a poll, only while fresh, and a gated
// poll spends no upstream call yet still advances the tracker-facing block.

// countingDirectRPCConnection wraps the shared mock and counts upstream SendRequest calls,
// so a test can assert that a gated poll made ZERO upstream round-trips.
type countingDirectRPCConnection struct {
	mockDirectRPCConnection
	sends atomic.Int32
}

func (c *countingDirectRPCConnection) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	c.sends.Add(1)
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
		require.True(t, ok, "a fresh relay tip is borrowable")
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

	t.Run("unknown endpoint is not borrowable", func(t *testing.T) {
		m := newGatedMonitor(t, 400*time.Millisecond)
		_, ok := m.freshRelayTip("http://nope:8545", now)
		require.False(t, ok)
	})
}

// TestFreshRelayTip_FreshnessBoundary pins the staleness window: a relay tip exactly at
// the threshold is still borrowable; one past it falls through to a real poll.
func TestFreshRelayTip_FreshnessBoundary(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	m := newGatedMonitor(t, freshness)
	gen := m.registerGenForTest(url)

	observedAt := time.Now()
	m.RecordRelayObservation(url, gen, 1000, observedAt)

	// Exactly at the window (age == freshness): still fresh (the gate rejects only age > window).
	_, ok := m.freshRelayTip(url, observedAt.Add(freshness))
	require.True(t, ok, "a tip exactly at the freshness window is still borrowable")

	// One nanosecond past the window: stale, poll runs.
	_, ok = m.freshRelayTip(url, observedAt.Add(freshness+time.Nanosecond))
	require.False(t, ok, "a tip past the freshness window must fall through to a real poll")
}

// TestEndpointPoller_RelayGate_BorrowsTipWithoutUpstreamCall is the end-to-end proof:
// when the gate reports a fresh relay tip, FetchLatestBlockNum returns it, makes ZERO
// upstream calls, and records NO poll observation (poll-health untouched). With no fresh
// relay tip, the poll runs upstream and records exactly one observation.
func TestEndpointPoller_RelayGate_BorrowsTipWithoutUpstreamCall(t *testing.T) {
	chainParser := newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)
	const url = "http://eth-ep:8545"
	conn := &countingDirectRPCConnection{mockDirectRPCConnection: mockDirectRPCConnection{url: url}}

	poller := NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		conn,
		chainParser,
		"ETH1",
		spectypes.APIInterfaceJsonRPC,
	)

	var observations int32
	poller.onPollObservation = func(block int64, latency time.Duration, pollErr error, at time.Time) {
		atomic.AddInt32(&observations, 1)
	}

	// Gate OPEN: report a fresh relay tip of 999.
	var gateOpen atomic.Bool
	gateOpen.Store(true)
	poller.relayGate = func(now time.Time) (int64, bool) {
		if gateOpen.Load() {
			return 999, true
		}
		return 0, false
	}

	block, err := poller.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(999), block, "the gated poll returns the borrowed relay tip")
	require.Equal(t, int32(0), conn.sends.Load(), "a gated poll must make NO upstream call")
	require.Equal(t, int32(0), atomic.LoadInt32(&observations), "a gated poll records NO poll observation")
	require.Equal(t, int64(999), atomic.LoadInt64(&poller.latestBlock), "the borrowed tip advances the cached latest")

	// Gate CLOSED: the poll falls through to a real upstream fetch (the liveness floor).
	gateOpen.Store(false)
	block, err = poller.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(256), block, "the real poll parses the upstream 0x100 response")
	require.Equal(t, int32(1), conn.sends.Load(), "the un-gated poll makes exactly one upstream call")
	require.Equal(t, int32(1), atomic.LoadInt32(&observations), "the un-gated poll records exactly one observation")
}

// TestGetOrCreateTracker_WiresRelayGate is the integration seam: it proves the real
// registration path actually wires fetcher.relayGate to freshRelayTip. Both halves are
// tested in isolation elsewhere (freshRelayTip as a predicate; FetchLatestBlockNum
// honoring a mock gate), but nothing else asserts the monitor connects them — and a
// dropped wiring line would silently degrade the gate to "never fires" (polls exactly as
// before, quota win gone) with no other test going red. We call the WIRED closure
// directly, so this is deterministic: no poll loop, no timer.
func TestGetOrCreateTracker_WiresRelayGate(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 400 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	const url = "http://eth-ep:8545"
	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	conn := &countingDirectRPCConnection{mockDirectRPCConnection: mockDirectRPCConnection{url: url}}
	_, err := m.GetOrCreateTracker(ep, conn)
	require.NoError(t, err)

	// Grab the real fetcher the monitor built and confirm its gate is wired.
	m.mu.RLock()
	fetcher := m.fetchers[url]
	m.mu.RUnlock()
	require.NotNil(t, fetcher)
	require.NotNil(t, fetcher.relayGate, "GetOrCreateTracker must wire the relay gate")

	now := time.Now()

	// No relay observation yet → the wired gate reports nothing borrowable (poll runs).
	_, ok := fetcher.relayGate(now)
	require.False(t, ok, "with no relay observation the wired gate must not suppress the poll")

	// Record a fresh relay observation through the real generation, then the SAME wired
	// closure must report it borrowable — proving GetOrCreateTracker → freshRelayTip is live.
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)
	m.RecordRelayObservation(url, gen, 999, now)

	blk, ok := fetcher.relayGate(now)
	require.True(t, ok, "a fresh relay observation must make the wired gate borrowable")
	require.Equal(t, int64(999), blk)
}

// TestEndpointPoller_NilGate_AlwaysPolls guards the standalone/test contract: a bare
// poller (no gate wired) always polls and records, so every existing poll-path test and
// the global-tracker fetcher are unaffected.
func TestEndpointPoller_NilGate_AlwaysPolls(t *testing.T) {
	chainParser := newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)
	const url = "http://eth-ep:8545"
	conn := &countingDirectRPCConnection{mockDirectRPCConnection: mockDirectRPCConnection{url: url}}

	poller := NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		conn,
		chainParser,
		"ETH1",
		spectypes.APIInterfaceJsonRPC,
	)
	require.Nil(t, poller.relayGate, "a bare poller has no gate")

	block, err := poller.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(256), block)
	require.Equal(t, int32(1), conn.sends.Load(), "with no gate the poll always hits upstream")
}
