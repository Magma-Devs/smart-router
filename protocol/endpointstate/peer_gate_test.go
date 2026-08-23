package endpointstate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The fleet tracker gate (MAG-2981): a fresh poll observation published by ANOTHER pod
// suppresses this pod's dedicated poll, the peer block is adopted under SourcePeer, and
// every successful local poll is published. These tests pin the predicate's invariants
// (self-exclusion, freshness, miss/error → poll), the publish contract, and the end-to-end
// suppression through a real SOLANA tracker — the path the relay gate also proves.

// fakePeerStore is an in-memory PeerObservationStore with one live observation and a
// counter per method, so tests can assert what the monitor asked of it.
type fakePeerStore struct {
	mu        sync.Mutex
	block     int64
	podID     string
	age       time.Duration
	found     bool
	fetchErr  error
	fetches   atomic.Int32
	publishes atomic.Int32

	lastPublish struct {
		chainID, apiInterface, endpointID, podID string
		block                                    int64
		ttl                                      time.Duration
	}
}

func (f *fakePeerStore) Publish(ctx context.Context, chainID, apiInterface, endpointID, podID string, block int64, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishes.Add(1)
	f.lastPublish.chainID, f.lastPublish.apiInterface, f.lastPublish.endpointID, f.lastPublish.podID = chainID, apiInterface, endpointID, podID
	f.lastPublish.block, f.lastPublish.ttl = block, ttl
	return nil
}

func (f *fakePeerStore) Fetch(ctx context.Context, chainID, apiInterface, endpointID string) (int64, string, time.Duration, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches.Add(1)
	if f.fetchErr != nil {
		return 0, "", 0, false, f.fetchErr
	}
	return f.block, f.podID, f.age, f.found, nil
}

func (f *fakePeerStore) set(block int64, podID string, age time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block, f.podID, f.age, f.found = block, podID, age, true
}

// newPeerGatedMonitor builds a monitor with the fake store wired and a deterministic
// freshness window (relayGateFreshness = avgBlockTime, shared by both gate halves).
func newPeerGatedMonitor(t *testing.T, freshness time.Duration, store PeerObservationStore) *EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: freshness,
		BlocksToSave:     1,
		PeerObservations: store,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

func TestEndpointID_IsStableAndOpaque(t *testing.T) {
	const url = "https://rpc.example/v1/SECRET-API-KEY"
	id := EndpointID(url)
	require.Equal(t, id, EndpointID(url), "the same URL must digest identically on every pod")
	require.NotEqual(t, id, EndpointID(url+"x"))
	require.Len(t, id, 32, "16-byte digest, hex")
	require.False(t, strings.Contains(id, "SECRET"), "the digest must not leak the URL")
	require.NotEmpty(t, LocalPodID())
	require.Equal(t, LocalPodID(), LocalPodID(), "pod id is stable for the process lifetime")
}

func TestFreshPeerTip_PeerObservationSuppressesAndIsAdopted(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, freshness, store)
	gen := m.registerGenForTest(url)

	var fedTip atomic.Int64
	m.onTipObservation = func(block int64) { fedTip.Store(block) }

	now := time.Now()
	store.set(5000, "other-pod/abcd", 100*time.Millisecond)

	block, ok := m.freshPeerTip(url, gen, now)
	require.True(t, ok, "a fresh observation from another pod suppresses the poll")
	require.Equal(t, int64(5000), block)

	o, exists := m.GetObservation(url)
	require.True(t, exists)
	require.Equal(t, int64(5000), o.LatestBlock)
	require.Equal(t, ObservationSourcePeer, o.Source, "adopted under SourcePeer")
	require.WithinDuration(t, now.Add(-100*time.Millisecond), o.ObservedAt, time.Millisecond, "ObservedAt is now minus the store-measured age")
	require.True(t, o.LastSuccessfulPoll.IsZero(), "a peer observation never touches poll-health")
	require.Equal(t, int64(5000), fedTip.Load(), "an adopted peer block feeds the per-chain tip")
}

func TestFreshPeerTip_NeverBorrowsOwnObservation(t *testing.T) {
	const url = "http://ep:8545"
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
	gen := m.registerGenForTest(url)

	store.set(5000, m.podID, 10*time.Millisecond)
	_, ok := m.freshPeerTip(url, gen, time.Now())
	require.False(t, ok, "a pod's own published poll must never suppress its next poll")
	_, exists := m.GetObservation(url)
	require.False(t, exists, "nothing adopted")
}

func TestFreshPeerTip_FreshnessBoundary(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, freshness, store)
	gen := m.registerGenForTest(url)

	store.set(5000, "other-pod/abcd", freshness)
	_, ok := m.freshPeerTip(url, gen, time.Now())
	require.True(t, ok, "an observation exactly at the window still suppresses")

	store.set(5001, "other-pod/abcd", freshness+time.Millisecond)
	_, ok = m.freshPeerTip(url, gen, time.Now())
	require.False(t, ok, "an observation past the window falls through to a real poll")
}

func TestFreshPeerTip_MissErrorOrUnwiredMeansPoll(t *testing.T) {
	const url = "http://ep:8545"
	now := time.Now()

	t.Run("miss", func(t *testing.T) {
		store := &fakePeerStore{}
		m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
		gen := m.registerGenForTest(url)
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok)
		require.Equal(t, int32(1), store.fetches.Load())
	})

	t.Run("fetch error", func(t *testing.T) {
		store := &fakePeerStore{fetchErr: errors.New("cache unavailable")}
		m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
		gen := m.registerGenForTest(url)
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok, "a store error must degrade to a local poll, never a skip")
	})

	t.Run("no store wired", func(t *testing.T) {
		m := newPeerGatedMonitor(t, 400*time.Millisecond, nil)
		gen := m.registerGenForTest(url)
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok)
	})

	t.Run("stale generation is not adopted", func(t *testing.T) {
		store := &fakePeerStore{}
		m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
		gen := m.registerGenForTest(url)
		store.set(5000, "other-pod/abcd", 10*time.Millisecond)
		_, ok := m.freshPeerTip(url, gen+1, now)
		require.True(t, ok, "the cycle is still redundant")
		_, exists := m.GetObservation(url)
		require.False(t, exists, "a replaced tracker's generation writes nothing")
	})
}

func TestRecordPollObservation_PublishesSuccessOnly(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, freshness, store)
	gen := m.registerGenForTest(url)

	m.recordPollObservation(url, gen, 0, 0, errors.New("boom"), time.Now())
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int32(0), store.publishes.Load(), "a failed poll publishes nothing")

	m.recordPollObservation(url, gen, 7000, 3*time.Millisecond, nil, time.Now())
	require.Eventually(t, func() bool { return store.publishes.Load() == 1 }, time.Second, 5*time.Millisecond)

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, "ETH1", store.lastPublish.chainID)
	require.Equal(t, spectypes.APIInterfaceJsonRPC, store.lastPublish.apiInterface)
	require.Equal(t, EndpointID(url), store.lastPublish.endpointID, "published under the digest, never the URL")
	require.Equal(t, m.podID, store.lastPublish.podID)
	require.Equal(t, int64(7000), store.lastPublish.block)
	require.Equal(t, freshness*peerObservationTTLMultiplier, store.lastPublish.ttl)
}

// TestRecordPollObservation_PublishesEvenWhenLocalStoreRejects: a poll that returns a block
// lower than a fresh peer-fed tip is a straggler for the LOCAL store but still a genuine
// "I saw this endpoint just now" for the fleet; the fleet store applies its own rule.
func TestRecordPollObservation_PublishesEvenWhenLocalStoreRejects(t *testing.T) {
	const url = "http://ep:8545"
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
	gen := m.registerGenForTest(url)
	now := time.Now()

	require.True(t, m.recordPeerObservation(url, gen, 9000, now))
	m.recordPollObservation(url, gen, 8990, time.Millisecond, nil, now.Add(time.Millisecond))

	o, _ := m.GetObservation(url)
	require.Equal(t, int64(9000), o.LatestBlock, "the local tip keeps the fresher, higher peer block")
	require.Eventually(t, func() bool { return store.publishes.Load() == 1 }, time.Second, 5*time.Millisecond)
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, int64(8990), store.lastPublish.block)
}

// TestGate_RelayBeforePeer: a fresh relay tip answers the gate without touching the store,
// and the skip source reported to OnGateSkip names the half that fired.
func TestGate_RelayBeforePeer(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const url = "https://solana-ep:443/"
	store := &fakePeerStore{}
	var skipsMu sync.Mutex
	skips := map[string]int{}
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "SOLANA", spectypes.APIInterfaceJsonRPC),
		ChainID:          "SOLANA",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
		PeerObservations: store,
		OnGateSkip: func(_ string, source string) {
			skipsMu.Lock()
			defer skipsMu.Unlock()
			skips[source]++
		},
	})
	t.Cleanup(m.Stop)

	conn := &countingDirectRPCConnection{
		mockDirectRPCConnection: mockDirectRPCConnection{
			url: url,
			responses: map[string][]byte{
				svmLatestBlockRequest: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":250000000},"value":{"blockhash":"solhash"}}}`),
			},
		},
		matchSubstr: "getLatestBlockhash",
	}
	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)

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
	time.Sleep(600 * time.Millisecond)
	close(stop)
	time.Sleep(20 * time.Millisecond)

	require.Equal(t, int32(0), store.fetches.Load(), "a fresh relay tip never consults the fleet store")
	skipsMu.Lock()
	defer skipsMu.Unlock()
	require.Greater(t, skips["relay"], 0, "relay skips are reported")
	require.Equal(t, 0, skips["peer"])
}

// TestEndpointMonitor_PeerGate_SuppressesUpstreamPoll is the end-to-end proof on the same
// SOLANA path the relay gate uses: with a continuously fresh observation from ANOTHER pod in
// the store and no relay traffic at all, a real tracker built by GetOrCreateTracker must skip
// most of its getLatestBlockhash polls, still publish the polls it does make, and adopt the
// peer block under SourcePeer. Without the gate wired it would poll on every tick (>=10).
func TestEndpointMonitor_PeerGate_SuppressesUpstreamPoll(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const url = "https://solana-ep:443/"
	store := &fakePeerStore{}
	store.set(250000005, "other-pod/abcd", 10*time.Millisecond) // always fresh vs the 100ms window

	conn := &countingDirectRPCConnection{
		mockDirectRPCConnection: mockDirectRPCConnection{
			url: url,
			responses: map[string][]byte{
				svmLatestBlockRequest: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":250000000},"value":{"blockhash":"solhash"}}}`),
			},
		},
		matchSubstr: "getLatestBlockhash",
	}

	var peerSkips atomic.Int32
	// avgBlockTime 100ms => flat poll 50ms, gate freshness window 100ms.
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "SOLANA", spectypes.APIInterfaceJsonRPC),
		ChainID:          "SOLANA",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
		PeerObservations: store,
		OnGateSkip: func(_ string, source string) {
			if source == "peer" {
				peerSkips.Add(1)
			}
		},
	})
	t.Cleanup(m.Stop)

	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	// ~600ms / 50ms ≈ 12 ticks. The gate forces ~1 real poll per 5 ticks plus init polls.
	time.Sleep(600 * time.Millisecond)

	polls := conn.matchedSends.Load()
	require.LessOrEqual(t, polls, int32(6),
		"a peer-gated SOLANA tracker must suppress most getLatestBlockhash polls; got %d", polls)
	require.GreaterOrEqual(t, polls, int32(1),
		"the skip budget must still force a local poll; got %d", polls)
	require.Greater(t, peerSkips.Load(), int32(0), "peer skips are reported to OnGateSkip")
	require.GreaterOrEqual(t, store.publishes.Load(), int32(1), "every successful local poll is published")

	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(250000005), o.LatestBlock, "the higher peer block is the endpoint's tip")
	require.Equal(t, endpointtip.SourcePeer, o.Source)
}
