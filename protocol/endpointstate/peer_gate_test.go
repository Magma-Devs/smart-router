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

// registerHealthyGenForTest registers a generation and records one successful poll — the state a
// live tracker is in by the time the gate runs. freshPeerTip borrows only on proven local
// reachability, so a test that wants a borrow must say the pod is healthy first. Seed a low block
// and an `at` older than the peer observation, so the peer's block still wins the monotonic guard.
func (m *EndpointMonitor) registerHealthyGenForTest(url string, block int64, at time.Time) uint64 {
	gen := m.registerGenForTest(url)
	m.recordPollObservation(url, gen, block, time.Millisecond, nil, at)
	return gen
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

	now := time.Now()
	gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
	before, _ := m.GetObservation(url)
	require.False(t, before.LastSuccessfulPoll.IsZero(), "the seed poll is the pod's own reachability evidence")

	var fedTip atomic.Int64
	m.onTipObservation = func(block int64) { fedTip.Store(block) }

	store.set(5000, "other-pod/abcd", 100*time.Millisecond)

	block, ok := m.freshPeerTip(url, gen, now)
	require.True(t, ok, "a fresh observation from another pod suppresses the poll")
	require.Equal(t, int64(5000), block)

	o, exists := m.GetObservation(url)
	require.True(t, exists)
	require.Equal(t, int64(5000), o.LatestBlock)
	require.Equal(t, ObservationSourcePeer, o.Source, "adopted under SourcePeer")
	require.WithinDuration(t, now.Add(-100*time.Millisecond), o.ObservedAt, time.Millisecond, "ObservedAt is now minus the store-measured age")
	require.Equal(t, before.LastSuccessfulPoll, o.LastSuccessfulPoll, "a peer observation never touches poll-health")
	require.Zero(t, o.ConsecutivePollFailures)
	require.Equal(t, int64(5000), fedTip.Load(), "an adopted peer block feeds the per-chain tip")
}

func TestFreshPeerTip_NeverBorrowsOwnObservation(t *testing.T) {
	const url = "http://ep:8545"
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
	now := time.Now()
	gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))

	store.set(5000, m.podID, 10*time.Millisecond)
	_, ok := m.freshPeerTip(url, gen, now)
	require.False(t, ok, "a pod's own published poll must never suppress its next poll")
	require.Equal(t, int32(1), store.fetches.Load(), "the pod is locally healthy, so it got as far as the pod-id check")
	o, _ := m.GetObservation(url)
	require.Equal(t, int64(1), o.LatestBlock, "nothing adopted — the tip is still the pod's own poll")
	require.Equal(t, ObservationSourcePoll, o.Source)
}

func TestFreshPeerTip_FreshnessBoundary(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, freshness, store)
	gen := m.registerHealthyGenForTest(url, 1, time.Now().Add(-time.Second))

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
		gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok)
		require.Equal(t, int32(1), store.fetches.Load())
	})

	t.Run("fetch error", func(t *testing.T) {
		store := &fakePeerStore{fetchErr: errors.New("cache unavailable")}
		m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
		gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok, "a store error must degrade to a local poll, never a skip")
		require.Equal(t, int32(1), store.fetches.Load())
	})

	t.Run("no store wired", func(t *testing.T) {
		m := newPeerGatedMonitor(t, 400*time.Millisecond, nil)
		gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok)
	})

	t.Run("local polls failing", func(t *testing.T) {
		store := &fakePeerStore{}
		m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
		gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
		store.set(5000, "other-pod/abcd", 10*time.Millisecond)
		m.recordPollObservation(url, gen, 0, 0, errors.New("connection refused"), now.Add(-500*time.Millisecond))
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok, "a pod that cannot reach the endpoint must not borrow evidence that it can")
		require.Equal(t, int32(0), store.fetches.Load(), "the health guard runs BEFORE the cache round-trip")
	})

	t.Run("never polled", func(t *testing.T) {
		store := &fakePeerStore{}
		m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
		gen := m.registerGenForTest(url) // generation only: no observation record at all
		store.set(5000, "other-pod/abcd", 10*time.Millisecond)
		_, ok := m.freshPeerTip(url, gen, now)
		require.False(t, ok, "no local evidence yet → no borrow")
		require.Equal(t, int32(0), store.fetches.Load())
	})

	t.Run("stale generation is not adopted", func(t *testing.T) {
		store := &fakePeerStore{}
		m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
		gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
		store.set(5000, "other-pod/abcd", 10*time.Millisecond)
		_, ok := m.freshPeerTip(url, gen+1, now)
		require.True(t, ok, "the cycle is still redundant")
		o, _ := m.GetObservation(url)
		require.Equal(t, int64(1), o.LatestBlock, "a replaced tracker's generation writes nothing")
		require.Equal(t, ObservationSourcePoll, o.Source)
	})
}

// A served relay is first-hand contact with the node, so it publishes on the same terms as a poll.
// Without this the pods that know an endpoint best publish least: relay traffic is exactly what
// suppresses the poll that would have published.
func TestRecordRelayObservation_PublishesLikeAPoll(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, freshness, store)
	gen := m.registerGenForTest(url)

	require.True(t, m.RecordRelayObservation(url, gen, 7000, time.Now()))
	require.Eventually(t, func() bool { return store.publishes.Load() == 1 }, time.Second, 5*time.Millisecond)

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, EndpointID(url), store.lastPublish.endpointID, "published under the digest, never the URL")
	require.Equal(t, m.podID, store.lastPublish.podID)
	require.Equal(t, int64(7000), store.lastPublish.block)
	require.Equal(t, freshness*peerObservationTTLMultiplier, store.lastPublish.ttl)
}

// The invariant that keeps a note from circulating without anyone contacting the node: a borrowed
// value is second-hand, so it must never be republished. Were it, each pod's republish would look
// like someone else's work to every other pod and refresh the fleet note's stamp, so a stale block
// would read as permanently fresh across the whole fleet.
func TestRecordPeerObservation_NeverPublishes(t *testing.T) {
	const url = "http://ep:8545"
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
	now := time.Now()
	gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
	require.Eventually(t, func() bool { return store.publishes.Load() == 1 }, time.Second, 5*time.Millisecond)

	store.set(5000, "other-pod/abcd", 10*time.Millisecond)
	_, ok := m.freshPeerTip(url, gen, now)
	require.True(t, ok, "the borrow happened")
	o, _ := m.GetObservation(url)
	require.Equal(t, ObservationSourcePeer, o.Source)

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), store.publishes.Load(), "adopting a peer value must publish nothing")
}

// Both first-hand paths share ONE per-endpoint throttle, so a busy endpoint cannot publish once per
// served relay. The window is half the freshness horizon, which keeps the note continuously
// borrowable with margin.
func TestShouldPublish_ThrottledAcrossBothLocalPaths(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, freshness, store)
	gen := m.registerGenForTest(url)
	now := time.Now()

	m.recordPollObservation(url, gen, 7000, time.Millisecond, nil, now)
	require.Eventually(t, func() bool { return store.publishes.Load() == 1 }, time.Second, 5*time.Millisecond)

	// A relay 1ms later is throttled by the poll's publish — the throttle is per endpoint, not
	// per path.
	m.RecordRelayObservation(url, gen, 7001, now.Add(time.Millisecond))
	// ...and so is a burst of further relays inside the window.
	for i := 2; i < 20; i++ {
		m.RecordRelayObservation(url, gen, 7000+int64(i), now.Add(time.Duration(i)*time.Millisecond))
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), store.publishes.Load(), "everything inside freshness/2 shares one publish")

	// Past the window, the next first-hand observation publishes again — from either path.
	m.RecordRelayObservation(url, gen, 7100, now.Add(freshness/2))
	require.Eventually(t, func() bool { return store.publishes.Load() == 2 }, time.Second, 5*time.Millisecond)
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, int64(7100), store.lastPublish.block)
}

// The gate's precondition as a cycle: a healthy pod borrows, one failed local poll withdraws the
// borrow (without spending a cache round-trip), and the next successful poll restores it — so a
// transient failure costs one extra poll, not a sustained cadence change.
func TestFreshPeerTip_LocalFailureWithdrawsAndRestoresTheBorrow(t *testing.T) {
	const url = "http://ep:8545"
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
	now := time.Now()
	gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))
	store.set(5000, "other-pod/abcd", 10*time.Millisecond)

	_, ok := m.freshPeerTip(url, gen, now)
	require.True(t, ok, "a healthy pod borrows")
	require.Equal(t, int32(1), store.fetches.Load())

	m.recordPollObservation(url, gen, 0, 0, errors.New("dial tcp: connect: connection refused"), now.Add(time.Millisecond))
	_, ok = m.freshPeerTip(url, gen, now.Add(2*time.Millisecond))
	require.False(t, ok, "one failed local poll withdraws the borrow")
	require.Equal(t, int32(1), store.fetches.Load(), "and skips the cache round-trip entirely")

	m.recordPollObservation(url, gen, 6000, time.Millisecond, nil, now.Add(3*time.Millisecond))
	_, ok = m.freshPeerTip(url, gen, now.Add(4*time.Millisecond))
	require.True(t, ok, "a successful local poll restores the borrow")
	require.Equal(t, int32(2), store.fetches.Load())
}

// The F1 regression: probing.Verdict computes `alive` from LatestBlock + ObservedAt, and a failed
// poll moves neither. So while this pod cannot reach the endpoint nothing may refresh ObservedAt,
// or the staleness window never expires and the endpoint reads ALIVE forever on borrowed evidence.
func TestFreshPeerTip_FailingPodStopsRefreshingObservedAt(t *testing.T) {
	const url = "http://ep:8545"
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
	now := time.Now()
	gen := m.registerHealthyGenForTest(url, 1, now.Add(-time.Second))

	// Healthy pod, fresh peer note: the borrow is adopted and stamps the endpoint's tip.
	store.set(5000, "other-pod/abcd", 10*time.Millisecond)
	_, ok := m.freshPeerTip(url, gen, now)
	require.True(t, ok)
	adopted, _ := m.GetObservation(url)
	require.Equal(t, int64(5000), adopted.LatestBlock)

	// The pod's path breaks: its own polls fail while peers keep publishing newer observations.
	for i := 1; i <= 5; i++ {
		at := now.Add(time.Duration(i) * 10 * time.Millisecond)
		m.recordPollObservation(url, gen, 0, 0, errors.New("connection refused"), at)
		store.set(5000+int64(i), "other-pod/abcd", 10*time.Millisecond)
		_, ok := m.freshPeerTip(url, gen, at.Add(time.Millisecond))
		require.False(t, ok, "tick %d: a pod that cannot reach the endpoint must not borrow", i)
	}

	o, _ := m.GetObservation(url)
	require.True(t, adopted.ObservedAt.Equal(o.ObservedAt),
		"ObservedAt must be frozen at the last observation made while the pod was healthy: was %s, now %s", adopted.ObservedAt, o.ObservedAt)
	require.Equal(t, int64(5000), o.LatestBlock, "no peer block is adopted while local polls fail")
	require.Equal(t, 5, o.ConsecutivePollFailures)
	require.Equal(t, int32(1), store.fetches.Load(), "only the one healthy tick reached the cache")
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

// A first-hand observation BEHIND our own fresh tip — a straggler, routine when relays complete out
// of order — publishes nothing and burns no throttle window. The fleet store applies the same
// monotonic rule, so the write would be rejected there too; what it would really cost is the next
// publish's window, and losing one is enough to reopen the gap freshness/2 exists to prevent.
//
// This replaces a test asserting the opposite. Publishing regardless was right while a wasted
// publish cost one RPC and nothing else; adding the throttle repriced it.
func TestRecordPollObservation_StragglerDoesNotPublishOrBurnTheWindow(t *testing.T) {
	const url = "http://ep:8545"
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, 400*time.Millisecond, store)
	gen := m.registerGenForTest(url)
	now := time.Now()

	require.True(t, m.recordPeerObservation(url, gen, 9000, now))
	m.recordPollObservation(url, gen, 8990, time.Millisecond, nil, now.Add(time.Millisecond))

	o, _ := m.GetObservation(url)
	require.Equal(t, int64(9000), o.LatestBlock, "the local tip keeps the fresher, higher peer block")
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), store.publishes.Load(), "a rejected write publishes nothing")

	// And because it burned no window, the next ACCEPTED observation still publishes immediately.
	m.recordPollObservation(url, gen, 9001, time.Millisecond, nil, now.Add(2*time.Millisecond))
	require.Eventually(t, func() bool { return store.publishes.Load() == 1 }, time.Second, 5*time.Millisecond)
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, int64(9001), store.lastPublish.block)
}

// The gate is Set's RETURN, not "the block advanced": Set accepts an equal block unconditionally, so
// an endpoint parked on one block keeps publishing every window. Tying publishing to block movement
// instead would stop feeding the fleet exactly when a chain stalls — while it is still being checked.
func TestRecordPollObservation_UnchangedBlockStillPublishes(t *testing.T) {
	const url = "http://ep:8545"
	freshness := 400 * time.Millisecond
	store := &fakePeerStore{}
	m := newPeerGatedMonitor(t, freshness, store)
	gen := m.registerGenForTest(url)
	now := time.Now()

	m.recordPollObservation(url, gen, 7000, time.Millisecond, nil, now)
	require.Eventually(t, func() bool { return store.publishes.Load() == 1 }, time.Second, 5*time.Millisecond)

	m.recordPollObservation(url, gen, 7000, time.Millisecond, nil, now.Add(freshness/2))
	require.Eventually(t, func() bool { return store.publishes.Load() == 2 }, time.Second, 5*time.Millisecond,
		"the same block a window later still refreshes the fleet note")
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
