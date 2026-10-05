package endpointstate

// Tests for idle mode (idle.go): the monitor-side decision (IsIdle), the relay-path stamp and
// wake (NoteClientRelay), the per-cycle gate the trackers ask (idleGate, including the
// heartbeat), and the knob validation. The wake burst is driven through a REAL tracker (the
// poll_now_test harness) so the production PollNow path — not a stub — is what records the fresh tip.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// idleEvents records the gate-skip telemetry idle mode drives.
type idleEvents struct {
	mu    sync.Mutex
	skips []string
}

func (e *idleEvents) onSkip(_ string, source string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.skips = append(e.skips, source)
}

func (e *idleEvents) snapshot() (skips []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.skips...)
}

// newIdleMonitor builds a monitor with idle mode on and no trackers: enough for every decision
// that does not need a poll to actually run.
func newIdleMonitor(t *testing.T, idleAfter, idlePollInterval time.Duration, events *idleEvents) *EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
		IdleAfter:        idleAfter,
		IdlePollInterval: idlePollInterval,
	}
	if events != nil {
		cfg.OnGateSkip = events.onSkip
	}
	m := NewEndpointMonitor(ctx, cfg)
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

// forceIdle backdates the last-client-relay stamp so the chain reads idle at time.Now().
func (m *EndpointMonitor) forceIdleForTest(quietFor time.Duration) {
	m.lastClientRelayNanos.Store(time.Now().Add(-quietFor).UnixNano())
}

func TestResolveIdleConfig(t *testing.T) {
	const flat = 30 * time.Second
	cases := []struct {
		name                        string
		idleAfter, idlePollInterval time.Duration
		wantAfter, wantInterval     time.Duration
	}{
		{"off by default", 0, 0, 0, 0},
		{"interval without idle-after is ignored", 0, time.Minute, 0, 0},
		{"below the minimum reverts to off", MinIdleAfter - time.Second, 0, 0, 0},
		{"minimum is accepted", MinIdleAfter, 0, MinIdleAfter, 0},
		{"stop entirely while idle", 5 * time.Minute, 0, 5 * time.Minute, 0},
		{"heartbeat slower than the cadence is kept", 5 * time.Minute, 2 * time.Minute, 5 * time.Minute, 2 * time.Minute},
		{"heartbeat faster than the cadence is raised to it", 5 * time.Minute, time.Second, 5 * time.Minute, flat},
		{"negative heartbeat means stop", 5 * time.Minute, -time.Second, 5 * time.Minute, 0},
		{"negative idle-after is below the minimum: off", -time.Minute, time.Minute, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			after, interval := resolveIdleConfig(tc.idleAfter, tc.idlePollInterval, flat, "ETH1", spectypes.APIInterfaceJsonRPC)
			require.Equal(t, tc.wantAfter, after)
			require.Equal(t, tc.wantInterval, interval)
		})
	}
}

// TestIdleMode_OffByDefault: an unconfigured monitor is never idle, stamps nothing, and hands the
// relay path nothing to wait on — today's behaviour, byte for byte.
func TestIdleMode_OffByDefault(t *testing.T) {
	m := newIdleMonitor(t, 0, 0, nil)
	require.Zero(t, m.IdleAfter())
	require.False(t, m.IsIdle(time.Now().Add(24*time.Hour)))
	require.Nil(t, m.NoteClientRelay(time.Now().Add(24*time.Hour)))
	require.False(t, m.idleGate("http://ep:8545", time.Now().Add(24*time.Hour), time.Hour))
	require.Equal(t, IdleSnapshot{}, m.IdleSnapshot(time.Now()))
}

// TestIdleMode_StartsActive: a fresh process is active for one full idle window — it can never
// boot straight into paused polls — and reads idle only once the window has passed with no relay.
func TestIdleMode_StartsActive(t *testing.T) {
	m := newIdleMonitor(t, MinIdleAfter, 0, nil)
	now := time.Now()
	require.False(t, m.IsIdle(now))
	// Measure from the seed stamp itself (the construction time), not from a later clock read.
	seed := m.IdleSnapshot(now).LastClientRelay
	require.False(t, m.IsIdle(seed.Add(MinIdleAfter)), "exactly at the window is still active (idle is age > window)")
	require.True(t, m.IsIdle(seed.Add(MinIdleAfter+time.Nanosecond)))

	snap := m.IdleSnapshot(now.Add(MinIdleAfter + time.Second))
	require.True(t, snap.Enabled)
	require.True(t, snap.Idle)
	require.Equal(t, MinIdleAfter, snap.IdleAfter)
	require.Equal(t, snap.LastClientRelay.Add(MinIdleAfter), snap.IdleSince)
	require.False(t, snap.WakeInProgress)
}

// TestIdleMode_ClientRelayKeepsAwake: relays inside the window keep the chain active and never
// start a wake (nil), so the hot path pays one atomic swap and nothing else.
func TestIdleMode_ClientRelayKeepsAwake(t *testing.T) {
	m := newIdleMonitor(t, MinIdleAfter, 0, nil)
	now := time.Now()
	for i := 0; i < 5; i++ {
		at := now.Add(time.Duration(i) * (MinIdleAfter / 2))
		require.Nil(t, m.NoteClientRelay(at), "relay %d inside the window: nothing to wait for", i)
		require.False(t, m.IsIdle(at))
	}
	require.False(t, m.IsIdle(now.Add(2*MinIdleAfter+MinIdleAfter/2)), "the stamp advanced with the relays")
}

// TestIdleGate_HeartbeatAndTelemetry pins the per-cycle decision the trackers ask, the idle
// heartbeat, and the transition telemetry (one log line per direction, one skip metric
// per skipped cycle).
func TestIdleGate_HeartbeatAndTelemetry(t *testing.T) {
	const url = "http://ep:8545"

	t.Run("stop entirely: every idle cycle skips", func(t *testing.T) {
		events := &idleEvents{}
		m := newIdleMonitor(t, MinIdleAfter, 0, events)
		m.forceIdleForTest(2 * MinIdleAfter)
		now := time.Now()
		for i := 0; i < 3; i++ {
			require.True(t, m.idleGate(url, now, time.Duration(i)*time.Hour))
		}
		skips := events.snapshot()
		require.True(t, m.idleAnnounced.Load(), "the idle transition is announced on the first skip")
		require.Equal(t, []string{metrics.TrackerGateSkipSourceIdle, metrics.TrackerGateSkipSourceIdle, metrics.TrackerGateSkipSourceIdle}, skips)
	})

	t.Run("heartbeat: the cycle runs once the endpoint has gone a full interval without a poll", func(t *testing.T) {
		events := &idleEvents{}
		m := newIdleMonitor(t, MinIdleAfter, 2*time.Minute, events)
		require.Equal(t, 2*time.Minute, m.IdlePollInterval())
		m.forceIdleForTest(2 * MinIdleAfter)
		now := time.Now()
		require.True(t, m.idleGate(url, now, time.Minute), "one minute since the last real poll: skip")
		require.True(t, m.idleGate(url, now, 2*time.Minute-time.Nanosecond), "just under the interval: skip")
		require.False(t, m.idleGate(url, now, 2*time.Minute), "heartbeat due: this cycle polls")
		skips := events.snapshot()
		require.Len(t, skips, 2, "only skipped cycles count as skips")
	})

	t.Run("not idle: the gate never skips and announces nothing", func(t *testing.T) {
		events := &idleEvents{}
		m := newIdleMonitor(t, MinIdleAfter, 0, events)
		require.False(t, m.idleGate(url, time.Now(), time.Hour))
		require.False(t, m.idleAnnounced.Load(), "nothing announced")
		require.Empty(t, events.snapshot())
	})
}

// TestIdleMode_WakeWithNoTrackers: the relay that ends an idle period gets a channel that is
// closed as soon as the burst has nothing to poll, and the idle announcement is re-armed.
func TestIdleMode_WakeWithNoTrackers(t *testing.T) {
	events := &idleEvents{}
	m := newIdleMonitor(t, MinIdleAfter, 0, events)
	m.forceIdleForTest(2 * MinIdleAfter)
	require.True(t, m.idleGate("http://ep:8545", time.Now(), time.Hour), "announce idle first")

	wait := m.NoteClientRelay(time.Now())
	require.NotNil(t, wait, "the relay that ends the idle period is handed the burst's channel")
	select {
	case <-wait:
	case <-time.After(5 * time.Second):
		t.Fatal("a wake with no trackers must complete immediately")
	}
	require.False(t, m.IsIdle(time.Now()), "the waking relay's stamp makes the chain active")
	require.Eventually(t, func() bool { return !m.IdleSnapshot(time.Now()).WakeInProgress }, 5*time.Second, 10*time.Millisecond)
	require.False(t, m.idleAnnounced.Load(), "the wake re-arms the next idle period's announcement")
}

// TestIdleMode_WakeBurstPollsEveryEndpoint is the end-to-end proof through a REAL tracker: the
// chain goes quiet, the head moves while nobody is polling, and the relay that ends the idle
// period gets a channel that closes only once the burst has polled the endpoint and recorded the
// NEW head — via the production PollNow path, with the tracker's own timer still ~30 s away.
func TestIdleMode_WakeBurstPollsEveryEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureRandSeeded()

	const url = "http://eth-idle-wake:8545"
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
		IdleAfter:        MinIdleAfter,
	})
	require.NotNil(t, m)
	defer m.Stop()

	conn := &pollNowConn{url: url}
	conn.block.Store(1000)
	_, err := m.GetOrCreateTracker("", &lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		state, _, exists := m.GetTrackerState(url)
		return exists && state == EndpointChainTrackerPolling
	}, 10*time.Second, 20*time.Millisecond)

	before, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(1000), before.LatestBlock, "the init poll seeds the record")

	// Quiet period: the head moves, nobody polls (the gate would skip, and the timer is far away).
	m.forceIdleForTest(2 * MinIdleAfter)
	require.True(t, m.IsIdle(time.Now()))
	require.True(t, m.idleGate(url, time.Now(), time.Hour), "the tracker's next tick would be skipped")
	conn.block.Store(1042)

	// The first client relay after the quiet period wakes the chain.
	wait := m.NoteClientRelay(time.Now())
	require.NotNil(t, wait)
	select {
	case <-wait:
	case <-time.After(15 * time.Second):
		t.Fatal("the wake burst did not report a fresh tip in time")
	}
	after, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(1042), after.LatestBlock, "the burst polled the endpoint and recorded the new head")
	require.Equal(t, ObservationSourcePoll, after.Source)
	require.True(t, after.LastSuccessfulPoll.After(before.LastSuccessfulPoll))

	require.Equal(t, int64(1042), endpointtip.Default().Block(m.tipKey(url)), "the consistency reader sees the fresh tip right away")

	require.False(t, m.IsIdle(time.Now()))
	require.Eventually(t, func() bool { return !m.IdleSnapshot(time.Now()).WakeInProgress }, 10*time.Second, 10*time.Millisecond)
}

// TestIdleMode_ConcurrentWakersShareOneBurst: relays racing to end the same idle period start
// exactly ONE burst; the others join its channel. Exact, not probabilistic — the stamp swap
// hands the old value to precisely one caller.
func TestIdleMode_ConcurrentWakersShareOneBurst(t *testing.T) {
	events := &idleEvents{}
	m := newIdleMonitor(t, MinIdleAfter, 0, events)
	m.forceIdleForTest(2 * MinIdleAfter)
	require.True(t, m.idleGate("http://ep:8545", time.Now(), time.Hour))

	const relays = 16
	var wakers atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	now := time.Now()
	for i := 0; i < relays; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Every caller sees the same instant so no relay lands "inside the window" merely
			// because a goroutine was scheduled later.
			if ch := m.NoteClientRelay(now); ch != nil {
				<-ch
				wakers.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	require.GreaterOrEqual(t, wakers.Load(), int32(1), "the waker itself always gets a channel")
	require.Equal(t, uint64(1), m.wakeBursts.Load(), "one idle period, one wake — never a second burst")
}

// TestIdleMode_WakeChannelStaysOpenWhileTrackerIsStarting: a tracker still retrying its init has
// no poll goroutine, so the burst cannot get a fresh tip from it; the channel closes when the
// burst gives up on delivery (pollNowUnstartedGrace), not before — the relay path's own cap is
// what keeps the request from waiting that long.
func TestIdleMode_WakeChannelStaysOpenWhileTrackerIsStarting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureRandSeeded()

	const url = "http://eth-idle-starting:8545"
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
		IdleAfter:        MinIdleAfter,
	})
	defer m.Stop()

	conn := &pollNowConn{url: url}
	conn.fail.Store(true) // the init fetch fails; the tracker sits in starting/retrying_start
	_, err := m.GetOrCreateTracker("", &lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	m.forceIdleForTest(2 * MinIdleAfter)
	started := time.Now()
	wait := m.NoteClientRelay(started)
	require.NotNil(t, wait)
	select {
	case <-wait:
	case <-time.After(15 * time.Second):
		t.Fatal("the burst must give up on an unstarted tracker")
	}
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, pollNowUnstartedGrace, "the channel stays open until delivery is given up")
	require.Less(t, elapsed, pollNowUnstartedGrace+5*time.Second)
}

// TestIdleMode_IdleOrWakingHoldsUntilTheBurstEnds: the relay that ends an idle period clears
// IsIdle at once, but IdleOrWaking stays true until the wake burst finishes, so a probe cycle that
// lands mid-burst does not judge the endpoints not yet re-polled (QoS feed, head-stall).
func TestIdleMode_IdleOrWakingHoldsUntilTheBurstEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureRandSeeded()

	const url = "http://eth-idle-feed:8545"
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
		IdleAfter:        MinIdleAfter,
	})
	defer m.Stop()

	require.False(t, m.IdleOrWaking(time.Now()), "an active chain is neither idle nor waking")

	conn := &pollNowConn{url: url}
	conn.fail.Store(true) // an unstarted tracker holds the burst open for pollNowUnstartedGrace
	_, err := m.GetOrCreateTracker("", &lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	m.forceIdleForTest(2 * MinIdleAfter)
	require.True(t, m.IdleOrWaking(time.Now()), "an idle chain")

	wait := m.NoteClientRelay(time.Now())
	require.NotNil(t, wait)
	require.False(t, m.IsIdle(time.Now()), "the relay ends the idle period at once")
	require.True(t, m.IdleOrWaking(time.Now()), "but it is waking while the burst polls")

	require.Eventually(t, func() bool { return !m.IdleOrWaking(time.Now()) },
		pollNowUnstartedGrace+10*time.Second, 20*time.Millisecond, "it ends with the burst")
}
