package redisstore

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// refusedAddr is a port nothing listens on: a dial to it fails on the
// endpoint's terms, at once, with the caller's budget untouched.
func refusedAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

// MAG-3653: which failed dials prove an endpoint cannot be reached, and which
// prove nothing. The second half is why the fix cannot simply call every failed
// dial an outage: go-redis abandons a queued dial whose caller has gone, and the
// read budget is sized for a same-zone backend, so a healthy cache a network away
// fails a cold dial too. Reporting THAT as an outage would send an operator
// hunting a cache that is up — the same wrong turn as the bug, reversed.
func TestBaseDialerRecordsOnlyWhatProvesAnEndpointIsGone(t *testing.T) {
	dead := refusedAddr(t)

	t.Run("refused with budget to spare is an outage", func(t *testing.T) {
		tracker := &endpointTracker{}
		dial := baseDialer(nil, DefaultDialTimeout, tracker)
		before := time.Now()
		conn, err := dial(context.Background(), "tcp", dead)
		require.Nil(t, conn)
		require.Error(t, err)
		require.ErrorIs(t, tracker.faultSince(before), err,
			"a refused connection is the endpoint answering that it is not there")
	})

	t.Run("a dial the caller cut short proves nothing", func(t *testing.T) {
		tracker := &endpointTracker{}
		dial := baseDialer(nil, time.Hour, tracker)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		before := time.Now()
		conn, err := dial(ctx, "tcp", dead)
		require.Nil(t, conn)
		require.Error(t, err, "a cancelled context fails the dial")
		require.NoError(t, tracker.faultSince(before),
			"the attempt never ran to a verdict — a distant healthy cache fails this way too")
	})

	t.Run("a successful dial records nothing", func(t *testing.T) {
		live, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer live.Close()

		tracker := &endpointTracker{}
		dial := baseDialer(nil, DefaultDialTimeout, tracker)
		before := time.Now()
		conn, err := dial(context.Background(), "tcp", live.Addr().String())
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer conn.Close()
		require.NoError(t, tracker.faultSince(before), "the endpoint answered its handshake: it is there")
	})

	t.Run("a dialer without a tracker still dials", func(t *testing.T) {
		// tls_test builds one, and so would any future caller outside a store.
		dial := baseDialer(nil, DefaultDialTimeout, nil)
		conn, err := dial(context.Background(), "tcp", dead)
		require.Nil(t, conn)
		require.Error(t, err, "recording is a no-op with no tracker, not a panic")
	})
}

// The window is what keeps a stale verdict out. A pool warm for hours has not
// dialled, so the endpoint's last fault may be an outage long since recovered —
// and attributing that to the failure in hand would label every later saturation
// timeout an outage.
func TestOpWindowAttributesOnlyFaultsSeenWhileItRan(t *testing.T) {
	tracker := &endpointTracker{}
	store := &Store{readEndpoint: tracker, writeEndpoint: tracker}
	opErr := errors.Join(errors.New("lookup failed"), context.DeadlineExceeded)
	refused := errors.New("dial tcp: connect: connection refused")

	t.Run("a fault before the window is not this operation's", func(t *testing.T) {
		tracker.noteFault(refused)
		time.Sleep(time.Millisecond)
		window := store.BeginRead()
		require.False(t, window.unreachable())
		require.Equal(t, opErr, window.Annotate(opErr), "an unmarked failure is returned untouched")
	})

	t.Run("a fault during the window is", func(t *testing.T) {
		window := store.BeginRead()
		time.Sleep(time.Millisecond)
		tracker.noteFault(refused)

		annotated := window.Annotate(opErr)
		require.ErrorIs(t, annotated, ErrEndpointUnreachable, "the marker rides out on the operation's error")
		require.ErrorIs(t, annotated, refused, "and carries the reason with it")
		require.ErrorIs(t, annotated, context.DeadlineExceeded,
			"and only rides along: what the caller's deadline did is still true")
	})

	t.Run("a successful operation is never marked", func(t *testing.T) {
		window := store.BeginRead()
		tracker.noteFault(refused)
		require.NoError(t, window.Annotate(nil))
	})

	t.Run("both sides of a split store are windowed", func(t *testing.T) {
		readTracker, writeTracker := &endpointTracker{}, &endpointTracker{}
		split := &Store{readEndpoint: readTracker, writeEndpoint: writeTracker}
		read, write := split.BeginRead(), split.BeginWrite()
		writeTracker.noteFault(refused)

		require.ErrorIs(t, write.Annotate(opErr), ErrEndpointUnreachable)
		require.NotErrorIs(t, read.Annotate(opErr), ErrEndpointUnreachable,
			"a write endpoint's outage must not be reported against a healthy read endpoint")
	})

	t.Run("a nil store and a nil window are safe", func(t *testing.T) {
		var absent *Store
		require.Equal(t, opErr, absent.BeginRead().Annotate(opErr))
		require.Equal(t, opErr, OpWindow{}.Annotate(opErr))
		require.False(t, OpWindow{}.unreachable())
	})
}

// The discriminator, one quadrant at a time. Testing it through the dialer could
// not do this: with a live context the cheap `ctxErr == nil` disjunct decided
// every case reachable from a test, so the black-hole term could be deleted with
// the whole suite still green — a term that cannot fail is not a term.
func TestDialFailureProvesEndpointGone(t *testing.T) {
	for _, tc := range []struct {
		name          string
		elapsed       time.Duration
		dialTimeout   time.Duration
		ctxErr        error
		provesItsGone bool
	}{
		{
			name:    "answered no, with budget to spare — refused, no route, no such name",
			elapsed: time.Millisecond, dialTimeout: 500 * time.Millisecond, ctxErr: nil,
			provesItsGone: true,
		},
		{
			name:    "spent its own whole budget and heard nothing — a black hole",
			elapsed: 500 * time.Millisecond, dialTimeout: 500 * time.Millisecond, ctxErr: context.DeadlineExceeded,
			provesItsGone: true,
		},
		{
			name:    "the caller's deadline cut it short — a distant healthy cache fails this way",
			elapsed: 50 * time.Millisecond, dialTimeout: 500 * time.Millisecond, ctxErr: context.DeadlineExceeded,
			provesItsGone: false,
		},
		{
			name:    "the caller went away — go-redis abandons a queued dial",
			elapsed: time.Millisecond, dialTimeout: 500 * time.Millisecond, ctxErr: context.Canceled,
			provesItsGone: false,
		},
		{
			name:    "no dial budget configured: only the context can answer",
			elapsed: time.Hour, dialTimeout: 0, ctxErr: context.Canceled,
			provesItsGone: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.provesItsGone,
				dialFailureProvesEndpointGone(tc.elapsed, tc.dialTimeout, tc.ctxErr))
		})
	}
}

// A dial that succeeds is the endpoint answering, and must retire the refusal
// before it. Without this a refusal in the first millisecond of a write's
// 5-second window labelled every later failure in that window an outage —
// including a genuine saturation timeout against an endpoint by then proven up.
func TestASuccessfulDialRetiresTheFault(t *testing.T) {
	live, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer live.Close()
	dead := refusedAddr(t)

	tracker := &endpointTracker{}
	dial := baseDialer(nil, DefaultDialTimeout, tracker)
	before := time.Now()

	_, err = dial(context.Background(), "tcp", dead)
	require.Error(t, err)
	require.Error(t, tracker.faultSince(before), "the refusal is recorded")

	conn, err := dial(context.Background(), "tcp", live.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, tracker.faultSince(before),
		"the endpoint answered a handshake: the earlier refusal must not still be standing")
}

// Reachability is recorded for standalone only. Under sentinel the same dialer
// carries the control-plane connections — to quorum members, including ones
// discovered at runtime that no configured list names — and a down sentinel is a
// non-paging condition. Recording those would report a healthy master
// unreachable for as long as one sentinel stayed down, and relabel every
// saturation timeout with it: the bug this fixes, inverted.
//
// The mirror of TestSentinelTrackerRecordsOnlyMarkedDataDials, on the
// reachability channel instead of the address channel.
func TestSentinelControlPlaneDialsNeverReportTheDataEndpointGone(t *testing.T) {
	downSentinel := refusedAddr(t)
	tracker := &endpointTracker{}
	dial := trackingDialerMarkedOnly(nil, time.Second, tracker)

	before := time.Now()
	conn, err := dial(context.Background(), "tcp", downSentinel) // unmarked: control-plane
	require.Nil(t, conn)
	require.Error(t, err, "the dial itself still fails, and still reaches the caller")
	require.Empty(t, tracker.current(), "the address channel filters it, as it always did")
	require.NoError(t, tracker.faultSince(before),
		"and so must the reachability channel: this says nothing about the master")
}

// Under cluster one tracker stands behind every shard and a fault carries no
// address, so a fault cannot be attributed to the node an operation used. One
// down node would otherwise report the whole cache unreachable for as long as it
// stayed down.
func TestClusterDialsNeverReportTheCacheGone(t *testing.T) {
	downNode := refusedAddr(t)
	tracker := &endpointTracker{}
	dial := trackingDialer(nil, time.Second, tracker, false)

	before := time.Now()
	_, err := dial(context.Background(), "tcp", downNode)
	require.Error(t, err)
	require.NoError(t, tracker.faultSince(before),
		"one shard's refusal is not the cache being gone")
}

// The topology decision is wired, not merely available: the options each
// constructor builds must carry the dialer that matches its topology.
func TestOnlyStandaloneOptionsRecordReachability(t *testing.T) {
	dead := refusedAddr(t)
	for _, tc := range []struct {
		topology string
		dialer   func(*endpointTracker) func(context.Context, string, string) (net.Conn, error)
		records  bool
	}{
		{"standalone", func(tr *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
			return Config{Addresses: []string{dead}}.standaloneOptions([]string{dead}, nil, nil, tr).Dialer
		}, true},
		{"cluster", func(tr *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
			return Config{Addresses: []string{dead}}.clusterOptions([]string{dead}, nil, nil, tr).Dialer
		}, false},
	} {
		t.Run(tc.topology, func(t *testing.T) {
			tracker := &endpointTracker{}
			before := time.Now()
			_, err := tc.dialer(tracker)(context.Background(), "tcp", dead)
			require.Error(t, err)
			if tc.records {
				require.Error(t, tracker.faultSince(before), "standalone has one endpoint: the fault is about it")
			} else {
				require.NoError(t, tracker.faultSince(before), "no dial here can be attributed to one endpoint")
			}
		})
	}
}
