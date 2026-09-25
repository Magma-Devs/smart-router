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

	t.Run("a dial that used up its own budget is an outage", func(t *testing.T) {
		// A dial budget nothing can beat stands in for the black hole that
		// swallows the handshake: the attempt was answered by nobody, and no
		// caller cut it short.
		tracker := &endpointTracker{}
		dial := baseDialer(nil, time.Nanosecond, tracker)
		before := time.Now()
		conn, err := dial(context.Background(), "tcp", dead)
		require.Nil(t, conn)
		require.Error(t, err)
		require.Error(t, tracker.faultSince(before),
			"the dial spent its whole DialTimeout without an answer: unreachable, not slow")
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
