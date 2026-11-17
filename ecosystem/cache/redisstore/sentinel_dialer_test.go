package redisstore

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Only a Sentinel connection is bounded by the answer timeout: a data
// connection keeps the deadlines go-redis sets on it.
func TestSentinelDialerWrapsOnlySentinelConnections(t *testing.T) {
	node := listenLocal(t)
	dial := sentinelDialer(nil, time.Second, &endpointTracker{})

	control, err := dial(context.Background(), "tcp", node.Addr().String())
	require.NoError(t, err)
	defer control.Close()
	wrapped, ok := control.(*sentinelSocketConn)
	require.True(t, ok, "an unmarked dial is a Sentinel dial")
	require.Equal(t, 400*time.Millisecond, wrapped.answerTimeout, "two fifths of the dial budget")

	data, err := markDataDialsHook{}.DialHook(dial)(context.Background(), "tcp", node.Addr().String())
	require.NoError(t, err)
	defer data.Close()
	_, ok = data.(*sentinelSocketConn)
	require.False(t, ok, "a marked dial is a data dial")
}

func TestSentinelConnClampsDeadlines(t *testing.T) {
	const answerTimeout = 100 * time.Millisecond
	readAfter := func(t *testing.T, deadline func() time.Time) (time.Duration, error) {
		t.Helper()
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
		conn := &sentinelConn{Conn: client, answerTimeout: answerTimeout}
		start := time.Now()
		require.NoError(t, conn.SetReadDeadline(deadline()))
		_, err := conn.Read(make([]byte, 1))
		return time.Since(start), err
	}

	t.Run("a later deadline is brought forward", func(t *testing.T) {
		elapsed, err := readAfter(t, func() time.Time { return time.Now().Add(time.Minute) })
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		require.GreaterOrEqual(t, elapsed, answerTimeout)
		require.Less(t, elapsed, 10*answerTimeout)
	})

	t.Run("an earlier deadline is kept", func(t *testing.T) {
		elapsed, err := readAfter(t, func() time.Time { return time.Now().Add(answerTimeout / 10) })
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		require.Less(t, elapsed, answerTimeout)
	})

	t.Run("no deadline stays no deadline", func(t *testing.T) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
		conn := &sentinelConn{Conn: client, answerTimeout: answerTimeout}
		require.NoError(t, conn.SetReadDeadline(time.Time{}))
		go func() {
			time.Sleep(3 * answerTimeout)
			_, _ = server.Write([]byte("x"))
		}()
		_, err := conn.Read(make([]byte, 1))
		require.NoError(t, err, "the subscription's blocking receive outlives the answer timeout")
	})
}

// go-redis retries a failed dial inside one question to a Sentinel, and tells
// a dial by its *net.OpError. A Sentinel dial that timed out is returned
// without that identity; any other dial error is returned as it is.
func TestSentinelDialTimeoutLosesItsDialIdentity(t *testing.T) {
	node := listenLocal(t)
	var opErr *net.OpError

	_, err := dialSentinel(context.Background(), baseDialer(nil, time.Nanosecond, nil), time.Nanosecond, "tcp", node.Addr().String())
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "still a timeout to anything that classifies it")
	require.False(t, errors.As(err, &opErr), "no longer a dial error to go-redis: %v", err)

	_, err = dialSentinel(context.Background(), baseDialer(nil, time.Second, nil), time.Second, "tcp", refusedAddr(t))
	require.ErrorAs(t, err, &opErr, "a refused dial keeps its identity")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = dialSentinel(cancelled, baseDialer(nil, time.Second, nil), time.Second, "tcp", node.Addr().String())
	require.ErrorAs(t, err, &opErr, "a dial the caller gave up on keeps its identity")
}

// The answer timeout bounds the TLS handshake too: a Sentinel that accepts the
// connection and never speaks is given up on well inside the dial budget.
func TestSentinelDialBoundsTheTLSHandshake(t *testing.T) {
	silent := listenLocal(t)
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, conn) }()
		}
	}()
	const dialTimeout = time.Second
	dial := sentinelDialer(&tls.Config{MinVersion: tls.VersionTLS12}, dialTimeout, &endpointTracker{})

	start := time.Now()
	_, err := dial(context.Background(), "tcp", silent.Addr().String())
	elapsed := time.Since(start)

	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout())
	require.GreaterOrEqual(t, elapsed, sentinelAnswerTimeout(dialTimeout))
	require.Less(t, elapsed, dialTimeout)
}

// go-redis checks an idle pooled connection before it reuses it, by peeking at
// its socket, so a connection the Sentinel closed is dropped instead of being
// sent the next question. It reaches the socket only through the connection
// the dialer returned, so the Sentinel wrapper has to pass it on.
func TestSentinelConnKeepsThePoolHealthCheck(t *testing.T) {
	sentinel := miniredis.RunT(t)
	client := redis.NewSentinelClient(&redis.Options{
		Addr:   sentinel.Addr(),
		Dialer: sentinelDialer(nil, time.Second, &endpointTracker{}),
		// No command retry, so a closed connection handed out by the pool
		// fails the command instead of being retried away.
		MaxRetries: -1,
		PoolSize:   1,
	})
	defer client.Close()
	ctx := context.Background()
	require.NoError(t, client.Ping(ctx).Err())

	sentinel.Close()
	require.NoError(t, sentinel.Restart())
	time.Sleep(50 * time.Millisecond) // the close reaches the client's socket

	require.NoError(t, client.Ping(ctx).Err(), "the pool drops the closed connection and dials a new one")
}
