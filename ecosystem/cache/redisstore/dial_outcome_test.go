package redisstore

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
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
//
// Every dial here runs under the configuration the store ships
// (DefaultDialTimeout), because the verdict must not depend on the budget: it
// is read off the error, not off whose clock ran out.
func TestBaseDialerRecordsOnlyWhatProvesAnEndpointIsGone(t *testing.T) {
	dead := refusedAddr(t)

	t.Run("refused is an outage", func(t *testing.T) {
		tracker := &endpointTracker{}
		dial := baseDialer(nil, DefaultDialTimeout, tracker)
		before := time.Now()
		conn, err := dial(context.Background(), "tcp", dead)
		require.Nil(t, conn)
		require.ErrorIs(t, err, syscall.ECONNREFUSED, "sanity: this is the refusal the test is about")
		require.ErrorIs(t, tracker.faultSince(before), err,
			"a refused connection is the endpoint answering that it is not there")
	})

	t.Run("a dial the caller cut short proves nothing", func(t *testing.T) {
		tracker := &endpointTracker{}
		dial := baseDialer(nil, DefaultDialTimeout, tracker)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		before := time.Now()
		conn, err := dial(ctx, "tcp", dead)
		require.Nil(t, conn)
		require.ErrorIs(t, err, context.Canceled, "a cancelled context fails the dial")
		require.NoError(t, tracker.faultSince(before),
			"the attempt never ran to a verdict — go-redis abandons a queued dial whose caller has gone")
	})

	t.Run("a dial that timed out proves nothing", func(t *testing.T) {
		// go-redis bounds every attempt with a context carrying DialTimeout
		// (pool.dialConn); this is that context after it has expired, and the
		// error is the same *net.OpError a black hole produces on the wire.
		tracker := &endpointTracker{}
		dial := baseDialer(nil, DefaultDialTimeout, tracker)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		before := time.Now()
		conn, err := dial(ctx, "tcp", dead)
		require.Nil(t, conn)
		var netErr net.Error
		require.ErrorAs(t, err, &netErr)
		require.True(t, netErr.Timeout(), "sanity: the failure is a timeout, not the refusal the port would give")
		require.NoError(t, tracker.faultSince(before),
			"nobody answered: a black hole and a healthy cache too far away fail identically")
	})

	t.Run("a healthy endpoint whose handshake outlasts the dial budget is not gone", func(t *testing.T) {
		live, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer live.Close()

		// The dialer's own budget, spent before the handshake can complete:
		// the cross-region case, where a cold TCP+TLS handshake takes longer
		// than DialTimeout against a cache that is up.
		tracker := &endpointTracker{}
		dial := baseDialer(nil, time.Nanosecond, tracker)
		before := time.Now()
		conn, err := dial(context.Background(), "tcp", live.Addr().String())
		require.Nil(t, conn)
		var netErr net.Error
		require.ErrorAs(t, err, &netErr)
		require.True(t, netErr.Timeout())
		require.NoError(t, tracker.faultSince(before),
			"the endpoint is listening; a dial budget too small for it says nothing about reachability")
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
		time.Sleep(time.Millisecond)
		tracker.noteFault(refused)
		require.True(t, window.unreachable(), "sanity: the fault is inside the window, so only the nil guard keeps it off")
		require.NoError(t, window.Annotate(nil), "a lookup that succeeded is not an outage, whatever a neighbour saw")
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

// The discriminator, one answer at a time. The wire-shaped errors are built the
// way the net package builds them (*net.OpError around *os.SyscallError around
// the errno; *net.DNSError for a name), so what is asserted is the unwrapping
// the dialer actually relies on, not the classifier's own fixtures.
func TestDialErrorProvesEndpointGone(t *testing.T) {
	dialErr := func(err error) error {
		return &net.OpError{Op: "dial", Net: "tcp", Err: err}
	}
	connectErr := func(errno syscall.Errno) error {
		return dialErr(os.NewSyscallError("connect", errno))
	}
	for _, tc := range []struct {
		name          string
		err           error
		provesItsGone bool
	}{
		{"connection refused", connectErr(syscall.ECONNREFUSED), true},
		{"connection reset during the handshake", connectErr(syscall.ECONNRESET), true},
		{"no route to host", connectErr(syscall.EHOSTUNREACH), true},
		{"network unreachable", connectErr(syscall.ENETUNREACH), true},
		{"no such host", dialErr(&net.DNSError{Err: "no such host", Name: "cache.invalid", IsNotFound: true}), true},
		{"the name server did not answer", dialErr(&net.DNSError{Err: "i/o timeout", Name: "cache.example", IsTimeout: true}), false},
		{"the caller's context expired — a dial cut short", dialErr(context.DeadlineExceeded), false},
		{"the caller went away", dialErr(context.Canceled), false},
		{"an I/O deadline inside the handshake", dialErr(os.ErrDeadlineExceeded), false},
		{"the endpoint answered, but not with a handshake", dialErr(io.EOF), false},
		{"a local resource ran out", connectErr(syscall.EMFILE), false},
		{"nothing at all", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.provesItsGone, dialErrorProvesEndpointGone(tc.err))
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

// Only a dial that BEGAN after the refusal retires it. The pool dials from many
// slots at once and go-redis's prober dials on its own schedule, so a handshake
// already in flight when the refusal lands completes alongside it — and would
// otherwise erase the fault a failing read is about to be labelled by, leaving
// that read a "timeout" against an endpoint that refused it.
func TestASuccessfulDialRetiresOnlyFaultsBeforeItBegan(t *testing.T) {
	refused := errors.New("dial tcp: connect: connection refused")

	t.Run("on the tracker", func(t *testing.T) {
		tracker := &endpointTracker{}
		dialBegan := time.Now()
		time.Sleep(time.Millisecond)
		tracker.noteFault(refused)
		faultAt := time.Now()

		tracker.noteReached(dialBegan)
		require.ErrorIs(t, tracker.faultSince(dialBegan), refused,
			"a dial that began before the refusal cannot vouch for the endpoint after it")

		time.Sleep(time.Millisecond)
		tracker.noteReached(time.Now())
		require.NoError(t, tracker.faultSince(faultAt),
			"a dial that began after the refusal and completed is the endpoint back")
	})

	t.Run("through the dialer, with the handshake held open", func(t *testing.T) {
		// A TLS handshake completes only when the server takes part, which is
		// the one handshake a test can hold open: a plain TCP dial is answered
		// by the kernel before Accept is ever called.
		pki := newTestPKI(t)
		lis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{pki.serverTLS}, MinVersion: tls.VersionTLS12})
		require.NoError(t, err)
		defer lis.Close()
		release := make(chan struct{})
		accepted := make(chan struct{}, 8)
		go func() {
			for {
				conn, acceptErr := lis.Accept()
				if acceptErr != nil {
					return
				}
				accepted <- struct{}{}
				go func(c net.Conn) {
					<-release
					_ = c.(*tls.Conn).Handshake()
					_, _ = io.Copy(io.Discard, c)
				}(conn)
			}
		}()

		tracker := &endpointTracker{}
		dial := baseDialer(&tls.Config{RootCAs: pki.caPool, ServerName: "localhost", MinVersion: tls.VersionTLS12}, 5*time.Second, tracker)
		dead := refusedAddr(t)

		inFlight := make(chan error, 1)
		go func() {
			conn, dialErr := dial(context.Background(), "tcp", lis.Addr().String())
			if conn != nil {
				defer conn.Close()
			}
			inFlight <- dialErr
		}()
		select {
		case <-accepted:
		case <-time.After(5 * time.Second):
			t.Fatal("the held handshake never reached the listener")
		}

		time.Sleep(time.Millisecond)
		before := time.Now()
		_, err = dial(context.Background(), "tcp", dead)
		require.Error(t, err)
		require.Error(t, tracker.faultSince(before), "the refusal is recorded while the other dial is still in flight")

		close(release)
		require.NoError(t, <-inFlight, "the held handshake completes")
		require.Error(t, tracker.faultSince(before),
			"it began before the refusal, so it says nothing about the endpoint after the refusal")

		conn, err := dial(context.Background(), "tcp", lis.Addr().String())
		require.NoError(t, err)
		defer conn.Close()
		require.NoError(t, tracker.faultSince(before),
			"a dial begun after the refusal, and answered, retires it")
	})
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
	sentinel := Config{Topology: TopologySentinel, Addresses: []string{dead}, MasterName: "mymaster"}
	// A sentinel data dial carries the mark markDataDialsHook stamps; the
	// control-plane dials do not. Neither may record a fault.
	markedData := context.WithValue(context.Background(), dataDialMarkKey{}, struct{}{})
	for _, tc := range []struct {
		topology string
		ctx      context.Context
		dialer   func(*endpointTracker) func(context.Context, string, string) (net.Conn, error)
		records  bool
	}{
		{"standalone", context.Background(), func(tr *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
			return Config{Addresses: []string{dead}}.standaloneOptions([]string{dead}, nil, nil, tr).Dialer
		}, true},
		{"cluster", context.Background(), func(tr *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
			return Config{Addresses: []string{dead}}.clusterOptions([]string{dead}, nil, nil, tr).Dialer
		}, false},
		{"sentinel control plane", context.Background(), func(tr *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
			return sentinel.failoverOptions([]string{dead}, nil, sentinel.credentialsSource(), "", tr).Dialer
		}, false},
		{"sentinel data node", markedData, func(tr *endpointTracker) func(context.Context, string, string) (net.Conn, error) {
			return sentinel.failoverOptions([]string{dead}, nil, sentinel.credentialsSource(), "", tr).Dialer
		}, false},
	} {
		t.Run(tc.topology, func(t *testing.T) {
			tracker := &endpointTracker{}
			before := time.Now()
			_, err := tc.dialer(tracker)(tc.ctx, "tcp", dead)
			require.ErrorIs(t, err, syscall.ECONNREFUSED, "sanity: the dial is one standalone would record")
			if tc.records {
				require.Error(t, tracker.faultSince(before), "standalone has one endpoint: the fault is about it")
			} else {
				require.NoError(t, tracker.faultSince(before), "no dial here can be attributed to one endpoint")
			}
		})
	}
}
