package redisstore

import (
	"errors"
	"net"
	"syscall"
	"time"
)

// ErrEndpointUnreachable marks an operation that failed while the store could
// not reach its endpoint at all — an outage, as distinct from a backend that was
// reachable and answered too slowly.
//
// It exists because the operation's own error cannot be trusted to say so. A
// dial that fails on the endpoint's terms is retried — by the pool, five
// attempts a hundred milliseconds apart, and again by the command — and
// whichever retry the caller's deadline lands in, go-redis returns that
// deadline and drops the refusal it had in hand. The read budget
// (common.CacheTimeout, 50ms) is shorter than the first backoff, so on reads
// that happened EVERY time: a dead cache arrived as a bare
// context.DeadlineExceeded and was recorded as saturation, which is the
// opposite first move for whoever is reading it. Writes, whose budget outlasts
// the retries, recorded the same outage correctly — so the two halves of one
// incident disagreed (MAG-3653).
//
// Joined onto the operation's error rather than replacing it: the caller's
// deadline really did expire, so anything already classifying on that keeps its
// answer; this only adds the reason it could never have been met.
var ErrEndpointUnreachable = errors.New("resp-cache: endpoint unreachable")

// endpointFault is one observation that an endpoint is not there, and when it
// was made. Held whole behind a single atomic pointer so a reader cannot see a
// cause from one fault with the timestamp of another.
type endpointFault struct {
	at    time.Time
	cause error
}

// noteFault records that this endpoint could not be reached. Written from dial
// callbacks on arbitrary goroutines — including go-redis's own dial queue, which
// runs them detached from any caller — and read from the relay path.
//
// Last observation wins, deliberately: what an operation needs to know is
// whether the endpoint was faulting while IT ran, so only the freshest
// observation can answer. clearFault is the other half of that — a dial that
// succeeded is the endpoint answering, and leaving a refusal standing behind it
// would attribute an outage to an operation that failed later, for its own
// reasons, against an endpoint since proven reachable.
func (t *endpointTracker) noteFault(cause error) {
	if t == nil || cause == nil {
		return
	}
	t.fault.Store(&endpointFault{at: time.Now(), cause: cause})
}

// noteReached records that a dial begun at started completed its handshake,
// which is direct proof the endpoint was there — and retires a fault observed
// BEFORE that dial began. A fault observed after it began is left standing: the
// pool dials from many slots at once and go-redis's own prober dials on its own
// schedule, so a handshake that began before a refusal ran side by side with
// it, and its success does not say the endpoint was back after the refusal.
// The reader this protects is an operation that saw the refusal inside its
// window and is about to be labelled by it.
func (t *endpointTracker) noteReached(started time.Time) {
	if t == nil {
		return
	}
	for {
		fault := t.fault.Load()
		if fault == nil || !fault.at.Before(started) {
			return
		}
		if t.fault.CompareAndSwap(fault, nil) {
			return
		}
	}
}

// dialErrorProvesEndpointGone classifies a failed dial by WHAT ANSWERED, which
// is the only thing separating an endpoint that is gone from a budget too small
// for a healthy one.
//
// A refused connection, no route to the host or the network, a reset during the
// handshake and a name that does not resolve are each an answer: something on
// the path said the endpoint is not there. A timeout is the absence of one — a
// black hole and a healthy cache a network away that cannot finish a handshake
// inside the dial budget fail identically, and nothing in the error tells them
// apart. Neither does a dial the caller cut short (go-redis abandons a queued
// dial whose caller has gone). Reporting either as an outage would send an
// operator hunting a cache that is up — the same wrong turn as the bug this
// exists to fix, reversed — and docs/RESP-CACHE.md already promises the timeout
// reading for a too-distant backend. Everything else is left unclassified for
// the same reason: a TLS alert or an EOF mid-handshake came from an endpoint
// that answered, and a local resource error (descriptors, ephemeral ports) says
// nothing about the endpoint at all.
//
// Classified on the error rather than on whose clock ran out, because on this
// path the clocks cannot tell: go-redis bounds every attempt with a context
// carrying the same DialTimeout the net.Dialer holds (pool.dialConn), so which
// of the two expired first — and with it the verdict — was decided by
// microseconds, and no dial ever arrived under the caller's own deadline.
func dialErrorProvesEndpointGone(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsNotFound
	}
	for _, answered := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.EHOSTUNREACH, syscall.ENETUNREACH} {
		if errors.Is(err, answered) {
			return true
		}
	}
	return false
}

// faultSince returns the endpoint's failure to be reached if one was observed at
// or after the given instant, and nil otherwise — including when the endpoint
// has never faulted, or last faulted before the caller began and has been
// serving since.
func (t *endpointTracker) faultSince(since time.Time) error {
	if t == nil {
		return nil
	}
	fault := t.fault.Load()
	if fault == nil || fault.at.Before(since) {
		return nil
	}
	return fault.cause
}

// OpWindow is the span of one cache operation, against the endpoint that
// operation uses. It exists so a failure the store observed WHILE the operation
// ran can be attributed to it — which is the only attribution available, because
// go-redis dials from a shared queue under a context of its own and the caller's
// never reaches the dialer (see queuedNewConn), and because the operation's
// returned error has had the reason stripped out of it by then.
//
// A window, rather than a flag on the Store, because a stale verdict is worse
// than none: a pool warm for hours has not dialled at all, and a transient
// refusal long since recovered would then label every later saturation timeout
// an outage.
//
// The observation need not be this operation's own — a concurrent lookup, the
// 10s health probe or go-redis's own 1 Hz dial prober (pool.tryDial, which has
// no operation behind it) dials the same endpoint — and that is the right answer
// either way: what it establishes is that the endpoint was unreachable while
// this operation was failing against it, which is the question the label asks.
// A write's window is as wide as its budget (common.CacheWriteTimeout, 5s), so
// a refusal anywhere in those seconds marks a write that then failed; a dial
// that began after the refusal and succeeded retires it (noteReached).
//
// It holds only where ONE endpoint stands behind the tracker, which is why
// faults are recorded for standalone alone (see trackingDialer). Under sentinel
// a dial may be a control-plane dial to any quorum member, including one
// discovered at runtime that no configured list names; under cluster one tracker
// stands behind every shard and a fault carries no address. In both, a single
// down member — a non-paging condition under sentinel quorum — would report a
// healthy master unreachable, continuously, and relabel every saturation
// timeout with it. That is the ticket's misdirection inverted, which is worse
// than leaving it (MAG-3653 follow-up: the health probe already reaches each
// endpoint role-correctly under every topology, and is the candidate signal
// there).
type OpWindow struct {
	endpoint *endpointTracker
	since    time.Time
}

// BeginRead opens a window on the endpoint lookups go to; BeginWrite on the one
// writes go to. With no read/write split both name the same endpoint, as
// everything else about the store does.
func (s *Store) BeginRead() OpWindow {
	if s == nil {
		return OpWindow{}
	}
	return OpWindow{endpoint: s.readEndpoint, since: time.Now()}
}

func (s *Store) BeginWrite() OpWindow {
	if s == nil {
		return OpWindow{}
	}
	return OpWindow{endpoint: s.writeEndpoint, since: time.Now()}
}

// Annotate joins the unreachable marker, and the failure that proves it, onto an
// operation's error. A successful operation and one whose endpoint never faulted
// while it ran are both returned untouched, so the marker appears only where the
// store actually failed to reach the backend.
func (w OpWindow) Annotate(err error) error {
	if err == nil || w.endpoint == nil {
		return err
	}
	cause := w.endpoint.faultSince(w.since)
	if cause == nil {
		return err
	}
	return errors.Join(err, ErrEndpointUnreachable, cause)
}

// unreachable reports whether the endpoint faulted during this window. For
// tests; the relay path wants Annotate, which carries the reason out too.
func (w OpWindow) unreachable() bool {
	return w.endpoint != nil && w.endpoint.faultSince(w.since) != nil
}
