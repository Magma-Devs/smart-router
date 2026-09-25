package redisstore

import (
	"errors"
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

// clearFault records that the endpoint answered a handshake, which is direct
// proof it is there.
func (t *endpointTracker) clearFault() {
	if t == nil {
		return
	}
	t.fault.Store(nil)
}

// dialFailureProvesEndpointGone classifies a failed dial by WHOSE clock ran out,
// which is the only thing separating an endpoint that is gone from a budget too
// small for a healthy one.
//
// An attempt that spent its own whole budget was answered by nobody: a black
// hole. One that failed with budget to spare was answered — refused, no route,
// no such name. One the CALLER's context cut short proves neither: go-redis
// abandons a queued dial whose caller has gone, and a healthy cache a network
// away cannot finish a handshake inside a same-zone read budget either.
// Reporting that as an outage would send an operator hunting a cache that is up
// — the same wrong turn as the bug this exists to fix, reversed. It is also the
// reading docs/RESP-CACHE.md already promises for a too-distant backend.
//
// A pure function of its three inputs so each quadrant is testable on its own.
// Inside the dialer the cheap disjunct alone decided every case a test could
// reach, and the black-hole term could be deleted with the suite still green —
// a term that cannot fail is not a term.
func dialFailureProvesEndpointGone(elapsed, dialTimeout time.Duration, ctxErr error) bool {
	if dialTimeout > 0 && elapsed >= dialTimeout {
		return true
	}
	return ctxErr == nil
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
// The observation need not be this operation's own — a concurrent lookup or the
// health probe dials the same endpoint — and that is the right answer either
// way: what it establishes is that the endpoint was unreachable while this
// operation was failing against it, which is the question the label asks.
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
