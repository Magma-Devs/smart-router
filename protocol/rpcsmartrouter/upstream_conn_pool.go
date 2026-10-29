package rpcsmartrouter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

// defaultUpstreamDialTimeout bounds one dial of a pooled upstream connection: the gRPC stream
// pool's default ConnectionTimeout, and the WebSocket pool's bound. Without it a WebSocket dial
// to a node that accepts TCP and never answers the upgrade held the pool lock for as long as the
// caller's context allowed, which for a subscription is its whole life.
const defaultUpstreamDialTimeout = 30 * time.Second

// pooledConn is what connPool needs from a connection: its health, a way to close it, and the
// count of subscriptions or streams it carries (its slots).
type pooledConn interface {
	comparable
	IsHealthy() bool
	Close() error
	slots() int32
	addSlots(delta int32) int32
}

// poolVocabulary is how a pool names itself and its slots in log lines.
type poolVocabulary struct {
	name     string // "WebSocket" or "gRPC"
	slotAttr string // log attribute for a connection's slot count
}

// reconnectPolicy is where the WebSocket and gRPC pools reconnect differently. Each field is a
// behaviour one of them has and the other does not; none was unified, because each decides when
// an upstream that just failed is dialed again.
type reconnectPolicy struct {
	// skipWhenHealthy returns from a reconnect at once when the pool already holds a healthy
	// connection (WebSocket). Otherwise every reconnect dials one more connection (gRPC).
	skipWhenHealthy bool
	// retryOnFreshLadder keeps one reconnect dialing on a fresh copy of the ladder until a dial
	// succeeds or the ladder runs out (WebSocket). Otherwise a reconnect waits once on the pool's
	// shared ladder and dials once (gRPC); that ladder is reset only by a successful dial, and
	// past its last rung it returns no wait at all.
	retryOnFreshLadder bool
	// dropUnhealthyOnReconnect closes and drops the pool's unhealthy connections when a
	// reconnect succeeds (WebSocket). Otherwise the new connection is only added.
	dropUnhealthyOnReconnect bool
	// waitersJudgeByHealth answers a caller that waited on another caller's reconnect by whether
	// the pool then holds a healthy connection, and never retries an abandoned attempt
	// (WebSocket). Otherwise it gets the attempt's own outcome, and dials itself when the
	// attempt was abandoned (gRPC).
	waitersJudgeByHealth bool
	// pruneDeadOnPick closes and drops connections marked unhealthy before picking one for a new
	// slot (WebSocket, whose listeners mark a dead connection with MarkUnhealthy).
	pruneDeadOnPick bool
}

// connPool is one upstream endpoint's pool of connections, shared by the WebSocket and gRPC
// subscription paths. It scales between minConnections and maxConnections, aiming at perConn
// slots per connection, and reconnects after an outage by the pool's reconnectPolicy.
//
// A connection is never closed under the pool lock: closing a WebSocket waits on the socket,
// and every pick and release on the endpoint needs that lock meanwhile.
type connPool[C pooledConn] struct {
	vocabulary   poolVocabulary
	sanitizedURL string
	dial         func(context.Context) (C, error)
	policy       reconnectPolicy

	connections []C
	backoff     *ExponentialBackoff
	lock        sync.RWMutex
	closed      atomic.Bool
	// reconnectAttempt is the ReconnectWithBackoff attempt in flight, nil when none. The
	// caller that installs it dials; every other caller waits on it.
	reconnectAttempt atomic.Pointer[reconnectAttempt]
	// lastReconnectErr is the shared ladder's previous failed dial, so the next reconnect's
	// wait honours a Retry-After that dial carried. Guarded by lock.
	lastReconnectErr error
	onReconnect      func() // called after a successful reconnect (to restore subscriptions)
	onClose          func() // called once, as Close begins

	minConnections int
	maxConnections int
	perConn        int // target slots per connection
}

func newConnPool[C pooledConn](
	vocabulary poolVocabulary,
	url string,
	dial func(context.Context) (C, error),
	policy reconnectPolicy,
	minConnections, maxConnections, perConn int,
) *connPool[C] {
	return &connPool[C]{
		vocabulary:     vocabulary,
		sanitizedURL:   sanitizeEndpointURL(url),
		dial:           dial,
		policy:         policy,
		connections:    make([]C, 0),
		backoff:        NewWebSocketBackoff(),
		minConnections: minConnections,
		maxConnections: maxConnections,
		perConn:        perConn,
	}
}

func (p *connPool[C]) logPrefix() string {
	return p.vocabulary.name + " pool: "
}

// SetReconnectCallback sets a callback to be called after a successful reconnection, to
// restore the subscriptions that ran on the connections the outage took.
func (p *connPool[C]) SetReconnectCallback(callback func()) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.onReconnect = callback
}

// take returns the connection for a new slot, and with reserve also counts the slot on it.
//
// The reservation is made under the pool lock, with the pick: counted any later, the
// connection looks unused in the meantime, and maybeScaleDown closes a connection past
// minConnections that carries no counted slot (MAG-4177, MAG-4278 item 11). The caller releases
// a reserved slot with release, including when opening the subscription fails.
func (p *connPool[C]) take(ctx context.Context, reserve bool) (C, error) {
	var none C
	if p.closed.Load() {
		return none, fmt.Errorf("pool is closed")
	}

	p.lock.Lock()
	// Checked again under the lock: a Close that ran between the check above and the lock
	// has already taken the connection list, so a connection dialed now would be added to a
	// closed pool and never closed.
	if p.closed.Load() {
		p.lock.Unlock()
		return none, fmt.Errorf("pool is closed")
	}
	var dropped []C
	if p.policy.pruneDeadOnPick {
		dropped = p.dropUnhealthyLocked()
	}
	conn, err := p.pickConnectionLocked(ctx)
	if err == nil && reserve {
		conn.addSlots(1)
	}
	p.lock.Unlock()

	closeConns(dropped)
	return conn, err
}

// pickConnectionLocked chooses or creates the connection for a new slot. Caller holds p.lock.
func (p *connPool[C]) pickConnectionLocked(ctx context.Context) (C, error) {
	// Find the best connection (healthy with the lowest slot count)
	var bestConn C
	found := false
	var lowestSlots int32 = int32(p.perConn + 1) // Start higher than target

	for _, conn := range p.connections {
		if conn.IsHealthy() {
			slots := conn.slots()
			if slots < lowestSlots {
				lowestSlots = slots
				bestConn = conn
				found = true
			}
		}
	}

	// If we have a connection with capacity, use it
	if found && lowestSlots < int32(p.perConn) {
		return bestConn, nil
	}

	// Need to scale up if possible
	if len(p.connections) < p.maxConnections {
		newConn, err := p.createConnectionLocked(ctx)
		if err != nil {
			// If we can't create a new connection but have an existing one, use it
			if found {
				utils.LavaFormatWarning(p.logPrefix()+"failed to scale up, using existing connection", err,
					utils.LogAttr("endpoint", p.sanitizedURL),
					utils.LogAttr("currentConnections", len(p.connections)),
					utils.LogAttr(p.vocabulary.slotAttr, lowestSlots),
				)
				return bestConn, nil
			}
			var none C
			return none, fmt.Errorf("failed to create connection: %w", err)
		}

		utils.LavaFormatInfo(p.logPrefix()+"scaled up",
			utils.LogAttr("endpoint", p.sanitizedURL),
			utils.LogAttr("totalConnections", len(p.connections)),
			utils.LogAttr("reason", "all connections near capacity"),
		)
		return newConn, nil
	}

	// At max connections - use the one with the fewest slots even if over target
	if found {
		return bestConn, nil
	}

	// No healthy connections - try to create one
	return p.createConnectionLocked(ctx)
}

// dropUnhealthyLocked removes the connections marked unhealthy from the pool and returns them
// for the caller to close once it has released the lock. Caller holds p.lock.
func (p *connPool[C]) dropUnhealthyLocked() []C {
	var dropped []C
	kept := make([]C, 0, len(p.connections))
	for _, conn := range p.connections {
		if conn.IsHealthy() {
			kept = append(kept, conn)
			continue
		}
		dropped = append(dropped, conn)
		utils.LavaFormatDebug(p.logPrefix()+"dropped a dead connection",
			utils.LogAttr("endpoint", p.sanitizedURL),
			utils.LogAttr("remainingConnections", len(kept)),
		)
	}
	p.connections = kept
	return dropped
}

// createConnectionLocked dials and adds a new connection. Caller holds p.lock.
func (p *connPool[C]) createConnectionLocked(ctx context.Context) (C, error) {
	conn, err := p.dial(ctx)
	if err != nil {
		var none C
		return none, err
	}

	p.connections = append(p.connections, conn)
	p.backoff.Reset()

	utils.LavaFormatDebug(p.logPrefix()+"connection added",
		utils.LogAttr("endpoint", p.sanitizedURL),
		utils.LogAttr("totalConnections", len(p.connections)),
	)

	return conn, nil
}

// release gives back a slot taken on conn, and lets the pool scale down if connections are
// underused.
func (p *connPool[C]) release(conn C) {
	var none C
	if conn == none {
		return
	}

	conn.addSlots(-1)

	// Consider scaling down if we have excess connections with no slots
	p.maybeScaleDown()
}

// maybeScaleDown removes empty connections if we have more than minConnections
func (p *connPool[C]) maybeScaleDown() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.connections) <= p.minConnections {
		return
	}

	// Find connections with no slots (excluding the first minConnections)
	var toRemove []C
	kept := make([]C, 0, len(p.connections))

	for i, conn := range p.connections {
		if i < p.minConnections {
			// Keep minimum connections
			kept = append(kept, conn)
		} else if conn.slots() == 0 && !conn.IsHealthy() {
			// Remove unhealthy empty connections
			toRemove = append(toRemove, conn)
		} else if conn.slots() == 0 {
			// Mark for potential removal (only if we have excess)
			if len(kept)+len(p.connections)-i-1 >= p.minConnections {
				toRemove = append(toRemove, conn)
			} else {
				kept = append(kept, conn)
			}
		} else {
			kept = append(kept, conn)
		}
	}

	if len(toRemove) > 0 {
		p.connections = kept

		// Close removed connections asynchronously
		go func(conns []C) {
			closeConns(conns)
			utils.LavaFormatDebug(p.logPrefix()+"scaled down",
				utils.LogAttr("endpoint", p.sanitizedURL),
				utils.LogAttr("removedConnections", len(conns)),
				utils.LogAttr("remainingConnections", len(kept)),
			)
		}(toRemove)
	}
}

// reconnectAttempt is one ReconnectWithBackoff attempt. done closes once err and abandoned
// hold its outcome, so every caller waiting on the attempt wakes as it ends.
type reconnectAttempt struct {
	done chan struct{}
	err  error
	// abandoned is set when the attempt ended because its own caller's context ended, not
	// because the dial failed. That says nothing about the upstream, so a waiter whose
	// context is still live tries again rather than giving up on it.
	abandoned bool
}

// ReconnectWithBackoff reconnects the pool after an outage.
//
// A connection that dies fails every subscription on it at once, and each of them restores
// through here. One caller dials; the others wait for that attempt and are answered by the
// pool's reconnectPolicy, then open their subscriptions on what the pool holds.
//
// The attempt runs on the dialing caller's context. When that context ends first (its
// subscription's last client left), the attempt is abandoned rather than failed.
func (p *connPool[C]) ReconnectWithBackoff(ctx context.Context) error {
	for {
		if inFlight := p.reconnectAttempt.Load(); inFlight != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-inFlight.done:
			}
			if p.policy.waitersJudgeByHealth {
				if p.IsHealthy() {
					return nil
				}
				return fmt.Errorf("reconnection failed")
			}
			if inFlight.abandoned {
				continue
			}
			if inFlight.err != nil {
				return fmt.Errorf("the reconnect this caller waited for failed: %w", inFlight.err)
			}
			return nil
		}

		attempt := &reconnectAttempt{done: make(chan struct{})}
		if !p.reconnectAttempt.CompareAndSwap(nil, attempt) {
			continue // another caller installed its attempt first: wait for that one
		}
		err := p.reconnect(ctx)
		p.endReconnect(attempt, err, err != nil && ctx.Err() != nil)
		return err
	}
}

// endReconnect records attempt's outcome and wakes every caller waiting on it. The attempt
// is uninstalled before the wake, so a waiter that retries an abandoned attempt finds none
// in flight and dials itself.
func (p *connPool[C]) endReconnect(attempt *reconnectAttempt, err error, abandoned bool) {
	attempt.err, attempt.abandoned = err, abandoned
	p.reconnectAttempt.CompareAndSwap(attempt, nil)
	close(attempt.done)
}

// reconnect is the dialing caller's half of ReconnectWithBackoff.
func (p *connPool[C]) reconnect(ctx context.Context) error {
	if p.policy.skipWhenHealthy && p.IsHealthy() {
		return nil // At least one connection is healthy
	}
	if p.policy.retryOnFreshLadder {
		return p.reconnectOnFreshLadder(ctx)
	}
	return p.reconnectOnSharedLadder(ctx)
}

// reconnectOnFreshLadder dials until a dial succeeds, waiting between failures on a fresh copy
// of the ladder, and gives up when the ladder runs out.
func (p *connPool[C]) reconnectOnFreshLadder(ctx context.Context) error {
	ladder := p.backoff.Clone()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Dialed outside the pool lock: every subscription opened or released on this
		// endpoint needs that lock meanwhile.
		conn, err := p.dial(ctx)
		if err == nil {
			return p.adoptReconnected(conn, utils.LogAttr("attempts", ladder.Attempt()))
		}

		delay, shouldRetry := ladder.NextBackoff()
		if !shouldRetry {
			return fmt.Errorf("max retries (%d) exceeded for endpoint %s: %w",
				ladder.MaxRetries, p.sanitizedURL, err)
		}
		delay = reconnectDelay(delay, err)

		utils.LavaFormatDebug(p.logPrefix()+"reconnect failed, retrying",
			utils.LogAttr("endpoint", p.sanitizedURL),
			utils.LogAttr("attempt", ladder.Attempt()),
			utils.LogAttr("next_delay", delay),
			utils.LogAttr("error", err.Error()),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// reconnectOnSharedLadder waits on the pool's ladder, floored by the Retry-After the previous
// failed dial carried, and dials once.
func (p *connPool[C]) reconnectOnSharedLadder(ctx context.Context) error {
	delay, _ := p.backoff.NextBackoff()
	p.lock.RLock()
	delay = reconnectDelay(delay, p.lastReconnectErr)
	p.lock.RUnlock()

	utils.LavaFormatDebug(p.logPrefix()+"waiting before reconnect",
		utils.LogAttr("endpoint", p.sanitizedURL),
		utils.LogAttr("backoff", delay),
	)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}

	// Dialed outside the pool lock: a dial can take up to its timeout, and every subscription
	// opened or released on this endpoint needs that lock meanwhile.
	conn, err := p.dial(ctx)
	if err != nil {
		p.lock.Lock()
		p.lastReconnectErr = err
		p.lock.Unlock()
		return fmt.Errorf("failed to reconnect: %w", err)
	}
	return p.adoptReconnected(conn)
}

// adoptReconnected adds a connection a reconnect dialed, unless the pool closed while it was
// dialing: Close marks the pool closed before it takes the lock, so checked under the lock the
// new connection is either closed here or among those Close releases.
func (p *connPool[C]) adoptReconnected(conn C, logAttrs ...utils.Attribute) error {
	p.lock.Lock()
	if p.closed.Load() {
		p.lock.Unlock()
		conn.Close()
		return fmt.Errorf("pool closed while reconnecting")
	}
	var dropped []C
	if p.policy.dropUnhealthyOnReconnect {
		dropped = p.dropUnhealthyLocked()
	}
	p.connections = append(p.connections, conn)
	p.backoff.Reset()
	p.lastReconnectErr = nil
	total := len(p.connections)
	callback := p.onReconnect
	p.lock.Unlock()

	closeConns(dropped)

	utils.LavaFormatInfo(p.logPrefix()+"reconnected successfully",
		append([]utils.Attribute{
			utils.LogAttr("endpoint", p.sanitizedURL),
			utils.LogAttr("totalConnections", total),
		}, logAttrs...)...,
	)

	// Call the reconnect callback (e.g., to restore subscriptions)
	if callback != nil {
		callback()
	}

	return nil
}

// reconnectDelay floors the backoff ladder's delay with a rate-limited dial's
// Retry-After: an upstream that named its own wait must not be re-dialed at the
// 100ms–30s cadence. Any other failure keeps the ladder's delay.
func reconnectDelay(base time.Duration, err error) time.Duration {
	if retryAfter, ok := common.RetryAfterFrom(err); ok && retryAfter > base {
		return retryAfter
	}
	return base
}

// Close closes the pool and every connection in it. The connections are closed after the lock
// is released.
func (p *connPool[C]) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}
	if p.onClose != nil {
		p.onClose()
	}

	p.lock.Lock()
	conns := p.connections
	p.connections = nil
	p.lock.Unlock()

	closeConns(conns)

	utils.LavaFormatDebug(p.logPrefix()+"closed",
		utils.LogAttr("endpoint", p.sanitizedURL),
		utils.LogAttr("closedConnections", len(conns)),
	)

	return nil
}

// IsHealthy returns true if the pool has at least one healthy connection
func (p *connPool[C]) IsHealthy() bool {
	if p.closed.Load() {
		return false
	}

	p.lock.RLock()
	defer p.lock.RUnlock()

	for _, conn := range p.connections {
		if conn.IsHealthy() {
			return true
		}
	}
	return false
}

// GetEndpoint returns the sanitized endpoint URL
func (p *connPool[C]) GetEndpoint() string {
	return p.sanitizedURL
}

// ConnectionCount returns the current number of connections in the pool
func (p *connPool[C]) ConnectionCount() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.connections)
}

// totalSlots returns the number of slots taken across all connections
func (p *connPool[C]) totalSlots() int32 {
	p.lock.RLock()
	defer p.lock.RUnlock()

	var total int32
	for _, conn := range p.connections {
		total += conn.slots()
	}
	return total
}

func closeConns[C pooledConn](conns []C) {
	for _, conn := range conns {
		conn.Close()
	}
}
