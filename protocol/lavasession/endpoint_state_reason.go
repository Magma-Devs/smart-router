package lavasession

import (
	"sort"
	"time"
)

// StateReasonRecord is what an endpoint remembers about one state reason (MAG-3986): when it was
// added and the block the endpoint was on at that moment. For head-stalled the block is the one the
// endpoint was stuck on, and the reason comes off only when the endpoint answers ABOVE it. That test
// is deliberately not "the stall counter went back to zero": the counter lives with the endpoint's
// tracker and restarts at zero whenever the tracker is recreated, which says nothing about the node.
type StateReasonRecord struct {
	Reason EndpointDisableReason
	Since  time.Time
	Block  int64
}

// heldByStateLocked reports whether any state reason holds the endpoint out. Caller must hold e.mu.
func (e *Endpoint) heldByStateLocked() bool {
	return len(e.stateReasons) > 0
}

// usableLocked reports whether selection may route to this endpoint: the event slot is enabled and
// no state reason holds. Caller must hold e.mu (read or write).
func (e *Endpoint) usableLocked() bool {
	return e.Enabled && !e.heldByStateLocked()
}

// IsUsable reports, under e.mu, whether selection may route to this endpoint right now. It differs
// from IsEnabled, which describes only the event slot: an endpoint can be Enabled and still out of
// rotation because a state reason (head-stalled) holds.
func (e *Endpoint) IsUsable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.usableLocked()
}

// AddStateReason records a state reason at the given time and block. It returns false, and changes
// nothing, when the reason is already held — the original time and block stand, so "how long has it
// been out" and "what block was it stuck on" keep describing the first observation.
func (e *Endpoint) AddStateReason(reason EndpointDisableReason, at time.Time, block int64) (added bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, held := e.stateReasons[reason]; held {
		return false
	}
	if e.stateReasons == nil {
		e.stateReasons = make(map[EndpointDisableReason]StateReasonRecord, 1)
	}
	e.stateReasons[reason] = StateReasonRecord{Reason: reason, Since: at, Block: block}
	return true
}

// RemoveStateReason drops one state reason. It returns false when the reason was not held. It never
// touches the event slot, so an endpoint that is also disabled for node-error stays disabled.
func (e *Endpoint) RemoveStateReason(reason EndpointDisableReason) (removed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, held := e.stateReasons[reason]; !held {
		return false
	}
	delete(e.stateReasons, reason)
	return true
}

// StateReason returns the record of one state reason and whether it is held.
func (e *Endpoint) StateReason(reason EndpointDisableReason) (StateReasonRecord, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	rec, held := e.stateReasons[reason]
	return rec, held
}

// ClearStateReasons drops every state reason and returns how many were held. Only the operator
// reset calls it: no automatic path may clear a state reason it does not own.
func (e *Endpoint) ClearStateReasons() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := len(e.stateReasons)
	e.stateReasons = nil
	return n
}

// stateReasonsLocked lists the state reasons in a stable order (by reason name). Caller must hold
// e.mu.
func (e *Endpoint) stateReasonsLocked() []StateReasonRecord {
	if len(e.stateReasons) == 0 {
		return nil
	}
	out := make([]StateReasonRecord, 0, len(e.stateReasons))
	for _, rec := range e.stateReasons {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Reason < out[j].Reason })
	return out
}
