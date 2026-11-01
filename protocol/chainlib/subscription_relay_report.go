package chainlib

import (
	"context"
	"sync"
	"time"
)

// SubscriptionRelayReporter is implemented by a relay sender that records each subscription request
// a listener hands to a subscription manager instead of SendParsedRelay: a WebSocket subscribe,
// unsubscribe or unsubscribe-all frame, and the open of a gRPC server stream. The listener calls it
// once per request, after the manager returns, with what the manager returned.
type SubscriptionRelayReporter interface {
	ReportSubscriptionRelay(ctx context.Context, protocolMessage ProtocolMessage, start time.Time, err error)
}

// SubscriptionUpstream is what a subscription manager did upstream for one subscription request. The
// listener attaches one to the request's context with WithSubscriptionUpstream before handing the
// request over; the manager records on it, and the request's report reads it. A nil one records
// nothing, so a manager can record on whatever SubscriptionUpstreamFrom returns.
type SubscriptionUpstream struct {
	lock   sync.Mutex
	record SubscriptionUpstreamRecord
}

// SubscriptionUpstreamRecord is what a SubscriptionUpstream holds.
type SubscriptionUpstreamRecord struct {
	// Upstream names the provider the request went to, or whose open subscription it joined.
	Upstream string
	// Joined: answered from a subscription already open upstream, so nothing was sent.
	Joined bool
	// Sent: a request went to Upstream at SentAt and ended Latency later, with Err.
	Sent    bool
	SentAt  time.Time
	Latency time.Duration
	Err     error
}

// Joined records that the request was answered from upstream's open subscription.
func (s *SubscriptionUpstream) Joined(upstream string) {
	if s == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.record = SubscriptionUpstreamRecord{Upstream: upstream, Joined: true}
}

// Sent records the request sent to upstream at sentAt, which ended latency later with err.
func (s *SubscriptionUpstream) Sent(upstream string, sentAt time.Time, latency time.Duration, err error) {
	if s == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.record = SubscriptionUpstreamRecord{Upstream: upstream, Sent: true, SentAt: sentAt, Latency: latency, Err: err}
}

// Record returns what was recorded; the zero record when nothing was.
func (s *SubscriptionUpstream) Record() SubscriptionUpstreamRecord {
	if s == nil {
		return SubscriptionUpstreamRecord{}
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.record
}

type subscriptionUpstreamCtxKey struct{}

// WithSubscriptionUpstream attaches a fresh SubscriptionUpstream to ctx.
func WithSubscriptionUpstream(ctx context.Context) context.Context {
	return context.WithValue(ctx, subscriptionUpstreamCtxKey{}, &SubscriptionUpstream{})
}

// SubscriptionUpstreamFrom returns the SubscriptionUpstream attached to ctx, or nil.
func SubscriptionUpstreamFrom(ctx context.Context) *SubscriptionUpstream {
	upstream, _ := ctx.Value(subscriptionUpstreamCtxKey{}).(*SubscriptionUpstream)
	return upstream
}
