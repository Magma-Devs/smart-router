package common

import (
	"context"
	"time"
)

type callerDeadlineKey struct{}

// WithCallerDeadline records ctx's deadline as the caller's own: the deadline a gRPC client sent in
// grpc-timeout, which grpc-go puts on the call's context. A listener calls it once, on the context
// it hands the relay. A context with no deadline is returned as is.
//
// The router adds deadlines of its own below this one (an attempt's budget, the request's
// processing timeout), and a context only reports the earliest. The record is what tells the two
// apart afterwards: see CallerDeadlinePassed.
func WithCallerDeadline(ctx context.Context) context.Context {
	deadline, ok := ctx.Deadline()
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, callerDeadlineKey{}, deadline)
}

// CallerDeadlinePassed reports whether the deadline that bounds ctx is the caller's own, and the
// clock has reached it. The rulebook (D10) puts that ending on the caller: nobody is blamed when a
// call ends at the deadline its caller set.
//
// ctx's deadline must be the recorded one exactly. An earlier deadline is the router's own, and
// ending there still counts against the endpoint (MAG-4925). A context detached from the caller's
// (a write's delivery, a cross-validation attempt) carries the record but not the deadline, so it
// is never read as ended by it. The clock decides rather than ctx.Err(): a node that echoes the
// deadline back can answer before the context's timer has run (routerDeadlineEchoed).
func CallerDeadlinePassed(ctx context.Context, now time.Time) bool {
	if ctx == nil {
		return false
	}
	callerDeadline, ok := ctx.Value(callerDeadlineKey{}).(time.Time)
	if !ok {
		return false
	}
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.Equal(callerDeadline) {
		return false
	}
	return !now.Before(callerDeadline)
}
