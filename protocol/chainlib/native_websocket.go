package chainlib

import (
	"context"
	"time"
)

// NativeWebSocketSession handles an explicitly configured non-JSON-RPC protocol.
// Each downstream connection owns a session; its upstream state is never shared
// with another caller. False from Handle leaves the frame on the ordinary path.
type NativeWebSocketSession interface {
	Handle([]byte) (handled bool, err error)
	Close()
}

// NativeWebSocketSessionFactory is optional on a relay sender. Its frames use the
// ingress's bounded single writer, rate limits and connection lifecycle. A failed
// upstream must disconnect the caller, rather than silently dropping event streams.
type NativeWebSocketSessionFactory interface {
	NewNativeWebSocketSession(context.Context, func(context.Context, []byte) bool, func(), func(string, string, time.Duration, error)) NativeWebSocketSession
}
