package lavasession

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deadlinePassedCtx is a context in the state a context.WithTimeout is in between the instant its
// deadline passes and the instant the runtime runs its timer: Deadline() is in the past and Err()
// is still nil. handleGRPCError used to read Err() alone, and in that state it took the node's
// DEADLINE_EXCEEDED for the node's answer (MAG-4925).
type deadlinePassedCtx struct {
	context.Context
	deadline time.Time
}

func (c deadlinePassedCtx) Deadline() (time.Time, bool) { return c.deadline, true }

// MAG-4925: the node's gRPC server enforces the router's deadline too, and reports it back as a
// DEADLINE_EXCEEDED status of its own. Read in the gap before the router's own timer has run, that
// status is still the router's deadline: a timeout with no response, exactly what the router's own
// timer produces, so the attempt is counted against the endpoint and the caller gets the router's
// error, not a made-up answer of the node.
func TestHandleGRPCError_DeadlineEchoedByTheNodeIsTheRoutersTimeout(t *testing.T) {
	ctx := deadlinePassedCtx{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond)}
	echoed := status.Error(codes.DeadlineExceeded, "Deadline Exceeded")

	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(ctx, echoed, nil, true)

	require.Nil(t, resp, "the router's own deadline is not an answer from the node")
	require.ErrorIs(t, err, context.DeadlineExceeded, "the sentinel every timeout carries")
	require.ErrorIs(t, err, echoed, "the node's status stays readable for logs")
	var statusErr *GRPCStatusError
	require.False(t, errors.As(err, &statusErr), "a timeout is not typed as the node's status")
}

// The guard on the other side, kept: a node that answers DEADLINE_EXCEEDED while the router's
// deadline is still open timed out inside the node. That is its answer, and a node error.
func TestHandleGRPCError_NodeOwnDeadlineBeforeTheRoutersStaysANodeError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(ctx, status.Error(codes.DeadlineExceeded, "query timed out on the node"), nil, true)

	require.NotNil(t, resp, "the node answered")
	require.Equal(t, int(codes.DeadlineExceeded), resp.StatusCode)
	var statusErr *GRPCStatusError
	require.ErrorAs(t, err, &statusErr)
	require.False(t, errors.Is(err, context.DeadlineExceeded), "the node's deadline is not the router's")
}

// Only the deadline status is read against the clock. Another status that arrives at the same
// instant is the node's answer, arrived late, as a late HTTP reply is on every other transport.
func TestHandleGRPCError_OtherStatusAfterTheDeadlineStaysANodeError(t *testing.T) {
	ctx := deadlinePassedCtx{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond)}

	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(ctx, status.Error(codes.Internal, "boom"), nil, true)

	require.NotNil(t, resp)
	require.Equal(t, int(codes.Internal), resp.StatusCode)
	var statusErr *GRPCStatusError
	require.ErrorAs(t, err, &statusErr)
}

// The decision itself, with the clock held still. The boundary is inclusive: the node's deadline
// is never earlier than the router's, so a status read at the deadline is already the echo.
func TestRouterDeadlineEchoed(t *testing.T) {
	deadline := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	withDeadline := deadlinePassedCtx{Context: context.Background(), deadline: deadline}
	echoed := status.Error(codes.DeadlineExceeded, "Deadline Exceeded")

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		now  time.Time
		want bool
	}{
		{"at the deadline", withDeadline, echoed, deadline, true},
		{"after the deadline", withDeadline, echoed, deadline.Add(3 * time.Millisecond), true},
		{"before the deadline: the node's own", withDeadline, echoed, deadline.Add(-time.Nanosecond), false},
		{"another status after the deadline", withDeadline, status.Error(codes.Unavailable, "shutting down"), deadline.Add(time.Second), false},
		{"grpc-go's own local deadline wording", withDeadline, status.Error(codes.DeadlineExceeded, "context deadline exceeded"), deadline, true},
		{"not a status error", withDeadline, errors.New("deadline exceeded"), deadline.Add(time.Second), false},
		{"no deadline on the context", context.Background(), echoed, deadline.Add(time.Hour), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, routerDeadlineEchoed(tc.ctx, tc.err, tc.now))
		})
	}
}
