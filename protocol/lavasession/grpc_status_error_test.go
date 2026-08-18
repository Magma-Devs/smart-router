package lavasession

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestHandleGRPCError_CancelledContextReturnsNilResponse is the MAG-2687 pin.
//
// The phantom-success arm in sendGRPCRelay is only reachable when handleGRPCError
// returns a NON-nil response. On a router-initiated cancellation it must return
// nil, so the error flows through normal classification instead of being recorded
// as a completed relay with a latency sample that was never measured.
func TestHandleGRPCError_CancelledContextReturnsNilResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a sibling relay won the stateful fan-out race

	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(ctx, status.Error(codes.Canceled, "context canceled"), nil)

	require.Nil(t, resp,
		"a cancelled relay must not carry a response — that is the arm that records a success")
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled),
		"the context.Canceled sentinel must be re-attached so the relay-race carve-out can see it")
}

// TestHandleGRPCError_CancelledContextWinsOverTheReportedStatus closes the hole
// the branch above was originally written with.
//
// That branch required BOTH a cancelled local context AND status.Code(err) ==
// codes.Canceled. grpc-go does not promise the second: cancelling a call while its
// connection is being torn down surfaces Unavailable, and a cancel racing a
// response can surface Unknown or the status the endpoint had already sent. With
// the conjunct in place those cases fell through to the node-error arm, which
// returns (result, nil) — and `err != nil` at the relay-race carve-out
// (rpcsmartrouter_server.go) then sees nil and scores a relay WE abandoned against
// a healthy endpoint. Same bug class as MAG-2687, narrower window.
//
// The local context is the authoritative signal and is sufficient on its own. The
// risk is asymmetric: missing a local cancel penalises an endpoint that did nothing
// wrong, over-detecting one merely skips scoring for a result we were discarding.
func TestHandleGRPCError_CancelledContextWinsOverTheReportedStatus(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Unavailable, // connection torn down by the cancel itself
		codes.Unknown,     // grpc-go's fallback when the cancel races the response
		codes.Internal,    // a status the endpoint had already sent
		codes.Canceled,    // the shape the old conjunct required
	} {
		t.Run(code.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // a sibling relay won the race, or the client hung up

			g := &GRPCDirectRPCConnection{}
			resp, err := g.handleGRPCError(ctx, status.Error(code, "boom"), nil)

			require.Nil(t, resp,
				"a non-nil response routes this into the node-error arm, which returns err == nil "+
					"and makes the cancellation invisible to the carve-out")
			require.True(t, errors.Is(err, context.Canceled),
				"the local context is the authoritative signal; the status the endpoint reported "+
					"must not be able to veto it")
		})
	}
}

// TestHandleGRPCError_CancelledRelayKeepsItsCause covers the other half: the
// branch resolves the cancellation but must not throw away what the endpoint was
// doing when we pulled the plug — in a change whose other half is about preserving
// causes, wrapping only the sentinel would be the same omission one level up.
func TestHandleGRPCError_CancelledRelayKeepsItsCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cause := status.Error(codes.Unavailable, "connection reset by peer")
	g := &GRPCDirectRPCConnection{}
	_, err := g.handleGRPCError(ctx, cause, nil)

	require.True(t, errors.Is(err, context.Canceled),
		"the sentinel the carve-out resolves must still be reachable")
	require.ErrorIs(t, err, cause,
		"the originating gRPC status must survive alongside it, for logs and for any later inspector")
}

// TestHandleGRPCError_ExpiredDeadlineIsNotACancellation is the collateral guard
// for the two tests above: dropping the status-code conjunct widens the branch, so
// the remaining condition has to stay exact. A ctx whose DEADLINE expired is also
// non-nil, and relabelling a timeout as a client cancellation would let a genuinely
// slow endpoint escape being blamed for it.
func TestHandleGRPCError_ExpiredDeadlineIsNotACancellation(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded, "precondition: expired, not cancelled")

	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(ctx, status.Error(codes.DeadlineExceeded, "context deadline exceeded"), nil)

	require.NotNil(t, resp,
		"a timeout is the endpoint's problem and must keep its response so the node-error arm runs")
	require.False(t, errors.Is(err, context.Canceled),
		"a slow endpoint must not be excused as a relay we cancelled")
}

// TestHandleGRPCError_RemoteCancellationIsNotLocal is the collateral guard for
// the test above, and the one that actually constrains the classifier.
//
// Same gRPC status codes, but a LIVE context: nothing local was cancelled, so the
// status came from the endpoint. Three things must hold, and each corresponds to a
// distinct way this has gone wrong:
//
//  1. the response is non-nil, so sendGRPCRelay takes its node-error arm and the
//     endpoint is actually held responsible;
//  2. no context sentinel is attached, so common.IsClientCancellation's relay-race
//     carve-out cannot fire and excuse the endpoint;
//  3. the error does not classify as a LOCAL context error. This is the assertion
//     that fails if the "rpc error" guard in stringConnectionFallbacks is narrowed
//     to "rpc error: code =" — the wrapper renders as "gRPC error 1: context
//     canceled", which slips past the narrower prefix and lands on
//     PROTOCOL_CONTEXT_CANCELED (Retryable=false), suppressing the retry.
func TestHandleGRPCError_RemoteCancellationIsNotLocal(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
		msg  string
	}{
		{"remote canceled", codes.Canceled, "context canceled"},
		{"remote deadline", codes.DeadlineExceeded, "context deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &GRPCDirectRPCConnection{}
			// context.Background() is never cancelled — the production shape of a
			// status the endpoint reported on a healthy local request.
			resp, err := g.handleGRPCError(context.Background(), status.Error(tc.code, tc.msg), nil)

			require.NotNil(t, resp,
				"a remote status must keep its response so the node-error arm runs")
			require.Equal(t, int(tc.code), resp.StatusCode)
			require.Error(t, err)

			require.False(t, errors.Is(err, context.Canceled),
				"the local-cancellation sentinel must NOT be attached to a remote status")
			require.False(t, errors.Is(err, context.DeadlineExceeded),
				"the local-deadline sentinel must NOT be attached to a remote status")

			if le := common.DetectConnectionError(err); le != nil {
				require.NotEqual(t, common.LavaErrorContextCanceled.Code, le.Code,
					"remote cancel classified as local — the rpc-error guard was narrowed")
				require.NotEqual(t, common.LavaErrorContextDeadline.Code, le.Code,
					"remote deadline classified as local — the rpc-error guard was narrowed")
			}
		})
	}
}

// TestGrpcGoDoesNotWrapCancelSentinel documents WHY the sentinel has to be
// re-attached explicitly rather than merely preserved via Unwrap: grpc-go's own
// status error does not carry it. If this ever starts failing, the explicit
// re-attach in handleGRPCError can be simplified.
func TestGrpcGoDoesNotWrapCancelSentinel(t *testing.T) {
	require.False(t,
		errors.Is(status.Error(codes.Canceled, "context canceled"), context.Canceled),
		"grpc-go began wrapping context.Canceled — revisit handleGRPCError")
}

// TestHandleGRPCError_PreservesCause covers the non-cancellation path: a real
// node error still takes the response arm, and its cause now survives so
// errors.Is / errors.As can see through GRPCStatusError.
func TestHandleGRPCError_PreservesCause(t *testing.T) {
	cause := status.Error(codes.InvalidArgument, "malformed transaction")

	g := &GRPCDirectRPCConnection{}
	resp, err := g.handleGRPCError(context.Background(), cause, nil)

	require.NotNil(t, resp, "a genuine node error still carries its response body")
	require.Equal(t, int(codes.InvalidArgument), resp.StatusCode)

	var statusErr *GRPCStatusError
	require.True(t, errors.As(err, &statusErr))
	require.Equal(t, uint32(codes.InvalidArgument), statusErr.Code)
	require.ErrorIs(t, err, cause,
		"cause must survive; without Unwrap this type was a dead end for sentinel checks")
}

// TestGRPCStatusError_ResolvesThroughStatusFromError pins the behaviour change that
// Unwrap causes for every grpc-go-based inspector of this error.
//
// Before Unwrap, status.FromError on a *GRPCStatusError returned ok=false, so
// chainlib.ExtractNodeErrorDetails fell past its gRPC branch and reported code 0.
// It now returns ok=true with the real code, which is what Tier-1 CodeEquals
// matchers key on.
//
// The CODE is the whole of the change. The message is deliberately asserted to be
// UNCHANGED: when grpc-go's FromError resolves via errors.As it overwrites the
// status message with the OUTER error's Error() string, so st.Message() is our
// "gRPC error N: ..." rendering, not the node's bare message. ExtractNodeErrorDetails
// had already defaulted errorMessage to nodeError.Error(), so it sees the same
// string either way — code 0 -> 3 is the only observable difference.
//
// Pinned here rather than in chainlib because `cause` is unexported: only this
// package can build a wrapper that actually carries one, and package lavasession
// cannot import chainlib (chainlib imports lavasession).
func TestGRPCStatusError_ResolvesThroughStatusFromError(t *testing.T) {
	cause := status.Error(codes.InvalidArgument, "malformed transaction")
	wrapped := &GRPCStatusError{Code: uint32(codes.InvalidArgument), Message: "malformed transaction", cause: cause}

	st, ok := status.FromError(wrapped)
	require.True(t, ok,
		"Unwrap must let grpc-go resolve this wrapper; ExtractNodeErrorDetails depends on it")
	require.Equal(t, codes.InvalidArgument, st.Code(),
		"the real gRPC code must surface, not Unknown — this is the behaviour change")
	require.Equal(t, "gRPC error 3: malformed transaction", st.Message(),
		"FromError rewrites Message to the outer Error(); the node's bare message does NOT surface")

	// Without a cause the wrapper stays opaque — which is exactly the pre-fix
	// behaviour, and confirms the cause is what does the work here.
	_, okNoCause := status.FromError(&GRPCStatusError{Code: 3, Message: "x"})
	require.False(t, okNoCause,
		"a cause-less wrapper must remain unresolvable; if this flips, the fix is masked")
}

// TestIsSessionSyncLoss_UnaffectedByUnwrap is MAG-2687 step 0 — and it records a
// correction to that ticket's premise.
//
// The ticket warns that adding Unwrap makes status.Code(err) resolve via
// errors.As where it previously returned Unknown, that IsSessionSyncLoss reads
// that, and so the behaviour must be pinned before it becomes reachable.
//
// Verified empirically: it never becomes reachable. IsSessionSyncLoss lives in
// common.go, which imports github.com/gogo/status — NOT
// google.golang.org/grpc/status — and gogo's Code() does not unwrap. So
// GRPCStatusError is opaque to it both before and after this change, and step 0's
// hazard does not exist on this path.
//
// Still worth pinning: it is the test that will catch someone swapping gogo/status
// for grpc/status in common.go, which WOULD make the wrapper transparent here.
func TestIsSessionSyncLoss_UnaffectedByUnwrap(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Canceled,         // 1
		codes.DeadlineExceeded, // 4
		codes.InvalidArgument,  // 3
		codes.Internal,         // 13
		codes.Unavailable,      // 14
	} {
		err := &GRPCStatusError{
			Code:    uint32(code),
			Message: "x",
			cause:   status.Error(code, "x"),
		}
		require.False(t, IsSessionSyncLoss(err),
			"real gRPC code %v must not be read as session sync loss", code)
	}

	// Even the genuine sync-loss code stays invisible THROUGH the wrapper, because
	// gogo/status does not unwrap. This is not a regression introduced here: before
	// this change GRPCStatusError carried no cause at all, so the result was the
	// same. Adding Unwrap is neutral for this function.
	wrapped := &GRPCStatusError{
		Code:    SessionOutOfSyncGRPCCode,
		Message: "session out of sync",
		cause:   status.Error(codes.Code(SessionOutOfSyncGRPCCode), "session out of sync"),
	}
	require.False(t, IsSessionSyncLoss(wrapped),
		"gogo/status does not unwrap — if this flips, common.go changed status packages")

	// The undecorated forms are the ones that actually occur in production, and
	// both must keep working.
	require.True(t, IsSessionSyncLoss(SessionOutOfSyncError),
		"the sentinel path must still fire")
}
