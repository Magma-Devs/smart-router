package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// callerContext is the context a gRPC listener hands the relay: one carrying the caller's own
// deadline, recorded as such.
func callerContext(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return WithCallerDeadline(ctx)
}

func TestCallerDeadlinePassed(t *testing.T) {
	callerCtx := callerContext(t, time.Second)
	callerDeadline, _ := callerCtx.Deadline()
	before, after := callerDeadline.Add(-time.Millisecond), callerDeadline.Add(time.Millisecond)

	t.Run("the caller's deadline, reached", func(t *testing.T) {
		require.True(t, CallerDeadlinePassed(callerCtx, after))
		require.True(t, CallerDeadlinePassed(callerCtx, callerDeadline))
	})

	t.Run("the caller's deadline, not reached yet", func(t *testing.T) {
		require.False(t, CallerDeadlinePassed(callerCtx, before))
	})

	t.Run("a context below it that keeps its deadline", func(t *testing.T) {
		// An attempt whose own budget is longer than what the caller left: bounded by the caller.
		attemptCtx, cancel := context.WithTimeout(callerCtx, time.Hour)
		defer cancel()
		require.True(t, CallerDeadlinePassed(attemptCtx, after))
	})

	t.Run("an earlier deadline of the router's own", func(t *testing.T) {
		attemptCtx, cancel := context.WithDeadline(callerCtx, before)
		defer cancel()
		require.False(t, CallerDeadlinePassed(attemptCtx, after), "the router's own deadline ended it, and that one counts")
	})

	t.Run("a context detached from the caller", func(t *testing.T) {
		// A write's delivery: the caller's deadline does not bound it, whatever its own deadline is.
		detached, cancel := context.WithTimeout(context.WithoutCancel(callerCtx), time.Hour)
		defer cancel()
		require.False(t, CallerDeadlinePassed(detached, after))
		require.False(t, CallerDeadlinePassed(context.WithoutCancel(callerCtx), after))
	})

	t.Run("no deadline recorded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		require.False(t, CallerDeadlinePassed(ctx, deadline.Add(time.Millisecond)), "a deadline nobody recorded is the router's")
		require.False(t, CallerDeadlinePassed(context.Background(), time.Now()))
		require.False(t, CallerDeadlinePassed(WithCallerDeadline(context.Background()), time.Now()), "a caller that sent no deadline")
	})
}

func TestIsClientCancellation_TheCallersOwnDeadline(t *testing.T) {
	timedOut := fmt.Errorf("gRPC relay timed out: %w", context.DeadlineExceeded)

	t.Run("an attempt ended at the caller's deadline", func(t *testing.T) {
		callerCtx := callerContext(t, 10*time.Millisecond)
		<-callerCtx.Done()
		require.True(t, IsClientCancellation(timedOut, callerCtx))
	})

	t.Run("an attempt ended at the router's own deadline", func(t *testing.T) {
		callerCtx := callerContext(t, time.Hour)
		attemptCtx, cancel := context.WithTimeout(callerCtx, 10*time.Millisecond)
		defer cancel()
		<-attemptCtx.Done()
		require.False(t, IsClientCancellation(timedOut, attemptCtx))
		require.False(t, IsClientCancellation(timedOut, callerCtx), "the caller's deadline has not passed")
	})

	t.Run("a deadline nobody recorded as the caller's", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		<-ctx.Done()
		require.False(t, IsClientCancellation(timedOut, ctx))
	})

	t.Run("a failure that is not a deadline, after the caller's deadline", func(t *testing.T) {
		callerCtx := callerContext(t, 10*time.Millisecond)
		<-callerCtx.Done()
		require.False(t, IsClientCancellation(fmt.Errorf("connection reset"), callerCtx))
	})
}
