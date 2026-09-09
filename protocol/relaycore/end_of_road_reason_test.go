package relaycore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Availability scoring blames an endpoint only when the request "ran out of road", and it decides
// that by reading the stop reason. So the two ways a request's context can end must not be spelled
// the same way: our budget expiring is the endpoint's problem, a caller hanging up is not.
//
// Before this, both produced ProcessingTimeout, so a websocket or gRPC client closing its connection
// mid-request could record an availability failure against an endpoint that was healthy and still
// working on the answer. HTTP listeners build their context from Background and cannot reach it.
func TestEndOfRoadReason(t *testing.T) {
	t.Run("our own deadline is a timeout", func(t *testing.T) {
		sm := &UnifiedRelayStateMachine{}
		require.Equal(t, StopReasonProcessingTimeout, sm.endOfRoadReason(context.DeadlineExceeded))
	})

	t.Run("a cancel from outside is not", func(t *testing.T) {
		sm := &UnifiedRelayStateMachine{}
		require.Equal(t, StopReasonCallerGone, sm.endOfRoadReason(context.Canceled),
			"a caller closing its connection must not be recorded as the endpoint running out of road")
	})

	t.Run("a real cancelled context reads as caller-gone", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		sm := &UnifiedRelayStateMachine{}
		require.Equal(t, StopReasonCallerGone, sm.endOfRoadReason(ctx.Err()))
	})

	t.Run("a real expired deadline reads as a timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()
		sm := &UnifiedRelayStateMachine{}
		require.Equal(t, StopReasonProcessingTimeout, sm.endOfRoadReason(ctx.Err()))
	})

	// A reason the policy already recorded describes the request better than either ending does, so
	// it still wins — the timeout is only ever a fallback.
	t.Run("a recorded policy reason outranks both", func(t *testing.T) {
		for _, err := range []error{context.DeadlineExceeded, context.Canceled} {
			sm := &UnifiedRelayStateMachine{}
			sm.setStopReason("Stateful")
			require.Equal(t, "Stateful", sm.endOfRoadReason(err))
		}
	})

	// The two reasons must stay distinct strings, or the distinction this test exists for collapses
	// silently the next time someone edits the constants.
	t.Run("the two reasons are different", func(t *testing.T) {
		require.NotEqual(t, StopReasonProcessingTimeout, StopReasonCallerGone)
	})
}
