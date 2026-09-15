package relaycore

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// SendParsedRelay reads the reason off the processor for the final line and prints whatever it
// finds, so the empty default is part of the contract: a request the state machine never named
// must render a blank field, not a stale one.
func TestRelayProcessor_StopReasonRoundTrips(t *testing.T) {
	rp := &RelayProcessor{}

	if got := rp.GetStopReason(); got != "" {
		t.Fatalf("a processor that was never told should report %q, got %q", "", got)
	}

	rp.SetStopReason("Stateful")
	if got := rp.GetStopReason(); got != "Stateful" {
		t.Fatalf("GetStopReason() = %q, want %q", got, "Stateful")
	}

	// Last writer wins: a request that exhausts retries and then hits the processing timeout
	// stopped for the timeout, and that is what the final line should say.
	rp.SetStopReason("ProcessingTimeout")
	if got := rp.GetStopReason(); got != "ProcessingTimeout" {
		t.Fatalf("GetStopReason() = %q, want the most recent reason", got)
	}
}

// The deadline paths call stopReasonOr: the timeout ends the wait, but if the policy had already
// decided to stop, THAT is why there was no further attempt and it is the more useful answer.
// Reporting "ProcessingTimeout" over a recorded "Stateful" loses the fact the field exists for.
func TestStateMachine_StopReasonOrPrefersTheRecordedReason(t *testing.T) {
	sm := &UnifiedRelayStateMachine{}

	if got := sm.stopReasonOr("ProcessingTimeout"); got != "ProcessingTimeout" {
		t.Fatalf("with nothing recorded the fallback stands: got %q", got)
	}

	sm.setStopReason("Stateful")
	if got := sm.stopReasonOr("ProcessingTimeout"); got != "Stateful" {
		t.Fatalf("a recorded policy reason outranks the deadline fallback: got %q", got)
	}
}

// A reason decided while attempts are still running describes the DISPATCHER, not the request.
//
// Availability scoring blames an endpoint only on StopReasonProcessingTimeout, so any other reason
// filed before the request is actually over takes that slot and forgives a hang. The circuit
// breaker's own warning is the clearest statement of the problem — "stopping new attempts — relays
// already in flight may still answer" — and the policy branch says the same thing. That was
// harmless while an attempt died at its window, because "cannot start more" and "the request is
// over" were the same instant; separating those two clocks is what made it a bug.
func TestStateMachine_StopReasonIsHeldWhileRelaysAreInFlight(t *testing.T) {
	inFlight := func(addresses ...string) *lavasession.UsedProviders {
		usedProviders := lavasession.NewUsedProviders(nil)
		sessions := lavasession.ConsumerSessionsMap{}
		for _, address := range addresses {
			sessions[address] = &lavasession.SessionInfo{}
		}
		usedProviders.AddUsed(sessions, nil)
		return usedProviders
	}

	t.Run("nothing in flight: filed immediately, as before", func(t *testing.T) {
		sm := &UnifiedRelayStateMachine{usedProviders: lavasession.NewUsedProviders(nil)}
		sm.recordStopReason("MaxRetriesReached")
		if got := sm.getStopReason(); got != "MaxRetriesReached" {
			t.Fatalf("a settled request stops for the reason it was given: got %q", got)
		}
		if got := sm.endOfRoadReason(context.DeadlineExceeded); got != "MaxRetriesReached" {
			t.Fatalf("and that reason still outranks the deadline that ended the wait: got %q", got)
		}
	})

	t.Run("still in flight: held back, so the timeout can name the request", func(t *testing.T) {
		sm := &UnifiedRelayStateMachine{usedProviders: inFlight("lava@a")}
		sm.recordStopReason("ErrorToleranceExceeded")

		if got := sm.getStopReason(); got != "" {
			t.Fatalf("a reason decided with a relay still running must not become the request's: got %q", got)
		}
		// The endpoint stayed silent until the budget ran out. This is the verdict the whole blame
		// path turns on, and the reason above would have shadowed it.
		if got := sm.endOfRoadReason(context.DeadlineExceeded); got != StopReasonProcessingTimeout {
			t.Fatalf("the request ran out of budget with an endpoint silent, so it stopped for the timeout: got %q", got)
		}
	})

	t.Run("a caller hanging up is still not a timeout", func(t *testing.T) {
		sm := &UnifiedRelayStateMachine{usedProviders: inFlight("lava@a")}
		sm.recordStopReason("AllProvidersExhausted")
		if got := sm.endOfRoadReason(context.Canceled); got != StopReasonCallerGone {
			t.Fatalf("holding the reason back must not turn a client disconnect into a blameable timeout: got %q", got)
		}
	})

	t.Run("held reason is promoted once the last relay reports", func(t *testing.T) {
		usedProviders := inFlight("lava@a")
		sm := &UnifiedRelayStateMachine{usedProviders: usedProviders}
		sm.recordStopReason("ErrorToleranceExceeded")

		// The relay answers, so the request ends on the return condition rather than the budget.
		usedProviders.RemoveUsed("lava@a", lavasession.NewRouterKey(nil), nil)

		if got := sm.settleStopReason(); got != "ErrorToleranceExceeded" {
			t.Fatalf("held is not dropped — with nothing in flight this is the honest reason, and a blank field is what the reason exists to prevent: got %q", got)
		}
		if got := sm.getStopReason(); got != "ErrorToleranceExceeded" {
			t.Fatalf("and it is now the recorded reason: got %q", got)
		}
	})

	t.Run("settle prefers a reason that was already filed", func(t *testing.T) {
		sm := &UnifiedRelayStateMachine{usedProviders: lavasession.NewUsedProviders(nil)}
		sm.setStopReason("Success")
		sm.pendingStopReason = "ErrorToleranceExceeded"
		if got := sm.settleStopReason(); got != "Success" {
			t.Fatalf("a filed reason describes the request; a held one only describes why no more were started: got %q", got)
		}
	})
}
