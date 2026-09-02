package relaycore

import "testing"

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
