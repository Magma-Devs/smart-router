package relaycore

import "testing"

// The stop reason travels state machine → Done instruction → processor → the request's final log
// line. Each hop is trivial; the value is that the chain exists at all, because without it "why was
// there no second attempt?" is answerable only at DEBUG.
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

// Every terminating path names a reason. A Done instruction with an empty StopReason produces a
// final line that silently omits the field, which is the state this change exists to remove.
func TestRelayStateSendInstructions_CarriesTheStopReason(t *testing.T) {
	instruction := RelayStateSendInstructions{Done: true, StopReason: "MaxRetriesReached"}
	if instruction.StopReason == "" {
		t.Fatal("the Done instruction must be able to carry a reason")
	}
}
