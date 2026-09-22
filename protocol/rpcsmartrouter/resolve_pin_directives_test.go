package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestResolvePinDirectives covers MAG-2228 Fix 1: lava-select-provider / lava-stickiness
// are honored on the FIRST attempt but dropped on retries so the relay can fall through to
// a different provider instead of re-pinning the one that just failed.
func TestResolvePinDirectives(t *testing.T) {
	headers := map[string]string{
		common.SELECT_PROVIDER_HEADER_NAME: "simprovider1",
		common.STICKINESS_HEADER_NAME:      "sticky-1",
	}

	t.Run("first attempt honors the pin", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), headers, true)
		if sel != "simprovider1" {
			t.Fatalf("first attempt: selectedProvider = %q, want simprovider1", sel)
		}
		if sticky != "sticky-1" {
			t.Fatalf("first attempt: stickiness = %q, want sticky-1", sticky)
		}
	})

	t.Run("retry drops the pin so failover can pick a different provider", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), headers, false)
		if sel != "" {
			t.Fatalf("retry: selectedProvider = %q, want empty", sel)
		}
		if sticky != "" {
			t.Fatalf("retry: stickiness = %q, want empty", sticky)
		}
	})

	t.Run("no headers yields no pin on first attempt", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), map[string]string{}, true)
		if sel != "" || sticky != "" {
			t.Fatalf("no headers: got (%q, %q), want empty", sel, sticky)
		}
	})
}

// TestCrossValidationOverridesPin covers the other half of the pin's life: a directive that
// survives resolvePinDirectives can still be displaced by an operator mandate.
//
// The case that matters is cross-validation enabled WITHOUT group diversity, which is the default
// shape of an enabled policy (MinGroups defaults to 1). The override used to be keyed on
// minGroups > 1 and applied deep inside selection, so this configuration kept the pin: selection
// returned the one pinned address, lowered its own target to match, and a policy asking for
// MaxParticipants participants was satisfied by a single provider with no error — an answer
// returned as validated having been compared against nothing.
func TestCrossValidationOverridesPin(t *testing.T) {
	cases := []struct {
		name             string
		crossValidation  bool
		selectedProvider string
		stickiness       string
		want             bool
	}{
		// The regression this exists for. No group diversity anywhere in sight.
		{"cross-validation on, header pin", true, "simprovider1", "", true},
		{"cross-validation on, sticky claim", true, "", "sticky-1", true},
		{"cross-validation on, both", true, "simprovider1", "sticky-1", true},

		// Nothing to displace.
		{"cross-validation on, no directive", true, "", "", false},

		// Cross-validation off: the caller's directive is the only instruction there is, and a
		// single-provider relay is exactly what a pin asks for.
		{"cross-validation off, header pin", false, "simprovider1", "", false},
		{"cross-validation off, sticky claim", false, "", "sticky-1", false},
		{"cross-validation off, no directive", false, "", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := crossValidationOverridesPin(tc.crossValidation, tc.selectedProvider, tc.stickiness)
			if got != tc.want {
				t.Fatalf("crossValidationOverridesPin(%v, %q, %q) = %v, want %v",
					tc.crossValidation, tc.selectedProvider, tc.stickiness, got, tc.want)
			}
		})
	}
}
