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
