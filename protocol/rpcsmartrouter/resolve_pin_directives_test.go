package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// lava-select-provider and lava-stickiness are read on every attempt. A pinned request is never
// retried on another upstream (MAG-2280, MAG-4123), and should a second batch ever be sent anyway,
// the pin must still be there so selection refuses the already-tried upstream loudly instead of
// quietly serving the request elsewhere. MAG-2228 had the resolver drop the pin on retries; that
// is the behaviour MAG-2280 reversed.
func TestResolvePinDirectives(t *testing.T) {
	headers := map[string]string{
		common.SELECT_PROVIDER_HEADER_NAME: "simprovider1",
		common.STICKINESS_HEADER_NAME:      "sticky-1",
	}

	t.Run("both pins are read", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), headers, true)
		if sel != "simprovider1" {
			t.Fatalf("selectedProvider = %q, want simprovider1", sel)
		}
		if sticky != "sticky-1" {
			t.Fatalf("stickiness = %q, want sticky-1", sticky)
		}
	})

	t.Run("without lava-pin-allow-retry the pin holds on a later attempt too", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), headers, false)
		if sel != "simprovider1" || sticky != "sticky-1" {
			t.Fatalf("later attempt: got (%q, %q), want the pin kept", sel, sticky)
		}
	})

	allowRetry := map[string]string{
		common.SELECT_PROVIDER_HEADER_NAME: "simprovider1",
		common.STICKINESS_HEADER_NAME:      "sticky-1",
		common.PIN_ALLOW_RETRY_HEADER_NAME: "true",
	}
	t.Run("with lava-pin-allow-retry the first attempt is pinned", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), allowRetry, true)
		if sel != "simprovider1" || sticky != "sticky-1" {
			t.Fatalf("first attempt: got (%q, %q), want the pin", sel, sticky)
		}
	})
	t.Run("with lava-pin-allow-retry a later attempt reads no pin", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), allowRetry, false)
		if sel != "" || sticky != "" {
			t.Fatalf("later attempt: got (%q, %q), want no pin", sel, sticky)
		}
	})

	t.Run("no headers yields no pin", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), map[string]string{}, true)
		if sel != "" || sticky != "" {
			t.Fatalf("no headers: got (%q, %q), want empty", sel, sticky)
		}
	})
}
