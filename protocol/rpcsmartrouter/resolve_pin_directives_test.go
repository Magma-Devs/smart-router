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
		sel, sticky := resolvePinDirectives(context.Background(), headers)
		if sel != "simprovider1" {
			t.Fatalf("selectedProvider = %q, want simprovider1", sel)
		}
		if sticky != "sticky-1" {
			t.Fatalf("stickiness = %q, want sticky-1", sticky)
		}
	})

	t.Run("no headers yields no pin", func(t *testing.T) {
		sel, sticky := resolvePinDirectives(context.Background(), map[string]string{})
		if sel != "" || sticky != "" {
			t.Fatalf("no headers: got (%q, %q), want empty", sel, sticky)
		}
	})
}
