package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// A strict pin (lava-stickiness, or lava-select-provider with lava-select-provider-strict: true)
// is read on every attempt, so should a second batch ever be sent the pin is still there and
// selection refuses the already-tried upstream loudly (MAG-2280, MAG-4123). A plain
// lava-select-provider is read on the first attempt only, its documented failover (MAG-2228,
// kept by MAG-4826): a retry may reach the rest of the pool.
func TestResolvePinDirectives(t *testing.T) {
	const (
		sel    = common.SELECT_PROVIDER_HEADER_NAME
		sticky = common.STICKINESS_HEADER_NAME
		strict = common.SELECT_PROVIDER_STRICT_HEADER_NAME
	)
	for _, tc := range []struct {
		name                   string
		headers                map[string]string
		firstAttempt           bool
		wantSelected, wantStky string
	}{
		{"plain select-provider, first attempt", map[string]string{sel: "p1"}, true, "p1", ""},
		{"plain select-provider, later attempt reads no pin", map[string]string{sel: "p1"}, false, "", ""},
		{"strict select-provider, later attempt keeps the pin", map[string]string{sel: "p1", strict: "true"}, false, "p1", ""},
		{"strict false, later attempt reads no pin", map[string]string{sel: "p1", strict: "false"}, false, "", ""},
		{"sticky, first attempt", map[string]string{sticky: "s1"}, true, "", "s1"},
		{"sticky, later attempt keeps the pin", map[string]string{sticky: "s1"}, false, "", "s1"},
		// D4: sticky makes the whole request strict, so the selection that picks the node is kept too.
		{"sticky with select-provider, later attempt keeps both", map[string]string{sel: "p1", sticky: "s1"}, false, "p1", "s1"},
		{"no headers", map[string]string{}, true, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotSel, gotSticky := resolvePinDirectives(context.Background(), tc.headers, tc.firstAttempt)
			if gotSel != tc.wantSelected || gotSticky != tc.wantStky {
				t.Fatalf("got (%q, %q), want (%q, %q)", gotSel, gotSticky, tc.wantSelected, tc.wantStky)
			}
		})
	}
}
