package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// A strict pin (router-stickiness, or router-select-provider without
// router-select-provider-allow-retry: true) is read on every attempt, so should a second batch
// ever be sent the pin is still there and selection refuses the already-tried upstream loudly
// (MAG-2280, MAG-4123, MAG-5711). A router-select-provider sent with the allow-retry header is
// read on the first attempt only (the MAG-2228 rule): a retry may reach the rest of the pool.
func TestResolvePinDirectives(t *testing.T) {
	const (
		sel        = common.SELECT_PROVIDER_HEADER_NAME
		sticky     = common.STICKINESS_HEADER_NAME
		allowRetry = common.SELECT_PROVIDER_ALLOW_RETRY_HEADER_NAME
	)
	for _, tc := range []struct {
		name                   string
		headers                map[string]string
		firstAttempt           bool
		wantSelected, wantStky string
	}{
		{"select-provider, first attempt", map[string]string{sel: "p1"}, true, "p1", ""},
		{"select-provider, later attempt keeps the pin", map[string]string{sel: "p1"}, false, "p1", ""},
		{"allow-retry, first attempt", map[string]string{sel: "p1", allowRetry: "true"}, true, "p1", ""},
		{"allow-retry, later attempt reads no pin", map[string]string{sel: "p1", allowRetry: "true"}, false, "", ""},
		{"allow-retry false, later attempt keeps the pin", map[string]string{sel: "p1", allowRetry: "false"}, false, "p1", ""},
		{"sticky, first attempt", map[string]string{sticky: "s1"}, true, "", "s1"},
		{"sticky, later attempt keeps the pin", map[string]string{sticky: "s1"}, false, "", "s1"},
		// Sticky makes the whole request strict, so the selection that picks the node is kept too,
		// and the allow-retry header does not loosen it.
		{"sticky with select-provider, later attempt keeps both", map[string]string{sel: "p1", sticky: "s1"}, false, "p1", "s1"},
		{"sticky with select-provider and allow-retry, later attempt keeps both", map[string]string{sel: "p1", sticky: "s1", allowRetry: "true"}, false, "p1", "s1"},
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
