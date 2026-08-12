package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// grpcRaceLoserError is the shape handleGRPCError returns once the router
// cancels a gRPC relay.
//
// Deliberately NOT a *url.Error. Every test added for MAG-2648 — including the
// one written to prove the fix was transport-general — built a *url.Error, which
// is precisely why the identical bug on the gRPC transport stayed invisible
// (MAG-2687). This file starts from the real gRPC shapes instead.
func grpcRaceLoserError() error {
	return fmt.Errorf("gRPC relay cancelled: %w", context.Canceled)
}

// TestGRPCRaceLoser_SurvivesClassification is the gRPC counterpart of
// TestRaceLoser_SurvivesClassification: a relay the router itself cancelled must
// reach the carve-out and must not be scored against the endpoint.
func TestGRPCRaceLoser_SurvivesClassification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a sibling relay won the race

	transportErr := grpcRaceLoserError()
	require.True(t, common.IsClientCancellation(transportErr, ctx),
		"precondition: the raw gRPC cancellation IS a client cancellation")

	wrapped := classifyAndWrap(transportErr, common.ChainFamily(-1), common.TransportGRPC)

	t.Run("classification is correct", func(t *testing.T) {
		require.Equal(t, common.RouterErrorContextCanceled.Code, extractRouterError(wrapped).Code,
			"a local gRPC cancellation must classify as context-canceled, not UNKNOWN_ERROR")
	})
	t.Run("sentinel survives wrapping", func(t *testing.T) {
		require.True(t, errors.Is(wrapped, context.Canceled))
	})
	t.Run("carve-out fires", func(t *testing.T) {
		require.True(t, common.IsClientCancellation(wrapped, ctx))
	})
	t.Run("endpoint is not blamed", func(t *testing.T) {
		markUnhealthy, backoff := classifyEndpointHealth(
			extractRouterError(wrapped), common.IsClientCancellation(wrapped, ctx))
		require.False(t, markUnhealthy, "a gRPC relay-race loser must not mark the endpoint unhealthy")
		require.False(t, backoff)
	})
}

// TestGRPCStatusError_IsNeverReadAsLocalCancellation is the inverse of a test
// that used to live here, and the inversion is the point.
//
// The earlier version asserted that a bare GRPCStatusError{Code: 1, Message:
// "context canceled"} SHOULD classify as PROTOCOL_CONTEXT_CANCELED, and narrowed
// the classifier guard from "rpc error" to "rpc error: code =" to make that pass.
// The premise was wrong. GRPCStatusError is built by handleGRPCError only AFTER
// the local-cancellation branch has already returned, so a value of this shape
// describes a status the ENDPOINT reported — remote, or at minimum not proven
// local. PROTOCOL_CONTEXT_CANCELED is Retryable=false, so reading it as local
// suppresses the retry that would have reached a healthy endpoint.
//
// A genuinely local cancellation never needs this table: handleGRPCError returns
// a nil response and an error wrapping the real context.Canceled sentinel, which
// DetectConnectionError resolves structurally in layer 1. That path is pinned in
// TestGRPCRaceLoser_SurvivesClassification above.
func TestGRPCStatusError_IsNeverReadAsLocalCancellation(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  *routersession.GRPCStatusError
	}{
		{"canceled", &routersession.GRPCStatusError{Code: 1, Message: "context canceled"}},
		{"deadline", &routersession.GRPCStatusError{Code: 4, Message: "context deadline exceeded"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Contains(t, strings.ToLower(tc.err.Error()), "rpc error",
				"precondition: the rendering really does contain the substring the guard keys on")

			wrapped := classifyAndWrap(tc.err, common.ChainFamily(-1), common.TransportGRPC)
			got := extractRouterError(wrapped)

			// got may be nil when nothing matched, which is itself a pass — the
			// assertion is only that it is not one of the two LOCAL verdicts.
			if got == nil {
				return
			}
			require.NotEqual(t, common.RouterErrorContextCanceled.Code, got.Code,
				"a status the endpoint reported must not be attributed to local orchestration")
			require.NotEqual(t, common.RouterErrorContextDeadline.Code, got.Code,
				"a status the endpoint reported must not be attributed to local orchestration")
		})
	}
}

// COLLATERAL GUARD — the most important test in this file.
//
// Narrowing the guard must not go too far: a genuinely REMOTE gRPC deadline or
// cancel carries the upstream's fault, not ours, and must still fall through to
// transport-scoped classification rather than being read as a local cancellation
// (which would silently stop blaming a broken endpoint).
func TestGRPCRemoteStatusError_StillNotLocalCancellation(t *testing.T) {
	for _, tc := range []struct{ name, msg string }{
		{"remote canceled", "rpc error: code = Canceled desc = context canceled"},
		{"remote deadline", "rpc error: code = DeadlineExceeded desc = context deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := classifyAndWrap(errors.New(tc.msg), common.ChainFamily(-1), common.TransportGRPC)
			got := extractRouterError(wrapped).Code

			require.NotEqual(t, common.RouterErrorContextCanceled.Code, got,
				"a remote cancel must not be attributed to local orchestration")
			require.NotEqual(t, common.RouterErrorContextDeadline.Code, got,
				"a remote deadline must not be attributed to local orchestration")
		})
	}
}

// TestRemoteStatusGuard_AppliesAcrossTransports makes the reach of the guard
// explicit.
//
// stringConnectionFallbacks is a single package-level table and
// detectConnectionErrorFromString takes no transport argument, so the width of
// the "rpc error" exclusion decides classification for EVERY transport, not just
// gRPC. These cases pin all three directions on non-gRPC transports.
func TestRemoteStatusGuard_AppliesAcrossTransports(t *testing.T) {
	for _, transport := range []common.TransportType{common.TransportJsonRPC, common.TransportREST} {
		t.Run(transport.String()+"/remote-prefix-still-excluded", func(t *testing.T) {
			wrapped := classifyAndWrap(
				errors.New("rpc error: code = Canceled desc = context canceled"),
				common.ChainFamily(-1), transport)
			require.NotEqual(t, common.RouterErrorContextCanceled.Code, extractRouterError(wrapped).Code,
				"the remote-status prefix must exclude on non-gRPC transports too")
		})

		t.Run(transport.String()+"/plain-local-cancel-still-matches", func(t *testing.T) {
			wrapped := classifyAndWrap(
				errors.New("context canceled"),
				common.ChainFamily(-1), transport)
			require.Equal(t, common.RouterErrorContextCanceled.Code, extractRouterError(wrapped).Code,
				"a plain local cancellation must still classify as such")
		})

		// The discriminating case: contains "rpc error" but NOT "rpc error: code =".
		// The two cases above pass under EITHER guard width, so this is the only one
		// that detects a widening — and it does so on a non-gRPC transport, which is
		// the reach this test exists to pin. routersession.GRPCStatusError's rendering
		// ("gRPC error 1: ...") is the concrete real-world instance of that string
		// class, and it denotes a status the endpoint reported.
		t.Run(transport.String()+"/bare-rpc-error-substring-still-excluded", func(t *testing.T) {
			wrapped := classifyAndWrap(
				errors.New("gRPC error 1: context canceled"),
				common.ChainFamily(-1), transport)
			got := extractRouterError(wrapped)
			if got == nil {
				return // nothing matched — the exclusion held
			}
			require.NotEqual(t, common.RouterErrorContextCanceled.Code, got.Code,
				"any rpc-error marker must exclude on every transport — a remote status is not local orchestration")
		})
	}
}
