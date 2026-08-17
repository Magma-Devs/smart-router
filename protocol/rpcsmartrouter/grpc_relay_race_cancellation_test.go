package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
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
		require.Equal(t, common.LavaErrorContextCanceled.Code, extractLavaError(wrapped).Code,
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
			extractLavaError(wrapped), common.IsClientCancellation(wrapped, ctx))
		require.False(t, markUnhealthy, "a gRPC relay-race loser must not mark the endpoint unhealthy")
		require.False(t, backoff)
	})
}

// TestGRPCLocalError_NotDisqualifiedByRpcErrorSubstring pins the substring
// collision. GRPCStatusError renders as "gRPC error 1: context canceled", which
// lowercased contains "rpc error" INSIDE the word "gRPC" — so a guard written to
// exclude remote status errors was disqualifying our own local ones.
func TestGRPCLocalError_NotDisqualifiedByRpcErrorSubstring(t *testing.T) {
	local := &lavasession.GRPCStatusError{Code: 1, Message: "context canceled"}

	require.Contains(t, strings.ToLower(local.Error()), "rpc error",
		"precondition: the local error really does contain the substring the guard keys on")

	wrapped := classifyAndWrap(local, common.ChainFamily(-1), common.TransportGRPC)
	require.Equal(t, common.LavaErrorContextCanceled.Code, extractLavaError(wrapped).Code,
		"the guard must key on the full remote prefix, not the bare substring")
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
			got := extractLavaError(wrapped).Code

			require.NotEqual(t, common.LavaErrorContextCanceled.Code, got,
				"a remote cancel must not be attributed to local orchestration")
			require.NotEqual(t, common.LavaErrorContextDeadline.Code, got,
				"a remote deadline must not be attributed to local orchestration")
		})
	}
}

// TestRemoteStatusGuard_AppliesAcrossTransports makes the reach of the guard change
// explicit.
//
// stringConnectionFallbacks is a single package-level table and
// detectConnectionErrorFromString takes no transport argument, so narrowing
// "rpc error" to "rpc error: code =" changes classification for EVERY transport —
// not just gRPC. That is intended (the discriminator is the remote status prefix,
// which is transport-independent), but it was previously verified only on
// gRPC-shaped inputs. These cases pin both directions on a non-gRPC transport.
func TestRemoteStatusGuard_AppliesAcrossTransports(t *testing.T) {
	for _, transport := range []common.TransportType{common.TransportJsonRPC, common.TransportREST} {
		t.Run(transport.String()+"/remote-prefix-still-excluded", func(t *testing.T) {
			wrapped := classifyAndWrap(
				errors.New("rpc error: code = Canceled desc = context canceled"),
				common.ChainFamily(-1), transport)
			require.NotEqual(t, common.LavaErrorContextCanceled.Code, extractLavaError(wrapped).Code,
				"the remote-status prefix must exclude on non-gRPC transports too")
		})

		t.Run(transport.String()+"/plain-local-cancel-still-matches", func(t *testing.T) {
			wrapped := classifyAndWrap(
				errors.New("context canceled"),
				common.ChainFamily(-1), transport)
			require.Equal(t, common.LavaErrorContextCanceled.Code, extractLavaError(wrapped).Code,
				"a plain local cancellation must still classify as such")
		})

		// The discriminating case: contains "rpc error" but NOT "rpc error: code =".
		// The two cases above pass under both the old and the new guard, so only this
		// one actually detects the widening — and it does so on a non-gRPC transport,
		// which is the reach this test exists to pin. GRPCStatusError's rendering is
		// the concrete real-world instance of that string class.
		t.Run(transport.String()+"/bare-rpc-error-substring-no-longer-excluded", func(t *testing.T) {
			wrapped := classifyAndWrap(
				errors.New("gRPC error 1: context canceled"),
				common.ChainFamily(-1), transport)
			require.Equal(t, common.LavaErrorContextCanceled.Code, extractLavaError(wrapped).Code,
				"only the full remote-status prefix may exclude — a bare 'rpc error' substring must not, on any transport")
		})
	}
}
