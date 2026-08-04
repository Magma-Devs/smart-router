package rpcsmartrouter

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// raceLoserError builds the error a relay-race loser actually produces: net/http
// returns a *url.Error carrying context.Canceled, and DoHTTPRequest wraps it with %w
// (direct_rpc_connection.go), so the sentinel is intact when it reaches classification.
func raceLoserError() error {
	return &url.Error{Op: "Post", URL: "http://bor-internal:8545", Err: context.Canceled}
}

// TestRaceLoser_SurvivesClassification is the regression test for MAG-2648.
//
// The pre-existing carve-out test (TestClassifyEndpointHealth_ClientCancellationCarvesOut)
// passes isClientCancellation as a literal `true`, so it verifies the branch but never
// that the flag is ever computed true on the real path. It stayed green for the entire
// life of the bug. This test starts from a real transport error and drives it through the
// actual wrapping the relay path performs.
func TestRaceLoser_SurvivesClassification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a sibling relay won the race

	transportErr := raceLoserError()

	// Precondition: before classification the sentinel is reachable.
	require.True(t, errors.Is(transportErr, context.Canceled))
	require.True(t, common.IsClientCancellation(transportErr, ctx),
		"precondition: the raw transport error IS a client cancellation")

	// Exactly what SendDirectRelay returns (direct_rpc_relay.go).
	wrapped := classifyAndWrap(transportErr, common.ChainFamily(-1), common.TransportJsonRPC)

	t.Run("classification is still correct", func(t *testing.T) {
		classified := extractLavaError(wrapped)
		require.NotNil(t, classified)
		require.Equal(t, common.LavaErrorContextCanceled.Code, classified.Code)
	})

	t.Run("sentinel survives wrapping", func(t *testing.T) {
		require.True(t, errors.Is(wrapped, context.Canceled),
			"the cause must stay reachable through LavaWrappedError")
	})

	t.Run("carve-out fires", func(t *testing.T) {
		require.True(t, common.IsClientCancellation(wrapped, ctx))
	})

	t.Run("endpoint is not blamed", func(t *testing.T) {
		classified := extractLavaError(wrapped)
		isClientCancel := common.IsClientCancellation(wrapped, ctx)

		markUnhealthy, backoff := classifyEndpointHealth(classified, isClientCancel)
		require.False(t, markUnhealthy, "a relay-race loser must not mark the endpoint unhealthy")
		require.False(t, backoff, "a relay-race loser must not trigger backoff")
	})
}

// TestClassification_PreservesOtherSentinels guards the general property, not just the
// context.Canceled case: whatever sentinel the cause carries must remain reachable.
func TestClassification_PreservesOtherSentinels(t *testing.T) {
	t.Run("deadline exceeded", func(t *testing.T) {
		cause := &url.Error{Op: "Post", URL: "http://bor:8545", Err: context.DeadlineExceeded}
		wrapped := classifyAndWrap(cause, common.ChainFamily(-1), common.TransportJsonRPC)

		require.True(t, errors.Is(wrapped, context.DeadlineExceeded))
		require.Equal(t, common.LavaErrorContextDeadline.Code, extractLavaError(wrapped).Code)
	})

	t.Run("typed http status error stays reachable", func(t *testing.T) {
		cause := &lavasession.HTTPStatusError{StatusCode: 503, Status: "503"}
		wrapped := classifyAndWrap(cause, common.ChainFamily(-1), common.TransportJsonRPC)

		var httpErr *lavasession.HTTPStatusError
		require.True(t, errors.As(wrapped, &httpErr))
		require.Equal(t, 503, httpErr.StatusCode)
	})
}

// TestDeadlineIsNotCarvedOut pins the deliberate asymmetry: a timeout is NOT a client
// cancellation. A slow or unreachable endpoint must still be penalised, otherwise this
// fix would paper over real faults.
func TestDeadlineIsNotCarvedOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cause := &url.Error{Op: "Post", URL: "http://bor:8545", Err: context.DeadlineExceeded}
	wrapped := classifyAndWrap(cause, common.ChainFamily(-1), common.TransportJsonRPC)

	require.False(t, common.IsClientCancellation(wrapped, ctx),
		"a deadline on a live context is an endpoint fault, not a cancellation")

	markUnhealthy, backoff := classifyEndpointHealth(extractLavaError(wrapped), false)
	require.True(t, markUnhealthy, "timeouts must still mark the endpoint unhealthy")
	require.True(t, backoff)
}
