package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

func raceLoserError() error {
	// net/http returns a *url.Error carrying context.Canceled; DoHTTPRequest wraps it
	// with %w, so the sentinel is intact when it reaches classification.
	return &url.Error{Op: "Post", URL: "http://bor-internal:8545", Err: context.Canceled}
}

// TestRaceLoser_SurvivesClassification is the regression test for MAG-2648.
//
// The pre-existing carve-out test passes isClientCancellation as a literal `true`, so it
// verifies the branch but never that the flag is ever computed true on the real path. It
// stayed green for the entire life of the bug. This starts from a real transport error and
// drives it through the actual wrapping the relay path performs.
func TestRaceLoser_SurvivesClassification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a sibling relay won the race

	transportErr := raceLoserError()
	require.True(t, common.IsClientCancellation(transportErr, ctx),
		"precondition: the raw transport error IS a client cancellation")

	wrapped := classifyAndWrap(transportErr, common.ChainFamily(-1), common.TransportJsonRPC)

	t.Run("classification is still correct", func(t *testing.T) {
		require.Equal(t, common.RouterErrorContextCanceled.Code, extractRouterError(wrapped).Code)
	})
	t.Run("sentinel survives wrapping", func(t *testing.T) {
		require.True(t, errors.Is(wrapped, context.Canceled))
	})
	t.Run("carve-out fires", func(t *testing.T) {
		require.True(t, common.IsClientCancellation(wrapped, ctx))
	})
	t.Run("endpoint is not blamed", func(t *testing.T) {
		markUnhealthy, backoff := classifyEndpointHealth(extractRouterError(wrapped), common.IsClientCancellation(wrapped, ctx))
		require.False(t, markUnhealthy, "a relay-race loser must not mark the endpoint unhealthy")
		require.False(t, backoff)
	})
}

// COLLATERAL GUARD — the most important test in this file.
//
// The fix must not swallow genuine faults. Every error below is a real endpoint problem
// and must still mark the endpoint unhealthy, even while the context is cancelled (which
// is the state a race loser is in). If any of these stop being penalised, the fix has
// traded a false-positive bug for a much worse false-negative one.
func TestGenuineFaults_StillPenalised(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name string
		err  error
	}{
		{"connection refused", &url.Error{Op: "Post", URL: "http://n:8545", Err: syscall.ECONNREFUSED}},
		{"connection reset", &url.Error{Op: "Post", URL: "http://n:8545", Err: syscall.ECONNRESET}},
		{"host unreachable", &url.Error{Op: "Post", URL: "http://n:8545", Err: syscall.EHOSTUNREACH}},
		{"deadline exceeded", &url.Error{Op: "Post", URL: "http://n:8545", Err: context.DeadlineExceeded}},
		{"eof", fmt.Errorf("http request failed: %w", errors.New("EOF"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := classifyAndWrap(tc.err, common.ChainFamily(-1), common.TransportJsonRPC)

			require.False(t, common.IsClientCancellation(wrapped, cancelledCtx),
				"%s must NOT be mistaken for a client cancellation even on a cancelled ctx", tc.name)

			markUnhealthy, backoff := classifyEndpointHealth(
				extractRouterError(wrapped),
				common.IsClientCancellation(wrapped, cancelledCtx),
			)
			require.True(t, markUnhealthy, "%s must still mark the endpoint unhealthy", tc.name)
			require.True(t, backoff, "%s must still trigger backoff", tc.name)
		})
	}
}

// Rate limiting keeps its distinct treatment: healthy but busy — backoff without
// marking unhealthy. The new cancellation branch must not have collapsed this case.
func TestRateLimit_StillHealthyButBusy(t *testing.T) {
	markUnhealthy, backoff := classifyEndpointHealth(common.RouterErrorNodeRateLimited, false)
	require.False(t, markUnhealthy, "a rate-limited endpoint is healthy, just busy")
	require.True(t, backoff)
}

// Cross-validation stragglers run on a WithoutCancel context (MAG-2187), so a batch
// cancel must NOT make them look cancelled — they should complete and be scored normally.
func TestCrossValidationStraggler_NotSeenAsCancelled(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	detached := context.WithoutCancel(parent)
	cancel() // batch cancel fires; the detached straggler must be unaffected

	require.NoError(t, detached.Err(), "precondition: WithoutCancel survives the parent cancel")

	wrapped := classifyAndWrap(raceLoserError(), common.ChainFamily(-1), common.TransportJsonRPC)
	require.False(t, common.IsClientCancellation(wrapped, detached),
		"a detached CV straggler must not be classified as a client cancellation")
}

// Sentinels other than context.Canceled must remain reachable through classification —
// the wrapping change is general, not special-cased to one error.
func TestClassification_PreservesOtherSentinels(t *testing.T) {
	t.Run("deadline exceeded", func(t *testing.T) {
		cause := &url.Error{Op: "Post", URL: "http://bor:8545", Err: context.DeadlineExceeded}
		wrapped := classifyAndWrap(cause, common.ChainFamily(-1), common.TransportJsonRPC)
		require.True(t, errors.Is(wrapped, context.DeadlineExceeded))
		require.Equal(t, common.RouterErrorContextDeadline.Code, extractRouterError(wrapped).Code)
	})
	t.Run("typed http status error stays reachable", func(t *testing.T) {
		cause := &lavasession.HTTPStatusError{StatusCode: 503, Status: "503"}
		wrapped := classifyAndWrap(cause, common.ChainFamily(-1), common.TransportJsonRPC)
		var httpErr *lavasession.HTTPStatusError
		require.True(t, errors.As(wrapped, &httpErr))
		require.Equal(t, 503, httpErr.StatusCode)
	})
}
