package common

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MAG-2648. LavaWrappedError used to discard the error it was built from, keeping only its
// text. Every sentinel check past the classification boundary therefore failed — most
// consequentially IsClientCancellation, which made the relay-race carve-out dead code.
// These tests pin the resolution paths that must all hold at once.

func TestLavaErrorWrapping_AllPathsResolve(t *testing.T) {
	cause := &url.Error{Op: "Post", URL: "http://node:8545", Err: context.Canceled}
	wrapped := NewLavaErrorWrapping(LavaErrorContextCanceled, cause)

	t.Run("cause sentinel is reachable", func(t *testing.T) {
		assert.True(t, errors.Is(wrapped, context.Canceled))
	})
	t.Run("lava error still matches by code", func(t *testing.T) {
		assert.True(t, errors.Is(wrapped, LavaErrorContextCanceled))
	})
	t.Run("lava error still resolves via As", func(t *testing.T) {
		var le *LavaError
		require.True(t, errors.As(wrapped, &le))
		assert.Equal(t, LavaErrorContextCanceled.Code, le.Code)
	})
	t.Run("wrapper type resolves via As", func(t *testing.T) {
		var lwe *LavaWrappedError
		require.True(t, errors.As(wrapped, &lwe))
		assert.Equal(t, LavaErrorContextCanceled.Code, lwe.LavaErr.Code)
	})
	t.Run("typed cause resolves via As", func(t *testing.T) {
		var ue *url.Error
		require.True(t, errors.As(wrapped, &ue))
		assert.Equal(t, "Post", ue.Op)
	})
	t.Run("errors.Unwrap stays non-nil", func(t *testing.T) {
		// The singular errors.Unwrap must keep working — utils/score walks chains with it,
		// and it returns nil for multi-unwrap types. This is why Unwrap is not []error.
		assert.NotNil(t, errors.Unwrap(wrapped))
	})
	t.Run("message shape is unchanged", func(t *testing.T) {
		assert.Contains(t, wrapped.Error(), cause.Error())
		assert.Contains(t, wrapped.Error(), LavaErrorContextCanceled.Description)
	})
}

// The causeless constructor must behave exactly as before — chainlib and this package
// both have tests that depend on it.
func TestLavaErrorWrapping_NoCauseIsUnchanged(t *testing.T) {
	wrapped := NewLavaError(LavaErrorChainNonceTooLow, "context")

	assert.Equal(t, LavaErrorChainNonceTooLow, errors.Unwrap(wrapped))
	assert.True(t, errors.Is(wrapped, LavaErrorChainNonceTooLow))
	assert.False(t, errors.Is(wrapped, context.Canceled))

	var le *LavaError
	require.True(t, errors.As(wrapped, &le))
	assert.Equal(t, LavaErrorChainNonceTooLow.Code, le.Code)
}

func TestLavaErrorWrapping_NilCause(t *testing.T) {
	wrapped := NewLavaErrorWrapping(LavaErrorUnknown, nil)
	require.NotNil(t, wrapped)
	assert.True(t, errors.Is(wrapped, LavaErrorUnknown))
	assert.NotNil(t, errors.Unwrap(wrapped))
}

// Collateral guard: wrapping a cause must not let an UNRELATED LavaError code match.
// The Is method compares codes, and the cause chain must not create false positives.
func TestLavaErrorWrapping_DoesNotOvermatch(t *testing.T) {
	cause := &url.Error{Op: "Post", URL: "http://node:8545", Err: context.Canceled}
	wrapped := NewLavaErrorWrapping(LavaErrorContextCanceled, cause)

	assert.False(t, errors.Is(wrapped, LavaErrorChainNonceTooLow))
	assert.False(t, errors.Is(wrapped, LavaErrorNodeRateLimited))
	assert.False(t, errors.Is(wrapped, context.DeadlineExceeded))
}

// IsClientCancellation had no test at all, which is the second half of why MAG-2648
// survived: the branch it guards was tested, the predicate feeding it never was.
func TestIsClientCancellation(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	liveCtx := context.Background()

	t.Run("raw cancellation on a cancelled context", func(t *testing.T) {
		assert.True(t, IsClientCancellation(context.Canceled, cancelledCtx))
	})
	t.Run("classified cancellation on a cancelled context", func(t *testing.T) {
		cause := &url.Error{Op: "Post", URL: "http://node:8545", Err: context.Canceled}
		assert.True(t, IsClientCancellation(NewLavaErrorWrapping(LavaErrorContextCanceled, cause), cancelledCtx))
	})
	t.Run("cancellation on a live context is not a client cancellation", func(t *testing.T) {
		assert.False(t, IsClientCancellation(context.Canceled, liveCtx))
	})
	t.Run("a node that says the words is not a cancellation", func(t *testing.T) {
		nodeErr := fmt.Errorf("rpc error: code = Canceled desc = context canceled")
		assert.False(t, IsClientCancellation(nodeErr, cancelledCtx),
			"a remote error whose TEXT says 'context canceled' carries no sentinel and must not match")
	})
	t.Run("deadline is not a cancellation", func(t *testing.T) {
		assert.False(t, IsClientCancellation(context.DeadlineExceeded, cancelledCtx),
			"a timeout is an endpoint fault and must still be penalised")
	})
	t.Run("nil inputs", func(t *testing.T) {
		assert.False(t, IsClientCancellation(nil, cancelledCtx))
		assert.False(t, IsClientCancellation(context.Canceled, nil))
	})
}
