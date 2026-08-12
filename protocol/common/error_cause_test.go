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

// MAG-2648. RouterWrappedError used to discard the error it was built from, keeping only its
// text. Every sentinel check past the classification boundary therefore failed — most
// consequentially IsClientCancellation, which made the relay-race carve-out dead code.
// These tests pin the resolution paths that must all hold at once.

func TestRouterErrorWrapping_AllPathsResolve(t *testing.T) {
	cause := &url.Error{Op: "Post", URL: "http://node:8545", Err: context.Canceled}
	wrapped := NewRouterErrorWrapping(RouterErrorContextCanceled, cause)

	t.Run("cause sentinel is reachable", func(t *testing.T) {
		assert.True(t, errors.Is(wrapped, context.Canceled))
	})
	t.Run("router error still matches by code", func(t *testing.T) {
		assert.True(t, errors.Is(wrapped, RouterErrorContextCanceled))
	})
	t.Run("router error still resolves via As", func(t *testing.T) {
		var le *RouterError
		require.True(t, errors.As(wrapped, &le))
		assert.Equal(t, RouterErrorContextCanceled.Code, le.Code)
	})
	t.Run("wrapper type resolves via As", func(t *testing.T) {
		var lwe *RouterWrappedError
		require.True(t, errors.As(wrapped, &lwe))
		assert.Equal(t, RouterErrorContextCanceled.Code, lwe.RouterErr.Code)
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
		assert.Contains(t, wrapped.Error(), RouterErrorContextCanceled.Description)
	})
}

// The causeless constructor must behave exactly as before — chainlib and this package
// both have tests that depend on it.
func TestRouterErrorWrapping_NoCauseIsUnchanged(t *testing.T) {
	wrapped := NewRouterError(RouterErrorChainNonceTooLow, "context")

	assert.Equal(t, RouterErrorChainNonceTooLow, errors.Unwrap(wrapped))
	assert.True(t, errors.Is(wrapped, RouterErrorChainNonceTooLow))
	assert.False(t, errors.Is(wrapped, context.Canceled))

	var le *RouterError
	require.True(t, errors.As(wrapped, &le))
	assert.Equal(t, RouterErrorChainNonceTooLow.Code, le.Code)
}

func TestRouterErrorWrapping_NilCause(t *testing.T) {
	wrapped := NewRouterErrorWrapping(RouterErrorUnknown, nil)
	require.NotNil(t, wrapped)
	assert.True(t, errors.Is(wrapped, RouterErrorUnknown))
	assert.NotNil(t, errors.Unwrap(wrapped))
}

// Collateral guard: wrapping a cause must not let an UNRELATED RouterError code match.
// The Is method compares codes, and the cause chain must not create false positives.
func TestRouterErrorWrapping_DoesNotOvermatch(t *testing.T) {
	cause := &url.Error{Op: "Post", URL: "http://node:8545", Err: context.Canceled}
	wrapped := NewRouterErrorWrapping(RouterErrorContextCanceled, cause)

	assert.False(t, errors.Is(wrapped, RouterErrorChainNonceTooLow))
	assert.False(t, errors.Is(wrapped, RouterErrorNodeRateLimited))
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
		assert.True(t, IsClientCancellation(NewRouterErrorWrapping(RouterErrorContextCanceled, cause), cancelledCtx))
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

// Nil-guard coverage. Raised in review of PR #252: the `As` method added by MAG-2648 was
// the new surface — returning true with a nil *RouterError both hands the caller a nil and
// halts errors.As before it can reach a real one deeper in the chain. Error/Is/Unwrap had
// the same unguarded dereference (pre-existing), and Error's failure mode is a panic
// inside a logging call.
//
// The constructors always set RouterErr today, so every case here is latent. They are pinned
// anyway: this type exists because a silently unreachable resolution path was expensive.
func TestRouterWrappedError_NilLavaErrIsSafe(t *testing.T) {
	bare := &RouterWrappedError{Context: "some context"}

	t.Run("Error does not panic", func(t *testing.T) {
		require.NotPanics(t, func() { _ = bare.Error() })
		assert.Equal(t, "some context", bare.Error())
	})

	t.Run("Error with no context does not panic", func(t *testing.T) {
		empty := &RouterWrappedError{}
		require.NotPanics(t, func() { _ = empty.Error() })
	})

	t.Run("Is reports no match instead of panicking", func(t *testing.T) {
		require.NotPanics(t, func() { _ = errors.Is(bare, RouterErrorContextCanceled) })
		assert.False(t, errors.Is(bare, RouterErrorContextCanceled))
	})

	t.Run("As declines instead of yielding a nil", func(t *testing.T) {
		var le *RouterError
		require.NotPanics(t, func() { _ = errors.As(bare, &le) })
		assert.False(t, errors.As(bare, &le), "As must not claim a match it cannot satisfy")
		assert.Nil(t, le, "target must be left untouched")
	})

	t.Run("As does not halt the walk to a real RouterError deeper in the chain", func(t *testing.T) {
		// The failure this guard exists for: an unguarded As returns true at the outer
		// wrapper, so errors.As stops and never reaches the resolvable inner one.
		inner := NewRouterError(RouterErrorChainNonceTooLow, "inner")
		outer := &RouterWrappedError{Context: "outer", cause: inner}

		var le *RouterError
		require.True(t, errors.As(outer, &le), "the walk must continue past a RouterErr-less wrapper")
		assert.Equal(t, RouterErrorChainNonceTooLow.Code, le.Code)
	})

	t.Run("Unwrap yields no typed nil", func(t *testing.T) {
		// A typed nil here would be a non-nil error interface holding a nil *RouterError,
		// and the next errors.Is would call RouterError.Is on a nil receiver.
		require.NotPanics(t, func() { _ = errors.Unwrap(bare) })
		assert.Nil(t, errors.Unwrap(bare))
		require.NotPanics(t, func() { _ = errors.Is(bare, RouterErrorUnknown) })
	})
}

func TestRouterWrappedError_NilReceiverIsSafe(t *testing.T) {
	var nilWrapped *RouterWrappedError
	require.NotPanics(t, func() { _ = nilWrapped.Error() })
	require.NotPanics(t, func() { _ = nilWrapped.Is(RouterErrorUnknown) })
	require.NotPanics(t, func() { _ = nilWrapped.Unwrap() })

	var le *RouterError
	require.NotPanics(t, func() { _ = nilWrapped.As(&le) })
	assert.False(t, nilWrapped.As(&le))
}
