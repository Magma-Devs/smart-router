package relaycore

import (
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestRelayProcessorRetryPrevention(t *testing.T) {
	// The retry verdict the relay processor applies to protocol errors (ShouldRetryError).

	t.Run("shouldRetryWithNodeMethodNotFound", func(t *testing.T) {
		// MAG-2771: a node saying the method is not on its surface is a claim about that node, so
		// another provider is asked rather than the request failing.
		methodNotFound := common.NewLavaError(common.LavaErrorNodeMethodNotFound, `unsupported method "eth_test"`)

		result := chainlib.ShouldRetryError(methodNotFound)
		require.True(t, result, "a node's method-not-found must be retried on another provider")
	})

	t.Run("shouldRetryWithNormalError", func(t *testing.T) {
		// Test that normal errors still allow retries
		normalErr := errors.New("network timeout")

		result := chainlib.ShouldRetryError(normalErr)
		require.True(t, result, "Should allow retry for normal errors")
	})
}
