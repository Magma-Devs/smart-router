package utils_test

import (
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

func TestRedactPayload(t *testing.T) {
	// Ensure the global toggle is restored regardless of how the test exits, so
	// other tests in the package are not affected by the writes below.
	t.Cleanup(func() { utils.SetLogUnsafePayloads(false) })

	const body = `{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"]}`

	// Default (safe): payload is redacted to a length-only placeholder, and the
	// raw contents never appear.
	utils.SetLogUnsafePayloads(false)
	require.False(t, utils.LogUnsafePayloadsEnabled())

	redacted := utils.RedactPayload(body)
	require.NotContains(t, redacted, "eth_sendRawTransaction")
	require.NotContains(t, redacted, "0xdeadbeef")
	require.Contains(t, redacted, "REDACTED")
	require.Contains(t, redacted, "bytes", "redacted form should preserve the byte length for traffic shape")

	// Empty bodies stay empty (no noisy "[REDACTED](0 bytes)").
	require.Equal(t, "", utils.RedactPayload(""))

	// Unsafe mode: the raw payload passes through verbatim.
	utils.SetLogUnsafePayloads(true)
	require.True(t, utils.LogUnsafePayloadsEnabled())
	require.Equal(t, body, utils.RedactPayload(body))

	// Toggling back off re-redacts.
	utils.SetLogUnsafePayloads(false)
	require.True(t, strings.HasPrefix(utils.RedactPayload(body), "[REDACTED]"))
}
