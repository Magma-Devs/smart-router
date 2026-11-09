package rpcsmartrouter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSha256Short_Pinned pins the client-key part of a router subscription id.
func TestSha256Short_Pinned(t *testing.T) {
	require.Equal(t, "b13798", sha256Short("sticky-user-42"))
}
