package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestURLFingerprint_Pinned pins the url label: the first 4 bytes of the url's sha256, in hex.
// Dashboards and alerts key on it, so the format cannot change.
func TestURLFingerprint_Pinned(t *testing.T) {
	require.Equal(t, "d5a62e16", URLFingerprint("https://node.example/v1/SECRETKEY"))
}
