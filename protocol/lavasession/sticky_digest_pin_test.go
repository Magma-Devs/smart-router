package lavasession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStickyIDDigest_Pinned pins the shared-store key: the first 16 bytes of the id's sha256, in
// hex. Pods on different versions must agree on it, so the format cannot change.
func TestStickyIDDigest_Pinned(t *testing.T) {
	require.Equal(t, "b13798dfb5e990c77498ac5329280c2b", StickyIDDigest("sticky-user-42"))
}
