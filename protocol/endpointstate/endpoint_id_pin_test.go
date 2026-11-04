package endpointstate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEndpointID_Pinned pins the published digest: the first 16 bytes of the url's sha256, in hex.
// Every pod in the fleet must derive the same id, so the format cannot change.
func TestEndpointID_Pinned(t *testing.T) {
	require.Equal(t, "d5a62e160e77f1895d9d14b715409ddf", EndpointID("https://node.example/v1/SECRETKEY"))
	require.Equal(t, "e3b0c44298fc1c149afbf4c8996fb924", EndpointID(""))
}
