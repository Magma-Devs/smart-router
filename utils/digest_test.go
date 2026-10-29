package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDigest(t *testing.T) {
	const url = "https://node.example/v1/SECRETKEY"
	require.Equal(t, "d5a62e16", Digest(url, 4))
	require.Equal(t, "d5a62e160e77f1895d9d14b715409ddf", Digest(url, 16))
	require.Equal(t, "d5a62e160e77f1895d9d14b715409ddf4f6276861da6b06f193de72974603b49", Digest(url, 32))
	require.Equal(t, "", Digest(url, 0))
}
