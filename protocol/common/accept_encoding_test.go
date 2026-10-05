package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpstreamAcceptEncoding(t *testing.T) {
	playbook := []struct {
		configured string
		want       string
	}{
		{"", AcceptEncodingIdentity},
		{"identity", AcceptEncodingIdentity},
		{"gzip", AcceptEncodingGzip},
		{"GZIP", AcceptEncodingGzip},
		{" gzip ", AcceptEncodingGzip},
		// Refused at config load (ValidateAcceptEncoding); should one get here
		// anyway, it asks for identity, never for nothing.
		{"br", AcceptEncodingIdentity},
	}
	for _, tc := range playbook {
		nurl := NodeUrl{Url: "https://example.invalid", AcceptEncoding: tc.configured}
		require.Equal(t, tc.want, nurl.UpstreamAcceptEncoding(), "accept-encoding %q", tc.configured)
	}
}

func TestValidateAcceptEncoding(t *testing.T) {
	for _, ok := range []string{"", "identity", "gzip", "Gzip", " gzip "} {
		nurl := NodeUrl{Url: "https://example.invalid", AcceptEncoding: ok}
		require.NoError(t, nurl.ValidateAcceptEncoding(), "accept-encoding %q", ok)
	}

	for _, bad := range []string{"br", "deflate", "zstd", "gzip, br", "gzp", "*"} {
		nurl := NodeUrl{Url: "https://solana-mainnet.example.invalid/v2/SECRETKEY", AcceptEncoding: bad}
		err := nurl.ValidateAcceptEncoding()
		require.Error(t, err, "accept-encoding %q", bad)
		require.Contains(t, err.Error(), bad, "the error names the value to fix")
		require.Contains(t, err.Error(), "solana-mainnet.example.invalid", "and the url it is on")
		require.NotContains(t, err.Error(), "SECRETKEY", "without the key in the url's path")
	}
}
