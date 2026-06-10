package chainlib

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestHashCacheRequestExplicitExtensionDirectiveSeparatesLane is the regression guard for the
// archive-header cache collision: two requests can resolve to the SAME Extensions=["archive"]
// (one because the client sent lava-extension: archive, the other because the router auto-promoted
// an old-block request), yet the explicitly-directed request must land in its own cache lane so it
// does not get served the auto-promoted request's cached response.
func TestHashCacheRequestExplicitExtensionDirectiveSeparatesLane(t *testing.T) {
	const chainId = "ETH1"

	newRelayData := func() *pairingtypes.RelayPrivateData {
		return &pairingtypes.RelayPrivateData{
			ConnectionType: "POST",
			Data:           []byte(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1224567",false],"id":1}`),
			RequestBlock:   19017063,
			ApiInterface:   spectypes.APIInterfaceJsonRPC,
			Salt:           []byte{1, 2, 3},
			Extensions:     []string{"archive"}, // identical resolved extensions for both requests
		}
	}

	hashWith := func(directiveHeaders map[string]string) []byte {
		pm := &BaseProtocolMessage{
			relayRequestData: newRelayData(),
			directiveHeaders: directiveHeaders,
		}
		hash, _, err := pm.HashCacheRequest(chainId)
		require.NoError(t, err)
		return hash
	}

	// Bare request (auto-promoted to archive): no directive header.
	autoPromoted := hashWith(nil)
	// Explicit request: client sent lava-extension: archive.
	explicitArchive := hashWith(map[string]string{common.EXTENSION_OVERRIDE_HEADER_NAME: "archive"})

	// The fix: explicit archive must NOT collide with the auto-promoted lane despite identical Extensions.
	require.NotEqual(t, autoPromoted, explicitArchive,
		"explicit lava-extension:archive must produce a different cache key than an auto-promoted request with identical Extensions")

	// Backward compatibility: the no-directive path must reproduce the legacy package-level hash byte-for-byte,
	// so existing cache entries and all non-directive traffic are unaffected.
	legacy, _, err := HashCacheRequest(newRelayData(), chainId)
	require.NoError(t, err)
	require.Equal(t, legacy, autoPromoted, "absent directive must reproduce the historical cache key")

	// Deterministic lane: repeated explicit-archive requests share a lane (so the second one can hit the first).
	require.Equal(t, explicitArchive, hashWith(map[string]string{common.EXTENSION_OVERRIDE_HEADER_NAME: "archive"}))

	// Distinct directives get distinct lanes.
	require.NotEqual(t, explicitArchive, hashWith(map[string]string{common.EXTENSION_OVERRIDE_HEADER_NAME: "debug"}))

	// Normalization: case/whitespace variants collapse onto the same lane as "archive".
	require.Equal(t, explicitArchive, hashWith(map[string]string{common.EXTENSION_OVERRIDE_HEADER_NAME: "  ARCHIVE "}))
}

func TestNormalizeExtensionDirective(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"whitespace only", "   ", ""},
		{"plain", "archive", "archive"},
		{"uppercase", "Archive", "archive"},
		{"padded", "  archive  ", "archive"},
		{"sorted already", "archive,debug", "archive,debug"},
		{"unsorted", "debug,archive", "archive,debug"},
		{"mixed case and space", "DEBUG, Archive", "archive,debug"},
		{"empty segments", ",archive,,", "archive"},
		{"none", "none", "none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, normalizeExtensionDirective(tc.in))
		})
	}
}
