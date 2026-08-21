package rpcsmartrouter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestMetadataFromDirectiveHeadersRoundTrip guards the earliest-block re-parse fix in
// updateProtocolMessageIfNeededWithNewEarliestData: directive headers rebuilt into metadata must
// survive being re-split by DirectiveHeaders (the same path ParseRelay uses), so a re-parsed
// protocol message keeps the client's smartrouter-extension (and other) directives. Without this, the
// rebuilt message's cache key would disagree with the original message's key on the archive path,
// undermining the per-directive cache lane.
func TestMetadataFromDirectiveHeadersRoundTrip(t *testing.T) {
	rpcss := &RPCSmartRouterServer{}

	t.Run("directives survive the round-trip", func(t *testing.T) {
		original := map[string]string{
			common.EXTENSION_OVERRIDE_HEADER_NAME:  "archive",
			common.FORCE_CACHE_REFRESH_HEADER_NAME: "true",
		}
		forwarded, directives := rpcss.DirectiveHeaders(metadataFromDirectiveHeaders(original))
		require.Empty(t, forwarded, "directive headers must not leak into forwarded metadata")
		require.Equal(t, original, directives, "directives must survive the metadata round-trip")
	})

	t.Run("empty maps to nil metadata", func(t *testing.T) {
		require.Nil(t, metadataFromDirectiveHeaders(nil))
		require.Nil(t, metadataFromDirectiveHeaders(map[string]string{}))
	})
}
