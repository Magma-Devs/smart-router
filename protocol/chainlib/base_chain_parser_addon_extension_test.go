package chainlib

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSeparateAddonsExtensionsArchiveIsExtension documents the classification the
// lava-extension: archive directive relies on. "archive" is registered only in the extension set
// (no shipped spec puts it in allowedAddons), so SeparateAddonsExtensions classifies it as an
// extension instead of swallowing it as an addon — which is why the directive header is honored
// and resolves to an archive extension rather than being dropped.
func TestSeparateAddonsExtensionsArchiveIsExtension(t *testing.T) {
	bcp := BaseChainParser{
		// Mirrors the ethereum spec: archive lives in the extension set; addons are debug/trace/bundler.
		allowedAddons:     map[string]bool{"": true, "debug": true, "trace": true, "bundler": true},
		allowedExtensions: map[string]struct{}{"archive": {}},
	}

	addons, extensions, err := bcp.SeparateAddonsExtensions(context.Background(), []string{"archive"})
	require.NoError(t, err)
	require.Empty(t, addons, "archive must not be classified as an addon")
	require.Equal(t, []string{"archive"}, extensions, "archive must be classified as an extension so the directive is honored")
}
