package chainlib

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3935: a lava-extension the spec knows but no node on the router offers used to vanish with
// only a trace-level log. It is still not applied — nothing could serve it — but it is now
// recorded, so the reply can tell the caller the request was served without it.

func newJSONRPCMessageContainer() *baseChainMessageContainer {
	return &baseChainMessageContainer{
		api:           &spectypes.Api{Name: "eth_getBalance", ComputeUnits: 10},
		apiCollection: &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{ApiInterface: "jsonrpc"}},
	}
}

func archiveKey() extensionslib.ExtensionKey {
	return extensionslib.ExtensionKey{Extension: extensionslib.ArchiveExtension, ConnectionType: "jsonrpc"}
}

func TestOverrideExtensions_NoNodeOffersIt_RecordedAsUnavailable(t *testing.T) {
	msg := newJSONRPCMessageContainer()
	parser := extensionslib.NewExtensionParser(map[extensionslib.ExtensionKey]*spectypes.Extension{})

	msg.OverrideExtensions([]string{extensionslib.ArchiveExtension}, &parser)

	require.Empty(t, msg.GetExtensions(), "no node offers archive, so it cannot be applied")
	require.Equal(t, []string{extensionslib.ArchiveExtension}, msg.GetUnavailableExtensions(),
		"the dropped extension must be recorded so the reply can report it")
}

// The control: with a node offering it, the extension is applied and nothing is reported.
func TestOverrideExtensions_OfferedExtensionIsAppliedNotReported(t *testing.T) {
	msg := newJSONRPCMessageContainer()
	archive := &spectypes.Extension{Name: extensionslib.ArchiveExtension, CuMultiplier: 5}
	parser := extensionslib.NewExtensionParser(map[extensionslib.ExtensionKey]*spectypes.Extension{archiveKey(): archive})

	msg.OverrideExtensions([]string{extensionslib.ArchiveExtension}, &parser)

	require.Len(t, msg.GetExtensions(), 1)
	require.Equal(t, extensionslib.ArchiveExtension, msg.GetExtensions()[0].Name)
	require.Empty(t, msg.GetUnavailableExtensions())
}

// ExtensionParsing calls OverrideExtensions once for the override list and again for the additional
// list, so the same name can arrive twice across calls. It is reported once.
func TestOverrideExtensions_RepeatedRequestReportedOnce(t *testing.T) {
	msg := newJSONRPCMessageContainer()
	parser := extensionslib.NewExtensionParser(map[extensionslib.ExtensionKey]*spectypes.Extension{})

	msg.OverrideExtensions([]string{extensionslib.ArchiveExtension}, &parser)
	msg.OverrideExtensions([]string{extensionslib.ArchiveExtension}, &parser)

	require.Equal(t, []string{extensionslib.ArchiveExtension}, msg.GetUnavailableExtensions())
}
