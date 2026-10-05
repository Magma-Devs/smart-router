package chainlib

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
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

// The router's own archive promotion for a deep eth_call is kept apart from the caller's
// lava-extension so that it is never reported back, but it must still be applied when a node
// offers archive.
func TestParseMsg_DeepEthCallStillGetsArchiveWhenANodeOffersIt(t *testing.T) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "ETH1")
	require.NoError(t, err)
	chainParser, err := NewJrpcChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(spec)
	chainParser.SetPolicyFromAddonAndExtensionMap(map[string]struct{}{extensionslib.ArchiveExtension: {}})

	// One block deeper than ethCallArchiveBlockDepth: deep enough for the eth_call promotion, but
	// not for ETH1's archive rule, which starts a block further back. So archive on the eth_call can
	// only come from the promotion, and the eth_getBalance at the same block shows the rule is idle.
	const tip = 1_000_000
	block := uint64(tip - ethCallArchiveBlockDepth - 1)
	parse := func(body string) ChainMessage {
		t.Helper()
		chainMessage, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: tip})
		require.NoError(t, err)
		return chainMessage
	}

	ethCall := parse(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x1111111111111111111111111111111111111111","data":"0x"},"0x%x"]}`, block))
	require.Len(t, ethCall.GetExtensions(), 1, "the promotion must still route a deep eth_call to archive")
	require.Equal(t, extensionslib.ArchiveExtension, ethCall.GetExtensions()[0].Name)
	require.Empty(t, ethCall.GetUnavailableExtensions())

	getBalance := parse(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x%x"]}`, block))
	require.Empty(t, getBalance.GetExtensions(), "the archive rule does not reach this block")
}
