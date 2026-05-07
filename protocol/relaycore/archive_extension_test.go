package relaycore

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// passthroughRelayParser implements RelayParserInf by re-parsing through the
// real chainParser, mirroring the production flow.
type passthroughRelayParser struct {
	chainParser chainlib.ChainParser
	calls       int
}

func (p *passthroughRelayParser) ParseRelay(
	ctx context.Context,
	url string,
	req string,
	connectionType string,
	dappID string,
	consumerIp string,
	metadata []pairingtypes.Metadata,
) (chainlib.ProtocolMessage, error) {
	p.calls++

	directiveHeaders := map[string]string{}
	filtered := make([]pairingtypes.Metadata, 0, len(metadata))
	for _, m := range metadata {
		if _, ok := common.SPECIAL_LAVA_DIRECTIVE_HEADERS[m.Name]; ok {
			directiveHeaders[m.Name] = m.Value
		} else {
			filtered = append(filtered, m)
		}
	}

	var extensions extensionslib.ExtensionInfo
	if extOverride, ok := directiveHeaders[common.EXTENSION_OVERRIDE_HEADER_NAME]; ok && extOverride != "" {
		extensions = extensionslib.ExtensionInfo{LatestBlock: 0, AdditionalExtensions: []string{extOverride}}
	}

	chainMsg, err := p.chainParser.ParseMsg(url, []byte(req), connectionType, filtered, extensions)
	if err != nil {
		return nil, err
	}

	relayRequestData := &pairingtypes.RelayPrivateData{
		ApiUrl:         url,
		ConnectionType: connectionType,
		Data:           []byte(req),
	}
	return chainlib.NewProtocolMessage(chainMsg, directiveHeaders, relayRequestData, dappID, consumerIp), nil
}

func newTestProtocolMessage(t *testing.T, directiveHeaders map[string]string, extensions []string, forceCacheRefresh bool) (chainlib.ProtocolMessage, chainlib.ChainParser) {
	t.Helper()
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	chainMsg.SetForceCacheRefresh(forceCacheRefresh)

	relayRequestData := &pairingtypes.RelayPrivateData{
		ApiUrl:         "/cosmos/base/tendermint/v1beta1/blocks/17",
		ConnectionType: http.MethodGet,
		Data:           []byte(""),
		Extensions:     extensions,
	}
	return chainlib.NewProtocolMessage(chainMsg, directiveHeaders, relayRequestData, "dapp", "1.2.3.4"), chainParser
}

// TestArchiveAddPreservesForceCacheRefresh is a regression for MAG-1653 Bug #1:
// addArchiveExtension previously rebuilt metadata from scratch with only the
// extension override, silently dropping lava-force-cache-refresh. The user-set
// directive must survive into the post-upgrade protocol message.
func TestArchiveAddPreservesForceCacheRefresh(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, map[string]string{
		common.FORCE_CACHE_REFRESH_HEADER_NAME: "true",
	}, nil, true)
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.Equal(t, 1, relayParser.calls)
	require.NotSame(t, pm, upgraded, "upgrade should produce a new protocol message")
	require.True(t, upgraded.GetForceCacheRefresh(), "lava-force-cache-refresh must be preserved through archive add")
}

// TestArchiveRemovePreservesForceCacheRefresh is the symmetric regression for
// removeArchiveExtension.
func TestArchiveRemovePreservesForceCacheRefresh(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, map[string]string{
		common.FORCE_CACHE_REFRESH_HEADER_NAME: "true",
	}, []string{"archive"}, true)
	relayParser := &passthroughRelayParser{chainParser: parser}

	archiveStatus := &ArchiveStatus{}
	archiveStatus.isUpgraded.Store(true) // remove only runs after a prior upgrade
	archiveStatus.isArchive.Store(true)

	downgraded := removeArchiveExtension(context.Background(), pm, archiveStatus, relayParser)

	require.Equal(t, 1, relayParser.calls)
	require.NotSame(t, pm, downgraded)
	require.True(t, downgraded.GetForceCacheRefresh(), "lava-force-cache-refresh must be preserved through archive remove")
}

// TestArchiveAddDoesNotInventForceCacheRefresh verifies the preserve helper does
// not flip the directive on for a request that didn't ask for cache bypass.
func TestArchiveAddDoesNotInventForceCacheRefresh(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, map[string]string{}, nil, false)
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.False(t, upgraded.GetForceCacheRefresh(), "must not enable cache bypass when the client did not request it")
}
