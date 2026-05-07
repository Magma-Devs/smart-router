package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

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

type testProtocolMessageOpts struct {
	directiveHeaders  map[string]string
	extensions        []string
	forceCacheRefresh bool
	timeoutOverride   time.Duration
}

func newTestProtocolMessage(t *testing.T, opts testProtocolMessageOpts) (chainlib.ProtocolMessage, chainlib.ChainParser) {
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
	chainMsg.SetForceCacheRefresh(opts.forceCacheRefresh)
	if opts.timeoutOverride != 0 {
		chainMsg.TimeoutOverride(opts.timeoutOverride)
	}

	relayRequestData := &pairingtypes.RelayPrivateData{
		ApiUrl:         "/cosmos/base/tendermint/v1beta1/blocks/17",
		ConnectionType: http.MethodGet,
		Data:           []byte(""),
		Extensions:     opts.extensions,
	}
	return chainlib.NewProtocolMessage(chainMsg, opts.directiveHeaders, relayRequestData, "dapp", "1.2.3.4"), chainParser
}

// TestArchiveAddPreservesForceCacheRefresh is a regression for MAG-1653 Bug #1:
// addArchiveExtension previously rebuilt metadata from scratch with only the
// extension override, silently dropping lava-force-cache-refresh. The user-set
// directive must survive into the post-upgrade protocol message — both on the
// chainMessage flag (runtime check) and in the directiveHeaders map (parse-time
// input surface), so the two stay consistent.
func TestArchiveAddPreservesForceCacheRefresh(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, testProtocolMessageOpts{
		directiveHeaders:  map[string]string{common.FORCE_CACHE_REFRESH_HEADER_NAME: "true"},
		forceCacheRefresh: true,
	})
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.Equal(t, 1, relayParser.calls)
	require.NotSame(t, pm, upgraded, "upgrade should produce a new protocol message")
	require.True(t, upgraded.GetForceCacheRefresh(), "lava-force-cache-refresh must be preserved through archive add")
	require.Equal(t, "true", upgraded.GetDirectiveHeaders()[common.FORCE_CACHE_REFRESH_HEADER_NAME],
		"directiveHeaders map must stay in sync with the chainMessage flag")
}

// TestArchiveRemovePreservesForceCacheRefresh is the symmetric regression for
// removeArchiveExtension.
func TestArchiveRemovePreservesForceCacheRefresh(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, testProtocolMessageOpts{
		directiveHeaders:  map[string]string{common.FORCE_CACHE_REFRESH_HEADER_NAME: "true"},
		extensions:        []string{"archive"},
		forceCacheRefresh: true,
	})
	relayParser := &passthroughRelayParser{chainParser: parser}

	archiveStatus := &ArchiveStatus{}
	archiveStatus.isUpgraded.Store(true) // remove only runs after a prior upgrade
	archiveStatus.isArchive.Store(true)

	downgraded := removeArchiveExtension(context.Background(), pm, archiveStatus, relayParser)

	require.Equal(t, 1, relayParser.calls)
	require.NotSame(t, pm, downgraded)
	require.True(t, downgraded.GetForceCacheRefresh(), "lava-force-cache-refresh must be preserved through archive remove")
	require.Equal(t, "true", downgraded.GetDirectiveHeaders()[common.FORCE_CACHE_REFRESH_HEADER_NAME],
		"directiveHeaders map must stay in sync with the chainMessage flag")
}

// TestArchiveAddDoesNotInventForceCacheRefresh verifies the preserve helper does
// not flip the directive on for a request that didn't ask for cache bypass.
func TestArchiveAddDoesNotInventForceCacheRefresh(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, testProtocolMessageOpts{})
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.False(t, upgraded.GetForceCacheRefresh(), "must not enable cache bypass when the client did not request it")
}

// TestArchiveAddPreservesRelayTimeout is the same MAG-1653 shape applied to
// lava-relay-timeout: a client-set per-attempt timeout override must survive
// the rebuild, otherwise post-upgrade attempts fall back to the chain default
// instead of the user's value.
func TestArchiveAddPreservesRelayTimeout(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, testProtocolMessageOpts{
		directiveHeaders: map[string]string{common.RELAY_TIMEOUT_HEADER_NAME: "12s"},
		timeoutOverride:  12 * time.Second,
	})
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.Equal(t, 12*time.Second, upgraded.TimeoutOverride(), "lava-relay-timeout override must be preserved")
	require.Equal(t, (12 * time.Second).String(), upgraded.GetDirectiveHeaders()[common.RELAY_TIMEOUT_HEADER_NAME],
		"directiveHeaders map must stay in sync with the chainMessage timeout override")
}

func TestArchiveAddDoesNotInventRelayTimeout(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, testProtocolMessageOpts{})
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.Equal(t, time.Duration(0), upgraded.TimeoutOverride(), "must not synthesize a timeout the client did not request")
	require.NotContains(t, upgraded.GetDirectiveHeaders(), common.RELAY_TIMEOUT_HEADER_NAME)
}

// TestArchiveAddPreservesDebugRelay covers the directive-map-only directive.
// Unlike force-cache-refresh and relay-timeout, this one has no chainMessage
// field — its presence in the map is what gates emission of debug response
// headers (rpcsmartrouter_server.go:2339).
func TestArchiveAddPreservesDebugRelay(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, testProtocolMessageOpts{
		directiveHeaders: map[string]string{common.LAVA_DEBUG_RELAY: "true"},
	})
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.Contains(t, upgraded.GetDirectiveHeaders(), common.LAVA_DEBUG_RELAY,
		"lava-debug-relay must be preserved through archive add")
}

// TestArchiveAddDoesNotPreserveSelectProvider is a guard rail: the failover
// path may need to fall through to a different provider on retry, so the pin
// must NOT survive the rebuild. If this test ever fails, double-check the
// preserve helper hasn't grown a copy of lava-select-provider — that would
// regress test_2_1_one_provider_down_retry_to_next.
func TestArchiveAddDoesNotPreserveSelectProvider(t *testing.T) {
	pm, parser := newTestProtocolMessage(t, testProtocolMessageOpts{
		directiveHeaders: map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "lava@p1"},
	})
	relayParser := &passthroughRelayParser{chainParser: parser}

	upgraded := addArchiveExtension(context.Background(), pm, &ArchiveStatus{}, relayParser)

	require.NotContains(t, upgraded.GetDirectiveHeaders(), common.SELECT_PROVIDER_HEADER_NAME,
		"lava-select-provider must reset on retry so failover can choose a different provider")
}
