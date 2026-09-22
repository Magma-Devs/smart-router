package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// stubRelaySender satisfies RelaySenderInf. ParseRelay fails loudly: after the removal of the
// speculative archive upgrade nothing on the retry path may re-parse the request, so a call here
// is itself the bug this file guards against.
type stubRelaySender struct{ t *testing.T }

func (s *stubRelaySender) ParseRelay(
	_ context.Context, _, _, _, _, _ string, _ []pairingtypes.Metadata,
) (chainlib.ProtocolMessage, error) {
	s.t.Fatal("a retry re-parsed the request; the request must be re-sent unchanged, not rebuilt")
	return nil, nil
}

func (s *stubRelaySender) GetProcessingTimeout(chainlib.ChainMessage) (time.Duration, time.Duration) {
	return 30 * time.Second, time.Second
}

func (s *stubRelaySender) GetChainIdAndApiInterface() (string, string) { return "LAVA", "rest" }

func newRetryTestProtocolMessage(t *testing.T, extensions []string, directiveHeaders map[string]string) chainlib.ProtocolMessage {
	t.Helper()
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	relayRequestData := &pairingtypes.RelayPrivateData{
		ApiUrl:         "/cosmos/base/tendermint/v1beta1/blocks/17",
		ConnectionType: http.MethodGet,
		Data:           []byte(""),
		Extensions:     extensions,
	}
	return chainlib.NewProtocolMessage(chainMsg, directiveHeaders, relayRequestData, "dapp", "1.2.3.4")
}

func newRetryTestStateMachine(t *testing.T, pm chainlib.ProtocolMessage) *UnifiedRelayStateMachine {
	t.Helper()
	sm, err := NewUnifiedRelayStateMachine(
		context.Background(),
		lavasession.NewUsedProviders(nil),
		&stubRelaySender{t: t},
		pm,
		nil,
		false,
		StateMachineConfig{MaxRetries: 10, SendRelayAttempts: 3},
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	return sm.(*UnifiedRelayStateMachine)
}

// TestRetryReSendsTheSameRequest is the regression for the speculative archive upgrade.
//
// stateTransition used to rewrite the request between attempts — adding the archive extension on
// attempt 1 and removing it on attempt 2, keyed on the attempt number and nothing else. Because
// selection filters endpoints by extension, that turned one failed attempt into a pool of only
// the endpoints declaring the archive addon, reached into the backup tier to find one, and billed
// the archive CU multiplier for a request that was never archive.
//
// A retry now changes only WHICH endpoint serves the request.
func TestRetryReSendsTheSameRequest(t *testing.T) {
	t.Run("a plain request is not upgraded to archive on retry", func(t *testing.T) {
		pm := newRetryTestProtocolMessage(t, nil, nil)
		sm := newRetryTestStateMachine(t, pm)

		sm.stateTransition(nil)
		first := sm.GetProtocolMessage()
		require.Empty(t, common.GetExtensionNames(first.GetExtensions()), "attempt 0 has no extensions")

		for attempt := 1; attempt <= 3; attempt++ {
			sm.stateTransition(sm.getLatestState())
			require.Empty(t, common.GetExtensionNames(sm.GetProtocolMessage().GetExtensions()),
				"attempt %d must not acquire an extension the caller never asked for", attempt)
		}
	})

	t.Run("an archive request stays an archive request across retries", func(t *testing.T) {
		pm := newRetryTestProtocolMessage(t, []string{extensionslib.ArchiveExtension}, nil)
		sm := newRetryTestStateMachine(t, pm)

		sm.stateTransition(nil)
		require.True(t, sm.getLatestState().GetIsArchive(), "archive is decided before the first attempt")

		sm.stateTransition(sm.getLatestState())
		require.True(t, sm.getLatestState().GetIsArchive(), "a retry must not downgrade a genuine archive request")
	})

	t.Run("caller directives survive a retry because nothing is rebuilt", func(t *testing.T) {
		// MAG-1653 was about directives being dropped when the archive rebuild re-parsed the
		// request. With no rebuild the whole bug class is gone by construction: the retry carries
		// the identical message, so there is nothing to drop.
		headers := map[string]string{
			common.FORCE_CACHE_REFRESH_HEADER_NAME: "true",
			common.LAVA_DEBUG_RELAY:                "true",
		}
		pm := newRetryTestProtocolMessage(t, nil, headers)
		pm.SetForceCacheRefresh(true)
		pm.TimeoutOverride(42 * time.Second)

		sm := newRetryTestStateMachine(t, pm)
		sm.stateTransition(nil)
		sm.stateTransition(sm.getLatestState())

		retried := sm.GetProtocolMessage()
		require.True(t, retried.GetForceCacheRefresh())
		require.Equal(t, 42*time.Second, retried.TimeoutOverride())
		require.Equal(t, "true", retried.GetDirectiveHeaders()[common.LAVA_DEBUG_RELAY])
	})
}
