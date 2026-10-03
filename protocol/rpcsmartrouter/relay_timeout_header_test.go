package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3988: `lava-relay-timeout: -1s` parsed cleanly, reached time.NewTicker in the relay
// goroutine, and the panic ended the router process. This drives the real path — header, parse,
// GetProcessingTimeout — and requires the window the state machine receives to be positive.
func TestRelayTimeoutHeader_WindowHandedToTheStateMachine(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
		nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)
	rpcss := &RPCSmartRouterServer{chainParser: chainParser}

	windowFor := func(t *testing.T, header string) time.Duration {
		t.Helper()
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil,
			extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		rpcss.HandleDirectiveHeadersForMessage(chainMsg, map[string]string{common.RELAY_TIMEOUT_HEADER_NAME: header})
		_, window := rpcss.GetProcessingTimeout(chainMsg)
		return window
	}
	defaultWindow := windowFor(t, "not-a-duration") // unparseable: the router's own window
	require.Positive(t, defaultWindow)

	for _, header := range []string{"-1s", "-1ns", "-2562047h", "0s", "0"} {
		t.Run("ignored "+header, func(t *testing.T) {
			require.Equal(t, defaultWindow, windowFor(t, header),
				"a non-positive lava-relay-timeout must be ignored like an unparseable one")
		})
	}
	// The discriminating half: a valid value still takes effect, including one below the floor.
	for header, want := range map[string]time.Duration{"12s": 12 * time.Second, "250ms": 250 * time.Millisecond} {
		t.Run("honoured "+header, func(t *testing.T) {
			require.Equal(t, want, windowFor(t, header))
		})
	}
}
