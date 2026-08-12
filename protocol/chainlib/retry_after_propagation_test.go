package chainlib

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Capturing Retry-After is only worth anything if it survives the trip out of the chain proxy.
// Each of the three status-code call sites hands its error to FormatWarning, where passing
// it as an attribute instead of the cause severs the chain silently — the error still reads
// right in the log and no longer unwraps. These pin the whole path per interface.
func TestRetryAfterPropagatesOutOfChainProxies(t *testing.T) {
	rateLimitHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	requireRateLimited := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.ErrorIs(t, err, common.StatusCodeError429, "the sentinel must survive the proxy's error wrapping")
		d, ok := common.RetryAfterFrom(err)
		require.True(t, ok, "the upstream's Retry-After must reach the caller")
		require.Equal(t, 2*time.Minute, d)
	}

	t.Run("rest", func(t *testing.T) {
		ctx := context.Background()
		chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, rateLimitHandler, nil, "../../", nil)
		require.NoError(t, err)
		if closeServer != nil {
			defer closeServer()
		}

		parsing, apiCollection, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
		require.True(t, ok)
		chainMessage, err := chainParser.ParseMsg(parsing.ApiName, []byte{}, apiCollection.CollectionData.Type, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)

		_, _, _, _, _, err = chainRouter.SendNodeMsg(ctx, nil, chainMessage, nil)
		requireRateLimited(t, err)
	})

	t.Run("jsonrpc", func(t *testing.T) {
		ctx := context.Background()
		wsHandler := createWebSocketHandler(func(message string) string {
			return `{"jsonrpc":"2.0","id":1,"result":"0x10a7a08"}`
		})
		chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, rateLimitHandler, wsHandler, "../../", nil)
		require.NoError(t, err)
		if closeServer != nil {
			defer closeServer()
		}

		chainMessage, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)

		_, _, _, _, _, err = chainRouter.SendNodeMsg(ctx, nil, chainMessage, nil)
		requireRateLimited(t, err)
	})

	t.Run("tendermint uri", func(t *testing.T) {
		ctx := context.Background()
		chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceTendermintRPC, rateLimitHandler, nil, "../../", nil)
		require.NoError(t, err)
		if closeServer != nil {
			defer closeServer()
		}

		// Empty data routes the parser down the URI branch, which is the SendURI call site —
		// the JSON-RPC branch of the same proxy goes through the shared rpcclient instead.
		chainMessage, err := chainParser.ParseMsg("status", nil, "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)

		_, _, _, _, _, err = chainRouter.SendNodeMsg(ctx, nil, chainMessage, nil)
		requireRateLimited(t, err)
	})
}
