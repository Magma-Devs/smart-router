package chainlib

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-4524. A DELETE used to get 204 and an empty body from the shared listener setup, before any
// route ran: the caller was told it was done when nothing ran. On JSON-RPC and Tendermint RPC it is
// now refused exactly as a PUT is: 405, with the Allow a PUT gets on the same path, and no relay.
// OPTIONS, the CORS preflight, keeps its 204.
func TestListeners_ADeleteIsRefusedLikeAPut(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	send := func(t *testing.T, method, url string) (status int, allow, body string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(`{}`))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode, resp.Header.Get("Allow"), string(raw)
	}

	for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC} {
		relay := &scriptedRelay{}
		addr := serveGoldenListener(t, ctx, apiInterface, relay)
		for _, path := range []string{"/", "/some/other/path", "/ws", common.DEFAULT_HEALTH_PATH} {
			t.Run(apiInterface+" "+path, func(t *testing.T) {
				status, allow, body := send(t, http.MethodDelete, "http://"+addr+path)
				putStatus, putAllow, putBody := send(t, http.MethodPut, "http://"+addr+path)

				require.Equal(t, http.StatusMethodNotAllowed, status, "a DELETE is refused, not answered done")
				require.Equal(t, putStatus, status, "refused as a PUT is")
				require.NotEmpty(t, allow, "a 405 names the methods the path does serve (RFC 9110 15.5.6)")
				require.Equal(t, putAllow, allow)
				require.NotContains(t, allow, http.MethodDelete)
				require.Equal(t, putBody, body)

				relay.mu.Lock()
				calls := len(relay.calls)
				relay.mu.Unlock()
				require.Zero(t, calls, "nothing is relayed")

				optionsStatus, _, _ := send(t, http.MethodOptions, "http://"+addr+path)
				require.Equal(t, http.StatusNoContent, optionsStatus, "the CORS preflight keeps its 204")
			})
		}
	}
}
