package chainlib

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-4524. A DELETE used to get 204 and an empty body from the shared listener setup, before any
// route ran: the caller was told it was done when nothing ran. It is now refused with 405 at every
// path, and nothing is relayed:
//   - JSON-RPC answers it, and a PUT and a PATCH, the way it answers a GET without a WebSocket
//     upgrade: Allow: POST, the only method it serves outside the upgrade (rulebook Part IV,
//     section 1). A PUT and a PATCH used to get fiber's Allow of every routed method.
//   - Tendermint RPC answers it the way fiber answers a PUT: an Allow of the methods it serves.
//
// No Allow advertises DELETE. OPTIONS, the CORS preflight, keeps its 204.
func TestListeners_ADeleteIsRefused(t *testing.T) {
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
	paths := []string{"/", "/some/other/path", "/status", "/ws", "/websocket", common.DEFAULT_HEALTH_PATH}

	for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC} {
		relay := &recordingRelaySender{send: answeringWith(nil, nil)}
		addr := serveGoldenListener(t, ctx, apiInterface, relay)
		for _, path := range paths {
			t.Run(apiInterface+" "+path, func(t *testing.T) {
				status, allow, body := send(t, http.MethodDelete, "http://"+addr+path)
				putStatus, putAllow, putBody := send(t, http.MethodPut, "http://"+addr+path)
				patchStatus, patchAllow, patchBody := send(t, http.MethodPatch, "http://"+addr+path)

				require.Equal(t, http.StatusMethodNotAllowed, status, "a DELETE is refused, not answered done")
				require.Equal(t, http.StatusMethodNotAllowed, putStatus, "a PUT is still refused")
				require.Equal(t, http.StatusMethodNotAllowed, patchStatus, "a PATCH is still refused")
				require.NotContains(t, putAllow, http.MethodDelete, "a PUT's Allow does not advertise DELETE")
				require.NotContains(t, patchAllow, http.MethodDelete, "a PATCH's Allow does not advertise DELETE")
				require.NotContains(t, allow, http.MethodDelete)
				switch apiInterface {
				case spectypes.APIInterfaceJsonRPC:
					for method, reply := range map[string][2]string{
						http.MethodDelete: {allow, body},
						http.MethodPut:    {putAllow, putBody},
						http.MethodPatch:  {patchAllow, patchBody},
					} {
						require.Equal(t, fiber.MethodPost, reply[0], "%s: the only method served outside the upgrade", method)
						require.Equal(t, "Method Not Allowed", reply[1], method)
					}
				case spectypes.APIInterfaceTendermintRPC:
					require.Equal(t, putAllow, allow, "refused as a PUT is")
					require.Equal(t, putBody, body)
				}

				calls := len(relay.sends())
				require.Zero(t, calls, "nothing is relayed")

				optionsStatus, _, _ := send(t, http.MethodOptions, "http://"+addr+path)
				require.Equal(t, http.StatusNoContent, optionsStatus, "the CORS preflight keeps its 204")
			})
		}
	}
}
