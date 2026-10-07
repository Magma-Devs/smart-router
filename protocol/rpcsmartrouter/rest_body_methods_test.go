package rpcsmartrouter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restBodyMethodPath is the route the test adds to the LAVA spec under PUT and PATCH. It has
// the shape of Concordium's wallet-proxy submissions: a stateful write whose body is opaque bytes.
const restBodyMethodPath = "/v0/submitRawTransaction"

// TestRESTListener_PutAndPatchBodiesReachTheNode sends a PUT and a PATCH write through the real
// REST listener, the router's parser and session selection, and the direct REST sender, and
// checks what the node receives: the client's method, its body byte for byte, and its
// Content-Type. The listener used to relay these methods with an empty body (MAG-4008). The
// chainlib listener test stops at a stub sender; this one covers the rest of the path to the wire.
func TestRESTListener_PutAndPatchBodiesReachTheNode(t *testing.T) {
	rand.InitRandomSeed()
	const contentType = "application/octet-stream"
	body := []byte("\x00\x01\xfe raw {bytes")

	type nodeRequest struct {
		method, path, contentType string
		body                      []byte
	}
	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			received := make(chan nodeRequest, 1)
			parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
				ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					data, readErr := io.ReadAll(r.Body)
					assert.NoError(t, readErr)
					select {
					case received <- nodeRequest{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type"), body: data}:
					default:
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"submissionId":"abc"}`))
				}), nil, "../../", nil)
			require.NoError(t, err)
			defer closeServer()

			// No bundled spec has a PUT or PATCH collection, so add one.
			spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "LAVA")
			require.NoError(t, err)
			spec.ApiCollections = append(spec.ApiCollections, &spectypes.ApiCollection{
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: method},
				Apis: []*spectypes.Api{{
					Name:         restBodyMethodPath,
					Enabled:      true,
					ComputeUnits: 10,
					Category:     spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS},
					BlockParsing: spectypes.BlockParser{ParserFunc: spectypes.PARSER_FUNC_DEFAULT, ParserArg: []string{"latest"}},
				}},
			})
			parser.SetSpec(spec)

			conn, err := lavasession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, spectypes.APIInterfaceRest)
			require.NoError(t, err)
			defer conn.Close()
			provider := lavasession.NewConsumerSessionWithProvider("rest-upstream", []*lavasession.Endpoint{{
				NetworkAddress: endpoint.NodeUrls[0].Url, Enabled: true,
				DirectConnections: []lavasession.DirectRPCConnection{conn},
			}}, 100000, 1, 1)
			provider.StaticProvider = true
			sessionManager, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
			rpcEndpoint.NetworkAddress = "127.0.0.1:0"
			require.NoError(t, sessionManager.UpdateAllProviders(1,
				map[uint64]*lavasession.ConsumerSessionsWithProvider{0: provider}, nil))
			logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			server := &RPCSmartRouterServer{
				chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
				rpcSmartRouterLogs: logs,
				consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
			}
			listener := chainlib.NewRestChainListener(ctx, rpcEndpoint, server, nil, logs)
			listenerDone := make(chan struct{})
			go func() {
				defer close(listenerDone)
				listener.Serve(ctx, common.ConsumerCmdFlags{})
			}()
			t.Cleanup(func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
				defer shutdownCancel()
				assert.NoError(t, listener.Shutdown(shutdownCtx))
				select {
				case <-listenerDone:
				case <-shutdownCtx.Done():
					t.Error("REST listener did not stop")
				}
			})
			require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)

			req, err := http.NewRequestWithContext(ctx, method, "http://"+listener.GetListeningAddress()+restBodyMethodPath, bytes.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", contentType)
			response, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer response.Body.Close()
			reply, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode, "reply: %s", reply)
			require.JSONEq(t, `{"submissionId":"abc"}`, string(reply))

			select {
			case node := <-received:
				require.Equal(t, method, node.method)
				require.Equal(t, restBodyMethodPath, node.path)
				require.Equal(t, body, node.body, "the node must receive the client's bytes")
				require.Equal(t, contentType, node.contentType, "the forwarded Content-Type must describe those bytes")
			case <-ctx.Done():
				t.Fatal("the write never reached the node")
			}
		})
	}
}
