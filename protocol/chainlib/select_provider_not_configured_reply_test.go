package chainlib

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The provider names the router's own error text lists for a lava-select-provider refusal. None of
// them may reach the caller.
var configuredProviderNames = []string{"provider-name-alpha", "provider-name-beta"}

// notConfiguredRelayError is the error SendRelay returns for a lava-select-provider name that none
// of the router's providers carries, built the way GetSessions builds it: the marker wrapped around
// the selection's own error, whose text lists every valid provider name.
func notConfiguredRelayError() error {
	selection := utils.FormatError("Selected provider not available", routersession.SelectedProviderUnavailableError,
		utils.LogAttr("selectedProvider", "no-such-provider"),
		utils.LogAttr("validProviders", configuredProviderNames))
	return fmt.Errorf("%w: %w", routersession.SelectedProviderNotConfiguredError, selection)
}

// MAG-3800. A request pinned to a provider name the router does not have is the caller's mistake,
// so every HTTP interface answers 400, not the 500 that says the server failed. The reply carries
// the one fixed sentence, the request's Lava-Guid, and none of the router's provider names, in
// masked mode too.
func TestListeners_ASelectedProviderThatIsNotConfiguredIsAClientError(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	jsonRPCBody := `{"jsonrpc":"2.0","id":42,"method":"eth_blockNumber","params":[]}`
	requests := []struct {
		name, apiInterface, method, path, body string
	}{
		{"jsonrpc POST", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/", jsonRPCBody},
		{"tendermintrpc POST", spectypes.APIInterfaceTendermintRPC, http.MethodPost, "/", jsonRPCBody},
		{"tendermintrpc GET", spectypes.APIInterfaceTendermintRPC, http.MethodGet, "/status", ""},
		{"rest GET", spectypes.APIInterfaceRest, http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/latest", ""},
		{"rest POST", spectypes.APIInterfaceRest, http.MethodPost, "/cosmos/tx/v1beta1/simulate", `{"tx":1}`},
		{"graphql POST", spectypes.APIInterfaceGraphQL, http.MethodPost, "/graphql", `{"query":"{ chainIdentifier }"}`},
	}
	for _, masked := range []string{"false", "true"} {
		t.Run("masked errors "+masked, func(t *testing.T) {
			original := metrics.ReturnMaskedErrors
			metrics.ReturnMaskedErrors = masked
			t.Cleanup(func() { metrics.ReturnMaskedErrors = original })

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			relay := &scriptedRelay{err: notConfiguredRelayError()}
			addrs := map[string]string{}
			for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC, spectypes.APIInterfaceRest, spectypes.APIInterfaceGraphQL} {
				addrs[apiInterface] = serveGoldenListener(t, ctx, apiInterface, relay)
			}

			for _, req := range requests {
				t.Run(req.name, func(t *testing.T) {
					var body io.Reader
					if req.body != "" {
						body = strings.NewReader(req.body)
					}
					httpReq, err := http.NewRequestWithContext(ctx, req.method, "http://"+addrs[req.apiInterface]+req.path, body)
					require.NoError(t, err)
					httpReq.Header.Set("Content-Type", "application/json")
					resp, err := http.DefaultClient.Do(httpReq)
					require.NoError(t, err)
					respBody, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					require.NoError(t, resp.Body.Close())
					text := string(respBody)

					require.Equal(t, http.StatusBadRequest, resp.StatusCode, "a refused request is a client error: %s", text)
					require.Contains(t, text, "Selected provider is not configured")
					for _, name := range configuredProviderNames {
						require.NotContains(t, text, name, "the reply must not list the router's providers")
					}
					guid := resp.Header.Get(common.GUID_HEADER_NAME)
					require.NotEmpty(t, guid, "the reply carries the request's id")
					require.Contains(t, text, guid, "the body's guid is the header's")

					switch req.apiInterface {
					case spectypes.APIInterfaceJsonRPC:
						var reply struct {
							ID    json.RawMessage `json:"id"`
							Error struct {
								Code    int    `json:"code"`
								Message string `json:"message"`
							} `json:"error"`
						}
						require.NoError(t, json.Unmarshal(respBody, &reply))
						require.Equal(t, "42", string(reply.ID), "the caller's id")
						require.Equal(t, -32000, reply.Error.Code)
						require.Equal(t, "Selected provider is not configured", reply.Error.Message)
					case spectypes.APIInterfaceGraphQL:
						var reply map[string]json.RawMessage
						require.NoError(t, json.Unmarshal(respBody, &reply))
						require.NotContains(t, reply, "data", "a refusal has no data key")
						var errorsList []struct {
							Message    string            `json:"message"`
							Extensions map[string]string `json:"extensions"`
						}
						require.NoError(t, json.Unmarshal(reply["errors"], &errorsList))
						require.Len(t, errorsList, 1)
						require.Equal(t, "Selected provider is not configured", errorsList[0].Message)
						require.Equal(t, "REQUEST_REFUSED", errorsList[0].Extensions["code"])
						require.Equal(t, guid, errorsList[0].Extensions["guid"])
					}
				})
			}
		})
	}
}

// Any other pinned refusal (a blocked provider, one that lacks the add-on) is not the caller's
// mistake and keeps today's reply.
func TestListeners_AnUnavailableSelectedProviderKeepsTheServerReply(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relay := &scriptedRelay{err: utils.FormatError("Selected provider not available", routersession.SelectedProviderUnavailableError)}
	addr := serveGoldenListener(t, ctx, spectypes.APIInterfaceJsonRPC, relay)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// gRPC gives a wrong request INVALID_ARGUMENT. A plain error, which every other relay failure
// returns, reaches the client as Unknown.
func TestGrpcChainListener_ASelectedProviderThatIsNotConfiguredIsInvalidArgument(t *testing.T) {
	sender := &stubRelaySender{parser: grpcParserWithSubscription(), sendErr: notConfiguredRelayError()}
	addr := startTestGrpcListener(t, sender)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reply []byte
	var trailer metadata.MD
	err = conn.Invoke(ctx, "/"+unaryApiName, []byte("{}"), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true), grpc.Trailer(&trailer))

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "a gRPC status: %v", err)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Equal(t, "Selected provider is not configured", st.Message())
	for _, name := range configuredProviderNames {
		require.NotContains(t, st.Message(), name)
	}
	require.NotEmpty(t, trailer.Get(common.GUID_HEADER_NAME), "the refusal carries the request id, as a served call does")
}

// A GraphQL write refused this way says Lava-Write-Outcome: not_sent. The router puts it on the
// reply it returns beside the error, since only it knows the request was a mutation, and the
// listener sends that reply's headers with its refusal document.
func TestGraphQLListener_ARefusedPinnedWriteSaysNotSent(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refused := &common.RelayResult{Reply: &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{{Name: common.WRITE_OUTCOME_HEADER_NAME, Value: "not_sent"}}}}
	relay := &scriptedRelay{result: refused, err: notConfiguredRelayError()}
	addr := serveGoldenListener(t, ctx, spectypes.APIInterfaceGraphQL, relay)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/graphql", strings.NewReader(`{"query":"mutation { executeTransaction(transactionDataBcs: \"AA==\", signatures: []) { digest } }"}`))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", body)
	require.Equal(t, "not_sent", resp.Header.Get(common.WRITE_OUTCOME_HEADER_NAME))
	require.Contains(t, string(body), "REQUEST_REFUSED")
}
