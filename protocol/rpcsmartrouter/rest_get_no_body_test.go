package rpcsmartrouter

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restWireRequest is what the node saw.
type restWireRequest struct {
	method, path, query string
	body                []byte
}

// restWireFixture is a LAVA REST parser over an upstream that records every request, and a direct
// sender to it. The spec is loaded so a test can change it before the parser reads it.
type restWireFixture struct {
	parser   chainlib.ChainParser
	spec     spectypes.Spec
	sender   *DirectRPCRelaySender
	received chan restWireRequest
}

func newRESTWireFixture(t *testing.T) *restWireFixture {
	t.Helper()
	ctx := t.Context()
	received := make(chan restWireRequest, 1)
	parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, readErr := io.ReadAll(r.Body)
			assert.NoError(t, readErr)
			select {
			case received <- restWireRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, body: data}:
			default:
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"block":{"header":{"height":"17"}}}`))
		}), nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "LAVA")
	require.NoError(t, err)
	conn, err := lavasession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, spectypes.APIInterfaceRest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &restWireFixture{
		parser:   parser,
		spec:     spec,
		sender:   &DirectRPCRelaySender{directConnection: conn, endpointName: "rest-upstream"},
		received: received,
	}
}

func (f *restWireFixture) send(t *testing.T, chainMessage chainlib.ChainMessage) restWireRequest {
	t.Helper()
	result, err := f.sender.SendDirectRelay(t.Context(), chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode)
	select {
	case node := <-f.received:
		return node
	default:
		t.Fatal("the request never reached the node")
		return restWireRequest{}
	}
}

// TestCraftRelay_RESTGetSendsItsTemplateAsTheURL drives the health check's own relay, craftRelay,
// to the wire for a REST GET whose GET_BLOCKNUM template carries a query string, the shape of the
// Hedera mirror node's `/api/v1/blocks?limit=1&order=desc`. craftRelay used to send the template
// as the BODY of a GET to the bare api name: the query never reached the node, and a node that
// refuses a GET with a body failed every health check, so its only provider stayed blocked.
func TestCraftRelay_RESTGetSendsItsTemplateAsTheURL(t *testing.T) {
	f := newRESTWireFixture(t)
	const apiName = "/cosmos/base/tendermint/v1beta1/blocks/latest"
	const template = apiName + "?limit=1&order=desc"
	found := false
	for _, collection := range f.spec.ApiCollections {
		if collection.CollectionData.ApiInterface != spectypes.APIInterfaceRest || collection.CollectionData.Type != http.MethodGet {
			continue
		}
		for _, directive := range collection.ParseDirectives {
			if directive.FunctionTag == spectypes.FUNCTION_TAG_GET_BLOCKNUM {
				require.Equal(t, apiName, directive.ApiName, "premise: the LAVA spec's REST latest-block directive")
				directive.FunctionTemplate = template
				found = true
			}
		}
	}
	require.True(t, found, "premise: the LAVA spec has a REST GET latest-block directive")
	f.parser.SetSpec(f.spec)

	_, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
	server := &RPCSmartRouterServer{chainParser: f.parser, listenEndpoint: rpcEndpoint}
	ok, relay, chainMessage, err := server.craftRelay(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, template, relay.ApiUrl)
	require.Empty(t, relay.Data)

	node := f.send(t, chainMessage)
	require.Equal(t, http.MethodGet, node.method)
	require.Equal(t, apiName, node.path)
	require.Equal(t, "limit=1&order=desc", node.query)
	require.Empty(t, node.body, "a GET carries no body")
}

// TestRESTRelay_OnlyBodyMethodsSendABody pins the sender's half: whatever the message holds, a GET
// or HEAD goes out without a body, and a POST still sends its body byte for byte.
func TestRESTRelay_OnlyBodyMethodsSendABody(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		body         []byte
		want         []byte
	}{
		{method: http.MethodGet, path: "/cosmos/base/tendermint/v1beta1/blocks/latest", body: []byte(`stray`)},
		{method: http.MethodHead, path: "/cosmos/base/tendermint/v1beta1/blocks/latest", body: []byte(`stray`)},
		{method: http.MethodPost, path: "/cosmos/tx/v1beta1/simulate", body: []byte(`{"tx_bytes":"AA=="}`), want: []byte(`{"tx_bytes":"AA=="}`)},
	} {
		t.Run(tc.method, func(t *testing.T) {
			f := newRESTWireFixture(t)
			if tc.method == http.MethodHead {
				// No bundled spec has a HEAD collection, so add one.
				f.spec.ApiCollections = append(f.spec.ApiCollections, &spectypes.ApiCollection{
					Enabled:        true,
					CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodHead},
					Apis: []*spectypes.Api{{
						Name: tc.path, Enabled: true, ComputeUnits: 10,
						BlockParsing: spectypes.BlockParser{ParserFunc: spectypes.PARSER_FUNC_DEFAULT, ParserArg: []string{"latest"}},
					}},
				})
				f.parser.SetSpec(f.spec)
			}
			chainMessage, err := f.parser.ParseMsg(tc.path, tc.body, tc.method, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)

			node := f.send(t, chainMessage)
			require.Equal(t, tc.method, node.method)
			require.Equal(t, tc.path, node.path)
			if tc.want == nil {
				require.Empty(t, node.body)
			} else {
				require.Equal(t, tc.want, node.body)
			}
		})
	}
}
