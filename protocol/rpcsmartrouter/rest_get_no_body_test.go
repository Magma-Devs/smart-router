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
	method, path, query, contentType string
	body                             []byte
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
			case received <- restWireRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, contentType: r.Header.Get("Content-Type"), body: data}:
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

// addCollection registers a REST collection of the given method serving one api, for the methods
// no bundled spec has a collection of.
func (f *restWireFixture) addCollection(method, apiName string) {
	f.spec.ApiCollections = append(f.spec.ApiCollections, &spectypes.ApiCollection{
		Enabled:        true,
		CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: method},
		Apis: []*spectypes.Api{{
			Name: apiName, Enabled: true, ComputeUnits: 10,
			BlockParsing: spectypes.BlockParser{ParserFunc: spectypes.PARSER_FUNC_DEFAULT, ParserArg: []string{"latest"}},
		}},
	})
	f.parser.SetSpec(f.spec)
}

// latestBlockDirective is the LAVA spec's REST GET latest-block directive, the one craftRelay builds
// the health check from. A test changes its template, then hands the spec back to the parser.
func (f *restWireFixture) latestBlockDirective(t *testing.T) *spectypes.ParseDirective {
	t.Helper()
	for _, collection := range f.spec.ApiCollections {
		if collection.CollectionData.ApiInterface != spectypes.APIInterfaceRest || collection.CollectionData.Type != http.MethodGet {
			continue
		}
		for _, directive := range collection.ParseDirectives {
			if directive.FunctionTag == spectypes.FUNCTION_TAG_GET_BLOCKNUM {
				return directive
			}
		}
	}
	t.Fatal("premise: the LAVA spec has a REST GET latest-block directive")
	return nil
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

const latestBlockApiName = "/cosmos/base/tendermint/v1beta1/blocks/latest"

// TestCraftRelay_RESTGetSendsItsTemplateAsTheURL drives the health check's own relay, craftRelay,
// to the wire for a REST GET whose GET_BLOCKNUM template carries a query string, the shape of the
// Hedera mirror node's `/api/v1/blocks?limit=1&order=desc`. craftRelay used to send the template
// as the BODY of a GET to the bare api name: the query never reached the node, and a node that
// refuses a GET with a body failed every health check, so its only provider stayed blocked.
func TestCraftRelay_RESTGetSendsItsTemplateAsTheURL(t *testing.T) {
	f := newRESTWireFixture(t)
	const template = latestBlockApiName + "?limit=1&order=desc"
	directive := f.latestBlockDirective(t)
	require.Equal(t, latestBlockApiName, directive.ApiName, "premise: the LAVA spec's REST latest-block directive")
	directive.FunctionTemplate = template
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
	require.Equal(t, latestBlockApiName, node.path)
	require.Equal(t, "limit=1&order=desc", node.query)
	require.Empty(t, node.body, "a GET carries no body")
	require.Empty(t, node.contentType, "no body, so no Content-Type")
}

// TestCraftRelay_RESTGetWithNoTemplateUsesTheApiName pins the chain fetcher's other half of the
// rule: a GET directive with an empty template is sent to its api name, the way
// FetchLatestBlockNumForCollection crafts it, rather than to an empty path no api matches.
func TestCraftRelay_RESTGetWithNoTemplateUsesTheApiName(t *testing.T) {
	f := newRESTWireFixture(t)
	directive := f.latestBlockDirective(t)
	require.Equal(t, latestBlockApiName, directive.ApiName, "premise: the LAVA spec's REST latest-block directive")
	directive.FunctionTemplate = ""
	f.parser.SetSpec(f.spec)

	_, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
	server := &RPCSmartRouterServer{chainParser: f.parser, listenEndpoint: rpcEndpoint}
	ok, relay, chainMessage, err := server.craftRelay(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, latestBlockApiName, relay.ApiUrl)
	require.Empty(t, relay.Data)

	node := f.send(t, chainMessage)
	require.Equal(t, http.MethodGet, node.method)
	require.Equal(t, latestBlockApiName, node.path)
	require.Empty(t, node.body)
}

// TestRESTRelay_OnlyBodyMethodsSendABody pins the sender's half: whatever the message holds, a
// method that carries no body goes out without one and without a Content-Type, and a POST still
// sends its body byte for byte under the router's default Content-Type.
func TestRESTRelay_OnlyBodyMethodsSendABody(t *testing.T) {
	for _, tc := range []struct {
		method, path    string
		body            []byte
		want            []byte
		wantContentType string
	}{
		{method: http.MethodGet, path: latestBlockApiName, body: []byte(`stray`)},
		{method: http.MethodHead, path: latestBlockApiName, body: []byte(`stray`)},
		{method: http.MethodDelete, path: latestBlockApiName, body: []byte(`stray`)},
		{method: http.MethodPost, path: "/cosmos/tx/v1beta1/simulate", body: []byte(`{"tx_bytes":"AA=="}`), want: []byte(`{"tx_bytes":"AA=="}`), wantContentType: "application/json"},
	} {
		t.Run(tc.method, func(t *testing.T) {
			f := newRESTWireFixture(t)
			if tc.method == http.MethodHead || tc.method == http.MethodDelete {
				// No bundled spec has a collection of this method, so add one.
				f.addCollection(tc.method, tc.path)
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
			require.Equal(t, tc.wantContentType, node.contentType)
		})
	}
}
