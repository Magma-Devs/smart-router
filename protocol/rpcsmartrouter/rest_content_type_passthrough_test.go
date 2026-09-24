package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

const (
	contentTypeFormURLEncoded = "application/x-www-form-urlencoded"
	cosmosSimulatePath        = "/cosmos/tx/v1beta1/simulate"
)

// contentTypeRelayHarness runs one direct relay through the real chain parser (so
// HandleHeaders decides what is forwarded) and the real HTTP transport (so the
// header ordering in DoHTTPRequest decides what wins), and returns the Content-Type
// the upstream received. specOverride, when set, edits the loaded spec before it
// is applied — how a test gets a directive no local spec declares.
func contentTypeRelayHarness(t *testing.T, chainID, apiInterface string, specOverride func(*spectypes.Spec)) func(t *testing.T, path, body string, clientHeaders []pairingtypes.Metadata) string {
	t.Helper()
	ctx := context.Background()
	received := make(chan string, 1)
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, chainID, apiInterface,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			received <- r.Header.Get("Content-Type")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
		}),
		nil, "../../", nil,
	)
	require.NoError(t, err)
	t.Cleanup(closeServer)

	if specOverride != nil {
		spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, chainID)
		require.NoError(t, err)
		specOverride(&spec)
		chainParser.SetSpec(spec)
	}

	directConn, err := lavasession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, "")
	require.NoError(t, err)
	sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "content-type-upstream"}

	return func(t *testing.T, path, body string, clientHeaders []pairingtypes.Metadata) string {
		t.Helper()
		chainMessage, err := chainParser.ParseMsg(path, []byte(body), http.MethodPost, clientHeaders, extensionslib.ExtensionInfo{})
		require.NoError(t, err)
		result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, result.StatusCode)
		select {
		case contentType := <-received:
			return contentType
		case <-time.After(5 * time.Second):
			t.Fatal("upstream never received the relay")
			return ""
		}
	}
}

// TestRESTRelay_ClientContentTypeReachesTheWire pins MAG-2745 end to end on the direct
// REST path: the client's Content-Type is what the node receives, the application/json
// default only fills in when the client sent none, and a spec pass_override (Stellar's
// /transactions, MAG-2744) keeps the last word.
func TestRESTRelay_ClientContentTypeReachesTheWire(t *testing.T) {
	relay := contentTypeRelayHarness(t, "LAVA", spectypes.APIInterfaceRest, nil)

	t.Run("the client's content-type is forwarded as sent", func(t *testing.T) {
		got := relay(t, cosmosSimulatePath, "tx_bytes=AAAA", []pairingtypes.Metadata{{Name: "Content-Type", Value: contentTypeFormURLEncoded}})
		require.Equal(t, contentTypeFormURLEncoded, got)
	})

	t.Run("application/json only when the client sent none", func(t *testing.T) {
		got := relay(t, cosmosSimulatePath, `{"tx_bytes":"AAAA"}`, nil)
		require.Equal(t, "application/json", got)
	})

	t.Run("an undeclared header other than content-type is still not forwarded", func(t *testing.T) {
		// The carve-out is for the header that describes the body, nothing wider; spec
		// directives keep governing every other header.
		got := relay(t, cosmosSimulatePath, `{"tx_bytes":"AAAA"}`, []pairingtypes.Metadata{{Name: "x-custom", Value: "1"}})
		require.Equal(t, "application/json", got)
	})
}

func TestRESTRelay_SpecOverrideStillWinsOverClientContentType(t *testing.T) {
	relay := contentTypeRelayHarness(t, "LAVA", spectypes.APIInterfaceRest, func(spec *spectypes.Spec) {
		var overridden bool
		for _, collection := range spec.ApiCollections {
			if collection.CollectionData.ApiInterface == spectypes.APIInterfaceRest && collection.CollectionData.Type == http.MethodPost {
				collection.Headers = append(collection.Headers, &spectypes.Header{Name: "content-type", Kind: spectypes.Header_pass_override, Value: contentTypeFormURLEncoded})
				overridden = true
			}
		}
		require.True(t, overridden, "the spec needs a REST POST collection to carry the override")
	})

	got := relay(t, cosmosSimulatePath, "tx_bytes=AAAA", []pairingtypes.Metadata{{Name: "Content-Type", Value: "application/json"}})
	require.Equal(t, contentTypeFormURLEncoded, got, "a spec pass_override is the operator's explicit intent and must beat the client's header")
}

// TestJSONRPCRelay_IgnoresClientContentType is the boundary of MAG-2745: JSON-RPC
// re-marshals the body it sends, so the content-type stays the router's. Forwarding a
// client's text/plain here would turn into a 415 from nodes that insist on JSON.
func TestJSONRPCRelay_IgnoresClientContentType(t *testing.T) {
	relay := contentTypeRelayHarness(t, "ETH1", spectypes.APIInterfaceJsonRPC, nil)

	got := relay(t, "", `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`, []pairingtypes.Metadata{{Name: "Content-Type", Value: "text/plain"}})
	require.Equal(t, "application/json", got)
}
