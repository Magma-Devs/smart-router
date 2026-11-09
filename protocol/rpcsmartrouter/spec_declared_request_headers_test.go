package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// TestDirectRelaySendsTheSpecDeclaredRequestHeaders follows MAG-3079 from the parser to the node
// on the two interfaces that dropped them: a request header the spec declares reaches the node with
// the caller's value, an overriding one with the spec's, and neither a nullified nor an undeclared
// header is sent. chainlib's TestSpecDeclaredRequestHeadersAreSentOnEveryInterface holds the four
// interfaces to the same rule at the parser.
func TestDirectRelaySendsTheSpecDeclaredRequestHeaders(t *testing.T) {
	const (
		ethBlockNumber = `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
		ethChainID     = `{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}`
		tmStatus       = `{"jsonrpc":"2.0","id":1,"method":"status","params":[]}`
		tmAbciInfo     = `{"jsonrpc":"2.0","id":2,"method":"abci_info","params":[]}`
		singleReply    = `{"jsonrpc":"2.0","id":1,"result":"0x1"}`
		batchReply     = `[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x1"}]`
	)
	cases := []struct {
		name           string
		specIndex      string
		apiInterface   string
		connectionType string
		body           string
		reply          string
	}{
		{name: "jsonrpc", specIndex: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, connectionType: http.MethodPost, body: ethBlockNumber, reply: singleReply},
		{name: "jsonrpc batch", specIndex: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, connectionType: http.MethodPost, body: "[" + ethBlockNumber + "," + ethChainID + "]", reply: batchReply},
		{name: "tendermintrpc", specIndex: "LAVA", apiInterface: spectypes.APIInterfaceTendermintRPC, body: tmStatus, reply: singleReply},
		{name: "tendermintrpc batch", specIndex: "LAVA", apiInterface: spectypes.APIInterfaceTendermintRPC, body: "[" + tmStatus + "," + tmAbciInfo + "]", reply: batchReply},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			received := make(chan http.Header, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case received <- r.Header.Clone():
				default:
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.reply))
			}))
			defer upstream.Close()

			spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, tc.specIndex)
			require.NoError(t, err)
			for _, collection := range spec.ApiCollections {
				if collection.CollectionData.ApiInterface == tc.apiInterface {
					collection.Headers = append(collection.Headers,
						&spectypes.Header{Name: "x-spec-passed", Kind: spectypes.Header_pass_send},
						&spectypes.Header{Name: "x-spec-overridden", Kind: spectypes.Header_pass_override, Value: "from-spec"},
						&spectypes.Header{Name: "x-spec-nullified", Kind: spectypes.Header_pass_nullify},
					)
				}
			}
			chainParser, err := chainlib.NewChainParser(tc.apiInterface)
			require.NoError(t, err)
			chainParser.SetSpec(spec)

			chainMessage, err := chainParser.ParseMsg("", []byte(tc.body), tc.connectionType, []pairingtypes.Metadata{
				{Name: "x-spec-passed", Value: "from-caller"},
				{Name: "x-spec-overridden", Value: "from-caller"},
				{Name: "x-spec-nullified", Value: "from-caller"},
				{Name: "x-not-in-spec", Value: "from-caller"},
			}, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)

			sender := newTestDirectSender(t, upstream)
			sender.originalRequestData = []byte(tc.body)
			_, err = sender.SendDirectRelay(context.Background(), chainMessage, 5*time.Second)
			require.NoError(t, err)

			var atTheNode http.Header
			select {
			case atTheNode = <-received:
			case <-time.After(5 * time.Second):
				t.Fatal("the request never reached the node")
			}
			require.Equal(t, "from-caller", atTheNode.Get("x-spec-passed"), "a pass_send header reaches the node with the caller's value")
			require.Equal(t, "from-spec", atTheNode.Get("x-spec-overridden"), "a pass_override header reaches the node with the spec's value")
			require.Empty(t, atTheNode.Values("x-spec-nullified"), "a pass_nullify header does not reach the node")
			require.Empty(t, atTheNode.Values("x-not-in-spec"), "a header the spec does not declare does not reach the node")
		})
	}
}
