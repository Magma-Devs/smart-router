package chainlib

import (
	"net/http"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// MAG-4185: an api the spec marks enabled:false used to be dropped at load, so a request for it
// missed the lookup and was relayed as a Default- api (one provider, cacheable, latest-block
// parsing). It must be refused instead.

func jsonrpcCollection(addon string, apis ...*spectypes.Api) *spectypes.ApiCollection {
	return &spectypes.ApiCollection{
		Enabled: true,
		CollectionData: spectypes.CollectionData{
			ApiInterface: spectypes.APIInterfaceJsonRPC,
			Type:         http.MethodPost,
			AddOn:        addon,
		},
		Apis: apis,
	}
}

func parseJSONRPC(t *testing.T, parser *JsonRPCChainParser, method string) (ChainMessage, error) {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":[]}`
	return parser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
}

// An importing spec that disables an inherited method, the shape OPTM uses for eth_sendTransaction
// and TRX for eth_sendRawTransaction, built on the real ETH1 and expanded the way the router
// expands specs at boot.
func TestDisabledApi_InheritedOverrideIsRefused(t *testing.T) {
	specs, err := specutils.GetAllSpecsFromFile("../../specs/ethereum.json")
	require.NoError(t, err)
	specs["OPTM"] = spectypes.Spec{
		Index:   "OPTM",
		Name:    "optimism mainnet",
		Enabled: true,
		Imports: []string{"ETH1"},
		ApiCollections: []*spectypes.ApiCollection{jsonrpcCollection("", &spectypes.Api{
			Name:         "eth_sendTransaction",
			Enabled:      false,
			ComputeUnits: 10,
			BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
			Category:     spectypes.SpecCategory{Deterministic: true},
		})},
	}
	specs["OPTMS"] = spectypes.Spec{
		Index:          "OPTMS",
		Name:           "optimism sepolia",
		Enabled:        true,
		Imports:        []string{"OPTM"},
		ApiCollections: []*spectypes.ApiCollection{jsonrpcCollection("")},
	}
	spec, err := specutils.ExpandSpecWithDependencies(specs, "OPTMS")
	require.NoError(t, err)

	parser, err := NewJrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(*spec)

	_, err = parseJSONRPC(t, parser, "eth_sendTransaction")
	require.ErrorContains(t, err, "api is disabled")

	// The rest of the inherited spec is untouched, the write ETH1 keeps enabled included.
	msg, err := parseJSONRPC(t, parser, "eth_sendRawTransaction")
	require.NoError(t, err)
	require.Equal(t, "eth_sendRawTransaction", msg.GetApi().Name)
	msg, err = parseJSONRPC(t, parser, "eth_chainId")
	require.NoError(t, err)
	require.Equal(t, "eth_chainId", msg.GetApi().Name)

	// A method no spec names still falls through to a Default- api, as before.
	msg, err = parseJSONRPC(t, parser, "vendor_somethingElse")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(msg.GetApi().Name, DefaultApiName))

	// A batch carrying the disabled method is refused whole, like any batch with an element the
	// parser rejects.
	_, err = parser.ParseMsg("", []byte(`[{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_sendTransaction","params":[]}]`),
		http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.ErrorContains(t, err, "api is disabled")
}

// A method one collection disables and another serves stays served, whichever loads first.
func TestDisabledApi_EnabledEntryWinsInEitherOrder(t *testing.T) {
	disabled := &spectypes.Api{Name: "debug_traceCall", Enabled: false}
	enabled := &spectypes.Api{Name: "debug_traceCall", Enabled: true, ComputeUnits: 7}
	for name, collections := range map[string][]*spectypes.ApiCollection{
		"disabled first": {jsonrpcCollection("", disabled), jsonrpcCollection("", enabled)},
		"enabled first":  {jsonrpcCollection("", enabled), jsonrpcCollection("", disabled)},
	} {
		t.Run(name, func(t *testing.T) {
			spec := spectypes.Spec{Index: "TEST", Enabled: true, ApiCollections: collections}
			_, serverApis, _, _, _, _ := getServiceApis(spec, spectypes.APIInterfaceJsonRPC)
			apiCont, ok := serverApis[ApiKey{Name: "debug_traceCall", ConnectionType: http.MethodPost}]
			require.True(t, ok)
			require.True(t, apiCont.api.Enabled)
			require.Equal(t, uint64(7), apiCont.api.ComputeUnits)
		})
	}
}

// With an internal path, the bare key is still taken by the first enabled collection, and a
// disabled entry under one path does not shadow the method enabled under another.
func TestDisabledApi_InternalPathBareKey(t *testing.T) {
	onPath := func(path string, api *spectypes.Api) *spectypes.ApiCollection {
		collection := jsonrpcCollection("", api)
		collection.CollectionData.InternalPath = path
		return collection
	}
	spec := spectypes.Spec{Index: "TEST", Enabled: true, ApiCollections: []*spectypes.ApiCollection{
		onPath("/a", &spectypes.Api{Name: "m", Enabled: false}),
		onPath("/b", &spectypes.Api{Name: "m", Enabled: true, ComputeUnits: 2}),
		onPath("/c", &spectypes.Api{Name: "m", Enabled: true, ComputeUnits: 3}),
	}}
	_, serverApis, _, _, _, _ := getServiceApis(spec, spectypes.APIInterfaceJsonRPC)

	require.False(t, serverApis[ApiKey{Name: "m", ConnectionType: http.MethodPost, InternalPath: "/a"}].api.Enabled)
	require.True(t, serverApis[ApiKey{Name: "m", ConnectionType: http.MethodPost, InternalPath: "/b"}].api.Enabled)
	bare := serverApis[ApiKey{Name: "m", ConnectionType: http.MethodPost}]
	require.True(t, bare.api.Enabled)
	require.Equal(t, uint64(2), bare.api.ComputeUnits, "the first enabled collection keeps the bare key")
}

// REST ranks a disabled path against the enabled ones like any other: a disabled literal path is
// refused even where an enabled placeholder sibling also covers it, and the sibling keeps serving
// every other path.
func TestDisabledApi_RestLiteralBeatsPlaceholder(t *testing.T) {
	spec := spectypes.Spec{Index: "TEST", Enabled: true, ApiCollections: []*spectypes.ApiCollection{{
		Enabled:        true,
		CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodGet},
		Apis: []*spectypes.Api{
			{Name: "/tx/broadcast", Enabled: false},
			{Name: "/tx/{hash}", Enabled: true},
		},
	}}}
	parser, err := NewRestChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	_, err = parser.getSupportedApi("/tx/broadcast", http.MethodGet)
	require.ErrorContains(t, err, "api is disabled")

	apiCont, err := parser.getSupportedApi("/tx/0xabc", http.MethodGet)
	require.NoError(t, err)
	require.Equal(t, "/tx/{hash}", apiCont.api.Name)
}

// The readers that list what the spec serves keep answering as if the disabled api were absent.
func TestDisabledApi_NameReadersSkipIt(t *testing.T) {
	spec := spectypes.Spec{Index: "TEST", Enabled: true, ApiCollections: []*spectypes.ApiCollection{jsonrpcCollection("",
		&spectypes.Api{Name: "eth_sendTransaction", Enabled: false, Category: spectypes.SpecCategory{Stateful: 1}},
		&spectypes.Api{Name: "eth_chainId", Enabled: true},
	)}}
	parser, err := NewJrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	require.False(t, parser.ApiNameDefined("eth_sendTransaction"))
	require.False(t, parser.ApiHasStatefulCategory("eth_sendTransaction"))
	require.Empty(t, parser.ApiNamesLike("ETH_SENDTRANSACTION"))
	require.True(t, parser.ApiNameDefined("eth_chainId"))
}
