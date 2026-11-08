package chainlib

import (
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// The three request-header directives a spec can declare, plus a header it does not declare.
// HandleHeaders lowercases the caller's header before looking it up and looks the spec's name up as
// written, so the declared names are lowercase, as in every spec.
const (
	specHeaderPassed     = "x-spec-passed"
	specHeaderOverridden = "x-spec-overridden"
	specHeaderNullified  = "x-spec-nullified"
	undeclaredHeader     = "x-not-in-spec"
)

// specWithRequestHeaders loads a bundled spec and declares one header of each request directive on
// every collection of apiInterface. No bundled spec declares a request header on JSON-RPC or
// Tendermint (none on lava-specs main either, checked 2026-11-08), so the declarations are added here.
func specWithRequestHeaders(t *testing.T, specIndex, apiInterface string) spectypes.Spec {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, specIndex)
	require.NoError(t, err)
	declaredOn := 0
	for _, collection := range spec.ApiCollections {
		if collection.CollectionData.ApiInterface != apiInterface {
			continue
		}
		collection.Headers = append(collection.Headers,
			&spectypes.Header{Name: specHeaderPassed, Kind: spectypes.Header_pass_send},
			&spectypes.Header{Name: specHeaderOverridden, Kind: spectypes.Header_pass_override, Value: "from-spec"},
			&spectypes.Header{Name: specHeaderNullified, Kind: spectypes.Header_pass_nullify},
		)
		declaredOn++
	}
	require.NotZero(t, declaredOn, "spec %s has no %s collection to declare the headers on", specIndex, apiInterface)
	return spec
}

func parserForSpec(t *testing.T, apiInterface string, spec spectypes.Spec) ChainParser {
	t.Helper()
	chainParser, err := NewChainParser(apiInterface)
	require.NoError(t, err)
	chainParser.SetSpec(spec)
	return chainParser
}

// callerHeaders is what the caller sends: a value for each declared header and one undeclared.
func callerHeaders() []pairingtypes.Metadata {
	return []pairingtypes.Metadata{
		{Name: specHeaderPassed, Value: "from-caller"},
		{Name: specHeaderOverridden, Value: "from-caller"},
		{Name: specHeaderNullified, Value: "from-caller"},
		{Name: undeclaredHeader, Value: "from-caller"},
	}
}

// requireSpecHeadersApplied checks the headers a message sends, read the way the transports apply
// them (effectiveHeaders): the passed header carries the caller's value, the overridden one the
// spec's, and neither the nullified nor the undeclared header is sent.
func requireSpecHeadersApplied(t *testing.T, headers []pairingtypes.Metadata) {
	t.Helper()
	sent := effectiveHeaders(headers)
	require.Equal(t, "from-caller", sent.Get(specHeaderPassed), "a pass_send header carries the caller's value")
	require.Equal(t, "from-spec", sent.Get(specHeaderOverridden), "a pass_override header carries the spec's value")
	require.Empty(t, sent.Values(specHeaderNullified), "a pass_nullify header is not sent")
	require.Empty(t, sent.Values(undeclaredHeader), "a header the spec does not declare is not sent")
}

// TestSpecDeclaredRequestHeadersAreSentOnEveryInterface pins MAG-3079: a request header the spec
// declares is applied the same way on all four interfaces. JSON-RPC and Tendermint filtered the
// headers correctly and then wrote them to a copy of the request element (the range variable),
// while the message sent to the node was built from the element itself, so they sent none; a
// batch sent none on any path, because the batch message never carried headers.
func TestSpecDeclaredRequestHeadersAreSentOnEveryInterface(t *testing.T) {
	const (
		ethBlockNumber = `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
		ethChainID     = `{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}`
		tmStatus       = `{"jsonrpc":"2.0","id":1,"method":"status","params":[]}`
		tmAbciInfo     = `{"jsonrpc":"2.0","id":2,"method":"abci_info","params":[]}`
	)
	cases := []struct {
		name           string
		specIndex      string
		apiInterface   string
		url            string
		connectionType string
		body           string
	}{
		{name: "jsonrpc", specIndex: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, connectionType: http.MethodPost, body: ethBlockNumber},
		{name: "jsonrpc batch", specIndex: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, connectionType: http.MethodPost, body: "[" + ethBlockNumber + "," + ethChainID + "]"},
		{name: "tendermintrpc", specIndex: "LAVA", apiInterface: spectypes.APIInterfaceTendermintRPC, body: tmStatus},
		{name: "tendermintrpc uri", specIndex: "LAVA", apiInterface: spectypes.APIInterfaceTendermintRPC, url: "status?"},
		{name: "tendermintrpc batch", specIndex: "LAVA", apiInterface: spectypes.APIInterfaceTendermintRPC, body: "[" + tmStatus + "," + tmAbciInfo + "]"},
		{name: "rest", specIndex: "LAVA", apiInterface: spectypes.APIInterfaceRest, url: "/cosmos/base/tendermint/v1beta1/blocks/latest", connectionType: http.MethodGet},
		{name: "grpc", specIndex: "LAVA", apiInterface: spectypes.APIInterfaceGrpc, url: "cosmos.base.tendermint.v1beta1.Service/GetLatestBlock"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := parserForSpec(t, tc.apiInterface, specWithRequestHeaders(t, tc.specIndex, tc.apiInterface))
			chainMessage, err := chainParser.ParseMsg(tc.url, []byte(tc.body), tc.connectionType, callerHeaders(), extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			requireSpecHeadersApplied(t, chainMessage.GetRPCMessage().GetHeaders())
		})
	}
}

// TestJsonRPCCacheKeyFollowsTheSpecDeclaredHeaders: the headers a request sends are part of its
// cache key, so two JSON-RPC requests that differ only in a declared header, and so ask the node
// different things, no longer share a cache entry (MAG-3079). A request that sends no header keeps
// the key every JSON-RPC request had before: its header list stays nil, which the key marshals as
// "null", where an empty list would be "[]" and move every key on a chain that declares none.
func TestJsonRPCCacheKeyFollowsTheSpecDeclaredHeaders(t *testing.T) {
	const body = `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"]}`
	cacheKey := func(t *testing.T, headers []pairingtypes.Metadata) []byte {
		t.Helper()
		key, _, err := hashCacheRequest(&pairingtypes.RelayPrivateData{
			ConnectionType: http.MethodPost,
			Data:           []byte(body),
			RequestBlock:   spectypes.LATEST_BLOCK,
			ApiInterface:   spectypes.APIInterfaceJsonRPC,
			Metadata:       headers,
		}, "ETH1", "")
		require.NoError(t, err)
		return key
	}
	parse := func(t *testing.T, chainParser ChainParser, metadata []pairingtypes.Metadata) []pairingtypes.Metadata {
		t.Helper()
		chainMessage, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, metadata, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		return chainMessage.GetRPCMessage().GetHeaders()
	}

	t.Run("a declared header splits the key", func(t *testing.T) {
		chainParser := parserForSpec(t, spectypes.APIInterfaceJsonRPC, specWithRequestHeaders(t, "ETH1", spectypes.APIInterfaceJsonRPC))
		first := parse(t, chainParser, []pairingtypes.Metadata{{Name: specHeaderPassed, Value: "100"}})
		second := parse(t, chainParser, []pairingtypes.Metadata{{Name: specHeaderPassed, Value: "200"}})
		require.NotEqual(t, cacheKey(t, first), cacheKey(t, second))
	})

	t.Run("no declared header keeps the key", func(t *testing.T) {
		spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "ETH1")
		require.NoError(t, err)
		chainParser := parserForSpec(t, spectypes.APIInterfaceJsonRPC, spec)
		headers := parse(t, chainParser, []pairingtypes.Metadata{{Name: undeclaredHeader, Value: "from-caller"}})
		require.Nil(t, headers, "an empty header list must stay nil")
		require.Equal(t, cacheKey(t, nil), cacheKey(t, headers))
	})
}
