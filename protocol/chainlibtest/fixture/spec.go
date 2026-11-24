package fixture

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
)

// newMockSpec is the scaffolding the mock specs share: an enabled spec named after its index, with
// one enabled collection of the given interface and type.
func newMockSpec(index string, collectionData spectypes.CollectionData, apis ...*spectypes.Api) spectypes.Spec {
	spec := spectypes.Spec{}
	spec.Name = index
	spec.Index = index
	spec.Enabled = true
	spec.ApiCollections = []*spectypes.ApiCollection{
		{
			Enabled:        true,
			CollectionData: collectionData,
			Apis:           apis,
		},
	}
	return spec
}

// CreateMockSpec returns a minimal spectypes.Spec suitable for unit tests.
func CreateMockSpec() spectypes.Spec {
	specName := "mockspec"
	return newMockSpec(specName,
		spectypes.CollectionData{ApiInterface: "stub", Type: "GET"},
		&spectypes.Api{Name: specName + "API", ComputeUnits: 100, Enabled: true},
	)
}

// CreateMockXRPLSpec returns the part of an XRP Ledger spec a transaction submission needs: one
// JSON-RPC collection holding submit and submit_multisigned, stateful and hanging as in the
// catalog's ripple.json, beside one read (account_info). The router's bundled specs carry no XRPL
// chain, so it registers ripple.json's families itself, as loading the catalog would: XRPT takes
// the XRP family from its import of XRP.
func CreateMockXRPLSpec(index string) spectypes.Spec {
	common.RegisterChainFamilies(map[string]spectypes.Spec{
		"XRP":  {Index: "XRP"},
		"XRPT": {Index: "XRPT", Imports: []string{"XRP"}},
	})
	submission := spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS, HangingApi: true}
	spec := newMockSpec(index,
		spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC, Type: http.MethodPost},
		mockSpecApi("submit", submission), mockSpecApi("submit_multisigned", submission), mockSpecApi("account_info", spectypes.SpecCategory{}),
	)
	spec.AverageBlockTime = 4000
	return spec
}

// mockSpecApi is an enabled api of the mock specs, with no block parsing.
func mockSpecApi(name string, category spectypes.SpecCategory) *spectypes.Api {
	return &spectypes.Api{
		Name:         name,
		Enabled:      true,
		ComputeUnits: 10,
		BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
		Category:     category,
	}
}

// CreateMockCosmosBroadcastSpec returns the part of a Cosmos chain's spec a transaction broadcast
// needs, on one interface. On tendermintrpc: broadcast_tx_sync and broadcast_tx_commit (stateful and
// hanging), broadcast_tx_async (stateful) and abci_query. On grpc: cosmos.tx.v1beta1.Service/
// BroadcastTx (stateful and hanging) and Simulate. The categories are the catalog's (tendermint.json,
// cosmossdk.json). The chain imports TENDERMINT, so it is a CosmosSDK chain.
func CreateMockCosmosBroadcastSpec(index, apiInterface string) spectypes.Spec {
	common.RegisterChainFamilies(map[string]spectypes.Spec{
		"TENDERMINT": {Index: "TENDERMINT"},
		index:        {Index: index, Imports: []string{"TENDERMINT"}},
	})
	write := spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS, HangingApi: true}
	var spec spectypes.Spec
	if apiInterface == spectypes.APIInterfaceGrpc {
		spec = newMockSpec(index,
			spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGrpc},
			mockSpecApi("cosmos.tx.v1beta1.Service/BroadcastTx", write),
			mockSpecApi("cosmos.tx.v1beta1.Service/Simulate", spectypes.SpecCategory{}),
		)
	} else {
		spec = newMockSpec(index,
			spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceTendermintRPC},
			mockSpecApi("broadcast_tx_sync", write),
			mockSpecApi("broadcast_tx_commit", write),
			mockSpecApi("broadcast_tx_async", spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS}),
			mockSpecApi("abci_query", spectypes.SpecCategory{}),
		)
	}
	spec.AverageBlockTime = 6000
	return spec
}

// CreateMockTronSpec returns the part of a Tron spec a transaction broadcast needs: the REST POST
// collection holding /wallet/broadcasttransaction and /wallet/broadcasthex, stateful as in the
// catalog's tron.json, beside one read (/wallet/getnowblock). The chain imports TRX, so it is a Tron
// chain.
func CreateMockTronSpec(index string) spectypes.Spec {
	common.RegisterChainFamilies(map[string]spectypes.Spec{
		"TRX": {Index: "TRX"},
		index: {Index: index, Imports: []string{"TRX"}},
	})
	write := spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS}
	spec := newMockSpec(index,
		spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodPost},
		mockSpecApi("/wallet/broadcasttransaction", write),
		mockSpecApi("/wallet/broadcasthex", write),
		mockSpecApi("/wallet/getnowblock", spectypes.SpecCategory{}),
	)
	spec.AverageBlockTime = 3000
	return spec
}

// LoadSpec loads specIndex from the specs/ directory under getToTopMostPath, resolving its
// imports across every file there (e.g. COSMOSHUB -> COSMOSSDK50). apiInterface is either an interface
// name, returned as is, or the index of one of the spec's api collections, returned as that
// collection's interface.
func LoadSpec(getToTopMostPath, specIndex, apiInterface string) (spectypes.Spec, string, error) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{
		getToTopMostPath + "specs/",
	}, specIndex)
	if err != nil {
		return spectypes.Spec{}, "", err
	}
	// As the router does when it loads a catalog: a chain such as ARBITRUM takes its family from
	// its imports, and the parser reads it in SetSpec.
	catalog, err := specutils.GetAllSpecsFromLocalDir(getToTopMostPath + "specs/")
	if err != nil {
		return spectypes.Spec{}, "", err
	}
	common.RegisterChainFamilies(catalog)
	index, err := strconv.Atoi(apiInterface)
	if err == nil && index < len(spec.ApiCollections) {
		apiInterface = spec.ApiCollections[index].CollectionData.ApiInterface
	}
	return spec, apiInterface, nil
}

// setMockLogLevelOnce guards the one-time global log-level set in UseMockLogLevel.
var setMockLogLevelOnce sync.Once

// UseMockLogLevel makes the one global log-level write CreateChainLibMocks does. A
// test that starts logging goroutines without the mocks calls it first, so the write
// never lands under a goroutine it left running.
func UseMockLogLevel() {
	setMockLogLevelOnce.Do(func() { utils.SetGlobalLoggingLevel("debug") })
}
