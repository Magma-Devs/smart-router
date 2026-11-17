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
// chain.
func CreateMockXRPLSpec(index string) spectypes.Spec {
	api := func(name string, category spectypes.SpecCategory) *spectypes.Api {
		return &spectypes.Api{
			Name:         name,
			Enabled:      true,
			ComputeUnits: 10,
			BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
			Category:     category,
		}
	}
	submission := spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS, HangingApi: true}
	spec := newMockSpec(index,
		spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC, Type: http.MethodPost},
		api("submit", submission), api("submit_multisigned", submission), api("account_info", spectypes.SpecCategory{}),
	)
	spec.AverageBlockTime = 4000
	return spec
}

// LoadSpec loads specIndex from the specs/ directory under getToTopMostPath, resolving its
// imports across every file there (e.g. LAV1 -> COSMOSSDK). apiInterface is either an interface
// name, returned as is, or the index of one of the spec's api collections, returned as that
// collection's interface.
func LoadSpec(getToTopMostPath, specIndex, apiInterface string) (spectypes.Spec, string, error) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{
		getToTopMostPath + "specs/",
	}, specIndex)
	if err != nil {
		return spectypes.Spec{}, "", err
	}
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
