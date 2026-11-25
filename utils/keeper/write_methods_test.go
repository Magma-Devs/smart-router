package keeper

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	types "github.com/magma-Devs/smart-router/types/spec"
)

// specWith is a one-collection spec for the lock's tests: index, optional imports, the collection's
// interface, REST verb and internal path, and its apis.
func specWith(index string, imports []string, apiInterface, verb, internalPath string, apis ...*types.Api) types.Spec {
	return types.Spec{
		Index:   index,
		Name:    index,
		Enabled: true,
		Imports: imports,
		ApiCollections: []*types.ApiCollection{{
			Enabled:        true,
			CollectionData: types.CollectionData{ApiInterface: apiInterface, Type: verb, InternalPath: internalPath},
			Apis:           apis,
		}},
	}
}

func api(name string, stateful uint32) *types.Api {
	return &types.Api{Name: name, Enabled: true, ComputeUnits: 10, Category: types.SpecCategory{Stateful: stateful}}
}

func catalogOf(specs ...types.Spec) map[string]types.Spec {
	catalog := make(map[string]types.Spec, len(specs))
	for _, spec := range specs {
		catalog[spec.Index] = spec
	}
	return catalog
}

// statefulOf is the Stateful level of the named api in the expanded spec, -1 when it is not there.
func statefulOf(spec *types.Spec, apiInterface, name string) int {
	for _, collection := range spec.ApiCollections {
		if collection.CollectionData.ApiInterface != apiInterface {
			continue
		}
		for _, api := range collection.Apis {
			if api.Name == name {
				return int(api.Category.Stateful)
			}
		}
	}
	return -1
}

// A write the spec declares loads stateful whether the spec marks it or clears it (PRD §5.3 invariant
// 5, MAG-5593): eth_sendRawTransaction with stateful 0 single-homed the broadcast on main (MAG-3389).
func TestWriteLock_ForcesAClearedWrite(t *testing.T) {
	catalog := catalogOf(specWith("EVMX", nil, types.APIInterfaceJsonRPC, "POST", "",
		api("eth_sendRawTransaction", 0), api("eth_blockNumber", 0), api("eth_sendTransaction", 1)))

	spec, report, err := ExpandSpecWithWriteLock(catalog, "EVMX")
	require.NoError(t, err)
	require.Equal(t, 1, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_sendRawTransaction"), "the cleared write loads stateful")
	require.Equal(t, 0, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_blockNumber"), "a read is left alone")
	require.Equal(t, 1, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_sendTransaction"), "a declared write stays as declared")
	require.Equal(t, "EVMX", report.ChainID)
	require.Equal(t, map[string][]string{types.APIInterfaceJsonRPC: {"eth_sendRawTransaction"}}, report.Forced,
		"the report names the forced api under its interface, and nothing else")
	require.Equal(t, 1, report.Count(types.APIInterfaceJsonRPC))
	require.Equal(t, 0, report.Count(types.APIInterfaceRest), "an interface with nothing forced reads 0")
}

// The router relays a request stateful only at level 1: every reader compares Stateful with
// common.CONSISTENCY_SELECT_ALL_PROVIDERS. A write the spec marks at any other level is relayed as a
// cacheable read on one provider, as a cleared one is, so the lock loads it at 1 and counts it.
func TestWriteLock_ForcesAWriteTheSpecMarksAtAnotherLevel(t *testing.T) {
	catalog := catalogOf(specWith("EVMX", nil, types.APIInterfaceJsonRPC, "POST", "",
		api("eth_sendRawTransaction", 2), api("eth_blockNumber", 2)))

	spec, report, err := ExpandSpecWithWriteLock(catalog, "EVMX")
	require.NoError(t, err)
	require.Equal(t, 1, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_sendRawTransaction"), "the write loads at the level the router relays a write at")
	require.Equal(t, 2, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_blockNumber"), "an api the table does not name is left as the spec wrote it")
	require.Equal(t, []string{"eth_sendRawTransaction"}, report.Forced[types.APIInterfaceJsonRPC])
}

// An inherited api sits in every collection that inherits it, and the gauge counts apis: Starknet keeps
// its writes in one collection the spec disables and that its served collections inherit. One cleared
// write there is forced in every served collection, counted and named once, and the disabled
// collection, which the parser never reads, is left as the spec wrote it, as a disabled api is.
func TestWriteLock_CountsAnInheritedWriteOnce(t *testing.T) {
	template := types.CollectionData{ApiInterface: types.APIInterfaceJsonRPC, Type: "POST", InternalPath: "HTTP-ONLY"}
	inheriting := func(internalPath string) *types.ApiCollection {
		return &types.ApiCollection{
			Enabled:         true,
			CollectionData:  types.CollectionData{ApiInterface: types.APIInterfaceJsonRPC, Type: "POST", InternalPath: internalPath},
			InheritanceApis: []*types.CollectionData{&template},
		}
	}
	spec := types.Spec{Index: "STRKX", Name: "STRKX", Enabled: true, ApiCollections: []*types.ApiCollection{
		{CollectionData: template, Apis: []*types.Api{api("starknet_addInvokeTransaction", 0), api("starknet_blockNumber", 0)}},
		inheriting(""),
		inheriting("/rpc/v0_8"),
	}}

	expanded, report, err := ExpandSpecWithWriteLock(catalogOf(spec), "STRKX")
	require.NoError(t, err)
	served := 0
	for _, collection := range expanded.ApiCollections {
		for _, entry := range collection.Apis {
			if entry.Name != "starknet_addInvokeTransaction" {
				continue
			}
			if !collection.Enabled {
				require.Equal(t, uint32(0), entry.Category.Stateful, "the disabled collection is left alone")
				continue
			}
			served++
			require.Equal(t, uint32(1), entry.Category.Stateful, "served under %q", collection.CollectionData.InternalPath)
		}
	}
	require.Equal(t, 2, served, "setup: both served collections inherit the write")
	require.Equal(t, []string{"starknet_addInvokeTransaction"}, report.Forced[types.APIInterfaceJsonRPC], "one api, named once")
	require.Equal(t, 1, report.Count(types.APIInterfaceJsonRPC), "the gauge counts apis, not the collections that hold one")
}

// The lock speaks every interface's identity: a name on JSON-RPC, Tendermint RPC, gRPC and GraphQL
// (a URI-form Tendermint request resolves to the same api at relay time), the verb and the full route
// on REST, where the collection's internal path is part of the route.
func TestWriteLock_EveryInterface(t *testing.T) {
	for _, tc := range []struct {
		name, apiInterface, verb, internalPath, apiName string
	}{
		{"tendermint broadcast_tx_sync", types.APIInterfaceTendermintRPC, "", "", "broadcast_tx_sync"},
		{"tendermint broadcast_evidence", types.APIInterfaceTendermintRPC, "", "", "broadcast_evidence"},
		{"aptos POST /transactions/batch", types.APIInterfaceRest, "POST", "", "/transactions/batch"},
		{"ton's route under its internal path", types.APIInterfaceRest, "POST", "/v2", "/sendBoc"},
		{"the same route spelled whole", types.APIInterfaceRest, "POST", "", "/v2/sendBoc"},
		{"concordium PUT /v0/submitRawTransaction", types.APIInterfaceRest, "PUT", "", "/v0/submitRawTransaction"},
		{"cosmos gRPC BroadcastTx", types.APIInterfaceGrpc, "", "", "cosmos.tx.v1beta1.Service/BroadcastTx"},
		{"sui gRPC ExecuteTransaction", types.APIInterfaceGrpc, "", "", "sui.rpc.v2.TransactionExecutionService/ExecuteTransaction"},
		{"sui GraphQL executeTransaction", types.APIInterfaceGraphQL, "POST", "", "executeTransaction"},
		{"bitcoin sendrawtransaction", types.APIInterfaceJsonRPC, "POST", "", "sendrawtransaction"},
		{"near send_tx", types.APIInterfaceJsonRPC, "POST", "", "send_tx"},
		{"starknet under its HTTP-ONLY path", types.APIInterfaceJsonRPC, "POST", "HTTP-ONLY", "starknet_addInvokeTransaction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := catalogOf(specWith("CHAINX", nil, tc.apiInterface, tc.verb, tc.internalPath, api(tc.apiName, 0)))
			spec, report, err := ExpandSpecWithWriteLock(catalog, "CHAINX")
			require.NoError(t, err)
			require.Equal(t, 1, statefulOf(spec, tc.apiInterface, tc.apiName))
			require.Equal(t, []string{tc.apiName}, report.Forced[tc.apiInterface])
		})
	}
}

// On REST the verb is part of the identity: GET /cosmos/tx/v1beta1/txs is a search and POST to the
// same path is BroadcastTx (cosmossdk.json declares both). The lock reads the collection's verb.
func TestWriteLock_RESTVerbTellsAWriteFromARead(t *testing.T) {
	read := specWith("COSMOSX", nil, types.APIInterfaceRest, "GET", "", api("/cosmos/tx/v1beta1/txs", 0))
	read.ApiCollections = append(read.ApiCollections, &types.ApiCollection{
		Enabled:        true,
		CollectionData: types.CollectionData{ApiInterface: types.APIInterfaceRest, Type: "POST"},
		Apis:           []*types.Api{api("/cosmos/tx/v1beta1/txs", 0)},
	})
	spec, report, err := ExpandSpecWithWriteLock(catalogOf(read), "COSMOSX")
	require.NoError(t, err)
	require.Equal(t, []string{"/cosmos/tx/v1beta1/txs"}, report.Forced[types.APIInterfaceRest], "the POST is forced once")
	for _, collection := range spec.ApiCollections {
		want := 0
		if collection.CollectionData.Type == "POST" {
			want = 1
		}
		require.Equal(t, want, int(collection.Apis[0].Category.Stateful), "verb %s", collection.CollectionData.Type)
	}
}

// An entry the catalog also declares a read somewhere is qualified with the specs that declare it a
// write (scripts/gen-write-methods.sh): POST /block is Arweave's block upload and a Rosetta query on
// ICP and Mina. The lock forces it on the declaring spec and on a spec that imports it, and leaves
// every other chain's read alone.
func TestWriteLock_QualifiedEntryAppliesToItsSpecAndImporters(t *testing.T) {
	arweave := specWith("ARWEAVE", nil, types.APIInterfaceRest, "POST", "", api("/block", 0))
	arweaveTestnet := specWith("ARWEAVET", []string{"ARWEAVE"}, types.APIInterfaceRest, "POST", "", api("/block", 0))
	icp := specWith("ICP", nil, types.APIInterfaceRest, "POST", "", api("/block", 0))
	xrp := specWith("XRP", nil, types.APIInterfaceJsonRPC, "POST", "", api("submit", 0))
	other := specWith("OTHERX", nil, types.APIInterfaceJsonRPC, "POST", "", api("submit", 0))
	catalog := catalogOf(arweave, arweaveTestnet, icp, xrp, other)

	for _, tc := range []struct {
		index, apiInterface, apiName string
		want                         int
	}{
		{"ARWEAVE", types.APIInterfaceRest, "/block", 1},
		{"ARWEAVET", types.APIInterfaceRest, "/block", 1},
		{"ICP", types.APIInterfaceRest, "/block", 0},
		{"XRP", types.APIInterfaceJsonRPC, "submit", 1},
		{"OTHERX", types.APIInterfaceJsonRPC, "submit", 0},
	} {
		spec, report, err := ExpandSpecWithWriteLock(catalog, tc.index)
		require.NoError(t, err, tc.index)
		require.Equal(t, tc.want, statefulOf(spec, tc.apiInterface, tc.apiName), "%s %s", tc.index, tc.apiName)
		require.Equal(t, tc.want, report.Count(tc.apiInterface), "%s report", tc.index)
	}
}

// The lock changes the expanded spec, never the catalog it expands from: the catalog's collections
// are shared with every endpoint of the chain and with every chain that imports it, and each of them
// must read the spec's own value and report it, so the gauge names every chain that loads the cleared
// write, not whichever booted first.
func TestWriteLock_LeavesTheCatalogAlone(t *testing.T) {
	parent := specWith("PARENTX", nil, types.APIInterfaceJsonRPC, "POST", "", api("eth_sendRawTransaction", 0), api("eth_chainId", 0))
	child := specWith("CHILDX", []string{"PARENTX"}, types.APIInterfaceJsonRPC, "POST", "", api("eth_blockNumber", 0))
	catalog := catalogOf(parent, child)

	for round := 1; round <= 2; round++ {
		for _, index := range []string{"PARENTX", "CHILDX"} {
			spec, report, err := ExpandSpecWithWriteLock(catalog, index)
			require.NoError(t, err)
			require.Equal(t, 1, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_sendRawTransaction"), "%s round %d", index, round)
			require.Equal(t, []string{"eth_sendRawTransaction"}, report.Forced[types.APIInterfaceJsonRPC], "%s round %d: the inherited write is forced and reported on the child too", index, round)
		}
		require.Equal(t, uint32(0), catalog["PARENTX"].ApiCollections[0].Apis[0].Category.Stateful, "round %d: the catalog still holds the spec's own value", round)
	}
}

// What the lock leaves alone: a disabled api cannot be relayed, a declared write is as the spec says,
// and a write the table does not know stays a read, the rulebook's accepted risk (Part I §1).
func TestWriteLock_SkipsDisabledDeclaredAndUnknownWrites(t *testing.T) {
	disabled := api("eth_sendRawTransaction", 0)
	disabled.Enabled = false
	catalog := catalogOf(specWith("EVMX", nil, types.APIInterfaceJsonRPC, "POST", "",
		disabled, api("eth_sendTransaction", 1), api("eth_sendMoney", 0)))

	spec, report, err := ExpandSpecWithWriteLock(catalog, "EVMX")
	require.NoError(t, err)
	require.Nil(t, report.Forced, "nothing forced")
	require.Equal(t, 0, report.Count(types.APIInterfaceJsonRPC))
	require.Equal(t, 0, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_sendRawTransaction"), "disabled: untouched")
	require.Equal(t, 1, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_sendTransaction"))
	require.Equal(t, 0, statefulOf(spec, types.APIInterfaceJsonRPC, "eth_sendMoney"), "an undeclared write stays a read")
}

func TestWriteMethodKey(t *testing.T) {
	require.Equal(t, writeMethodKey("rest", "POST", "/v2", "/sendBoc"), writeMethodKey("rest", "POST", "", "/v2/sendBoc"), "a REST route is the internal path and the name")
	require.NotEqual(t, writeMethodKey("rest", "GET", "", "/txs"), writeMethodKey("rest", "POST", "", "/txs"), "the verb is part of a REST identity")
	require.Equal(t, writeMethodKey("jsonrpc", "POST", "/C/avax", "avax.issueTx"), writeMethodKey("jsonrpc", "", "", "avax.issueTx"), "every other interface is its name")
	require.NotEqual(t, writeMethodKey("jsonrpc", "", "", "submit"), writeMethodKey("rest", "", "", "submit"))
}

// The table is held to the catalog it was generated from, and to every later one: run with
// LAVA_SPECS_DIR pointing at a lava-specs checkout (the lava-specs chain families workflow does).
// Every api the catalog marks stateful has an entry that applies to its chain, so a spec that clears
// the flag tomorrow is caught, and the lock forces nothing on the catalog as it stands, so no read
// is relayed as a write. A new write in lava-specs fails here until scripts/gen-write-methods.sh is
// rerun. Without the variable the test skips, as it does in the required unit-tests; the workflow sets
// LAVA_SPECS_REQUIRED, so there a missing checkout fails instead of passing unnoticed.
func TestWriteMethodsTableCoversLavaSpecs(t *testing.T) {
	dir := os.Getenv("LAVA_SPECS_DIR")
	if dir == "" {
		require.Empty(t, os.Getenv("LAVA_SPECS_REQUIRED"), "LAVA_SPECS_DIR is unset: the lava-specs chain families workflow checks lava-specs out for this test")
		t.Skip("LAVA_SPECS_DIR is unset; point it at a lava-specs checkout to check its specs")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	require.NoError(t, err)
	catalog := map[string]types.Spec{}
	for _, file := range files {
		specs, err := GetAllSpecsFromFile(file)
		if err != nil {
			t.Logf("skipped, as the router skips it: %v", err)
			continue
		}
		maps.Copy(catalog, specs)
	}
	require.NotEmpty(t, catalog, "no spec found in %s", dir)

	table := writeMethods()
	var missing, forced []string
	for _, index := range slices.Sorted(maps.Keys(catalog)) {
		for _, collection := range catalog[index].ApiCollections {
			for _, api := range collection.Apis {
				if api.Category.Stateful >= StatefulAllProviders {
					method, listed := table[writeMethodKey(collection.CollectionData.ApiInterface, collection.CollectionData.Type, collection.CollectionData.InternalPath, api.Name)]
					if !listed || !appliesTo(method, index, catalog) {
						missing = append(missing, fmt.Sprintf("%s %s %s %s%s", index, collection.CollectionData.ApiInterface, collection.CollectionData.Type, collection.CollectionData.InternalPath, api.Name))
					}
				}
			}
		}
		_, report, err := ExpandSpecWithWriteLock(catalog, index)
		if err != nil {
			t.Logf("%s does not expand, as on the router: %v", index, err)
			continue
		}
		for apiInterface, names := range report.Forced {
			forced = append(forced, fmt.Sprintf("%s %s: %s", index, apiInterface, strings.Join(names, ", ")))
		}
	}
	assert.Empty(t, missing, "these stateful apis have no write-method entry that applies to their chain: "+
		"rerun scripts/gen-write-methods.sh against this checkout")
	assert.Empty(t, forced, "the lock forces these apis on the catalog as it stands: either the catalog cleared a write's "+
		"stateful (fix the spec, MAG-4413) or an entry applies to a chain that reads it (qualify it in scripts/gen-write-methods.sh)")
}
