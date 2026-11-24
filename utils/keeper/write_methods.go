package keeper

import (
	"sort"
	"strings"
	"sync"

	types "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// The write-method lock (PRD PER-CHAIN-ERROR-CLASSIFICATION §5.3, MAG-5593). A write the spec declares is
// relayed stateful, to every provider at once and never again, whatever the spec says about `stateful`:
// a `stateful: 0` on eth_sendRawTransaction single-homed the broadcast on main (MAG-3389), and a spec
// loaded from a source is the operator's to edit. The lock runs on the expanded spec, before the chain
// parser reads it, so an inherited api is covered too. A write the spec does not declare at all stays a
// read, the rulebook's accepted risk (Part I §1, "Unknown methods").

// StatefulAllProviders is the Stateful level the lock sets: common.CONSISTENCY_SELECT_ALL_PROVIDERS,
// which this package cannot import (protocol/common imports it). TestStatefulAllProvidersIsCommons
// pins the two to one value.
const StatefulAllProviders uint32 = 1

// writeMethod is one api lava-specs marks stateful (write_methods_table.go, generated). Specs, when set,
// names the specs the entry applies to, with every spec that imports them: the catalog declares the
// same api a read on other chains (eth_submitWork is ETC's write and fourteen EVM chains' read), or the
// PRD names it a generic name (XRP's submit). Nil applies to every spec that serves the api.
type writeMethod struct {
	Interface    string
	Verb         string
	InternalPath string
	Name         string
	Specs        []string
}

// writeMethodKey is how the lock identifies an api. On REST it is the verb and the full route, the
// collection's internal path followed by the api's name, so TON's "/v2" + "/sendBoc" and a bare
// "/v2/sendBoc" are one route. On every other interface it is the name, which is what the parser
// resolves a request to: a URI-form Tendermint request has its JSON-RPC name, a gRPC request its full
// service/method, a GraphQL request its root field.
func writeMethodKey(apiInterface, verb, internalPath, name string) string {
	if apiInterface == types.APIInterfaceRest {
		return apiInterface + "\x1f" + verb + "\x1f" + internalPath + name
	}
	return apiInterface + "\x1f\x1f" + name
}

var (
	writeMethodIndexOnce sync.Once
	writeMethodIndex     map[string]writeMethod
)

// writeMethods is the table indexed by writeMethodKey, built once.
func writeMethods() map[string]writeMethod {
	writeMethodIndexOnce.Do(func() {
		writeMethodIndex = make(map[string]writeMethod, len(writeMethodsTable))
		for _, method := range writeMethodsTable {
			writeMethodIndex[writeMethodKey(method.Interface, method.Verb, method.InternalPath, method.Name)] = method
		}
	})
	return writeMethodIndex
}

// WriteLockReport is what the lock did to one expanded spec: per api interface, the apis it loaded
// stateful because the spec had cleared or omitted their flag. An interface with nothing forced is
// absent, so Count reads 0 for it: the gauge is written for every interface, zeros included.
type WriteLockReport struct {
	ChainID string
	Forced  map[string][]string
}

// Count is how many apis the lock forced on apiInterface, the value of
// smartrouter_spec_lock_enforced{spec, apiInterface, rule="stateful"}.
func (r WriteLockReport) Count(apiInterface string) int { return len(r.Forced[apiInterface]) }

// enforceWriteMethods loads every api the table names stateful, and reports the ones the spec had not.
// catalog is the unexpanded specs the lock resolves a qualified entry's importers from.
//
// The spec's collections are copied before they are changed. The map a caller expands from holds the
// same *ApiCollection and *Api objects the expanded spec does, and every endpoint of a chain, and
// every chain that imports it, expands from that map in turn: a flag forced in place would be read
// as declared by the next endpoint, and the gauge would name whichever booted first.
func enforceWriteMethods(spec *types.Spec, catalog map[string]types.Spec) WriteLockReport {
	report := WriteLockReport{ChainID: spec.Index}
	table := writeMethods()
	var collections []*types.ApiCollection
	for i, collection := range spec.ApiCollections {
		if collection == nil {
			continue
		}
		var forced []int
		for j, api := range collection.Apis {
			if api == nil || !api.Enabled || api.Category.Stateful >= StatefulAllProviders {
				continue
			}
			method, listed := table[writeMethodKey(collection.CollectionData.ApiInterface, collection.CollectionData.Type, collection.CollectionData.InternalPath, api.Name)]
			if !listed || !appliesTo(method, spec.Index, catalog) {
				continue
			}
			forced = append(forced, j)
		}
		if len(forced) == 0 {
			continue
		}
		if collections == nil {
			collections = append(make([]*types.ApiCollection, 0, len(spec.ApiCollections)), spec.ApiCollections...)
		}
		copied := *collection
		copied.Apis = append(make([]*types.Api, 0, len(collection.Apis)), collection.Apis...)
		for _, j := range forced {
			api := *collection.Apis[j]
			api.Category.Stateful = StatefulAllProviders
			copied.Apis[j] = &api
			if report.Forced == nil {
				report.Forced = map[string][]string{}
			}
			report.Forced[collection.CollectionData.ApiInterface] = append(report.Forced[collection.CollectionData.ApiInterface], api.Name)
		}
		collections[i] = &copied
	}
	if collections != nil {
		spec.ApiCollections = collections
	}
	for apiInterface, names := range report.Forced {
		sort.Strings(names)
		utils.FormatWarning("Write methods loaded stateful although the spec did not mark them: fix the spec", nil,
			utils.LogAttr("chain_id", spec.Index),
			utils.LogAttr("api_interface", apiInterface),
			utils.LogAttr("apis", strings.Join(names, ", ")))
	}
	return report
}

// appliesTo reports whether method's entry governs the spec index: every spec when the entry names
// none, else the named specs and the specs that import one of them, however far up.
func appliesTo(method writeMethod, index string, catalog map[string]types.Spec) bool {
	if len(method.Specs) == 0 {
		return true
	}
	return derivesFromAny(index, method.Specs, catalog, map[string]bool{})
}

func derivesFromAny(index string, roots []string, catalog map[string]types.Spec, visiting map[string]bool) bool {
	for _, root := range roots {
		if index == root {
			return true
		}
	}
	if visiting[index] {
		return false
	}
	visiting[index] = true
	for _, parent := range catalog[index].Imports {
		if derivesFromAny(parent, roots, catalog, visiting) {
			return true
		}
	}
	return false
}
