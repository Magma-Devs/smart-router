package common

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	speckeeper "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// specsImporting builds a catalog from each spec's imports, listed in the order its spec file lists
// them.
func specsImporting(imports map[string][]string) map[string]spectypes.Spec {
	specs := make(map[string]spectypes.Spec, len(imports))
	for index, parents := range imports {
		specs[index] = spectypes.Spec{Index: index, Imports: parents}
	}
	return specs
}

// A chain the root table does not list takes its family from its imports, as lava-specs (main
// 4bbb5e9) declares them. The seven production chains of MAG-5246 had no family, so no chain row
// could ever apply to them: HOD1, AVALANCHEC and AVALANCHECT (EVM), POLKADOTASSETHUB and POLYMESH
// (Substrate), TEZOS and TEZOST (Tezos). AGR and MOVEMENT were listed as EVM against their imports.
func TestChainFamilyComesFromImports(t *testing.T) {
	RegisterChainFamilies(specsImporting(map[string][]string{
		"ETH1": nil, "HOD1": {"ETH1"}, "AVALANCHEC": {"ETH1"}, "AVALANCHECT": {"AVALANCHEC"},
		"BTC": nil, "DOGE": {"BTC"}, "PEP": {"DOGE"}, "QTUM": {"BTC"},
		"IBC": nil, "TENDERMINT": nil, "COSMOSWASM": nil, "COSMOSSDK45DEP": nil, "ETHERMINT": nil,
		"COSMOSSDK": {"IBC", "TENDERMINT"}, "COSMOSSDK50": {"COSMOSSDK"},
		"COSMOSSDKFULL": {"COSMOSSDK", "COSMOSWASM"}, "AGR": {"COSMOSSDKFULL", "COSMOSSDK45DEP"},
		"APT1": nil, "MOVEMENT": {"APT1"}, "MOVEMENTT": {"MOVEMENT"},
		"SOLANA": nil, "KOII": {"SOLANA"}, "KOIIT": {"KOII"},
		"POLKADOTASSETHUB": nil, "POLKADOTASSETHUBT": {"POLKADOTASSETHUB"},
		"POLYMESH": nil, "POLYMESHT": {"POLYMESH"},
		"TEZOS": nil, "TEZOST": {"TEZOS"},
		"MONERO": nil, "MONEROT": {"MONERO"},
		// Imports from two families: the first import that has a family decides.
		"CANTO": {"ETH1", "COSMOSSDK", "COSMOSSDK45DEP"},
		"EVMOS": {"COSMOSSDK50", "ETHERMINT", "ETH1"},
	}))

	tests := []struct {
		chainID string
		want    ChainFamily
	}{
		{"HOD1", ChainFamilyEVM},
		{"AVALANCHEC", ChainFamilyEVM},
		{"AVALANCHECT", ChainFamilyEVM}, // through AVALANCHEC
		{"PEP", ChainFamilyBitcoin},     // through DOGE
		{"QTUM", ChainFamilyBitcoin},
		{"COSMOSSDK", ChainFamilyCosmosSDK},
		{"AGR", ChainFamilyCosmosSDK},  // was EVM
		{"MOVEMENT", ChainFamilyAptos}, // was EVM
		{"MOVEMENTT", ChainFamilyAptos},
		{"KOIIT", ChainFamilySolana},
		{"POLKADOTASSETHUB", ChainFamilySubstrate},
		{"POLKADOTASSETHUBT", ChainFamilySubstrate},
		{"POLYMESH", ChainFamilySubstrate},
		{"POLYMESHT", ChainFamilySubstrate},
		{"TEZOS", ChainFamilyTezos},
		{"TEZOST", ChainFamilyTezos},
		{"MONEROT", ChainFamilyMonero}, // not Bitcoin: monero.json imports nothing
		{"CANTO", ChainFamilyEVM},
		{"EVMOS", ChainFamilyCosmosSDK},
	}
	for tcIndex, tc := range tests {
		family, ok := GetChainFamily(tc.chainID)
		require.True(t, ok, "%s has no family, tc #%d", tc.chainID, tcIndex)
		require.Equal(t, tc.want, family, "%s, tc #%d", tc.chainID, tcIndex)
		require.Equal(t, tc.want, GetChainFamilyOrDefault(tc.chainID), "%s, tc #%d", tc.chainID, tcIndex)
	}
}

// A family's name is what logs and test names show for it, so every family a spec can take has one
// of its own.
func TestEveryChainFamilyHasAName(t *testing.T) {
	names := map[string]ChainFamily{}
	for _, family := range rootChainFamilies {
		name := family.String()
		require.NotEqual(t, fmt.Sprintf("ChainFamily(%d)", int(family)), name, "family %d has no name", int(family))
		if other, taken := names[name]; taken {
			require.Equal(t, other, family, "families %d and %d are both named %s", int(other), int(family), name)
		}
		names[name] = family
	}
}

// A spec none of whose ancestors has a family reads as unknown, as an unlisted chain always has, so
// it falls through to the transport rows instead of borrowing another family's.
func TestChainFamilyWithoutAFamilyStaysUnknown(t *testing.T) {
	RegisterChainFamilies(specsImporting(map[string][]string{
		"FAMILYLESSROOT":  nil,
		"FAMILYLESSCHILD": {"FAMILYLESSROOT"},
		"ORPHANCHILD":     {"NOT_IN_THE_CATALOG"},
	}))
	for _, chainID := range []string{"FAMILYLESSROOT", "FAMILYLESSCHILD", "ORPHANCHILD"} {
		_, ok := GetChainFamily(chainID)
		require.False(t, ok, chainID)
		require.Equal(t, ChainFamilyUnknown, GetChainFamilyOrDefault(chainID), chainID)
	}
}

// Spec expansion refuses an import loop, but the families are resolved from the whole catalog,
// expanded or not, so a loop must end the walk rather than recurse forever.
func TestChainFamilyImportLoopEnds(t *testing.T) {
	RegisterChainFamilies(specsImporting(map[string][]string{
		"LOOPA": {"LOOPB"}, "LOOPB": {"LOOPA"},
		"LOOPC": {"LOOPD", "ETH1"}, "LOOPD": {"LOOPC"},
	}))
	_, ok := GetChainFamily("LOOPA")
	require.False(t, ok)
	require.Equal(t, ChainFamilyEVM, GetChainFamilyOrDefault("LOOPC"), "the loop is left and the next import decides")
}

// Every endpoint registers the catalog it loaded. A later catalog adds to the families; it does not
// drop the chains an earlier one registered.
func TestRegisterChainFamiliesKeepsEarlierCatalogs(t *testing.T) {
	RegisterChainFamilies(specsImporting(map[string][]string{"FIRSTCATALOGCHAIN": {"ETH1"}}))
	RegisterChainFamilies(specsImporting(map[string][]string{"SECONDCATALOGCHAIN": {"SOLANA"}}))
	require.Equal(t, ChainFamilyEVM, GetChainFamilyOrDefault("FIRSTCATALOGCHAIN"))
	require.Equal(t, ChainFamilySolana, GetChainFamilyOrDefault("SECONDCATALOGCHAIN"))
}

// Replies are classified while a later endpoint is still loading its catalog. Run under -race.
func TestRegisterChainFamiliesWhileClassifying(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			RegisterChainFamilies(specsImporting(map[string][]string{fmt.Sprintf("CONCURRENTCHAIN%d", i): {"ETH1"}}))
		}()
		go func() {
			defer wg.Done()
			for range 100 {
				assert.Equal(t, ChainFamilyEVM, GetChainFamilyOrDefault("ETH1"))
				GetChainFamilyOrDefault(fmt.Sprintf("CONCURRENTCHAIN%d", i))
			}
		}()
	}
	wg.Wait()
	for i := range 4 {
		require.Equal(t, ChainFamilyEVM, GetChainFamilyOrDefault(fmt.Sprintf("CONCURRENTCHAIN%d", i)))
	}
}

// TestEveryLavaSpecHasAChainFamily loads lava-specs the way the router loads a catalog and checks
// what the router relies on:
//   - every spec gets a family. A spec with none is classified by no chain row; the root it imports
//     from needs an entry in rootChainFamilies.
//   - no spec with imports is in rootChainFamilies, so a chain's family never disagrees with what it
//     imports (AGR and MOVEMENT did).
//   - a spec is Bitcoin exactly when it is BTC or imports it. Those chains' replies are reshaped to
//     JSON-RPC 1.0 (chainlib isUTXOFamily), which is right only for a Bitcoin Core derivative.
//
// LAVA_SPECS_DIR names a lava-specs checkout. The unit-tests workflow checks out lava-specs main for
// it, so a spec merged there without a family fails the next run (MAG-5246). Without the variable the
// test skips, except under CI, where a missing checkout must not pass unnoticed.
func TestEveryLavaSpecHasAChainFamily(t *testing.T) {
	dir := os.Getenv("LAVA_SPECS_DIR")
	if dir == "" {
		require.Empty(t, os.Getenv("CI"), "LAVA_SPECS_DIR is unset: the unit-tests workflow checks lava-specs out for this test")
		t.Skip("LAVA_SPECS_DIR is unset; point it at a lava-specs checkout to check its specs")
	}
	// The files the router fetches from the repository: the .json files at its top, not the test
	// fixtures under .claude/ (specfetcher isDirectJSONChild). A file that does not parse is skipped,
	// as the fetcher skips it.
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	require.NoError(t, err)
	catalog := map[string]spectypes.Spec{}
	for _, file := range files {
		specs, err := speckeeper.GetAllSpecsFromFile(file)
		if err != nil {
			t.Logf("skipped, as the router skips it: %v", err)
			continue
		}
		maps.Copy(catalog, specs)
	}
	require.NotEmpty(t, catalog, "no spec found in %s", dir)
	RegisterChainFamilies(catalog)

	var withoutFamily, listedWithImports, bitcoinMismatch []string
	for _, index := range slices.Sorted(maps.Keys(catalog)) {
		family, ok := GetChainFamily(index)
		if !ok {
			withoutFamily = append(withoutFamily, fmt.Sprintf("%s (root %s)", index, strings.Join(catalogRoots(index, catalog, map[string]bool{}), ", ")))
		}
		if imports := catalog[index].Imports; len(imports) > 0 {
			if listed, inTable := rootChainFamilies[index]; inTable {
				listedWithImports = append(listedWithImports, fmt.Sprintf("%s (listed %s, imports %s)", index, listed, strings.Join(imports, ", ")))
			}
		}
		if importsBTC, isBitcoin := derivesFrom(index, "BTC", catalog, map[string]bool{}), ok && family == ChainFamilyBitcoin; importsBTC != isBitcoin {
			bitcoinMismatch = append(bitcoinMismatch, fmt.Sprintf("%s (family %s, imports BTC: %t)", index, family, importsBTC))
		}
	}
	assert.Empty(t, withoutFamily, "these lava-specs specs have no chain family, so no chain row applies to them: "+
		"give the root of each a family in rootChainFamilies (protocol/common/error_registry.go)")
	assert.Empty(t, listedWithImports, "these specs import others, so they take their family from their imports: "+
		"remove them from rootChainFamilies")
	assert.Empty(t, bitcoinMismatch, "a chain is Bitcoin, and its replies reshaped to JSON-RPC 1.0, exactly when its spec is BTC or imports it")
}

// catalogRoots lists the specs index reaches through its imports that import nothing themselves, or
// are missing from the catalog: where a family has to be named for index to get one.
func catalogRoots(index string, catalog map[string]spectypes.Spec, seen map[string]bool) []string {
	if seen[index] {
		return nil
	}
	seen[index] = true
	spec, ok := catalog[index]
	if !ok || len(spec.Imports) == 0 {
		return []string{index}
	}
	var roots []string
	for _, parent := range spec.Imports {
		roots = append(roots, catalogRoots(parent, catalog, seen)...)
	}
	return roots
}

// derivesFrom reports whether index is ancestor, or imports it directly or through other imports.
func derivesFrom(index, ancestor string, catalog map[string]spectypes.Spec, seen map[string]bool) bool {
	if index == ancestor {
		return true
	}
	if seen[index] {
		return false
	}
	seen[index] = true
	for _, parent := range catalog[index].Imports {
		if derivesFrom(parent, ancestor, catalog, seen) {
			return true
		}
	}
	return false
}
