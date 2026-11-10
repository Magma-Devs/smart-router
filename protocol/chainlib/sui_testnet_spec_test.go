package chainlib

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
)

// The Sui testnet spec, SUIT, declares no api of its own: it imports the mainnet spec, SUI, and
// replaces only the answer each chain-id check expects. If the import breaks, a testnet router
// refuses to start or serves with the wrong rules, so these tests put the real spec file through the
// loader, the parser and the verification pass the router runs (MAG-4779).
//
// testdata/sui.json is lava-specs sui.json byte for byte, the file a deployed router downloads from
// lava-specs main when it starts. It was copied at lava-specs main bda0597 (2026-11-10); the last
// commit there to change it is 1c547bd (2026-10-07). To refresh it, run
//
//	curl -fsSL https://raw.githubusercontent.com/Magma-Devs/lava-specs/main/sui.json -o protocol/chainlib/testdata/sui.json
//
// then update suiSpecSHA256 and the two commits above.
const (
	suiSpecFile   = "testdata/sui.json"
	suiSpecSHA256 = "e556f3467344b99fffbcdc7d7ef1b22caf5471ab058afcf0df5b686721f1d940"

	// What graphql.mainnet.sui.io and graphql.testnet.sui.io answered to { chainIdentifier } on
	// 2026-11-10. The specs' gRPC chain-id checks expect the same ids.
	suiMainnetChainID = "4btiuiMPvEENsttpZC7CZ53DruC3MAgfznDbASZ7DR6S"
	suiTestnetChainID = "69WiPg3DAQiwdxfncX6wYQ2siKwAe6L9BZthQea3JNMD"
)

// The fixture is lava-specs' file as it is, not a spec written for these tests.
func TestSuiSpecFixtureIsTheLavaSpecsFile(t *testing.T) {
	raw, err := os.ReadFile(suiSpecFile)
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	require.Equal(t, suiSpecSHA256, hex.EncodeToString(sum[:]),
		"%s must stay a byte-for-byte copy of lava-specs sui.json; refresh it as the comment on suiSpecSHA256 says", suiSpecFile)
}

// Proof 1: after the import, SUIT holds every api SUI declares, each with SUI's rules, and the router
// serves each one by them. GraphQL's 37 fields are the ticket's case; the same import carries the 54
// JSON-RPC methods and the 28 gRPC methods.
func TestSuiTestnetSpecImportsTheMainnetApis(t *testing.T) {
	for _, tc := range []struct {
		apiInterface string
		apis         int
	}{
		{spectypes.APIInterfaceGraphQL, 37},
		{spectypes.APIInterfaceJsonRPC, 54},
		{spectypes.APIInterfaceGrpc, 28},
	} {
		t.Run(tc.apiInterface, func(t *testing.T) {
			mainnetSpec, testnetSpec := loadSuiSpec(t, "SUI"), loadSuiSpec(t, "SUIT")
			mainnet := suiCollection(t, mainnetSpec, tc.apiInterface)
			testnet := suiCollection(t, testnetSpec, tc.apiInterface)

			// The count keeps the comparison below from passing on two empty collections.
			require.Len(t, apisByName(mainnet), tc.apis, "SUI declares %d %s apis", tc.apis, tc.apiInterface)
			// Whole structs: Api.Equal compares only Enabled, Name and ComputeUnits, so it would miss a
			// changed category, block parsing or timeout.
			require.Equal(t, apisByName(mainnet), apisByName(testnet), "SUIT's %s apis are SUI's", tc.apiInterface)
			require.Equal(t, directivesByKey(mainnet), directivesByKey(testnet), "SUIT polls the head and block hashes as SUI does")

			// A field SUIT lacks still resolves, to the Default- fallback (20 compute units,
			// deterministic, latest block), so the comparison is what catches it, not an error.
			mainnetParser := baseParser(t, suiParser(t, mainnetSpec, tc.apiInterface))
			testnetParser := baseParser(t, suiParser(t, testnetSpec, tc.apiInterface))
			for _, api := range mainnet.Apis {
				key := ApiKey{Name: api.Name, ConnectionType: mainnet.CollectionData.Type}
				want, err := mainnetParser.getSupportedApi(key)
				require.NoError(t, err)
				got, err := testnetParser.getSupportedApi(key)
				require.NoError(t, err)
				require.Equal(t, want.api, got.api, "SUIT serves %s by SUI's rules", api.Name)
				require.Equal(t, want.collectionKey, got.collectionKey)
			}
		})
	}
}

// Proof 2: SUIT's chain-id check expects the testnet chain id, and asks the node what SUI's check
// asks. SUIT writes only the expected value; the request comes from SUI through the import.
func TestSuiTestnetChainIDCheckExpectsTheTestnetChainID(t *testing.T) {
	for _, tc := range []struct {
		apiInterface     string
		mainnet, testnet string
	}{
		{spectypes.APIInterfaceGraphQL, suiMainnetChainID, suiTestnetChainID},
		{spectypes.APIInterfaceGrpc, suiMainnetChainID, suiTestnetChainID},
		{spectypes.APIInterfaceJsonRPC, "35834a8a", "4c78adac"},
	} {
		t.Run(tc.apiInterface, func(t *testing.T) {
			mainnet := suiChainIDCheck(t, loadSuiSpec(t, "SUI"), tc.apiInterface)
			testnet := suiChainIDCheck(t, loadSuiSpec(t, "SUIT"), tc.apiInterface)

			require.Equal(t, tc.testnet, testnet.Value, "SUIT expects the testnet chain id")
			require.Equal(t, tc.mainnet, mainnet.Value, "SUI expects the mainnet chain id")
			require.Equal(t, mainnet.ParseDirective, testnet.ParseDirective, "SUIT asks the node what SUI asks")
			require.Equal(t, spectypes.ParseValue_Fail, EffectiveSeverity(testnet), "a node with another chain id is refused, not only logged")
		})
	}
}

// Proof 3: a SUIT router admits a GraphQL node that answers the testnet chain id and refuses one that
// answers the mainnet chain id. The pass is the one that admits a provider (validateProviderCollections
// in rpcsmartrouter): a router over each node url, then ValidateCollections.
//
// The two cases differ in the chain id alone. The testnet case passing shows that the request and the
// parsing work, so the refusal in the mainnet case is the value.
func TestSuiTestnetGraphQLChainIDCheckAdmitsOnlyATestnetNode(t *testing.T) {
	UseMockLogLevel()
	for _, tc := range []struct {
		name    string
		chainID string
		admit   bool
	}{
		{"testnet node", suiTestnetChainID, true},
		{"mainnet node", suiMainnetChainID, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newSuiGraphQLNode(t, tc.chainID)
			endpoint := &lavasession.RPCProviderEndpoint{
				ChainID:      "SUIT",
				ApiInterface: spectypes.APIInterfaceGraphQL,
				NodeUrls:     []common.NodeUrl{{Url: node.URL}},
			}
			nodeUrlRouter, err := NewNodeUrlRouterFactory(1, endpoint)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			fetcher := NewChainFetcher(ctx, &ChainFetcherOptions{
				ChainParser:   suiParser(t, loadSuiSpec(t, "SUIT"), spectypes.APIInterfaceGraphQL),
				Endpoint:      endpoint,
				NodeUrlRouter: nodeUrlRouter,
			})

			admission, err := fetcher.ValidateCollections(ctx)

			require.NotZero(t, node.chainIDQueries(), "the node was asked its chain id, so the verdict comes from a check that ran")
			if tc.admit {
				require.NoError(t, err)
				require.False(t, admission.Any(), "nothing is refused")
				return
			}
			require.Error(t, err, "the provider is refused")
			require.True(t, admission.URLRefused(0))
			require.Equal(t, "chain-id", FailedCheck(admission.URLRefusal(0)), "the chain-id check refused the url, not a failed dial")
		})
	}
}

// loadSuiSpec loads one spec from the fixture the way the router loads a static spec source
// (statetracker.RegisterForSpecUpdatesOrSetStaticSpecsWithToken): every spec in the file, then the
// requested one expanded with what it imports. Each call decodes the file again, so two specs loaded
// here share no collection or api that could make a comparison pass by itself.
func loadSuiSpec(t *testing.T, index string) spectypes.Spec {
	t.Helper()
	specs, err := specutils.GetAllSpecsFromPath(suiSpecFile)
	require.NoError(t, err)
	spec, err := specutils.ExpandSpecWithDependencies(specs, index)
	require.NoError(t, err)
	return *spec
}

// suiCollection is the expanded spec's one collection on the interface.
func suiCollection(t *testing.T, spec spectypes.Spec, apiInterface string) *spectypes.ApiCollection {
	t.Helper()
	var found []*spectypes.ApiCollection
	for _, collection := range spec.ApiCollections {
		if collection.CollectionData.ApiInterface == apiInterface {
			found = append(found, collection)
		}
	}
	require.Len(t, found, 1, "%s has one %s collection", spec.Index, apiInterface)
	require.True(t, found[0].Enabled, "%s's %s collection is enabled", spec.Index, apiInterface)
	return found[0]
}

// suiParser is the parser a router builds for spec on the interface. It first fails the test on a
// check that has no request: SetSpec dereferences the parse directive of every check on its
// interface, and SUIT's checks carry none of their own until the import gives them SUI's, so a lost
// import would panic the whole package run instead of failing one test.
func suiParser(t *testing.T, spec spectypes.Spec, apiInterface string) ChainParser {
	t.Helper()
	for _, collection := range spec.ApiCollections {
		if collection.CollectionData.ApiInterface != apiInterface {
			continue
		}
		for _, verification := range collection.Verifications {
			require.NotNil(t, verification.ParseDirective, "%s's %s %s check has no request to send",
				spec.Index, apiInterface, verification.Name)
		}
	}
	parser, err := NewChainParser(apiInterface)
	require.NoError(t, err)
	parser.SetSpec(spec)
	return parser
}

// baseParser is the table a parser of any interface keeps its apis in.
func baseParser(t *testing.T, parser ChainParser) *BaseChainParser {
	t.Helper()
	switch typed := parser.(type) {
	case *GraphQLChainParser:
		return &typed.BaseChainParser
	case *JsonRPCChainParser:
		return &typed.BaseChainParser
	case *GrpcChainParser:
		return &typed.BaseChainParser
	}
	require.FailNow(t, "no base parser", "%T", parser)
	return nil
}

func apisByName(collection *spectypes.ApiCollection) map[string]spectypes.Api {
	apis := make(map[string]spectypes.Api, len(collection.Apis))
	for _, api := range collection.Apis {
		apis[api.Name] = *api
	}
	return apis
}

func directivesByKey(collection *spectypes.ApiCollection) map[string]spectypes.ParseDirective {
	directives := make(map[string]spectypes.ParseDirective, len(collection.ParseDirectives))
	for _, directive := range collection.ParseDirectives {
		directives[directive.Differeniator()] = *directive
	}
	return directives
}

// suiChainIDCheck is spec's one chain-id check on the interface, as the router lists the checks for a
// node url with no add-ons at the root path.
func suiChainIDCheck(t *testing.T, spec spectypes.Spec, apiInterface string) VerificationContainer {
	t.Helper()
	checks, err := suiParser(t, spec, apiInterface).GetVerifications(nil, "", apiInterface)
	require.NoError(t, err)
	var chainID []VerificationContainer
	for _, check := range checks {
		if check.Name == "chain-id" {
			chainID = append(chainID, check)
		}
	}
	// The router checks each expected value on its own and a node answers one chain id, so a second
	// value (SUI's, say) would refuse every node.
	require.Len(t, chainID, 1, "%s runs one %s chain-id check, with one expected value", spec.Index, apiInterface)
	return chainID[0]
}

// suiGraphQLNode is a Sui GraphQL node that answers { chainIdentifier } in the body Sui's public
// servers answer it with, and refuses every other request.
type suiGraphQLNode struct {
	*httptest.Server
	mu     sync.Mutex
	chains int
}

func newSuiGraphQLNode(t *testing.T, chainID string) *suiGraphQLNode {
	t.Helper()
	node := &suiGraphQLNode{}
	node.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Query string `json:"query"`
		}
		body, err := io.ReadAll(r.Body)
		if err == nil {
			err = json.Unmarshal(body, &request)
		}
		w.Header().Set("Content-Type", "application/json")
		if err != nil || r.Method != http.MethodPost || request.Query != "{ chainIdentifier }" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errors":[{"message":"this node answers only { chainIdentifier }"}]}`))
			return
		}
		node.mu.Lock()
		node.chains++
		node.mu.Unlock()
		_, _ = w.Write([]byte(`{"data":{"chainIdentifier":"` + chainID + `"}}`))
	}))
	t.Cleanup(node.Close)
	return node
}

func (n *suiGraphQLNode) chainIDQueries() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.chains
}
