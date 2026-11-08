package rpcsmartrouter

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// tontRootHash is TONT's chain-id: the `/v2` collection's only verification.
const tontRootHash = "gj+B8wb/AmlPk1z1AhVI484rhrUpgSr2oSFIh56VoSg="

// fakeTon is a TON node serving the v2 api under /v2 and the v3 api under /v3, so a
// bare base url and the per-path urls the router generates from it reach it alike.
type fakeTon struct {
	srv  *httptest.Server
	root string // what /v2/getMasterchainInfo reports as init.root_hash

	mu   sync.Mutex
	hits map[string]int
}

func newFakeTon(t *testing.T, root string) *fakeTon {
	t.Helper()
	chainlib.UseMockLogLevel()
	f := &fakeTon{root: root, hits: map[string]int{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.hits[r.URL.Path]++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/getMasterchainInfo":
			fmt.Fprintf(w, `{"ok":true,"result":{"init":{"root_hash":%q},"last":{"workchain":-1,"seqno":100}}}`, f.root)
		case "/v3/masterchainInfo":
			fmt.Fprint(w, `{"last":{"workchain":-1,"seqno":100}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":"not found"}`)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeTon) asked(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[path]
}

// tontParser loads chainlib's TON test spec (lava-specs ton.json trimmed: chain-id on
// /v2 only, no root collection) as TONT.
func tontParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../chainlib/testdata"}, "TONT")
	require.NoError(t, err)
	parser, err := chainlib.NewChainParser(spectypes.APIInterfaceRest)
	require.NoError(t, err)
	parser.SetSpec(spec)
	return parser
}

func tontProvider(urls ...commonlib.NodeUrl) *lavasession.RPCStaticProviderEndpoint {
	return &lavasession.RPCStaticProviderEndpoint{Name: "cs", ChainID: "TONT", ApiInterface: spectypes.APIInterfaceRest, NodeUrls: urls}
}

// endpointsFor stands in for the session build: one endpoint per admitted url.
func endpointsFor(urls []commonlib.NodeUrl) []*lavasession.Endpoint {
	endpoints := make([]*lavasession.Endpoint, 0, len(urls))
	for _, url := range urls {
		endpoints = append(endpoints, &lavasession.Endpoint{NetworkAddress: url.Url, InternalPath: url.InternalPath})
	}
	return endpoints
}

// MAG-4214, the base-url shape: a url configured with no internal path is expanded
// into one url per path. Each generated url must be verified with its own path's
// checks. TONT has no root collection, so before the fix the base url had no checks
// at all and a node on another chain was admitted and served.
func TestPathAdmission_EachGeneratedUrlIsCheckedOnItsOwnPath(t *testing.T) {
	parser := tontParser(t)

	t.Run("a wrong-chain /v2 is refused alone", func(t *testing.T) {
		node := newFakeTon(t, "XXXXwrongchainXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=")
		provider := tontProvider(commonlib.NodeUrl{Url: node.srv.URL})

		admission, err := validateProviderCollections(t.Context(), provider, parser, 10*time.Second)
		require.NoError(t, err, "/v3 is still served")

		v2 := commonlib.NodeUrl{Url: node.srv.URL + "/v2", InternalPath: "/v2"}
		v3 := commonlib.NodeUrl{Url: node.srv.URL + "/v3", InternalPath: "/v3"}
		dialed := dialedNodeUrls(provider.NodeUrls, parser, spectypes.APIInterfaceRest)
		require.Equal(t, []commonlib.NodeUrl{v2, v3}, dialed,
			"TONT has no root collection: the bare base url is not dialed, only the urls generated from it")
		require.True(t, admission.RefusesURL(v2), "the generated /v2 url failed chain-id")
		require.False(t, admission.RefusesURL(v3))

		kept := admittedNodeUrls(provider, dialed, admission)
		require.Equal(t, []commonlib.NodeUrl{v3}, kept)
		require.Equal(t, map[string]struct{}{"/v2": {}}, unservedInternalPaths(dialed, endpointsFor(kept)),
			"the provider is passed over for v2 apis instead of sending them to its other urls")
	})

	t.Run("a healthy node is admitted, and its /v2 check was actually asked", func(t *testing.T) {
		node := newFakeTon(t, tontRootHash)
		provider := tontProvider(commonlib.NodeUrl{Url: node.srv.URL})

		admission, err := validateProviderCollections(t.Context(), provider, parser, 10*time.Second)
		require.NoError(t, err)
		require.False(t, admission.Any())
		require.Positive(t, node.asked("/v2/getMasterchainInfo"), "chain-id ran on the generated /v2 url")

		dialed := dialedNodeUrls(provider.NodeUrls, parser, spectypes.APIInterfaceRest)
		require.Nil(t, unservedInternalPaths(dialed, endpointsFor(admittedNodeUrls(provider, dialed, admission))))
	})
}

// MAG-4214, the two-url shape dfns TONT runs: a /v2 url failing chain-id costs the
// /v2 path, and the provider keeps serving /v3 from its healthy url.
func TestPathAdmission_AFailedPathKeepsTheProvider(t *testing.T) {
	parser := tontParser(t)
	node := newFakeTon(t, "XXXXwrongchainXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=")
	v2 := commonlib.NodeUrl{Url: node.srv.URL + "/v2", InternalPath: "/v2"}
	v3 := commonlib.NodeUrl{Url: node.srv.URL + "/v3", InternalPath: "/v3"}

	t.Run("one path left: admitted without the lost path", func(t *testing.T) {
		provider := tontProvider(v2, v3)
		admission, err := validateProviderCollections(t.Context(), provider, parser, 10*time.Second)
		require.NoError(t, err)
		require.True(t, admission.RefusesURL(v2))
		require.False(t, admission.RefusesURL(v3))

		dialed := dialedNodeUrls(provider.NodeUrls, parser, spectypes.APIInterfaceRest)
		kept := admittedNodeUrls(provider, dialed, admission)
		require.Equal(t, []commonlib.NodeUrl{v3}, kept)
		require.Equal(t, map[string]struct{}{"/v2": {}}, unservedInternalPaths(dialed, endpointsFor(kept)))
	})

	t.Run("no path left: the provider is refused", func(t *testing.T) {
		_, err := validateProviderCollections(t.Context(), tontProvider(v2), parser, 10*time.Second)
		require.ErrorContains(t, err, "no node url left serving internal path /v2")
	})
}

// dialedNodeUrls keeps the bare base url only on a spec that serves a root
// collection. The check must read the real specs right: ETH1 serves its whole
// method set at the root, TONT serves nothing there.
func TestDialedNodeUrls_RootCollectionCheckOnRealSpecs(t *testing.T) {
	require.True(t, liveParser(t, "ETH1", true).IsInternalPathEnabled("", spectypes.APIInterfaceJsonRPC, ""))
	require.False(t, tontParser(t).IsInternalPathEnabled("", spectypes.APIInterfaceRest, ""))

	eth := []commonlib.NodeUrl{{Url: "https://eth.example"}}
	require.Equal(t, eth, dialedNodeUrls(eth, liveParser(t, "ETH1", true), spectypes.APIInterfaceJsonRPC))
}
