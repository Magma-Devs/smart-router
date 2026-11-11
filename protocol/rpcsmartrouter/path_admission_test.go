package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// tontRootHash is TONT's chain-id: the `/v2` collection's only verification.
const tontRootHash = "gj+B8wb/AmlPk1z1AhVI484rhrUpgSr2oSFIh56VoSg="

// fakeTon is a TON node serving the v2 api under /v2 and the v3 api under /v3, so a
// bare base url and the per-path urls the router generates from it reach it alike.
type fakeTon struct {
	srv *httptest.Server

	mu   sync.Mutex
	root string // what /v2/getMasterchainInfo reports as init.root_hash
	hits map[string]int
	// revokedKey, when set, is an X-Api-Key value the node answers 401 to.
	revokedKey string
}

// setRoot changes the chain the node reports, as a vendor that is fixed or broken
// while the router runs.
func (f *fakeTon) setRoot(root string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.root = root
}

func newFakeTon(t *testing.T, root string) *fakeTon {
	t.Helper()
	chainlib.UseMockLogLevel()
	f := &fakeTon{root: root, hits: map[string]int{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.hits[r.URL.Path]++
		root, revoked := f.root, f.revokedKey != "" && r.Header.Get("X-Api-Key") == f.revokedKey
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if revoked {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"invalid api key"}`)
			return
		}
		switch r.URL.Path {
		case "/v2/getMasterchainInfo":
			fmt.Fprintf(w, `{"ok":true,"result":{"init":{"root_hash":%q},"last":{"workchain":-1,"seqno":100}}}`, root)
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

	t.Run("AVAX: a bare C-chain url stays, as on main", func(t *testing.T) {
		// /C/rpc inherits ETH1's eth_subscribe, so an https url never generates it.
		// The bare url is then the only endpoint C-chain relays reach; dropping it
		// sent every eth_* call to .../C/rpc/C/avax (MAG-4214 review).
		parser := realSpecParser(t, "AVAX", spectypes.APIInterfaceJsonRPC)
		require.False(t, parser.IsInternalPathEnabled("", spectypes.APIInterfaceJsonRPC, ""), "AVAX serves no root collection")
		base := "https://avax.example/ext/bc/C/rpc"
		require.Equal(t, []commonlib.NodeUrl{
			{Url: base},
			{Url: base + "/C/avax", InternalPath: "/C/avax"},
			{Url: base + "/P", InternalPath: "/P"},
			{Url: base + "/X", InternalPath: "/X"},
		}, dialedNodeUrls([]commonlib.NodeUrl{{Url: base}}, parser, spectypes.APIInterfaceJsonRPC))
	})

	t.Run("STRK: a root and versioned paths keep the bare url and add each version on its own transport", func(t *testing.T) {
		parser := realSpecParser(t, "STRK", spectypes.APIInterfaceJsonRPC)
		require.True(t, parser.IsInternalPathEnabled("", spectypes.APIInterfaceJsonRPC, ""), "STRK serves a root collection")
		for _, scheme := range []struct{ base, prefix string }{
			{"https://strk.example", "/rpc/"},
			{"wss://strk.example", "/ws/rpc/"},
		} {
			dialed := dialedNodeUrls([]commonlib.NodeUrl{{Url: scheme.base}}, parser, spectypes.APIInterfaceJsonRPC)
			require.Equal(t, commonlib.NodeUrl{Url: scheme.base}, dialed[0], "the root url stays the root endpoint")
			require.Greater(t, len(dialed), 1, "versions are generated")
			for _, url := range dialed[1:] {
				require.True(t, strings.HasPrefix(url.InternalPath, scheme.prefix), "%s generated %s", scheme.base, url.InternalPath)
				require.Equal(t, scheme.base+url.InternalPath, url.Url)
			}
		}
	})
}

// realSpecParser loads a lava-specs file copied into testdata/specs, with ETH1 from
// the repo's specs for the ones that import it.
func realSpecParser(t *testing.T, chainID, apiInterface string) chainlib.ChainParser {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs", "testdata/specs"}, chainID)
	require.NoError(t, err)
	parser, err := chainlib.NewChainParser(apiInterface)
	require.NoError(t, err)
	parser.SetSpec(spec)
	return parser
}

// Two entries on one url that differ only in their key are verified apart, and one's
// verdict is never read as the other's: a revoked key must not take the good entry
// down with it, nor ride into the session on the good one's pass (MAG-4214 review).
func TestPathAdmission_SameUrlWithTwoKeysIsJudgedPerEntry(t *testing.T) {
	parser := tontParser(t)
	node := newFakeTon(t, tontRootHash)
	node.revokedKey = "revoked"
	withKey := func(url commonlib.NodeUrl, key string) commonlib.NodeUrl {
		url.AuthConfig.AuthHeaders = map[string]string{"X-Api-Key": key}
		return url
	}
	v2 := commonlib.NodeUrl{Url: node.srv.URL + "/v2", InternalPath: "/v2"}
	revoked, good := withKey(v2, "revoked"), withKey(v2, "good")
	v3 := commonlib.NodeUrl{Url: node.srv.URL + "/v3", InternalPath: "/v3"}

	for name, urls := range map[string][]commonlib.NodeUrl{
		"revoked first": {revoked, good, v3},
		"good first":    {good, revoked, v3},
	} {
		t.Run(name, func(t *testing.T) {
			provider := tontProvider(urls...)
			dialed := dialedNodeUrls(provider.NodeUrls, parser, spectypes.APIInterfaceRest)
			require.Equal(t, urls, dialed, "entries that differ in their key are not duplicates")

			admission, err := validateProviderCollections(t.Context(), provider, parser, 10*time.Second)
			require.NoError(t, err)
			require.True(t, admission.RefusesURL(revoked))
			require.False(t, admission.RefusesURL(good))

			kept := admittedNodeUrls(provider, dialed, admission)
			require.Equal(t, []commonlib.NodeUrl{good, v3}, kept)
			require.Nil(t, unservedInternalPaths(dialed, endpointsFor(kept)))
		})
	}
}

// The real boot path, end to end. cs has one bare base url, and at boot its /v2 is
// on another chain: cs is admitted for /v3 alone, and its session records /v2 as
// lost so selection passes it over there. When cs's /v2 comes back, the
// verification retry (MAG-3950) re-runs the failed chain-id check on its own
// backoff and rebuilds cs with /v2 again, without waiting for the 15 minute epoch
// pass, and with one session per provider (MAG-4214 review).
func TestCreateSmartRouterEndpoint_PathLostAtBootComesBackOnRetry(t *testing.T) {
	rand.InitRandomSeed()
	shortenRetryIntervals(t)
	cs := newFakeTon(t, "XXXXwrongchainXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=")
	ta := newFakeTon(t, tontRootHash)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rpsr := createTestRPCSmartRouter()
	rpsr.reverifyInputs = make(map[string]*chainReverifyInputs)
	endpoint := &lavasession.RPCEndpoint{ChainID: "TONT", ApiInterface: spectypes.APIInterfaceRest, NetworkAddress: "127.0.0.1:0"}
	metricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	logs, err := metrics.NewRPCConsumerLogs(metricsManager, nil, nil)
	require.NoError(t, err)
	csProvider := tontProvider(commonlib.NodeUrl{Url: cs.srv.URL})
	taProvider := tontProvider(commonlib.NodeUrl{Url: ta.srv.URL})
	taProvider.Name = "ta"
	options := &rpcSmartRouterStartOptions{
		rpcEndpoints:        []*lavasession.RPCEndpoint{endpoint},
		strategy:            provideroptimizer.StrategyBalanced,
		cmdFlags:            commonlib.ConsumerCmdFlags{StaticSpecPaths: []string{"../chainlib/testdata/ton_internal_paths.json"}},
		staticProvidersList: []*lavasession.RPCStaticProviderEndpoint{csProvider, taProvider},
	}
	require.NoError(t, rpsr.CreateSmartRouterEndpoint(ctx, endpoint, make(chan error, 1), &commonlib.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{},
		map[string]*sync.Mutex{"TONT": {}}, options, "test-router", logs, nil, metricsManager, nil))

	// session reads the live session of a provider: its endpoints' paths and the paths
	// it lost. Sessions are counted too, so a rebuild that adds one beside the old
	// one is caught.
	session := func(name string) (paths []string, lost map[string]struct{}, sessions int) {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		for _, s := range rpsr.providerSessions[endpoint.Key()] {
			if s.PublicLavaAddress != name {
				continue
			}
			sessions++
			lost = s.UnservedInternalPaths
			paths = paths[:0]
			for _, e := range s.Endpoints {
				paths = append(paths, e.InternalPath)
			}
		}
		return paths, lost, sessions
	}

	paths, lost, sessions := session("cs")
	require.Equal(t, 1, sessions, "cs is admitted: /v3 is still served")
	require.Equal(t, []string{"/v3"}, paths, "no endpoint for the bare base url, and /v2 failed chain-id")
	require.Equal(t, map[string]struct{}{"/v2": {}}, lost)
	paths, lost, _ = session("ta")
	require.ElementsMatch(t, []string{"/v2", "/v3"}, paths)
	require.Empty(t, lost)

	cs.setRoot(tontRootHash)
	require.Eventually(t, func() bool {
		paths, lost, sessions := session("cs")
		return sessions == 1 && len(lost) == 0 && slices.Contains(paths, "/v2") && slices.Contains(paths, "/v3")
	}, 15*time.Second, 20*time.Millisecond, "the retry gives cs its /v2 back, in place")
	require.Len(t, rpsr.providerSessions[endpoint.Key()], 2, "one session per provider, no duplicate")
}
