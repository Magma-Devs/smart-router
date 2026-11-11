package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MAG-4729: the only node-url tagged archive is a backup that boot did not admit with archive —
// its archive check was refused, or it was unreachable. An archive request then has no node to go
// to, and the degrade path serves it from the pruned primary. The reply has to carry
// Smart-Router-Extension-Unavailable: archive, as it does when no node-url declares archive at all
// (MAG-3935); without it a non-archive answer to an archive request reads as a real one.

// bootArchiveBackupEndpoint runs the real CreateSmartRouterEndpoint for ETH1 JSON-RPC with one
// pruned primary and one backup whose url declares backupAddons, behind cache when it is not nil,
// and returns the server once it listens.
func bootArchiveBackupEndpoint(t *testing.T, primaryURL, backupURL string, backupAddons []string, cache performance.CacheBackend) *RPCSmartRouterServer {
	t.Helper()
	return bootETH1Endpoint(t,
		[]*routersession.RPCStaticProviderEndpoint{{
			Name: "primary-1", ChainID: "ETH1", ApiInterface: "jsonrpc",
			NodeUrls: []common.NodeUrl{{Url: primaryURL}},
		}},
		[]*routersession.RPCStaticProviderEndpoint{{
			Name: "backup-1", ChainID: "ETH1", ApiInterface: "jsonrpc",
			NodeUrls: []common.NodeUrl{{Url: backupURL, Addons: backupAddons}},
		}},
		cache)
}

// bootETH1Endpoint runs the real CreateSmartRouterEndpoint for ETH1 JSON-RPC with primaries and
// backups, behind cache when it is not nil, and returns the server once it listens.
func bootETH1Endpoint(t *testing.T, primaries, backups []*routersession.RPCStaticProviderEndpoint, cache performance.CacheBackend) *RPCSmartRouterServer {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	// The fake upstreams are HTTP only, as the helm chart's --skip-websocket-verification allows.
	prevSkipWS := chainlib.SkipWebsocketVerificationDefault
	chainlib.SkipWebsocketVerificationDefault = true
	t.Cleanup(func() { chainlib.SkipWebsocketVerificationDefault = prevSkipWS })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rpsr := createTestRPCSmartRouter()
	rpsr.reverifyInputs = make(map[string]*chainReverifyInputs)
	endpoint := &routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc", NetworkAddress: "127.0.0.1:0"}
	metricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	logs, err := metrics.NewRPCConsumerLogs(metricsManager, nil, nil)
	require.NoError(t, err)
	options := &rpcSmartRouterStartOptions{
		rpcEndpoints:        []*routersession.RPCEndpoint{endpoint},
		cache:               cache,
		strategy:            provideroptimizer.StrategyBalanced,
		cmdFlags:            common.ConsumerCmdFlags{StaticSpecPaths: []string{"../../specs/ethereum.json"}},
		staticProvidersList: primaries,
		backupProvidersList: backups,
	}
	err = rpsr.CreateSmartRouterEndpoint(ctx, endpoint, make(chan error, 1), &common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{},
		map[string]*sync.Mutex{"ETH1": {}}, options, "test-router", logs, nil, metricsManager, nil)
	require.NoError(t, err)
	server := rpsr.rpcServers[(&routersession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}).Key()]
	require.NotNil(t, server)
	require.Eventually(t, func() bool { return server.GetListeningAddress() != "" }, 5*time.Second, 10*time.Millisecond)
	return server
}

// relayGetCode sends one eth_getCode at block through the listener, as a client would, naming
// extension in smartrouter-extension unless it is empty.
func relayGetCode(t *testing.T, server *RPCSmartRouterServer, block, extension string) *http.Response {
	t.Helper()
	headers := map[string]string{}
	if extension != "" {
		headers[common.EXTENSION_OVERRIDE_HEADER_NAME] = extension
	}
	return relayGetCodeWithHeaders(t, server, block, headers)
}

// relayGetCodeWithHeaders is relayGetCode with the request headers given.
func relayGetCodeWithHeaders(t *testing.T, server *RPCSmartRouterServer, block string, headers map[string]string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["0x1111111111111111111111111111111111111111",%q]}`, block)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+server.GetListeningAddress()+"/", strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	return response
}

// getCodeHits counts the eth_getCode relays one url received. The boot's own checks reach every
// url too, so callers measure the change across one request.
func getCodeHits(u *fakeUpstreams, path string) int {
	hits := 0
	for _, method := range u.methods("http", path) {
		if method == "eth_getCode" {
			hits++
		}
	}
	return hits
}

func TestArchiveOnUnadmittedBackupReportsExtensionUnavailable(t *testing.T) {
	archive := extensionslib.ArchiveExtension
	cases := []struct {
		name         string
		backup       string   // personality of the backup's url
		backupAddons []string // what the backup's url declares
		block        string   // eth_getCode's block
		directive    string   // the request's smartrouter-extension; empty sends none
		servedBy     string
		unavailable  string
	}{
		{
			name:   "backup archive check refused: admitted without archive, the pruned primary serves",
			backup: upPruned, backupAddons: []string{archive}, block: "latest", directive: archive,
			servedBy: "primary-1", unavailable: archive,
		},
		{
			name:   "backup unreachable at boot: not admitted, the pruned primary serves",
			backup: upDead, backupAddons: []string{archive}, block: "latest", directive: archive,
			servedBy: "primary-1", unavailable: archive,
		},
		{
			name:   "control: backup admitted with archive serves, nothing to report",
			backup: upArchive, backupAddons: []string{archive}, block: "latest", directive: archive,
			servedBy: "backup-1", unavailable: "",
		},
		{
			// As before MAG-4729: parsing reports it and nothing degrades, so it is named once.
			name:   "no node-url declares archive: reported as before",
			backup: upPruned, backupAddons: nil, block: "latest", directive: archive,
			servedBy: "primary-1", unavailable: archive,
		},
		{
			// MAG-3935: the router promotes earliest to archive on its own and never reports that.
			name:   "router-added archive degrades unreported",
			backup: upPruned, backupAddons: []string{archive}, block: "earliest", directive: "",
			servedBy: "primary-1", unavailable: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := newFakeUpstreams(t)
			// Distinct paths, so each tier's hits are its own; the personality is the last segment.
			primaryPath, backupPath := "/primary/"+upPruned, "/backup/"+tc.backup
			server := bootArchiveBackupEndpoint(t, u.httpURL(primaryPath), u.httpURL(backupPath), tc.backupAddons, nil)
			primaryBefore, backupBefore := getCodeHits(u, primaryPath), getCodeHits(u, backupPath)

			response := relayGetCode(t, server, tc.block, tc.directive)

			require.Equal(t, tc.servedBy, response.Header.Get(common.PROVIDER_ADDRESS_HEADER_NAME))
			require.Equal(t, tc.unavailable, response.Header.Get(common.EXTENSION_UNAVAILABLE_HEADER_NAME),
				"served without an extension the caller asked for means the reply says so; otherwise no header")
			primaryHits, backupHits := getCodeHits(u, primaryPath)-primaryBefore, getCodeHits(u, backupPath)-backupBefore
			if tc.servedBy == "primary-1" {
				require.Equal(t, [2]int{1, 0}, [2]int{primaryHits, backupHits}, "[primary, backup] eth_getCode hits")
			} else {
				require.Equal(t, [2]int{0, 1}, [2]int{primaryHits, backupHits}, "[primary, backup] eth_getCode hits")
			}
		})
	}
}

// writeRecordingCache is a cache backend that keeps the reply of every write it passes on, so a
// test can tell which answers the router stored.
type writeRecordingCache struct {
	performance.CacheBackend
	mu      sync.Mutex
	written [][]byte
}

func (c *writeRecordingCache) SetEntry(ctx context.Context, cacheSet *pairingtypes.RelayCacheSet) error {
	c.mu.Lock()
	c.written = append(c.written, cacheSet.GetResponse().GetData())
	c.mu.Unlock()
	return c.CacheBackend.SetEntry(ctx, cacheSet)
}

// emptyCodeWrites counts the stored replies that are the empty code eth_getCode is answered with,
// leaving out what the router's own polls may store.
func (c *writeRecordingCache) emptyCodeWrites() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, data := range c.written {
		var reply struct{ Result any }
		if json.Unmarshal(data, &reply) == nil && reply.Result == "0x" {
			count++
		}
	}
	return count
}

// A reply served after the request degraded is not cached (MAG-4729). Its entry would sit under a
// key that names the caller's smartrouter-extension, so every repeat archive request would be answered
// from it: pruned-node data without Smart-Router-Extension-Unavailable, since a cache hit never reaches the
// degrade path that records it, and still after the archive node recovers.
func TestArchiveOnUnadmittedBackupIsNotCached(t *testing.T) {
	archive := extensionslib.ArchiveExtension
	boot := func(t *testing.T, primaryPath, backupPath string) (*writeRecordingCache, *fakeUpstreams, *RPCSmartRouterServer) {
		t.Helper()
		primaryCache, _ := startCacheServerForTest(t)
		cache := &writeRecordingCache{CacheBackend: primaryCache}
		u := newFakeUpstreams(t)
		server := bootArchiveBackupEndpoint(t, u.httpURL(primaryPath), u.httpURL(backupPath), []string{archive}, cache)
		return cache, u, server
	}

	t.Run("served without archive: not written, so the repeat is relayed and reported again", func(t *testing.T) {
		primaryPath := "/primary/" + upPruned
		cache, u, server := boot(t, primaryPath, "/backup/"+upPruned)
		for _, request := range []string{"first", "repeat"} {
			before, writesBefore := getCodeHits(u, primaryPath), cache.emptyCodeWrites()
			response := relayGetCode(t, server, "latest", archive)

			assert.Equal(t, archive, response.Header.Get(common.EXTENSION_UNAVAILABLE_HEADER_NAME),
				"the %s reply says it was served without archive", request)
			assert.Equal(t, 1, getCodeHits(u, primaryPath)-before, "the %s request reached the pruned primary, not the cache", request)
			// The router writes the cache from its own goroutine once the reply is out, so the
			// write gets the window it needs to land before the next request; none may.
			assert.Never(t, func() bool { return cache.emptyCodeWrites() > writesBefore }, capTestNoWriteWindow, 20*time.Millisecond,
				"a reply served without archive is not written to the cache after the %s request", request)
		}
	})

	// The same request served with archive is written, so the case above is the degrade's doing
	// and not a reply this harness never caches.
	t.Run("control: served with archive, the reply is written", func(t *testing.T) {
		cache, _, server := boot(t, "/primary/"+upPruned, "/backup/"+upArchive)
		response := relayGetCode(t, server, "latest", archive)

		require.Equal(t, "backup-1", response.Header.Get(common.PROVIDER_ADDRESS_HEADER_NAME))
		require.Empty(t, response.Header.Get(common.EXTENSION_UNAVAILABLE_HEADER_NAME))
		require.Eventually(t, func() bool { return cache.emptyCodeWrites() == 1 }, 3*time.Second, 20*time.Millisecond,
			"the reply is written to the cache")
	})
}

// The winner of a cross-validated request that degraded is not cached either (MAG-4729). It is
// written once, after the quorum, by SendParsedRelay rather than by a relay goroutine, so it has
// a skip of its own. The caller names archive and blocks both archive nodes with
// smartrouter-providers-block: the request degrades to the two pruned primaries, which agree.
func TestArchiveDegradedCrossValidationWinnerIsNotCached(t *testing.T) {
	archive := extensionslib.ArchiveExtension
	crossValidated := map[string]string{
		common.EXTENSION_OVERRIDE_HEADER_NAME:              archive,
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "2",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}
	boot := func(t *testing.T) (*writeRecordingCache, *RPCSmartRouterServer) {
		t.Helper()
		primaryCache, _ := startCacheServerForTest(t)
		cache := &writeRecordingCache{CacheBackend: primaryCache}
		u := newFakeUpstreams(t)
		primary := func(name, path string, addons ...string) *routersession.RPCStaticProviderEndpoint {
			return &routersession.RPCStaticProviderEndpoint{
				Name: name, ChainID: "ETH1", ApiInterface: "jsonrpc",
				NodeUrls: []common.NodeUrl{{Url: u.httpURL(path), Addons: addons}},
			}
		}
		server := bootETH1Endpoint(t, []*routersession.RPCStaticProviderEndpoint{
			primary("archive-1", "/archive-1/"+upArchive, archive),
			primary("archive-2", "/archive-2/"+upArchive, archive),
			primary("pruned-1", "/pruned-1/"+upPruned),
			primary("pruned-2", "/pruned-2/"+upPruned),
		}, nil, cache)
		return cache, server
	}

	t.Run("archive nodes blocked: the pruned winner is not written", func(t *testing.T) {
		cache, server := boot(t)
		headers := maps.Clone(crossValidated)
		headers[common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME] = "archive-1,archive-2"
		response := relayGetCodeWithHeaders(t, server, "latest", headers)

		require.Equal(t, "success", response.Header.Get(common.CROSS_VALIDATION_STATUS_HEADER_NAME))
		require.Equal(t, "pruned-1,pruned-2", response.Header.Get(common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER))
		require.Equal(t, archive, response.Header.Get(common.EXTENSION_UNAVAILABLE_HEADER_NAME))
		assert.Never(t, func() bool { return cache.emptyCodeWrites() > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
			"the winner of a request served without archive is not written to the cache")
	})

	// The same request with the archive nodes free to take it is written, so the case above is the
	// degrade's doing and not a cross-validation winner this harness never caches.
	t.Run("control: served by the archive nodes, the winner is written", func(t *testing.T) {
		cache, server := boot(t)
		response := relayGetCodeWithHeaders(t, server, "latest", crossValidated)

		require.Equal(t, "success", response.Header.Get(common.CROSS_VALIDATION_STATUS_HEADER_NAME))
		require.Equal(t, "archive-1,archive-2", response.Header.Get(common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER))
		require.Empty(t, response.Header.Get(common.EXTENSION_UNAVAILABLE_HEADER_NAME))
		require.Eventually(t, func() bool { return cache.emptyCodeWrites() == 1 }, 3*time.Second, 20*time.Millisecond,
			"the winner is written to the cache")
	})
}
