package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The stuck-provider alerts (MAG-4204) read three things the server wires, not the metrics
// manager: how long an answer counts on the chain's poll cadence, the url's spec path, and relays
// counted on the url they went to. A metrics test passes with any of those calls gone, so these
// drive the server's own paths.

// urlSeriesLabels reads every series of one per-url metric for provider, as its labels.
func urlSeriesLabels(t *testing.T, name, provider string) []map[string]string {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var out []map[string]string
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, mtr := range mf.GetMetric() {
			lm := map[string]string{}
			for _, lp := range mtr.GetLabel() {
				lm[lp.GetName()] = lp.GetValue()
			}
			if lm["endpoint_id"] == provider {
				out = append(out, lm)
			}
		}
	}
	return out
}

// An answer counts for two of the chain's poll intervals, so a url on a slow chain that answers
// every poll never reads as down between two polls. A 10-minute chain polls every 5 minutes, so
// the tracker's first answer counts for 10 — not the 2-minute floor.
func TestURLLatestBlock_AnsweredUntilFollowsTheChainsPollInterval(t *testing.T) {
	const provider = "lava@mag4204SlowChain"
	upstream := newHeadUpstream(t, func() int64 { return 1000 }, nil)
	label := metrics.URLFingerprint(upstream.srv.URL)

	before := time.Now()
	trackProviderURLsEvery(t, 10*time.Minute, provider, upstream)
	require.Eventually(t, func() bool { return urlBlocks(t, provider)[label] == 1000 }, 15*time.Second, 10*time.Millisecond)
	after := time.Now()

	until := answeredUntil(t, provider)[label]
	require.GreaterOrEqual(t, until, float64(before.Add(10*time.Minute).Unix()))
	require.LessOrEqual(t, until, float64(after.Add(10*time.Minute).Unix()))
}

// The node urls CreateSmartRouterEndpoint registers carry their spec path onto the per-url
// series: internal_path is how an alert tells TON's /v2 from its /v3.
func TestRegisterEndpointMetrics_GivesTheURLsSeriesItsPath(t *testing.T) {
	const provider = "lava@mag4204Path"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	listen := &lavasession.RPCEndpoint{ChainID: "TON", ApiInterface: spectypes.APIInterfaceRest}
	v2 := common.NodeUrl{Url: "http://ton-wiring.example/api/v2", InternalPath: "/v2"}
	v3 := common.NodeUrl{Url: "http://ton-wiring.example/api/v3", InternalPath: "/v3"}
	registerEndpointMetrics(mm, listen, v2, provider)
	registerEndpointMetrics(mm, listen, v3, provider)
	registerEndpointMetrics(nil, listen, v2, provider) // a server without metrics

	mm.SetEndpointURLLatestBlock("TON", spectypes.APIInterfaceRest, v2.Url, 1000, metrics.DefaultURLAnswerTimeout)
	mm.SetEndpointURLLatestBlock("TON", spectypes.APIInterfaceRest, v3.Url, 990, metrics.DefaultURLAnswerTimeout)
	series := func(url common.NodeUrl) map[string]string {
		return map[string]string{
			"spec": "TON", "apiInterface": spectypes.APIInterfaceRest, "endpoint_id": provider,
			"url": metrics.URLFingerprint(url.Url), "internal_path": url.InternalPath,
		}
	}
	require.ElementsMatch(t, []map[string]string{series(v2), series(v3)},
		urlSeriesLabels(t, "rpc_endpoint_url_latest_block", provider))
}

// A relay the router serves counts on the url it went to: a REST request through the listener,
// the session manager and the direct connection to a real upstream.
func TestDirectRelay_CountsTheRelayOnTheURLItWentTo(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const provider = "lava@mag4204Relays"
	const path = "/cosmos/base/tendermint/v1beta1/blocks/17"
	parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"block":{"header":{"height":"17"}}}`))
		}), nil, "../../", nil)
	require.NoError(t, err)
	defer closeServer()

	nodeURL := endpoint.NodeUrls[0]
	conn, err := lavasession.NewDirectRPCConnection(ctx, nodeURL, 5, spectypes.APIInterfaceRest)
	require.NoError(t, err)
	defer conn.Close()
	session := lavasession.NewConsumerSessionWithProvider(provider, []*lavasession.Endpoint{{
		NetworkAddress: nodeURL.Url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn},
	}}, 100000, 1, 1)
	session.StaticProvider = true
	sessionManager, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	require.NoError(t, sessionManager.UpdateAllProviders(1,
		map[uint64]*lavasession.ConsumerSessionsWithProvider{0: session}, nil))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	registerEndpointMetrics(mm, rpcEndpoint, nodeURL, provider)
	server := &RPCSmartRouterServer{
		chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs:         logs,
		consistencyConfig:          relaycore.DefaultConsistencyValidationConfig(),
		smartRouterEndpointMetrics: mm,
	}
	listener := chainlib.NewRestChainListener(ctx, rpcEndpoint, server, nil, logs)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		assert.NoError(t, listener.Shutdown(shutdownCtx))
		select {
		case <-listenerDone:
		case <-shutdownCtx.Done():
			t.Error("REST listener did not stop")
		}
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)

	for i := 0; i < 2; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.GetListeningAddress()+path, nil)
		require.NoError(t, err)
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
	}
	// Only this run's url: the registry is process-wide, and each run's upstream gets a new port,
	// so a second run in one process (-count=2) also sees the first run's series.
	series := gatherSeries(t, "rpc_endpoint_url_relays_serviced_total", provider)
	require.Equal(t, float64(2), series[metrics.URLFingerprint(nodeURL.Url)])
}

// A relay in flight when cleanupStaleTrackers removes its url's tracker completes after the url's
// series were deleted. Its count must not bring the url's relay counter back for the life of the
// pod: the harvest that would undo a late block write is dropped by the generation gate, so the
// count has to undo itself.
func TestDirectRelay_LateCountForARemovedURLIsUndone(t *testing.T) {
	const provider = "lava@mag4204LateCount"
	const removed = "http://removed.mag4204-late-count:8545"
	kept := newHeadUpstream(t, func() int64 { return 1000 }, nil)
	rpcss, mm := trackProviderURLs(t, provider, kept)
	mm.RegisterEndpoint("ETH1", "jsonrpc", removed, provider) // a url of the provider with no tracker (left)

	rpcss.countURLRelayServiced(provider, removed) // the late count
	rpcss.countURLRelayServiced(provider, kept.srv.URL)

	// Only this run's urls: the registry is process-wide, so a second run in one process sees the
	// first run's kept url too.
	series := gatherSeries(t, "rpc_endpoint_url_relays_serviced_total", provider)
	require.Equal(t, float64(1), series[metrics.URLFingerprint(kept.srv.URL)])
	require.NotContains(t, series, metrics.URLFingerprint(removed))
}

// The tests above wire the poll hook and register the urls themselves, so they pass with either
// production call gone. This boots the endpoint the way the router does — CreateSmartRouterEndpoint,
// which registers each node url (registerEndpointMetrics) and starts ServeRPCRequests, which hands
// the monitor its OnPollBlock — and moves each upstream's head after the boot. No relay is sent
// after the boot, so only the trackers' polls can carry the new head onto a url's series (the boot's
// own relays can write the old one through the harvest). Without either call no url gets the new
// head, and the stuck alerts fall back to the provider series this ticket exists to get past.
func TestCreateSmartRouterEndpoint_PollsWriteEachURLsSeries(t *testing.T) {
	const bootHead, movedHead = 20_000_000, 20_000_007
	var head atomic.Int64
	head.Store(bootHead)
	upstream := func() *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			hexHead := fmt.Sprintf("0x%x", head.Load())
			var result any = "0x0"
			switch req.Method {
			case "eth_blockNumber":
				result = hexHead
			case "eth_getBlockByNumber":
				result = map[string]any{"number": hexHead, "hash": "0x" + strings.Repeat("ab", 32)}
			case "eth_getCode":
				result = "0x"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
		}))
		t.Cleanup(srv.Close)
		return srv
	}
	// ETH1 polls every 6s by default; 8 polls per block time (1.5s) keeps the wait short.
	prevDivisor := viper.Get(endpointstate.PollDivisorFlagName)
	viper.Set(endpointstate.PollDivisorFlagName, endpointstate.MaxPollDivisor)
	t.Cleanup(func() { viper.Set(endpointstate.PollDivisorFlagName, prevDivisor) })
	up1, up2 := upstream(), upstream()
	_, err := bootFleetTestEndpoint(t,
		fleetTestPrimary("sim-1", "group-a", up1.URL),
		fleetTestPrimary("sim-2", "group-b", up2.URL))
	require.NoError(t, err)
	head.Store(movedHead)

	for provider, url := range map[string]string{"sim-1": up1.URL, "sim-2": up2.URL} {
		label := metrics.URLFingerprint(url)
		require.Eventually(t, func() bool { return urlBlocks(t, provider)[label] == movedHead }, 45*time.Second, 50*time.Millisecond,
			"%s's url gets the head its tracker polled after the boot", provider)
		require.Greater(t, answeredUntil(t, provider)[label], float64(time.Now().Unix()), "%s's url counts as answering", provider)
	}
}
