package rpcsmartrouter

// HTTP-contract tests for POST /debug/poll-now (MAG-2649). The poll mechanics themselves are
// proven at the layers that own them (chaintracker.TestChainTracker_PollNow_* and
// endpointstate.TestEndpointMonitor_PollNow_*); here we pin what the test harness actually codes
// against: the request/response shape, the status codes, and — end to end over a real HTTP
// upstream — that a block appearing after the last poll is reflected in the response body.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

func postDebugJSON(mux http.Handler, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func TestDebugPollNow_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := getDebugRouter(mux, "/debug/poll-now") // GET on a POST-only endpoint
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// TestDebugPollNow_RejectsUnusableRequests: the two ways a caller can ask for nothing in
// particular are refused up front, before any tracker is touched.
func TestDebugPollNow_RejectsUnusableRequests(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	require.Equal(t, http.StatusBadRequest, postDebugJSON(mux, "/debug/poll-now", `{not json`).Code)
	require.Equal(t, http.StatusBadRequest, postDebugJSON(mux, "/debug/poll-now", `{"chain_id":"ETH1"}`).Code,
		"network_address is required — poll-now targets one endpoint, never 'whatever matches'")
}

// TestDebugPollNow_UnknownEndpointIs404: an address with no ChainTracker (including every address
// on a fixture with no router wired) is a 404, not a 200 with an empty row that a harness could
// mistake for a completed poll.
func TestDebugPollNow_UnknownEndpointIs404(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: nil})

	rr := postDebugJSON(mux, "/debug/poll-now", `{"network_address":"http://not-registered:8545"}`)
	require.Equal(t, http.StatusNotFound, rr.Code)
}

// TestDebugPollNow_TrackerNotPollingIs504 covers the one status code with real logic behind it: an
// endpoint whose tracker is registered but cannot poll (its upstream is dead, so init never
// completes and no poll goroutine exists). The handler must say so in the status line AND in the
// row — a 200 here would let a harness read an untouched record as a fresh poll result.
func TestDebugPollNow_TrackerNotPollingIs504(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx := t.Context()

	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      newRealChainParserForHarvest(t, "ETH1"),
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond, // fail init fast instead of spacing retries out
		BlocksToSave:     1,
	})
	t.Cleanup(monitor.Stop)

	// A closed port: registration is synchronous (so the endpoint IS resolvable), but every fetch
	// fails, so the tracker never leaves startTrackerWithRetry and no poll goroutine exists.
	const deadURL = "http://127.0.0.1:0"
	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: deadURL}, 5, "jsonrpc")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })
	_, err = monitor.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: deadURL, Enabled: true}, directConn)
	require.NoError(t, err)

	var offsetNano atomic.Int64
	router := &RPCSmartRouter{
		rpcServers: map[string]*RPCSmartRouterServer{
			"ETH1-jsonrpc": {
				endpointChainTrackerManager: monitor,
				listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
			},
		},
	}
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})

	rr := postDebugJSON(mux, "/debug/poll-now", fmt.Sprintf(`{"network_address":%q}`, deadURL))
	require.Equal(t, http.StatusGatewayTimeout, rr.Code, "body=%q", rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"), "the body is still JSON on the failure path")

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.Equal(t, false, rows[0]["Polled"])
	require.NotEmpty(t, rows[0]["TriggerError"], "the row explains why no poll ran")
	require.Equal(t, "", rows[0]["PollError"], "no poll ran, so there is no poll error to report")
}

// newPollNowUpstream serves the two ETH poll requests from a live block counter, so a test can move
// the chain forward between polls. The block hash is derived from the REQUESTED block so re-reading
// an older block returns the hash it already had (a head-derived hash would look like a fork).
func newPollNowUpstream(t *testing.T, block *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if body.Method == "eth_getBlockByNumber" {
			var blockHex string
			if len(body.Params) > 0 {
				_ = json.Unmarshal(body.Params[0], &blockHex)
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"hash":"0x%064s"}}`, strings.TrimPrefix(blockHex, "0x"))
			return
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, block.Load())
	}))
	t.Cleanup(server.Close)
	return server
}

// TestDebugPollNow_PollsEndpointAndReturnsFreshRecord is the end-to-end contract check: with the
// tracker's next scheduled poll 30 s away, a block that appears now shows up in the poll-now
// response — proving the endpoint delivers what the ticket asks for (set up state → trigger → read,
// no waiting) all the way through a real HTTP upstream and the real poll cycle.
func TestDebugPollNow_PollsEndpointAndReturnsFreshRecord(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx := t.Context()

	var block atomic.Int64
	block.Store(5000)
	upstream := newPollNowUpstream(t, &block)

	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      newRealChainParserForHarvest(t, "ETH1"),
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: time.Minute, // → a 30 s dedicated-poll cadence: nothing here comes from the timer
		BlocksToSave:     1,
	})
	t.Cleanup(monitor.Stop)

	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "jsonrpc")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })

	_, err = monitor.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: upstream.URL, Enabled: true}, directConn)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		state, _, exists := monitor.GetTrackerState(upstream.URL)
		return exists && state == endpointstate.EndpointChainTrackerPolling
	}, 10*time.Second, 20*time.Millisecond, "the tracker must be polling before poll-now is triggered")

	var offsetNano atomic.Int64
	router := &RPCSmartRouter{
		rpcServers: map[string]*RPCSmartRouterServer{
			"ETH1-jsonrpc": {
				endpointChainTrackerManager: monitor,
				listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
			},
			"NIL-rest": {}, // no monitor, no listen endpoint: skipped, never a panic
		},
	}
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})

	block.Store(5123) // a new head, which nothing has polled yet

	rr := postDebugJSON(mux, "/debug/poll-now", fmt.Sprintf(`{"network_address":%q}`, upstream.URL))
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, "ETH1", row["ChainID"])
	require.Equal(t, "jsonrpc", row["ApiInterface"])
	require.Equal(t, upstream.URL, row["NetworkAddress"])
	require.Equal(t, true, row["Polled"])
	require.Equal(t, "", row["PollError"])
	require.Equal(t, "", row["TriggerError"])
	require.Equal(t, float64(5123), row["LatestBlock"], "the response carries the block the forced poll just observed")
	require.Equal(t, "poll", row["Source"])
	require.Equal(t, float64(0), row["ConsecutivePollFailures"])
	require.NotEmpty(t, row["LastSuccessfulPoll"])
	require.NotEmpty(t, row["ObservedAt"])

	// The optional filters narrow the match; a wrong chain matches nothing rather than polling the
	// endpoint anyway.
	rr = postDebugJSON(mux, "/debug/poll-now", fmt.Sprintf(`{"network_address":%q,"chain_id":"eth1","api_interface":"jsonrpc"}`, upstream.URL))
	require.Equal(t, http.StatusOK, rr.Code, "chain_id/api_interface match case-insensitively")
	rr = postDebugJSON(mux, "/debug/poll-now", fmt.Sprintf(`{"network_address":%q,"chain_id":"SOLANA"}`, upstream.URL))
	require.Equal(t, http.StatusNotFound, rr.Code, "a filter that matches no server polls nothing")
}
