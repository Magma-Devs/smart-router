package rpcsmartrouter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatherCounterSum sums one counter family over every series whose labels include match, read
// from the default registry the way a scrape would.
func gatherCounterSum(t *testing.T, name string, match map[string]string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			for k, v := range match {
				if labels[k] != v {
					continue series
				}
			}
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

// MAG-3536: smartrouter_backup_tier_served_total counts a request once, when it completes, if the
// bytes the caller got came from a backup. Drive the REST listener end to end with one primary and
// one backup, and check the counter against each upstream's own hit count. Two cases are the ones
// earlier designs got wrong. The hung primary: the hedge that reaches the backup is sent while the
// primary's attempt is still in flight, exactly like a hedge off a primary that is merely slow. The
// stateful write with no primary available: it reaches the backup only by falling over, because
// --stateful-to-backup is off by default.
func TestRESTListener_BackupTierServed(t *testing.T) {
	rand.InitRandomSeed()
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	servedLabels := map[string]string{"spec": "LAVA", "apiInterface": "rest"}
	const okBody = `{"block":{}}`

	answer := func(code int, body string) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = w.Write([]byte(body))
		}
	}
	dropAfterHeaders := func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, buf, err := hijacker.Hijack()
		if err != nil {
			return
		}
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n{\"block\":")
		_ = buf.Flush()
		_ = conn.Close()
	}
	// hang holds the attempt open until the router gives up on it, which it does once the hedge to
	// the backup has answered. The timer only bounds a leaked handler if the test fails first.
	hang := func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}

	blocksGet := func(ctx context.Context, addr string) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/cosmos/base/tendermint/v1beta1/blocks/17", nil)
	}
	// A transaction broadcast is a stateful relay in the LAVA spec.
	txPost := func(ctx context.Context, addr string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/cosmos/tx/v1beta1/txs",
			strings.NewReader(`{"tx_bytes":"CgQKAggB","mode":"BROADCAST_MODE_SYNC"}`))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}
		return req, err
	}

	for _, tc := range []struct {
		name       string
		primary    func(http.ResponseWriter, *http.Request)
		noPrimary  bool // configure the backup alone, as when every primary has been dropped
		request    func(context.Context, string) (*http.Request, error)
		wantServed float64
	}{
		{"a healthy primary serves, so no backup served", answer(http.StatusOK, okBody), false, blocksGet, 0},
		{"the primary fails with a 502 and the backup serves", answer(http.StatusBadGateway, `{"message":"bad gateway"}`), false, blocksGet, 1},
		{"the primary drops the connection and the backup serves", dropAfterHeaders, false, blocksGet, 1},
		{"the primary hangs and a hedge to the backup serves", hang, false, blocksGet, 1},
		{"a stateful write with no primary available is served by the backup", nil, true, txPost, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var primaryHits, backupHits atomic.Int32
			parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
				ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					primaryHits.Add(1)
					tc.primary(w, r)
				}), nil, "../../", nil)
			require.NoError(t, err)
			defer closeServer()
			backupServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				backupHits.Add(1)
				answer(http.StatusOK, okBody)(w, r)
			}))
			defer backupServer.Close()

			newProvider := func(name string, nodeURL common.NodeUrl) *lavasession.ConsumerSessionsWithProvider {
				conn, err := lavasession.NewDirectRPCConnection(ctx, nodeURL, 5, spectypes.APIInterfaceRest)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				ep := &lavasession.Endpoint{NetworkAddress: nodeURL.Url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn}}
				cswp := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{ep}, 100000, 1, 1)
				cswp.StaticProvider = true
				return cswp
			}
			backupURL := endpoint.NodeUrls[0]
			backupURL.Url = backupServer.URL
			sessionManager, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
			rpcEndpoint.NetworkAddress = "127.0.0.1:0"
			primaries := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: newProvider("primary", endpoint.NodeUrls[0])}
			if tc.noPrimary {
				primaries = nil
			}
			require.NoError(t, sessionManager.UpdateAllProviders(1, primaries,
				map[uint64]*lavasession.ConsumerSessionsWithProvider{0: newProvider("backup", backupURL)},
			))
			logs, err := metrics.NewRPCConsumerLogs(mm, nil, nil)
			require.NoError(t, err)
			server := &RPCSmartRouterServer{
				chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
				rpcSmartRouterLogs: logs, smartRouterEndpointMetrics: mm,
				relayRetriesManager: lavaprotocol.NewRelayRetriesManager(),
				consistencyConfig:   relaycore.DefaultConsistencyValidationConfig(),
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

			before := gatherCounterSum(t, "smartrouter_backup_tier_served_total", servedLabels)
			req, err := tc.request(ctx, listener.GetListeningAddress())
			require.NoError(t, err)
			response, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, response.Body)
			require.NoError(t, response.Body.Close())

			require.Equal(t, http.StatusOK, response.StatusCode, "one of the two tiers serves every case")
			if tc.noPrimary {
				require.Zero(t, primaryHits.Load(), "no primary is configured")
			} else {
				require.Positive(t, primaryHits.Load(), "the primary is always tried")
			}
			if tc.wantServed > 0 {
				require.Positive(t, backupHits.Load(), "the backup answered this request")
			} else {
				require.Zero(t, backupHits.Load(), "a healthy primary leaves the backup untouched")
			}
			// Counted on the request goroutine before the reply is written, so it is settled by now.
			require.Equal(t, tc.wantServed, gatherCounterSum(t, "smartrouter_backup_tier_served_total", servedLabels)-before)
		})
	}
}
