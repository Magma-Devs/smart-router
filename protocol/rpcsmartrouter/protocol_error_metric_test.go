package rpcsmartrouter

import (
	"context"
	"io"
	"net/http"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MAG-3536: smartrouter_protocol_errors_total was registered but nothing wrote to it, so an
// outage in which every endpoint failed at the transport looked like any other failure. Drive the
// REST listener end to end (parser, session selection, dispatcher, per-attempt verdict) and read
// the counters back through the registry. The expected counts come from a second source, the
// upstream's own request tally, so the test cannot pass by a metric agreeing with itself.
//
// Every case asserts four series against that tally. Protocol errors, node errors and rate-limit
// hold-offs are disjoint: no attempt may land in two of them. Failed attempts is the superset
// control. It moves for every failure, so the gateway case can show the cut this ticket asks for: a
// 502 fails the attempt, but the upstream answered, so it is not a protocol error. The node series
// has its own positive control, the 501 case, because a zero read from a series the harness never
// writes would prove nothing.
func TestRESTListener_ProtocolErrorsTotal(t *testing.T) {
	rand.InitRandomSeed()
	const provider = "rest-upstream"
	providerLabels := map[string]string{"spec": "LAVA", "apiInterface": "rest", "provider_address": provider}
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)

	counters := []struct {
		name   string
		metric string
		labels map[string]string
	}{
		{"protocol errors", "smartrouter_protocol_errors_total", providerLabels},
		{"node errors", "smartrouter_node_errors_total", providerLabels},
		{"rate-limit hold-offs", "smartrouter_rate_limit_holdoffs_total", map[string]string{"provider": provider, "event": "recorded"}},
		{"failed attempts", "smartrouter_requests_failed_total", map[string]string{"spec": "LAVA", "apiInterface": "rest"}},
	}

	// dropAfterHeaders answers with headers promising a body, then closes the connection part-way
	// through it: the automation's drop_connection scenario, which the HTTP client can only surface
	// as a body read that fails.
	dropAfterHeaders := func(w http.ResponseWriter) {
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
	status := func(code int, body string) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = w.Write([]byte(body))
		}
	}

	for _, tc := range []struct {
		name    string
		respond func(http.ResponseWriter)
		// Per upstream attempt, in counters order: protocol errors, node errors, hold-offs, failed attempts.
		perAttempt [4]bool
		failed     bool // no attempt can be served, so the client must not see a 200
	}{
		{"a dropped connection is a protocol error", dropAfterHeaders, [4]bool{true, false, false, true}, true},
		{"a gateway 502 fails the attempt but is an answer, not a protocol error", status(http.StatusBadGateway, `{"message":"bad gateway"}`), [4]bool{false, false, false, true}, true},
		{"unimplemented is a node error, not a protocol error", status(http.StatusNotImplemented, `{"code":12,"message":"Not Implemented"}`), [4]bool{false, true, false, false}, false},
		{"a rate limit is held off, not a protocol error", status(http.StatusTooManyRequests, `{"message":"rate limited"}`), [4]bool{false, false, true, true}, true},
		{"client rejection is a reply, not a protocol error", status(http.StatusBadRequest, `{"code":3,"message":"invalid height"}`), [4]bool{false, false, false, false}, false},
		{"success is not a protocol error", status(http.StatusOK, `{"block":{}}`), [4]bool{false, false, false, false}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A 429 holds the endpoint off. A fresh registry per case keeps that hold-off out of
			// the cases after it and out of every other test in the package.
			withFreshRelayHoldoff(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			path := "/cosmos/base/tendermint/v1beta1/blocks/17"
			var requests atomic.Int32
			parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
				ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					tc.respond(w)
				}), nil, "../../", nil)
			require.NoError(t, err)
			defer closeServer()

			conn, err := lavasession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, spectypes.APIInterfaceRest)
			require.NoError(t, err)
			defer conn.Close()
			providerEndpoint := &lavasession.Endpoint{
				NetworkAddress: endpoint.NodeUrls[0].Url, Enabled: true,
				DirectConnections: []lavasession.DirectRPCConnection{conn},
			}
			cswp := lavasession.NewConsumerSessionWithProvider(provider, []*lavasession.Endpoint{providerEndpoint}, 100000, 1, 1)
			cswp.StaticProvider = true
			sessionManager, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
			rpcEndpoint.NetworkAddress = "127.0.0.1:0"
			require.NoError(t, sessionManager.UpdateAllProviders(1, map[uint64]*lavasession.ConsumerSessionsWithProvider{0: cswp}, nil))
			// The logs wrapper is what the server stamps protocol errors through, and what the relay
			// processor stamps node errors through; the endpoint metrics are what book failed attempts.
			// Both get the real manager so every increment lands on the registry the assertions read.
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

			// The default registry accumulates across tests and -count reruns: assert deltas.
			before := make([]float64, len(counters))
			for i, c := range counters {
				before[i] = gatherCounterSum(t, c.metric, c.labels)
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.GetListeningAddress()+path, nil)
			require.NoError(t, err)
			response, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, response.Body)
			require.NoError(t, response.Body.Close())
			require.Positive(t, requests.Load(), "the request must actually reach the upstream")
			if tc.failed {
				require.NotEqual(t, http.StatusOK, response.StatusCode, "a failure on every attempt cannot be served")
			}

			// Each counter should have moved by the upstream's attempt count if this case books into
			// it, and not at all otherwise.
			want := func(i int) float64 {
				if tc.perAttempt[i] {
					return float64(requests.Load())
				}
				return 0
			}
			// The protocol error and the hold-off are stamped before the attempt reports its result,
			// but the relay processor writes node errors from a goroutine, so wait for them to land.
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				for i, c := range counters {
					assert.Equal(collect, want(i), gatherCounterSum(t, c.metric, c.labels)-before[i],
						"%s delta (upstream saw %d attempts)", c.name, requests.Load())
				}
			}, time.Second, 5*time.Millisecond)
			// Hold the values for a moment so an overshoot from a late attempt is caught too.
			time.Sleep(50 * time.Millisecond)
			for i, c := range counters {
				require.Equal(t, want(i), gatherCounterSum(t, c.metric, c.labels)-before[i],
					"%s must not keep climbing after the reply", c.name)
			}
		})
	}
}
