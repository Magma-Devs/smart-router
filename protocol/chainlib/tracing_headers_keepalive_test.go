package chainlib

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// contextKeepingRelay is a RelaySender that keeps the context of every relay it is handed, the
// way the router keeps a request's context past its handler: on the relay to the provider, on
// log lines written after the reply, and on any asynchronous work started for the request.
type contextKeepingRelay struct {
	mu   sync.Mutex
	kept []context.Context
}

func (r *contextKeepingRelay) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kept = append(r.kept, ctx)
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}, StatusCode: http.StatusOK}, nil
}

func (r *contextKeepingRelay) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	return nil, errors.New("not used")
}

func (r *contextKeepingRelay) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (r *contextKeepingRelay) CancelSubscriptionContext(subscriptionKey string) {}

// tracingOf reads, now, the ids on the context the i-th relay was handed.
func (r *contextKeepingRelay) tracingOf(t *testing.T, i int) tracingIds {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Greater(t, len(r.kept), i, "relay %d never reached the relay sender", i)
	return tracingIdsFromContext(r.kept[i])
}

// TestListeners_TracingIdsOutliveTheRequestsConnection sends two requests over one keep-alive
// connection to each HTTP entry point that reads the tracing headers. fiber hands the listener
// header values that alias fasthttp's per-connection buffers, and the second request's ids have
// the same lengths as the first's, so fasthttp writes them over the first request's values in
// place. The ids on the first relay's context are read only after the second request has
// finished, and must still read as the first caller sent them (MAG-3798).
func TestListeners_TracingIdsOutliveTheRequestsConnection(t *testing.T) {
	first := tracingIds{requestId: "first-id", taskId: "first-tk", txId: "first-tx"}
	second := tracingIds{requestId: "other-id", taskId: "other-tk", txId: "other-tx"}
	for _, ep := range httpTracingEntryPoints() {
		t.Run(ep.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			relay := &contextKeepingRelay{}
			addr := ep.serve(t, ctx, relay)

			client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
			defer client.CloseIdleConnections()
			var reused []bool
			trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = append(reused, info.Reused) }}
			for _, ids := range []tracingIds{first, second} {
				resp, err := client.Do(ep.newRequest(t, httptrace.WithClientTrace(ctx, trace), addr, ids))
				require.NoError(t, err)
				_, _ = io.Copy(io.Discard, resp.Body)
				require.NoError(t, resp.Body.Close())
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}
			require.Equal(t, []bool{false, true}, reused, "both requests on one connection, or the second cannot reach the first one's buffer")

			// Read only now, after the second request has come and gone through the same buffers.
			require.Equal(t, first, relay.tracingOf(t, 0), "the ids on the first relay's context changed under it")
			require.Equal(t, second, relay.tracingOf(t, 1))
		})
	}
}
