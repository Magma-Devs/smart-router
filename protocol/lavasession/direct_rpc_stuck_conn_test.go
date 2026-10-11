package lavasession

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// stuckConnUpstream stands in for a vendor load balancer that has pinned the
// router's first connection to a dead backend (MAG-4211): every request on that
// connection is answered 503, quickly and forever, while any newly dialed
// connection reaches a healthy backend and gets a 200.
type stuckConnUpstream struct {
	mu         sync.Mutex
	stuckAddr  string // remote address of the first connection seen; "" until then
	protoMajor int
}

func (u *stuckConnUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	if u.stuckAddr == "" {
		u.stuckAddr = r.RemoteAddr
	}
	stuck := r.RemoteAddr == u.stuckAddr
	u.protoMajor = r.ProtoMajor
	u.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if stuck {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("Node temporarily unavailable"))
		return
	}
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
}

// newStuckConnServer starts the upstream over TLS, with HTTP/2 when http2 is set,
// and returns a connection to it whose client trusts the test certificate. The
// client gets its own transport, configured as the shared one is, so the test
// neither depends on nor leaves connections in the process-wide pool.
func newStuckConnServer(t *testing.T, http2 bool) (*stuckConnUpstream, *HTTPDirectRPCConnection) {
	t.Helper()
	upstream := &stuckConnUpstream{}
	srv := httptest.NewUnstartedServer(upstream)
	srv.EnableHTTP2 = http2
	srv.StartTLS()
	t.Cleanup(srv.Close)

	srvTransport, ok := srv.Client().Transport.(*http.Transport)
	require.True(t, ok)
	transport := common.OptimizedHttpTransport()
	transport.TLSClientConfig.RootCAs = srvTransport.TLSClientConfig.RootCAs
	t.Cleanup(transport.CloseIdleConnections)

	conn := newHTTPConnection(t, common.NodeUrl{Url: srv.URL})
	conn.client = &http.Client{Transport: transport}
	return upstream, conn
}

// A connection that only ever answers 5xx is retired, so the requests after it
// reach a healthy backend on a fresh connection instead of sticking to the dead
// one until the process restarts (MAG-4211). Covered for both request paths
// (JSON-RPC SendRequest and REST DoHTTPRequest) and both protocols: HTTP/2 is
// the incident's case (one multiplexed connection per host), HTTP/1.1 keeps a
// single idle keep-alive connection under sequential traffic.
func TestHTTPDirectRPCConnection_RetiresConnectionStuckOn5xx(t *testing.T) {
	send := map[string]func(*HTTPDirectRPCConnection) int{
		"SendRequest": func(h *HTTPDirectRPCConnection) int {
			resp, err := h.SendRequest(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), nil)
			require.NotNil(t, resp, "a status reply must come back with its response, err=%v", err)
			return resp.StatusCode
		},
		"DoHTTPRequest": func(h *HTTPDirectRPCConnection) int {
			resp, err := h.DoHTTPRequest(context.Background(), HTTPRequestParams{Method: http.MethodGet, URL: h.nodeUrl.Url + "/status"})
			require.NoError(t, err)
			return resp.StatusCode
		},
	}
	for _, proto := range []struct {
		name  string
		http2 bool
		major int
	}{{"HTTP/2", true, 2}, {"HTTP/1.1", false, 1}} {
		for path, do := range send {
			t.Run(proto.name+"/"+path, func(t *testing.T) {
				upstream, conn := newStuckConnServer(t, proto.http2)

				// The streak that trips the retirement, then the request that
				// carries it out on the stuck connection; the one after must dial.
				budget := int(consecutive5xxBeforeConnRetire) + 1
				statuses := make([]int, 0, budget+3)
				for i := 0; i < budget; i++ {
					statuses = append(statuses, do(conn))
				}
				for i := 0; i < 3; i++ {
					statuses = append(statuses, do(conn))
				}

				upstream.mu.Lock()
				require.Equal(t, proto.major, upstream.protoMajor, "the test must exercise the protocol it names")
				upstream.mu.Unlock()
				for i, s := range statuses[:budget] {
					require.Equal(t, http.StatusServiceUnavailable, s, "request %d should still be on the stuck connection", i)
				}
				for i, s := range statuses[budget:] {
					require.Equal(t, http.StatusOK, s,
						"request %d after the retirement must reach a fresh connection; statuses=%v", budget+i, statuses)
				}
			})
		}
	}
}

// A 5xx that is not part of a streak leaves the connection alone: a vendor that
// fails one request in a while keeps its warm connection, and the count starts
// over after any non-5xx reply.
func TestHTTPDirectRPCConnection_Interleaved5xxKeepsConnection(t *testing.T) {
	h := &HTTPDirectRPCConnection{}
	for i := 0; i < 10; i++ {
		for j := uint32(1); j < consecutive5xxBeforeConnRetire; j++ {
			h.observeStatus(http.StatusBadGateway)
		}
		h.observeStatus(http.StatusOK)
	}
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)
	h.markRetireIfDue(req)
	require.False(t, req.Close, "no streak reached the threshold, so no connection should be retired")

	for j := uint32(0); j < consecutive5xxBeforeConnRetire; j++ {
		h.observeStatus(http.StatusServiceUnavailable)
	}
	h.markRetireIfDue(req)
	require.True(t, req.Close, "a full streak retires the connection the next request rides")

	again, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)
	h.markRetireIfDue(again)
	require.False(t, again.Close, "one streak retires one connection, not every request after it")
}
