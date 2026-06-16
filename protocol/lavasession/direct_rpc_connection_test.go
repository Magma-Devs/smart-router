package lavasession

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectProtocol(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected DirectRPCProtocol
		wantErr  bool
	}{
		{
			name:     "HTTP protocol",
			url:      "http://localhost:8545",
			expected: DirectRPCProtocolHTTP,
			wantErr:  false,
		},
		{
			name:     "HTTPS protocol",
			url:      "https://mainnet.infura.io",
			expected: DirectRPCProtocolHTTPS,
			wantErr:  false,
		},
		{
			name:     "WebSocket protocol",
			url:      "ws://localhost:8546",
			expected: DirectRPCProtocolWS,
			wantErr:  false,
		},
		{
			name:     "WebSocket Secure protocol",
			url:      "wss://eth-mainnet.g.alchemy.com/v2/KEY",
			expected: DirectRPCProtocolWSS,
			wantErr:  false,
		},
		{
			name:     "gRPC protocol",
			url:      "grpc://localhost:9090",
			expected: DirectRPCProtocolGRPC,
			wantErr:  false,
		},
		{
			name:     "gRPCs protocol",
			url:      "grpcs://localhost:9090",
			expected: DirectRPCProtocolGRPC,
			wantErr:  false,
		},
		{
			name:     "No scheme defaults to HTTPS",
			url:      "mainnet.infura.io",
			expected: DirectRPCProtocolHTTPS,
			wantErr:  false,
		},
		{
			name:     "Unsupported protocol",
			url:      "ftp://example.com",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Invalid URL",
			url:      "://invalid",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocol, err := DetectProtocol(tt.url, "")

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, DirectRPCProtocol(""), protocol)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, protocol)
			}
		})
	}
}

func TestHTTPConnectionCreation(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "http://localhost:8545"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.Equal(t, DirectRPCProtocolHTTP, conn.GetProtocol())
	assert.True(t, conn.IsHealthy())
	assert.Equal(t, "http://localhost:8545", conn.GetURL())

	err = conn.Close()
	assert.NoError(t, err)
}

func TestHTTPSConnectionCreation(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "https://eth-mainnet.g.alchemy.com/v2/test"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.Equal(t, DirectRPCProtocolHTTPS, conn.GetProtocol())
	assert.True(t, conn.IsHealthy())
	assert.Equal(t, "https://eth-mainnet.g.alchemy.com/v2/test", conn.GetURL())

	err = conn.Close()
	assert.NoError(t, err)
}

func TestWebSocketConnectionCreation(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "wss://eth-mainnet.g.alchemy.com/v2/test"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.Equal(t, DirectRPCProtocolWSS, conn.GetProtocol())
	assert.True(t, conn.IsHealthy())
	assert.Equal(t, "wss://eth-mainnet.g.alchemy.com/v2/test", conn.GetURL())

	err = conn.Close()
	assert.NoError(t, err)
}

func TestGRPCConnectionCreation(t *testing.T) {
	ctx := context.Background()
	// Use grpcs:// (secure) to avoid the allow-insecure requirement
	nodeUrl := common.NodeUrl{Url: "grpcs://localhost:9090"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.Equal(t, DirectRPCProtocolGRPC, conn.GetProtocol())
	assert.True(t, conn.IsHealthy())
	assert.Equal(t, "grpcs://localhost:9090", conn.GetURL())

	err = conn.Close()
	assert.NoError(t, err)
}

func TestGRPCConnectionCreationInsecure(t *testing.T) {
	ctx := context.Background()
	// grpc:// (insecure) requires AllowInsecure: true
	nodeUrl := common.NodeUrl{
		Url: "grpc://localhost:9090",
		GrpcConfig: common.GrpcConfig{
			AllowInsecure: true,
		},
	}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.Equal(t, DirectRPCProtocolGRPC, conn.GetProtocol())
	assert.True(t, conn.IsHealthy())

	err = conn.Close()
	assert.NoError(t, err)
}

func TestConnectionCreationWithInvalidURL(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "://invalid"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "failed to detect protocol")
}

func TestConnectionCreationWithUnsupportedProtocol(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "ftp://example.com"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "failed to detect protocol")
}

func TestHTTPConnectionInterface(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "https://test.example.com"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	// Verify it implements DirectRPCConnection interface
	var _ DirectRPCConnection = conn

	// Test interface methods
	assert.Equal(t, DirectRPCProtocolHTTPS, conn.GetProtocol())
	assert.True(t, conn.IsHealthy())
	assert.Equal(t, "https://test.example.com", conn.GetURL())
	assert.NoError(t, conn.Close())
}

// newTestWSJSONRPCServer starts a minimal JSON-RPC-over-WebSocket server that
// answers eth_blockNumber with a fixed block and echoes the request id. It
// returns the ws:// URL and a cleanup func.
func newTestWSJSONRPCServer(t *testing.T, result string) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if err := json.Unmarshal(msg, &req); err != nil {
				return
			}
			resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, string(req.ID), result)
			if err := c.WriteMessage(websocket.TextMessage, []byte(resp)); err != nil {
				return
			}
		}
	}))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") // http://host -> ws://host
	return wsURL, srv.Close
}

// TestWebSocketSendRequest_RoundTrip verifies request/response works over a real
// WebSocket connection: SendRequest ships the JSON-RPC frame, the id-correlated
// reply comes back, and the cached client is reused on the second call.
func TestWebSocketSendRequest_RoundTrip(t *testing.T) {
	wsURL, cleanup := newTestWSJSONRPCServer(t, `"0x10"`)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: wsURL}, 5, "")
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, DirectRPCProtocolWS, conn.GetProtocol())

	resp, err := conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Data), `"result":"0x10"`)
	assert.Contains(t, string(resp.Data), `"id":1`, "caller's id must be restored on the reply")

	// Second call reuses the cached client (no re-dial) and carries its own id.
	resp2, err := conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}`), nil)
	require.NoError(t, err)
	assert.Contains(t, string(resp2.Data), `"result":"0x10"`)
	assert.Contains(t, string(resp2.Data), `"id":2`)
}

// TestWebSocketSendRequest_ConcurrentSameID fires many concurrent requests that
// all reuse caller id "1". Because rpcclient multiplexes them on one socket and
// routes replies by id, the connection must issue unique wire ids internally —
// otherwise replies misroute. The server echoes each request's first param back
// as the result, so each goroutine can verify it got its own response.
func TestWebSocketSendRequest_ConcurrentSameID(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		var writeMu sync.Mutex // gorilla/websocket forbids concurrent writers
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID     json.RawMessage `json:"id"`
				Params []string        `json:"params"`
			}
			if err := json.Unmarshal(msg, &req); err != nil {
				return
			}
			go func(id json.RawMessage, params []string) {
				time.Sleep(20 * time.Millisecond) // keep many requests in-flight at once
				val := ""
				if len(params) > 0 {
					val = params[0]
				}
				resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%q}`, string(id), val)
				writeMu.Lock()
				_ = c.WriteMessage(websocket.TextMessage, []byte(resp))
				writeMu.Unlock()
			}(req.ID, req.Params)
		}
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: wsURL}, 5, "")
	require.NoError(t, err)
	defer conn.Close()

	const n = 25
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			want := fmt.Sprintf("v%d", i)
			// Every request deliberately reuses caller id "1".
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"echo","params":[%q]}`, want)
			resp, err := conn.SendRequest(ctx, []byte(body), nil)
			if err != nil {
				errs[i] = err
				return
			}
			got[i] = string(resp.Data)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoErrorf(t, errs[i], "request %d failed", i)
		assert.Containsf(t, got[i], fmt.Sprintf(`"result":"v%d"`, i),
			"request %d got a misrouted response: %s", i, got[i])
		assert.Containsf(t, got[i], `"id":1`, "request %d should carry caller id 1", i)
	}
}

// TestWebSocketSendRequest_AfterClose verifies Close() is terminal: a closed
// connection must not silently re-dial on a subsequent SendRequest.
func TestWebSocketSendRequest_AfterClose(t *testing.T) {
	wsURL, cleanup := newTestWSJSONRPCServer(t, `"0x10"`)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: wsURL}, 5, "")
	require.NoError(t, err)

	_, err = conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), nil)
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	_, err = conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}`), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// TestWebSocketSendRequest_DialFailure verifies that an unreachable WebSocket
// endpoint surfaces a dial error (which the chain-tracker retries) rather than
// the old "not implemented" stub.
func TestWebSocketSendRequest_DialFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: "ws://127.0.0.1:1"}, 5, "")
	require.NoError(t, err)

	_, err = conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to dial WebSocket")
}

func TestGRPCConnectionURLValidation(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		allowInsecure bool
		wantErr       bool
	}{
		{
			name:          "Valid grpcs URL",
			url:           "grpcs://cosmos-grpc.polkachu.com:14990",
			allowInsecure: false,
			wantErr:       false,
		},
		{
			name:          "gRPC URL with path",
			url:           "grpcs://example.com:443/some/path",
			allowInsecure: false,
			wantErr:       false,
		},
		{
			name:          "Insecure grpc with allow-insecure",
			url:           "grpc://localhost:9090",
			allowInsecure: true,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			nodeUrl := common.NodeUrl{
				Url: tt.url,
				GrpcConfig: common.GrpcConfig{
					AllowInsecure: tt.allowInsecure,
				},
			}

			conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, conn)

				grpcConn, ok := conn.(*GRPCDirectRPCConnection)
				require.True(t, ok, "expected GRPCDirectRPCConnection type")
				assert.Equal(t, DirectRPCProtocolGRPC, grpcConn.GetProtocol())
				assert.True(t, grpcConn.IsHealthy())

				err = conn.Close()
				assert.NoError(t, err)
			}
		})
	}
}

func TestGRPCConnectionSendRequestRequiresMethodHeader(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{
		Url: "grpcs://localhost:9090",
	}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	// SendRequest without x-grpc-method header should fail
	_, err = conn.SendRequest(ctx, []byte("{}"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), GRPCMethodHeader)
}

func TestGRPCStatusError(t *testing.T) {
	err := &GRPCStatusError{
		Code:    14,
		Message: "unavailable",
	}

	assert.Equal(t, "gRPC error 14: unavailable", err.Error())
}

func TestGRPCConnectionWithGrpcConfig(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{
		Url: "grpcs://cosmos-grpc.publicnode.com:443",
		GrpcConfig: common.GrpcConfig{
			DescriptorSource:  common.GrpcDescriptorSourceReflection,
			ReflectionTimeout: 2 * time.Second,
		},
	}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.Equal(t, DirectRPCProtocolGRPC, conn.GetProtocol())

	err = conn.Close()
	assert.NoError(t, err)
}

// TestHTTPDirectRPCConnection_IsHealthy_StartsTrue documents the
// initialize-as-healthy behavior. A fresh connection is optimistically healthy
// until proven otherwise — the first failed SendRequest flips it, after which
// IsHealthy reflects real transport outcomes.
func TestHTTPDirectRPCConnection_IsHealthy_StartsTrue(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "http://127.0.0.1:1"} // port 1 is not accepting
	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	require.True(t, conn.IsHealthy(),
		"a brand-new HTTP connection must start healthy (optimistic init); the first probe or request flips it")
}

// TestHTTPDirectRPCConnection_IsHealthy_FlipsOnDialFailure is the core fix: a
// transport-level failure (connection refused, DNS miss, TLS handshake failure,
// timeout) must drop IsHealthy to false so the comprehensive probe path in
// checkAndUnblockHealthyReBlockedProviders won't optimistically unblock a
// backup whose upstream is actually unreachable.
func TestHTTPDirectRPCConnection_IsHealthy_FlipsOnDialFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Port 1 never accepts — the Do call will fail with ECONNREFUSED (or time out
	// on platforms that don't fast-fail); both are transport errors.
	nodeUrl := common.NodeUrl{Url: "http://127.0.0.1:1"}
	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	httpConn, ok := conn.(*HTTPDirectRPCConnection)
	require.True(t, ok)

	_, sendErr := httpConn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","method":"probe","id":1}`), nil)
	require.Error(t, sendErr, "SendRequest must surface the transport failure")
	require.False(t, httpConn.IsHealthy(),
		"a transport-layer failure must flip IsHealthy to false so the comprehensive probe skips this backup")
}

// TestHTTPDirectRPCConnection_IsHealthy_Stays4xxHealthy ensures the fix doesn't
// overreach: a 4xx/5xx HTTP response is an *application* error (rate limit,
// forbidden, server-side bug) — transport is still fine. A connection must not
// be marked unhealthy just because the upstream RPC server returned a non-2xx.
// Without this nuance, dashboards would flap any time an endpoint hit a rate
// limit and the probe would wrongly refuse to unblock otherwise-healthy providers.
func TestHTTPDirectRPCConnection_IsHealthy_Stays4xxHealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests) // 429 — application error, transport is fine
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	nodeUrl := common.NodeUrl{Url: server.URL}
	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	httpConn, ok := conn.(*HTTPDirectRPCConnection)
	require.True(t, ok)

	// 429 returns an HTTPStatusError but the transport succeeded.
	_, sendErr := httpConn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0"}`), nil)
	require.Error(t, sendErr, "4xx/5xx still returns an HTTPStatusError to the caller")
	require.True(t, httpConn.IsHealthy(),
		"a 4xx/5xx response is an application error; transport reached the upstream and health must remain true")
}

// TestDirectRPCConnection_MarkHealthy_FlipsHTTPHealthyToTrue verifies the
// recovery-only escape: after a transient failure leaves healthy=false (a
// state the natural SendRequest-success path cannot reach because
// IsHealthy() gates the request itself), MarkHealthy must unstick it without
// requiring a successful exchange. Same idea for the gRPC variant — both
// have an internal atomic.Bool we expect to flip. WebSocket has no internal
// flag so MarkHealthy is a no-op there (covered in
// TestDirectRPCConnection_MarkHealthy_WebSocketIsNoOp).
func TestDirectRPCConnection_MarkHealthy_FlipsHTTPHealthyToTrue(t *testing.T) {
	httpConn := &HTTPDirectRPCConnection{}
	httpConn.healthy.Store(false)
	require.False(t, httpConn.IsHealthy(), "precondition: healthy starts false")

	httpConn.MarkHealthy()

	require.True(t, httpConn.IsHealthy(), "MarkHealthy must flip the atomic back to true")
}

func TestDirectRPCConnection_MarkHealthy_FlipsGRPCHealthyToTrue(t *testing.T) {
	grpcConn := &GRPCDirectRPCConnection{}
	grpcConn.healthy.Store(false)
	require.False(t, grpcConn.IsHealthy(), "precondition: healthy starts false")

	grpcConn.MarkHealthy()

	require.True(t, grpcConn.IsHealthy(), "MarkHealthy must flip the atomic back to true")
}

// TestDirectRPCConnection_MarkHealthy_WebSocketIsNoOp documents that
// WebSocketDirectRPCConnection has no internal healthy flag (IsHealthy is a
// constant true). MarkHealthy must still satisfy the interface without
// panicking; the call is intentionally a no-op.
func TestDirectRPCConnection_MarkHealthy_WebSocketIsNoOp(t *testing.T) {
	wsConn := &WebSocketDirectRPCConnection{}
	require.True(t, wsConn.IsHealthy(), "precondition: WebSocket IsHealthy is constant true")

	wsConn.MarkHealthy() // must not panic

	require.True(t, wsConn.IsHealthy())
}

// TestHTTPDirectRPCConnection_IsHealthy_RecoversAfterFailure verifies the full
// unhealthy → healthy transition: once a connection has observed a transport
// failure, a subsequent successful exchange must flip IsHealthy back to true.
// Without this, a single glitch would leave a backup permanently flagged as
// unhealthy even after upstream recovery.
func TestHTTPDirectRPCConnection_IsHealthy_RecoversAfterFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok","id":1}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	nodeUrl := common.NodeUrl{Url: server.URL}
	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	httpConn, ok := conn.(*HTTPDirectRPCConnection)
	require.True(t, ok)

	httpConn.healthy.Store(false) // simulate a prior failure

	_, sendErr := httpConn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0"}`), nil)
	require.NoError(t, sendErr)
	require.True(t, httpConn.IsHealthy(),
		"a successful transport exchange must restore IsHealthy=true after a prior failure")
}

// TestHTTPDirectRPCConnection_UsesSharedOptimizedTransport locks in the
// smart-router HTTP path using the shared optimized transport — NOT a fresh
// default http.Transport per connection. Regression here kills TLS session
// reuse and fragments the connection pool across every HTTPDirectRPCConnection.
func TestHTTPDirectRPCConnection_UsesSharedOptimizedTransport(t *testing.T) {
	ctx := context.Background()
	c1, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: "http://127.0.0.1:1"}, 5, "")
	require.NoError(t, err)
	c2, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: "https://127.0.0.1:1"}, 5, "")
	require.NoError(t, err)

	h1, ok := c1.(*HTTPDirectRPCConnection)
	require.True(t, ok, "c1 must be *HTTPDirectRPCConnection")
	h2, ok := c2.(*HTTPDirectRPCConnection)
	require.True(t, ok, "c2 must be *HTTPDirectRPCConnection")

	t1, ok := h1.client.Transport.(*http.Transport)
	require.True(t, ok, "http client must back onto *http.Transport")
	t2, ok := h2.client.Transport.(*http.Transport)
	require.True(t, ok, "http client must back onto *http.Transport")

	// Pool sharing: both instances must point at the same transport pointer.
	require.Same(t, t1, t2,
		"all HTTPDirectRPCConnection instances must share the same transport "+
			"so one connection pool + one TLS session cache serve every upstream")
	require.Same(t, t1, common.SharedHttpTransport(),
		"the shared transport must be common.SharedHttpTransport(); a local transport "+
			"fragments the connection pool and skips TLS session reuse")
}

// TestHTTPDirectRPCConnection_AdvertisesAcceptEncodingIdentity asserts that
// the smart-router HTTP path tells upstream not to gzip. This is the scoped
// replacement for disabling compression on the shared transport: provider
// chain proxies keep their standard auto-gzip behavior, and the smart router
// alone opts out via an outbound header.
//
// Without this, Go's http client auto-adds `Accept-Encoding: gzip` and
// auto-decodes every response — the hot path that showed up at ~30-39% CPU
// in production pprof before the scoped override.
func TestHTTPDirectRPCConnection_AdvertisesAcceptEncodingIdentity(t *testing.T) {
	// The handler runs in httptest.Server's goroutine; the assertions run in
	// the test goroutine. Guard the shared observations with a mutex so
	// `go test -race` is happy.
	var (
		mu                             sync.Mutex
		sendRequestAE, doHTTPRequestAE string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ae := r.Header.Get("Accept-Encoding")
		mu.Lock()
		if r.Method == http.MethodPost {
			sendRequestAE = ae
		} else {
			doHTTPRequestAE = ae
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: srv.URL}, 1, "")
	require.NoError(t, err)
	h, ok := conn.(*HTTPDirectRPCConnection)
	require.True(t, ok, "conn must be *HTTPDirectRPCConnection")

	// SendRequest — POST JSON-RPC path.
	sendResp, sendErr := h.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":1}`), nil)
	require.NoError(t, sendErr)
	require.NotNil(t, sendResp)
	require.Equal(t, `{"ok":true}`, string(sendResp.Data),
		"body must be the raw server payload; any transformation implies unexpected auto-decode")

	// DoHTTPRequest — REST path.
	doResp, doErr := h.DoHTTPRequest(ctx, HTTPRequestParams{
		Method: http.MethodGet,
		URL:    srv.URL,
	})
	require.NoError(t, doErr)
	require.NotNil(t, doResp)
	require.Equal(t, `{"ok":true}`, string(doResp.Body),
		"body must be the raw server payload; any transformation implies unexpected auto-decode")

	mu.Lock()
	sae, dae := sendRequestAE, doHTTPRequestAE
	mu.Unlock()
	require.Equal(t, "identity", sae,
		"SendRequest must advertise Accept-Encoding: identity so Go does not auto-negotiate gzip")
	require.Equal(t, "identity", dae,
		"DoHTTPRequest must advertise Accept-Encoding: identity so Go does not auto-negotiate gzip")
}
