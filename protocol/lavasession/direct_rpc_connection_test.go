package lavasession

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
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
	for i := range n {
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

	for i := range n {
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

// TestHTTPDirectRPCConnection_SendRequest_SurfacesTransportError verifies a
// transport-level failure (connection refused, DNS miss, TLS handshake failure,
// timeout) is surfaced to the caller as an error rather than swallowed. The
// relay path turns this error into an OnSessionFailure → QoS availability
// penalty, which is what lets the optimizer route away from a dead upstream now
// that selection no longer consults a per-socket health bit.
func TestHTTPDirectRPCConnection_SendRequest_SurfacesTransportError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Port 1 never accepts — the Do call will fail with ECONNREFUSED (or time out
	// on platforms that don't fast-fail); both are transport errors.
	nodeUrl := common.NodeUrl{Url: "http://127.0.0.1:1"}
	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	_, sendErr := conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","method":"probe","id":1}`), nil)
	require.Error(t, sendErr, "SendRequest must surface the transport failure to the caller")
}

// TestHTTPDirectRPCConnection_SendRequest_4xxReturnsResponseAndError ensures a
// 4xx/5xx HTTP response is treated as an *application* error: the transport
// reached the upstream, so the response body is returned alongside an
// HTTPStatusError. This is why a 429 alone must not take an endpoint out of
// rotation — it stays a candidate, and QoS (not transport) decides its fate.
func TestHTTPDirectRPCConnection_SendRequest_4xxReturnsResponseAndError(t *testing.T) {
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

	resp, sendErr := conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0"}`), nil)
	require.Error(t, sendErr, "4xx/5xx still returns an HTTPStatusError to the caller")
	require.NotNil(t, resp, "the response body must still be returned for a 4xx/5xx")
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
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

// TestDoHTTPRequest_PerRequestHeadersOverrideDefaultContentType locks in the
// precedence between HTTPRequestParams.ContentType and HTTPRequestParams.Headers.
//
// ContentType is a *default* the caller supplies for requests with a body (the
// REST and JSON-RPC relay paths both hardcode "application/json"). Headers carries
// what the chain spec resolved for this specific request. The spec is more specific
// than the default, so the spec must win.
//
// This regression exists because the two were applied in the opposite order:
// per-request headers were written first and the default was written over them.
// Stellar's REST POST collection overrides content-type to
// application/x-www-form-urlencoded — Horizon rejects anything else on
// /transactions with 415 unsupported_media_type — so every Stellar transaction
// submission through direct-RPC failed, and no spec change could fix it because
// the resolved value was discarded here. Do not reorder these two blocks.
func TestDoHTTPRequest_PerRequestHeadersOverrideDefaultContentType(t *testing.T) {
	tests := []struct {
		name        string
		headers     []pairingtypes.Metadata
		contentType string
		expected    string
		reason      string
	}{
		{
			name:        "spec-provided content-type beats the hardcoded default",
			headers:     []pairingtypes.Metadata{{Name: "content-type", Value: "application/x-www-form-urlencoded"}},
			contentType: "application/json",
			expected:    "application/x-www-form-urlencoded",
			reason:      "the Stellar case: a spec override must survive to the node, or Horizon answers 415",
		},
		{
			name:        "default still applies when the spec says nothing",
			headers:     nil,
			contentType: "application/json",
			expected:    "application/json",
			reason:      "chains without a content-type directive must keep the JSON default",
		},
		{
			name:        "unrelated spec headers do not disturb the default",
			headers:     []pairingtypes.Metadata{{Name: "x-custom", Value: "value"}},
			contentType: "application/json",
			expected:    "application/json",
			reason:      "only a content-type directive may replace the default",
		},
		{
			name:        "empty value deletes the header (delete semantics)",
			headers:     []pairingtypes.Metadata{{Name: "content-type", Value: ""}},
			contentType: "application/json",
			expected:    "",
			reason:      "a spec may remove the header entirely; the default must not resurrect it",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			var gotBody string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Content-Type")
				body, _ := io.ReadAll(r.Body)
				gotBody = string(body)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL}, 1, "rest")
			require.NoError(t, err)
			doer, ok := conn.(HTTPDirectRPCDoer)
			require.True(t, ok, "an HTTP connection must implement HTTPDirectRPCDoer")

			_, err = doer.DoHTTPRequest(ctx, HTTPRequestParams{
				Method:      http.MethodPost,
				URL:         server.URL + "/transactions",
				Body:        []byte("tx=AAAAtest"),
				Headers:     tt.headers,
				ContentType: tt.contentType,
			})
			require.NoError(t, err)

			require.Equal(t, tt.expected, got, tt.reason)
			require.Equal(t, "tx=AAAAtest", gotBody, "the body must reach the node untouched")
		})
	}
}

// TestGRPCDirectRPCConnection_DescriptorSourceFailureIsFatal pins that a descriptor
// source the node can never resolve fails the connection instead of warning past it.
//
// The old call site logged "will use reflection" and continued. That was true only
// while every path resolved through reflection regardless; in "file" mode
// getMethodDescriptor and parseInputMessage go through this same loader and return
// this same error, and LoadProtoset caches failures — so continuing bought one WARN
// at boot and then total relay failure on the endpoint, with no further signal and
// no recovery. That is the silent-misconfiguration shape MAG-2350 removes, not one
// to relocate.
func TestGRPCDirectRPCConnection_DescriptorSourceFailureIsFatal(t *testing.T) {
	newGrpcConn := func(t *testing.T, grpcConfig common.GrpcConfig) (*GRPCDirectRPCConnection, *atomic.Int32) {
		t.Helper()
		conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{
			Url:        "grpcs://localhost:9090",
			GrpcConfig: grpcConfig,
		}, 5, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })

		grpcConn, ok := conn.(*GRPCDirectRPCConnection)
		require.True(t, ok)

		// Fail the dial rather than perform it: reaching this at all is the bug.
		var dials atomic.Int32
		grpcConn.newConnector = func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
			dials.Add(1)
			return nil, fmt.Errorf("dial must not be reached")
		}
		return grpcConn, &dials
	}

	for _, tt := range []struct {
		name   string
		config common.GrpcConfig
	}{
		{
			name: "file mode with an unreadable descriptor set",
			config: common.GrpcConfig{
				DescriptorSource: common.GrpcDescriptorSourceFile,
				// Unique per run: LoadProtoset caches by path, including failures.
				DescriptorSetPath: filepath.Join(t.TempDir(), "missing.pb"),
			},
		},
		{
			name:   "file mode with no descriptor set at all",
			config: common.GrpcConfig{DescriptorSource: common.GrpcDescriptorSourceFile},
		},
		{
			// GrpcConfig.Validate has no callers, so an unrecognised mode reaches the
			// request path. It resolves to nothing, which makes it fatal here too.
			name:   "unrecognised descriptor-source",
			config: common.GrpcConfig{DescriptorSource: "astrology"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			grpcConn, dials := newGrpcConn(t, tt.config)

			require.Error(t, grpcConn.initialize(context.Background()))
			require.Zero(t, dials.Load(),
				"config that cannot resolve must fail before spending a dial budget on it")

			grpcConn.connMu.Lock()
			connector := grpcConn.connector
			grpcConn.connMu.Unlock()
			require.Nil(t, connector, "a failed initialize must leave no connector installed")

			// Not latched: ensureInitialized surfaces the error and leaves the
			// connection re-initializable, matching every other initialize() failure.
			require.Error(t, grpcConn.ensureInitialized(context.Background()))
			require.False(t, grpcConn.initialized.Load())
		})
	}

	// The vacuity guard. Making this fatal is only safe because it cannot fire for
	// the reflection default — which is every gRPC node-url in every config today.
	t.Run("reflection default never trips it", func(t *testing.T) {
		for _, config := range []common.GrpcConfig{
			{},
			{DescriptorSource: common.GrpcDescriptorSourceReflection},
			{DescriptorSource: common.GrpcDescriptorSourceReflection, ReflectionTimeout: 2 * time.Second},
			// Hybrid degrades to reflection rather than failing: an unusable protoset
			// is exactly the case its other half covers.
			{
				DescriptorSource:  common.GrpcDescriptorSourceHybrid,
				DescriptorSetPath: filepath.Join(t.TempDir(), "missing.pb"),
			},
			{DescriptorSource: common.GrpcDescriptorSourceHybrid},
		} {
			grpcConn, _ := newGrpcConn(t, config)
			require.NoError(t, grpcConn.initializeDescriptorSource())
		}
	})
}

// TestHTTPDirectRPCConnection_SendRequest_CapturesRetryAfter covers the direct path's own
// rate-limit surface: it mints its HTTPStatusError with the response in scope, so a caller
// deciding when to come back reads the upstream's answer through the same
// common.RetryAfterFrom the chainlib proxies feed. The existing type assertion on
// *HTTPStatusError (recovery_probe) must keep working alongside it.
func TestHTTPDirectRPCConnection_SendRequest_CapturesRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "90")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL}, 5, "")
	require.NoError(t, err)

	_, sendErr := conn.SendRequest(ctx, []byte(`{"jsonrpc":"2.0"}`), nil)
	require.Error(t, sendErr)

	httpErr, ok := sendErr.(*HTTPStatusError)
	require.True(t, ok, "callers type-asserting the concrete error must be unaffected")
	require.Equal(t, http.StatusTooManyRequests, httpErr.StatusCode)

	require.ErrorIs(t, sendErr, common.StatusCodeError429, "a direct-path 429 is a 429")
	d, ok := common.RetryAfterFrom(sendErr)
	require.True(t, ok)
	require.Equal(t, 90*time.Second, d)
}

// A non-429 must not read as a rate-limit just because it unwraps, and a 429 without the
// header carries no opinion — the caller stays on its own backoff.
func TestHTTPStatusError_UnwrapIsScopedToRateLimits(t *testing.T) {
	notRateLimited := &HTTPStatusError{StatusCode: http.StatusInternalServerError, Status: "500"}
	require.NotErrorIs(t, notRateLimited, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(notRateLimited)
	require.False(t, ok)

	noHeader := &HTTPStatusError{StatusCode: http.StatusTooManyRequests, Status: "429"}
	require.ErrorIs(t, noHeader, common.StatusCodeError429)
	_, ok = common.RetryAfterFrom(noHeader)
	require.False(t, ok, "no header means no opinion, not zero delay")
}
