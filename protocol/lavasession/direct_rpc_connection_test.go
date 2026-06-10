package lavasession

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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

func TestWebSocketSendRequestNotImplemented(t *testing.T) {
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: "wss://test.example.com"}

	conn, err := NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	// WebSocket SendRequest should return not implemented error
	_, err = conn.SendRequest(ctx, []byte("test"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WebSocket SendRequest not implemented")
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
