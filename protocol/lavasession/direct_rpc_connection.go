package lavasession

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DirectRPCProtocol represents the transport protocol for direct RPC connections
type DirectRPCProtocol string

const (
	DirectRPCProtocolHTTP  DirectRPCProtocol = "http"
	DirectRPCProtocolHTTPS DirectRPCProtocol = "https"
	DirectRPCProtocolWS    DirectRPCProtocol = "ws"  // WebSocket
	DirectRPCProtocolWSS   DirectRPCProtocol = "wss" // WebSocket Secure
	DirectRPCProtocolGRPC  DirectRPCProtocol = "grpc"
)

// DirectRPCResponse contains the response from a direct RPC request.
// This unified response type captures both data and metadata from all protocols
// (HTTP headers, gRPC metadata, etc.) for consistent handling.
type DirectRPCResponse struct {
	// Data contains the raw response bytes (JSON, protobuf, etc.)
	Data []byte

	// Metadata contains response headers/metadata from the underlying protocol.
	// For HTTP: response headers (Content-Type, X-Request-Id, etc.)
	// For gRPC: response metadata (trailers and headers)
	// For WebSocket: typically empty (metadata is per-message)
	Metadata map[string][]string

	// StatusCode contains the HTTP status code (for HTTP/REST) or gRPC status code.
	// For successful gRPC calls, this will be 200 (OK equivalent).
	StatusCode int
}

// DirectRPCConnection represents a direct connection to an RPC endpoint
// (no Lava provider-relay protocol involved)
type DirectRPCConnection interface {
	// SendRequest sends the already-built raw request bytes and returns a response
	// containing both data and metadata.
	//
	// IMPORTANT: DirectRPCConnection is transport-only. It must not need to interpret JSON-RPC
	// method/params or chain semantics. Those remain in chainParser + chainMessage.
	SendRequest(ctx context.Context, data []byte, headers map[string]string) (*DirectRPCResponse, error)

	// GetProtocol returns the transport protocol being used
	GetProtocol() DirectRPCProtocol

	// Close closes the connection and cleans up resources
	Close() error

	// GetURL returns the endpoint URL
	GetURL() string

	// GetNodeUrl returns the NodeUrl configuration (for timeout overrides, auth, etc.)
	GetNodeUrl() *common.NodeUrl
}

// GRPCDescriptorProvider is an optional interface that gRPC connections can implement
// to provide access to method descriptors. This is used for parsing gRPC responses
// to extract block heights for QoS sync tracking.
type GRPCDescriptorProvider interface {
	// GetCachedMethodDescriptor returns a cached method descriptor for the given method path.
	// Returns nil if no descriptor is cached for this method.
	// The methodPath should be in the format "service/method" (e.g., "cosmos.bank.v1beta1.Query/TotalSupply")
	GetCachedMethodDescriptor(methodPath string) *desc.MethodDescriptor
}

// HTTPDirectRPCResponse contains complete HTTP response data (Phase 4 REST support)
type HTTPDirectRPCResponse struct {
	StatusCode int                 // HTTP status code (200, 404, 500, etc.)
	Headers    map[string][]string // Response headers (http.Header compatible)
	Body       []byte              // Response body
}

// HTTPRequestParams defines HTTP request parameters for REST support (Phase 4)
type HTTPRequestParams struct {
	Method      string                  // HTTP method: GET, POST, PUT, DELETE
	URL         string                  // Full URL (will be auth-transformed)
	Body        []byte                  // Request body (nil for GET)
	Headers     []pairingtypes.Metadata // Preserves delete semantics (empty value = delete)
	ContentType string                  // Content-Type (only for requests with body)
}

// HTTPDirectRPCDoer is an HTTP-only extension interface for REST support (Phase 4)
// Only HTTPDirectRPCConnection implements this (not WebSocket/gRPC)
type HTTPDirectRPCDoer interface {
	DirectRPCConnection // Inherits base interface

	// DoHTTPRequest executes an HTTP request with variable method/URL
	// Returns: Complete HTTP response (status + headers + body)
	DoHTTPRequest(ctx context.Context, params HTTPRequestParams) (*HTTPDirectRPCResponse, error)
}

// HTTPDirectRPCConnection implements DirectRPCConnection for HTTP/HTTPS
type HTTPDirectRPCConnection struct {
	nodeUrl  common.NodeUrl
	protocol DirectRPCProtocol
	client   *http.Client
}

// WebSocketDirectRPCConnection implements DirectRPCConnection for WebSocket/WSS.
//
// Request/response is served over a persistent JSON-RPC-over-WebSocket client
// (rpcclient.Client), dialed lazily on first SendRequest and cached. The client
// multiplexes concurrent requests by JSON-RPC id and transparently reconnects a
// dropped socket, so callers (chain-tracker polls, direct relays) get the same
// raw-bytes contract as HTTP with no WebSocket-specific handling.
//
// Subscriptions/streaming are NOT served here — those use the dedicated
// UpstreamWSPool layer.
type WebSocketDirectRPCConnection struct {
	nodeUrl  common.NodeUrl
	protocol DirectRPCProtocol
	// isJsonRPC carries the EVM-vs-Tendermint distinction for parity with the
	// HTTP/Tendermint call sites and is forwarded to CallContext. NOTE: rpcclient
	// only consumes this flag on its HTTP path (non-2xx HTTP-status handling);
	// over a WebSocket frame it is currently ignored. Kept for forward-correctness.
	isJsonRPC bool

	mu     sync.Mutex
	client *rpcclient.Client // lazily dialed on first SendRequest, then cached
	closed bool              // set by Close(); prevents re-dialing a closed connection

	// wireID issues a connection-unique JSON-RPC id per request. rpcclient.Client
	// multiplexes concurrent requests on one socket and routes replies by id
	// (handler.respWait is a plain id→op map), so reusing a caller-supplied id
	// across concurrent calls would misroute responses. We send a unique wire id
	// and restore the caller's original id on the reply before returning.
	wireID atomic.Uint64
}

// GRPCDirectRPCConnection implements DirectRPCConnection for gRPC.
// It provides direct gRPC connections to RPC endpoints with:
//   - Connection pooling via GRPCConnector
//   - Dynamic protobuf handling via reflection or file descriptors
//   - Method descriptor caching for performance
//   - Proper error handling with gRPC status codes
type GRPCDirectRPCConnection struct {
	nodeUrl common.NodeUrl

	// Connection pooling
	connector grpcConnectorInterface
	connMu    sync.RWMutex

	// Descriptor handling
	registry         *dyncodec.Registry
	codec            *dyncodec.Codec
	descriptorsCache *common.SafeSyncMap[string, *desc.MethodDescriptor]

	// Initialization state
	initialized atomic.Bool
	initMu      sync.Mutex
	initErr     error
}

// grpcConnectorInterface abstracts GRPCConnector for testing
type grpcConnectorInterface interface {
	GetRpc(ctx context.Context, block bool) (*grpc.ClientConn, error)
	ReturnRpc(rpc *grpc.ClientConn)
	Close()
}

// GRPCMethodHeader is the header key for the gRPC method path
const GRPCMethodHeader = "x-grpc-method"

// GRPCContentTypeHeader is the header key for content type (proto or json)
const GRPCContentTypeHeader = "x-grpc-content-type"

// GRPCNodeErrorResponse represents a gRPC error returned as JSON
type GRPCNodeErrorResponse struct {
	ErrorMessage string `json:"error_message"`
	ErrorCode    uint32 `json:"error_code"`
}

// Default gRPC error code when status cannot be determined
const GRPCStatusCodeOnFailedMessages = 32

// NewDirectRPCConnection creates a new direct RPC connection based on URL protocol.
// The apiInterface parameter provides context for protocol detection when URL has no scheme
// (e.g., bare "host:port" should be treated as gRPC when apiInterface is "grpc").
func NewDirectRPCConnection(
	ctx context.Context,
	nodeUrl common.NodeUrl,
	parallelConnections uint,
	apiInterface string, // Optional: used for protocol detection hint (e.g., "grpc", "rest", "jsonrpc")
) (DirectRPCConnection, error) {
	protocol, err := DetectProtocol(nodeUrl.Url, apiInterface)
	if err != nil {
		return nil, fmt.Errorf("failed to detect protocol: %w", err)
	}

	switch protocol {
	case DirectRPCProtocolHTTP, DirectRPCProtocolHTTPS:
		// Use the shared optimized client so every smart-router HTTP connection
		// benefits from:
		//   - DisableCompression=true        (skips ~30% CPU on auto-gunzip inflate)
		//   - MaxIdleConnsPerHost pooling    (reuses TCP conns under load)
		//   - TLS session cache              (faster reconnects, less handshake CPU)
		//   - ForceAttemptHTTP2              (multiplexes streams on one conn)
		//
		// The backing transport is a singleton from common.SharedHttpTransport(),
		// so all HTTPDirectRPCConnection instances share one connection pool.
		// Per-request deadlines still come from the caller's context via
		// http.NewRequestWithContext; the client's own 5-minute timeout is a
		// safety backstop.
		conn := &HTTPDirectRPCConnection{
			nodeUrl:  nodeUrl,
			protocol: protocol,
			client:   common.OptimizedHttpClient(),
		}
		return conn, nil

	case DirectRPCProtocolWS, DirectRPCProtocolWSS:
		// Request/response over WebSocket uses the shared go-ethereum-derived
		// JSON-RPC client (rpcclient), dialed lazily on first SendRequest.
		// Subscriptions/streaming continue to use the dedicated UpstreamWSPool layer.
		return &WebSocketDirectRPCConnection{
			nodeUrl:   nodeUrl,
			protocol:  protocol,
			isJsonRPC: !strings.EqualFold(apiInterface, "tendermintrpc"),
		}, nil

	case DirectRPCProtocolGRPC:
		conn := &GRPCDirectRPCConnection{
			nodeUrl:          nodeUrl,
			descriptorsCache: &common.SafeSyncMap[string, *desc.MethodDescriptor]{},
		}
		return conn, nil

	default:
		return nil, fmt.Errorf("unsupported protocol: %s", protocol)
	}
}

// DetectProtocol detects the RPC protocol from URL scheme.
// The apiInterface parameter provides a hint when the URL has no explicit scheme:
//   - For "grpc" interface: bare "host:port" → gRPC
//   - For other interfaces: bare "host:port" → HTTPS (default)
//
// Bare host:port is treated as gRPC for the "grpc" interface, HTTPS otherwise.
func DetectProtocol(urlStr string, apiInterface string) (DirectRPCProtocol, error) {
	// Handle bare host:port URLs (e.g. "lava-grpc.publicnode.com:443") before url.Parse.
	// Go's url.Parse treats "word:" as scheme:opaque, so "host.com:443" becomes
	// scheme="host.com", opaque="443" — the scheme == "" branch below is never reached.
	// Checking for "://" is sufficient: all valid scheme URLs contain it.
	if !strings.Contains(urlStr, "://") {
		if strings.ToLower(apiInterface) == "grpc" {
			return DirectRPCProtocolGRPC, nil
		}
		return DirectRPCProtocolHTTPS, nil
	}

	parsed, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "http":
		return DirectRPCProtocolHTTP, nil
	case "https":
		return DirectRPCProtocolHTTPS, nil
	case "ws":
		return DirectRPCProtocolWS, nil
	case "wss":
		return DirectRPCProtocolWSS, nil
	case "grpc", "grpcs":
		return DirectRPCProtocolGRPC, nil
	default:
		return "", fmt.Errorf("unsupported URL scheme: %s", scheme)
	}
}

// SendRequest implements DirectRPCConnection for HTTP/HTTPS
func (h *HTTPDirectRPCConnection) SendRequest(
	ctx context.Context,
	data []byte,
	headers map[string]string,
) (*DirectRPCResponse, error) {
	// NOTE: for JSON-RPC we use POST with a JSON body.
	// For REST-style APIs (e.g. Cosmos LCD), the chain parser must provide the correct URL path and HTTP method
	// (GET/POST/etc.). This transport layer sends bytes and returns bytes; method selection is driven by chain spec.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.nodeUrl.AuthConfig.AddAuthPath(h.nodeUrl.Url), bytes.NewReader(data))
	if err != nil {
		// NewRequestWithContext only fails on malformed URL / method — a programmer
		// error, not a transport failure or upstream outage.
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	// Apply auth headers from config + per-request headers from chainMessage/chainParser.
	for k, v := range h.nodeUrl.GetAuthHeaders() {
		req.Header.Set(k, v)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	// Advertise Accept-Encoding: identity so Go's http client neither auto-adds
	// `gzip` nor auto-decodes the response. This scoping is smart-router-only:
	// the shared transport still lets provider chain proxies keep their
	// standard auto-gzip behavior. Production pprof attributed ~30-39% of CPU
	// to the auto-decode path (http2gzipReader → compress/flate.decompressor);
	// removing it dropped the eth router from 2.47 cores to 1.23 cores.
	// Set *after* caller headers so it cannot be accidentally overridden.
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}

	// Build response with metadata (HTTP headers)
	response := &DirectRPCResponse{
		Data:       body,
		Metadata:   resp.Header, // http.Header is map[string][]string
		StatusCode: resp.StatusCode,
	}

	// Check HTTP status code and return error for non-2xx responses
	// Note: For JSON-RPC, status 200 with RPC error in body is common and valid
	// We return the response in both cases - the caller will check for RPC errors
	if resp.StatusCode >= 400 {
		// For 4xx/5xx errors, include status code in error but also return the response
		return response, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       body,
		}
	}

	return response, nil
}

// HTTPStatusError represents an HTTP error response (4xx/5xx)
type HTTPStatusError struct {
	StatusCode int
	Status     string
	Body       []byte
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("HTTP %d %s", e.StatusCode, e.Status)
}

func (h *HTTPDirectRPCConnection) GetProtocol() DirectRPCProtocol {
	return h.protocol
}

func (h *HTTPDirectRPCConnection) Close() error {
	return nil
}

func (h *HTTPDirectRPCConnection) GetURL() string {
	return h.nodeUrl.Url
}

func (h *HTTPDirectRPCConnection) GetNodeUrl() *common.NodeUrl {
	return &h.nodeUrl
}

// DoHTTPRequest implements HTTPDirectRPCDoer for REST support (Phase 4)
// Executes HTTP request with variable method (GET/POST/PUT/DELETE)
func (h *HTTPDirectRPCConnection) DoHTTPRequest(
	ctx context.Context,
	params HTTPRequestParams,
) (*HTTPDirectRPCResponse, error) {
	// Build request body reader
	var bodyReader io.Reader
	if params.Body != nil {
		bodyReader = bytes.NewReader(params.Body)
	}

	// Apply auth path transformation (e.g., prepend /api/v1 if configured)
	fullURL := h.nodeUrl.AuthConfig.AddAuthPath(params.URL)

	req, err := http.NewRequestWithContext(ctx, params.Method, fullURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to build HTTP request: %w", err)
	}

	// Apply NodeUrl auth headers (API keys, bearer tokens, etc.)
	h.nodeUrl.SetAuthHeaders(ctx, req.Header.Set)

	// Apply IP forwarding if needed
	h.nodeUrl.SetIpForwardingIfNecessary(ctx, req.Header.Set)

	// Apply per-request headers with delete semantics
	for _, header := range params.Headers {
		if header.Value == "" {
			req.Header.Del(header.Name) // Empty value = delete header
		} else {
			req.Header.Set(header.Name, header.Value)
		}
	}

	// Set Content-Type for requests with body
	if params.Body != nil && params.ContentType != "" {
		req.Header.Set("Content-Type", params.ContentType)
	}
	// Scoped smart-router override: skip upstream gzip auto-negotiation. See
	// SendRequest above for the full rationale. Set last so it cannot be
	// overridden by per-request headers.
	req.Header.Set("Accept-Encoding", "identity")

	// Send request
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response: %w", err)
	}

	// Return complete response (status + headers + body)
	// Don't return error for 4xx/5xx - client needs the response
	return &HTTPDirectRPCResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
		Body:       body,
	}, nil
}

// SendRequest implements DirectRPCConnection for WebSocket/WSS.
//
// It ships the already-built request body as a single JSON-RPC frame over a
// persistent WebSocket connection and waits for the id-correlated response
// frame, presenting the same raw-bytes-in / raw-bytes-out contract as the HTTP
// transport. This is request/response only — subscriptions/streaming use the
// dedicated UpstreamWSPool layer.
func (w *WebSocketDirectRPCConnection) SendRequest(
	ctx context.Context,
	data []byte,
	headers map[string]string,
) (*DirectRPCResponse, error) {
	client, err := w.ensureClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WebSocket %s: %w", w.nodeUrl.Url, err)
	}

	// Decode the already-built request body to recover id/method/params for the
	// typed client API. Params is interface{}, so a JSON array decodes to
	// []interface{} and an object to map[string]interface{} — exactly the shapes
	// CallContext accepts.
	var reqMsg rpcInterfaceMessages.JsonrpcMessage
	if err := json.Unmarshal(data, &reqMsg); err != nil {
		return nil, fmt.Errorf("failed to parse JSON-RPC request for WebSocket %s: %w", w.nodeUrl.Url, err)
	}

	// Send a connection-unique wire id so concurrent requests can't collide in
	// the client's id→response map, then restore the caller's id on the reply.
	wireID := json.RawMessage(strconv.FormatUint(w.wireID.Add(1), 10))

	reply, err := client.CallContext(ctx, wireID, reqMsg.Method, reqMsg.Params, w.isJsonRPC, false)
	if err != nil {
		return nil, err
	}

	// Restore the caller's id on a COPY of the reply. The rpcclient dispatch
	// goroutine may still read the returned reply concurrently, so mutating it
	// in place is a data race — we only read it (to copy) and write the id on
	// our own value.
	out := *reply
	out.ID = reqMsg.ID // caller's id (omitted/empty for notifications)

	respBytes, err := json.Marshal(&out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal WebSocket response for %s: %w", w.nodeUrl.Url, err)
	}
	utils.LavaFormatTrace("WebSocket SendRequest succeeded",
		utils.LogAttr("endpoint", w.nodeUrl.Url),
		utils.LogAttr("method", reqMsg.Method),
		utils.LogAttr("responseBytes", len(respBytes)),
	)
	return &DirectRPCResponse{Data: respBytes, StatusCode: http.StatusOK}, nil
}

// ensureClient lazily dials the WebSocket on first use and caches the resulting
// rpcclient.Client. The client owns reconnection internally (Client.write ->
// reconnect, using each call's context), so a single successful dial survives
// transient socket drops; only a failed initial dial leaves client nil, and the
// next call retries the dial. The dial context is used only to establish the
// connection — it does not bound the connection's lifetime.
func (w *WebSocketDirectRPCConnection) ensureClient(ctx context.Context) (*rpcclient.Client, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, fmt.Errorf("WebSocket connection is closed for endpoint %s", w.nodeUrl.Url)
	}
	if w.client != nil {
		return w.client, nil
	}

	headers := make(map[string]string)
	for k, v := range w.nodeUrl.GetAuthHeaders() {
		headers[k] = v
	}
	endpoint := w.nodeUrl.AuthConfig.AddAuthPath(w.nodeUrl.Url)

	client, err := rpcclient.DialWebsocket(ctx, endpoint, headers)
	if err != nil {
		return nil, err
	}
	w.client = client
	return client, nil
}

func (w *WebSocketDirectRPCConnection) GetProtocol() DirectRPCProtocol {
	return w.protocol
}

func (w *WebSocketDirectRPCConnection) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.client != nil {
		w.client.Close()
		w.client = nil
	}
	return nil
}

func (w *WebSocketDirectRPCConnection) GetURL() string {
	return w.nodeUrl.Url
}

func (w *WebSocketDirectRPCConnection) GetNodeUrl() *common.NodeUrl {
	return &w.nodeUrl
}

// SendRequest implements DirectRPCConnection for gRPC.
// The method path should be provided in headers via GRPCMethodHeader ("x-grpc-method").
// The data can be either JSON or binary protobuf format.
func (g *GRPCDirectRPCConnection) SendRequest(
	ctx context.Context,
	data []byte,
	headers map[string]string,
) (*DirectRPCResponse, error) {
	// Validate required headers before initialization
	// This allows early validation without connecting to the server
	methodPath, ok := headers[GRPCMethodHeader]
	if !ok || methodPath == "" {
		return nil, fmt.Errorf("gRPC method path not provided in headers (expected %s header)", GRPCMethodHeader)
	}

	// Lazy initialization on first request
	if err := g.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	// Get gRPC connection from pool
	conn, err := g.connector.GetRpc(ctx, true)
	if err != nil {
		return nil, utils.LavaFormatError("gRPC get connection failed", err,
			utils.LogAttr("url", g.nodeUrl.Url))
	}
	defer g.connector.ReturnRpc(conn)

	// Build metadata context from headers
	metadataMap := make(map[string]string)
	for k, v := range headers {
		// Skip internal headers
		if k == GRPCMethodHeader || k == GRPCContentTypeHeader {
			continue
		}
		if v != "" {
			metadataMap[k] = v
		}
	}
	if len(metadataMap) > 0 {
		md := metadata.New(metadataMap)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	// Parse service and method name
	svc, methodName := rpcInterfaceMessages.ParseSymbol(methodPath)

	// Get method descriptor (with caching)
	methodDescriptor, err := g.getMethodDescriptor(ctx, conn, svc, methodName)
	if err != nil {
		return nil, utils.LavaFormatError("failed to get method descriptor", err,
			utils.LogAttr("method", methodPath))
	}

	// Create dynamic message factory
	msgFactory := dynamic.NewMessageFactoryWithDefaults()

	// Create input message
	inputMsg := msgFactory.NewMessage(methodDescriptor.GetInputType())

	// Parse input data (JSON or binary proto)
	if len(data) > 0 {
		if err := g.parseInputMessage(ctx, data, inputMsg, methodDescriptor, conn); err != nil {
			return nil, utils.LavaFormatError("failed to parse input message", err,
				utils.LogAttr("method", methodPath))
		}
	}

	// Create output message
	outputMsg := msgFactory.NewMessage(methodDescriptor.GetOutputType())

	// Invoke gRPC method - capture response headers/metadata
	var respHeaders metadata.MD
	err = conn.Invoke(ctx, "/"+methodPath, inputMsg, outputMsg, grpc.Header(&respHeaders))
	if err != nil {
		// Handle gRPC error - pass respHeaders for metadata in error response
		return g.handleGRPCError(ctx, err, respHeaders)
	}

	// Marshal response to binary proto
	respBytes, err := proto.Marshal(outputMsg)
	if err != nil {
		return nil, utils.LavaFormatError("failed to marshal gRPC response", err,
			utils.LogAttr("method", methodPath))
	}

	// Return response with metadata from gRPC headers
	return &DirectRPCResponse{
		Data:       respBytes,
		Metadata:   respHeaders, // gRPC metadata.MD is map[string][]string
		StatusCode: http.StatusOK,
	}, nil
}

// ensureInitialized performs lazy initialization of the gRPC connection.
// This includes creating the connection pool and setting up descriptor sources.
func (g *GRPCDirectRPCConnection) ensureInitialized(ctx context.Context) error {
	if g.initialized.Load() {
		return g.initErr
	}

	g.initMu.Lock()
	defer g.initMu.Unlock()

	// Double-check after acquiring lock
	if g.initialized.Load() {
		return g.initErr
	}

	g.initErr = g.initialize(ctx)
	g.initialized.Store(true)
	return g.initErr
}

// initialize performs the actual initialization
func (g *GRPCDirectRPCConnection) initialize(ctx context.Context) error {
	// Validate gRPC URL
	if err := g.validateURL(); err != nil {
		return err
	}

	// Create connection pool
	// Extract host from URL for GRPCConnector (it expects host:port without scheme)
	parsedURL, err := url.Parse(g.nodeUrl.Url)
	if err != nil {
		return utils.LavaFormatError("failed to parse gRPC URL", err,
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	// GRPCConnector expects URL without scheme prefix for internal handling
	connectorNodeUrl := g.nodeUrl
	connectorNodeUrl.Url = parsedURL.Host
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		connectorNodeUrl.Url += parsedURL.Path
	}

	// Determine TLS based on scheme
	if parsedURL.Scheme == "grpcs" {
		connectorNodeUrl.AuthConfig.UseTLS = true
	}

	connector, err := chainproxy.NewGRPCConnector(ctx, 10, connectorNodeUrl)
	if err != nil {
		return utils.LavaFormatError("failed to create gRPC connector", err,
			utils.LogAttr("url", g.nodeUrl.Url))
	}
	g.connector = connector

	// Initialize descriptor cache
	g.descriptorsCache = &common.SafeSyncMap[string, *desc.MethodDescriptor]{}

	// Initialize descriptor source based on config
	if err := g.initializeDescriptorSource(ctx); err != nil {
		// Log warning but don't fail - we'll try reflection on first request
		utils.LavaFormatWarning("failed to initialize descriptor source, will use reflection", err,
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	utils.LavaFormatInfo("gRPC direct connection initialized",
		utils.LogAttr("url", g.nodeUrl.Url),
		utils.LogAttr("tls", parsedURL.Scheme == "grpcs"))

	return nil
}

// validateURL checks if the gRPC URL is valid
func (g *GRPCDirectRPCConnection) validateURL() error {
	parsedURL, err := url.Parse(g.nodeUrl.Url)
	if err != nil {
		return utils.LavaFormatError("invalid gRPC URL", err,
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme != "grpc" && scheme != "grpcs" {
		return utils.LavaFormatError("invalid gRPC URL scheme", nil,
			utils.LogAttr("url", g.nodeUrl.Url),
			utils.LogAttr("scheme", scheme),
			utils.LogAttr("expected", "grpc:// or grpcs://"))
	}

	// Check insecure connections
	if scheme == "grpc" && !g.nodeUrl.GrpcConfig.AllowInsecure {
		return utils.LavaFormatError("insecure gRPC (grpc://) not allowed without allow-insecure: true", nil,
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	return nil
}

// initializeDescriptorSource sets up the descriptor source based on config
func (g *GRPCDirectRPCConnection) initializeDescriptorSource(ctx context.Context) error {
	grpcConfig := &g.nodeUrl.GrpcConfig
	source := grpcConfig.GetDescriptorSource()

	switch source {
	case common.GrpcDescriptorSourceFile:
		if grpcConfig.DescriptorSetPath == "" {
			return fmt.Errorf("descriptor-set-path required for file descriptor source")
		}
		fileReg, err := dyncodec.NewFileDescriptorSetRegistryFromPath(grpcConfig.DescriptorSetPath)
		if err != nil {
			return err
		}
		g.registry = dyncodec.NewRegistry(fileReg)
		g.codec = dyncodec.NewCodec(g.registry)

	case common.GrpcDescriptorSourceHybrid:
		// Will be set up on first connection with hybrid source
		// For now, try to load file if available
		if grpcConfig.DescriptorSetPath != "" {
			fileReg, err := dyncodec.NewFileDescriptorSetRegistryFromPath(grpcConfig.DescriptorSetPath)
			if err != nil {
				utils.LavaFormatWarning("failed to load file descriptors for hybrid mode", err,
					utils.LogAttr("path", grpcConfig.DescriptorSetPath))
			} else {
				g.registry = dyncodec.NewRegistry(fileReg)
				g.codec = dyncodec.NewCodec(g.registry)
			}
		}

	case common.GrpcDescriptorSourceReflection, "":
		// Reflection will be used on first request
		// No initialization needed here
	}

	return nil
}

// getMethodDescriptor retrieves the method descriptor, using cache or reflection
func (g *GRPCDirectRPCConnection) getMethodDescriptor(
	ctx context.Context,
	conn *grpc.ClientConn,
	service, methodName string,
) (*desc.MethodDescriptor, error) {
	fullMethodName := service + "." + methodName

	// Check cache first
	if methodDesc, found, _ := g.descriptorsCache.Load(fullMethodName); found {
		return methodDesc, nil
	}

	// Use reflection to get descriptor
	cl := grpcreflect.NewClientAuto(ctx, conn)
	defer cl.Reset()

	descriptorSource := rpcInterfaceMessages.DescriptorSourceFromServer(cl)

	descriptor, err := descriptorSource.FindSymbol(service)
	if err != nil {
		return nil, utils.LavaFormatError("failed to find service via reflection", err,
			utils.LogAttr("service", service))
	}

	serviceDescriptor, ok := descriptor.(*desc.ServiceDescriptor)
	if !ok {
		return nil, utils.LavaFormatError("descriptor is not a ServiceDescriptor", nil,
			utils.LogAttr("service", service))
	}

	methodDescriptor := serviceDescriptor.FindMethodByName(methodName)
	if methodDescriptor == nil {
		return nil, utils.LavaFormatError("method not found in service", nil,
			utils.LogAttr("service", service),
			utils.LogAttr("method", methodName))
	}

	// Cache the descriptor
	g.descriptorsCache.Store(fullMethodName, methodDescriptor)

	return methodDescriptor, nil
}

// GetCachedMethodDescriptor returns a cached method descriptor for the given method path.
// This implements the GRPCDescriptorProvider interface.
// The methodPath should be in format "service/method" (e.g., "cosmos.bank.v1beta1.Query/TotalSupply")
// Returns nil if no descriptor is cached (call SendRequest first to populate cache).
func (g *GRPCDirectRPCConnection) GetCachedMethodDescriptor(methodPath string) *desc.MethodDescriptor {
	// Parse the method path to get the full name used in cache
	svc, methodName := rpcInterfaceMessages.ParseSymbol(methodPath)
	fullMethodName := svc + "." + methodName

	if methodDesc, found, _ := g.descriptorsCache.Load(fullMethodName); found {
		return methodDesc
	}
	return nil
}

// parseInputMessage parses the input data into the dynamic message.
// The msg parameter must be a proto.Message that was created via dynamic.NewMessage().
func (g *GRPCDirectRPCConnection) parseInputMessage(
	ctx context.Context,
	data []byte,
	msg proto.Message,
	methodDesc *desc.MethodDescriptor,
	conn *grpc.ClientConn,
) error {
	// Detect if input is JSON or binary proto
	if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
		// JSON input - use grpcurl parser
		cl := grpcreflect.NewClientAuto(ctx, conn)
		defer cl.Reset()
		descriptorSource := rpcInterfaceMessages.DescriptorSourceFromServer(cl)

		rp, _, err := grpcurl.RequestParserAndFormatter(
			grpcurl.FormatJSON,
			descriptorSource,
			bytes.NewReader(data),
			grpcurl.FormatOptions{
				EmitJSONDefaultFields: false,
				IncludeTextSeparator:  false,
				AllowUnknownFields:    true,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create JSON parser: %w", err)
		}

		if err := rp.Next(msg); err != nil {
			return fmt.Errorf("failed to parse JSON input: %w", err)
		}
	} else {
		// Binary proto input
		if err := proto.Unmarshal(data, msg); err != nil {
			return fmt.Errorf("failed to unmarshal proto input: %w", err)
		}
	}

	return nil
}

// handleGRPCError handles gRPC errors and returns an appropriate response
func (g *GRPCDirectRPCConnection) handleGRPCError(ctx context.Context, err error, respHeaders metadata.MD) (*DirectRPCResponse, error) {
	var errorCode uint32 = GRPCStatusCodeOnFailedMessages
	var errorMessage string

	if statusErr, ok := status.FromError(err); ok {
		errorCode = uint32(statusErr.Code())
		errorMessage = statusErr.Message()
	} else {
		errorMessage = err.Error()
	}

	// Return error as JSON response
	resp := GRPCNodeErrorResponse{
		ErrorMessage: errorMessage,
		ErrorCode:    errorCode,
	}

	respBytes, marshalErr := json.Marshal(resp)
	if marshalErr != nil {
		return nil, utils.LavaFormatError("failed to marshal gRPC error response", marshalErr)
	}

	// Return the error response with metadata
	// The caller can inspect the response to determine if it's an error
	return &DirectRPCResponse{
			Data:       respBytes,
			Metadata:   respHeaders, // Include any headers received before the error
			StatusCode: int(errorCode),
		}, &GRPCStatusError{
			Code:    errorCode,
			Message: errorMessage,
		}
}

// GRPCStatusError represents a gRPC status error
type GRPCStatusError struct {
	Code    uint32
	Message string
}

func (e *GRPCStatusError) Error() string {
	return fmt.Sprintf("gRPC error %d: %s", e.Code, e.Message)
}

func (g *GRPCDirectRPCConnection) GetProtocol() DirectRPCProtocol {
	return DirectRPCProtocolGRPC
}

func (g *GRPCDirectRPCConnection) Close() error {
	g.connMu.Lock()
	defer g.connMu.Unlock()

	if g.connector != nil {
		g.connector.Close()
		g.connector = nil
	}

	if g.registry != nil {
		// Registry cleanup if needed
		g.registry = nil
	}

	return nil
}

func (g *GRPCDirectRPCConnection) GetURL() string {
	return g.nodeUrl.Url
}

func (g *GRPCDirectRPCConnection) GetNodeUrl() *common.NodeUrl {
	return &g.nodeUrl
}
