package lavasession

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
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

// DirectRPCPrewarmer is an optional interface for connections that have setup
// work worth doing BEFORE the endpoint holding them is published.
//
// It exists for gRPC descriptor resolution (MAG-2860). Everything else about a
// direct connection is either free at construction or bounded by the relay that
// needs it; descriptor reflection is neither, and a relay that arrives before it
// has happened is the one that pays — which for cross-validation, needing N
// successes in one request, reads as an outage rather than a slow first call.
//
// The contract is a boundary, not a guarantee of success: Prewarm returns when
// the connection is as ready as it is going to get, and a failure is a reason to
// publish the endpoint anyway, not to withhold it. Boot must not become fatal on
// upstream health.
type DirectRPCPrewarmer interface {
	// Prewarm establishes the connection and fills whatever caches the relay path
	// would otherwise fill on demand. It is idempotent, safe to call concurrently,
	// and bounded by ctx.
	Prewarm(ctx context.Context) error
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
//
// # Lifecycle and locking (MAG-2808)
//
// Lock order is initMu -> connMu. connMu is never held while acquiring initMu.
//
//	initMu  serializes lazy initialization against itself and against Close, so
//	        that once Close returns no initializer is still running: one already
//	        in flight has had its fate decided (it bails at the closed re-check
//	        under connMu and tears its own connector down) before Close's caller
//	        moves on.
//	connMu  guards the connector field. Requests hold it in read mode for the
//	        whole pool checkout, which turns "pick a connector" and "borrow one of
//	        its clients" into one step that Close must wait behind.
//
// After Close returns: connector is nil, no later write can install one, every
// borrowed client has been returned, and further requests get
// ErrGRPCConnectionClosed.
type GRPCDirectRPCConnection struct {
	nodeUrl common.NodeUrl

	// Connection pooling
	connector grpcConnectorInterface
	connMu    sync.RWMutex

	// connectorCtx bounds the pooled connector's life to THIS connection's, and
	// connectorCancel is what ends it — see newGRPCDirectRPCConnection for why it
	// cannot be the relay's context (MAG-2808).
	connectorCtx    context.Context
	connectorCancel context.CancelFunc

	// Descriptor handling
	descriptorsCache *common.SafeSyncMap[string, *desc.MethodDescriptor]

	// inflight holds the one running descriptor lookup per service, so N concurrent
	// relays for the same service share a single reflection round trip instead of
	// each opening their own. Set at construction like descriptorsCache; resolveMu
	// guards it and is a leaf lock — no other lock is taken while it is held.
	inflight  map[string]*descriptorResolution
	resolveMu sync.Mutex

	// The descriptor warm-up runs exactly once per connection, whichever path
	// triggers it: Prewarm during endpoint setup, or initialize() on a connection
	// that was never prewarmed. warmOnce is what makes it once; warmed is closed
	// when the sweep has finished, and is what Prewarm waits on — a second caller
	// must be able to WAIT for the sweep, which sync.Once alone cannot express
	// without also blocking uninterruptibly.
	warmOnce sync.Once
	warmed   chan struct{}

	// Initialization state. `initialized` latches on SUCCESS only: a failed
	// initialize is retried by the next relay rather than replayed from a cached
	// error, which would strand the connection for the life of the pairing.
	initialized atomic.Bool
	initMu      sync.Mutex

	// closed is set by Close and never cleared: a closed connection stays closed.
	// Without it, Close leaves `initialized` true with a nil connector, so
	// ensureInitialized short-circuits and SendRequest walks into the nil — and in
	// the not-yet-initialized case it would instead silently resurrect the
	// connection by building a fresh connector (MAG-2808).
	closed atomic.Bool

	// newConnector builds the pooled connector. Production leaves it nil and gets
	// newGRPCConnector; tests substitute a fake, or one that blocks mid-construction
	// to pin down the Close-during-initialization ordering. It is read under initMu
	// and must be set before the first request.
	newConnector grpcConnectorFactory
}

// newGRPCDirectRPCConnection builds a gRPC connection with its descriptor cache in
// place. The cache is a pointer with no lazy initialization, so every construction
// site has to set it or the first descriptor lookup dereferences nil.
//
// It also gives the connection its own context, which is the connector's lifetime
// (MAG-2808). chainproxy.NewGRPCConnector treats the context it is handed as
// exactly that: addClientsAsynchronouslyGrpc ends with `go connectorLoop(ctx)`,
// and connectorLoop is `<-ctx.Done(); connector.Close()`. Because the pool is
// built lazily inside the FIRST relay, passing that relay's context down bound
// the pool's life to one request — the connector was torn down the moment the
// relay that happened to initialize it returned.
//
// That used to be merely wasteful: Close was a pool drain, and the next GetRpc
// silently re-dialled, costing a reconnect per relay. Once Close became permanent
// it turned terminal — every subsequent checkout returns ErrGRPCConnectorClosed
// and the refill paths discard the dials that would have healed it, while
// endpoint.DirectConnections caches this object for the whole pairing. The
// context is deliberately rooted at Background rather than at any caller's:
// re-parenting it on a context this connection does not own is the bug.
func newGRPCDirectRPCConnection(nodeUrl common.NodeUrl) *GRPCDirectRPCConnection {
	connectorCtx, connectorCancel := context.WithCancel(context.Background())
	return &GRPCDirectRPCConnection{
		nodeUrl:          nodeUrl,
		descriptorsCache: &common.SafeSyncMap[string, *desc.MethodDescriptor]{},
		inflight:         map[string]*descriptorResolution{},
		warmed:           make(chan struct{}),
		connectorCtx:     connectorCtx,
		connectorCancel:  connectorCancel,
	}
}

// descriptorResolution is a single in-flight service-descriptor lookup, shared by
// every relay that asks for the same service while it runs.
//
// svc and err are written once, before done is closed, and read only after it —
// the close is the happens-before edge, so no lock is needed on them.
type descriptorResolution struct {
	done chan struct{}
	svc  *desc.ServiceDescriptor // nil unless err == nil
	err  error
}

// grpcConnectorFactory mirrors chainproxy.NewGRPCConnector, including its two
// contexts: lifetimeCtx owns the pool, dialCtx bounds only the initial blocking
// dial. It completes the seam grpcConnectorInterface already opens — without it,
// reaching initialize() in a test means dialing a real node.
type grpcConnectorFactory func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error)

// newGRPCConnector is the production factory. It returns an explicit nil interface
// on failure: returning the (*GRPCConnector)(nil) that NewGRPCConnector hands back
// alongside an error would produce a non-nil interface holding a nil pointer, and
// every downstream nil check would silently pass.
func newGRPCConnector(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
	connector, err := chainproxy.NewGRPCConnector(lifetimeCtx, dialCtx, nConns, nodeUrl)
	if err != nil {
		return nil, err
	}
	return connector, nil
}

// ErrGRPCConnectionClosed is returned when a request is attempted on a gRPC
// direct connection that has been closed. Callers should treat it as a normal
// relay failure and fail over, not as a fatal condition.
var ErrGRPCConnectionClosed = errors.New("gRPC connection is closed")

// DirectRPCPrewarmBudget bounds the readiness step that endpoint setup runs
// before publishing connections.
//
// It is a cap on how long setup WAITS, not on the work: a prewarm that overruns
// keeps going in the background and the endpoint is published regardless. That
// asymmetry is deliberate. Publishing an unwarmed endpoint costs at most one slow
// relay, which the detached-lookup half of MAG-2860 already makes recoverable,
// whereas letting an unreachable upstream hold up setup would make boot fatal on
// upstream health — which it must never be.
//
// Prewarms run concurrently, so this is the budget for the slowest endpoint, not
// the sum. It sits below the sweep's own 6x-reflection-timeout budget: setup
// waits for the common case and declines to wait out the pathological one.
const DirectRPCPrewarmBudget = 15 * time.Second

// PrewarmDirectConnections runs the pre-relay readiness step on every connection
// that has one, concurrently, and returns when they have all finished or the
// budget runs out.
//
// This is the boundary that makes "no relay is ever cold" true rather than
// merely likely (MAG-2860). Connections are constructed lazily — a gRPC one has
// made no network call at all when NewDirectRPCConnection returns — so without a
// step like this the first relay to an endpoint is the one that dials it and
// resolves its descriptors, and for cross-validation, which needs N successes in
// a single request, that first relay failing IS the reported bug. Call this
// before the endpoints holding these connections are published.
//
// It never returns an error and never withholds anything: a connection that
// fails or overruns is logged and left to the on-demand path.
func PrewarmDirectConnections(ctx context.Context, connections []DirectRPCConnection, budget time.Duration) {
	// Keep the connection next to its prewarmer rather than asserting back to it
	// later: the two views are of the same object, but only this loop has proof of
	// that.
	type target struct {
		connection DirectRPCConnection
		prewarmer  DirectRPCPrewarmer
	}
	targets := make([]target, 0, len(connections))
	for _, connection := range connections {
		if prewarmer, ok := connection.(DirectRPCPrewarmer); ok {
			targets = append(targets, target{connection: connection, prewarmer: prewarmer})
		}
	}
	if len(targets) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			if err := t.prewarmer.Prewarm(ctx); err != nil {
				// Warning, not error: an endpoint that could not be prewarmed is still
				// a usable endpoint, and saying otherwise at boot would read as a
				// failure the operator has to act on.
				utils.LavaFormatWarning("direct RPC connection prewarm did not complete", err,
					utils.LogAttr("url", t.connection.GetURL()))
			}
		}(t)
	}

	// Wait on a channel rather than wg.Wait() directly. Every Prewarm honours ctx,
	// so this should be redundant — but "should" is not a bound, and setup blocking
	// forever on a misbehaving transport is precisely the failure this budget exists
	// to rule out.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		utils.LavaFormatWarning("direct RPC prewarm budget elapsed, publishing endpoints anyway", ctx.Err(),
			utils.LogAttr("connections", len(targets)),
			utils.LogAttr("budget", budget))
	}
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
		return newGRPCDirectRPCConnection(nodeUrl), nil

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
		retryAfter, _ := common.ParseRetryAfter(resp.Header, time.Now())
		return response, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       body,
			RetryAfter: retryAfter,
		}
	}

	return response, nil
}

// HTTPStatusError represents an HTTP error response (4xx/5xx)
type HTTPStatusError struct {
	StatusCode int
	Status     string
	Body       []byte
	// RetryAfter is what a rate-limiting upstream asked for, or 0 when it sent no usable
	// Retry-After. Read it through common.RetryAfterFrom rather than the field, so a caller
	// works the same whichever path produced the error.
	RetryAfter time.Duration
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("HTTP %d %s", e.StatusCode, e.Status)
}

// Unwrap presents a 429 as the shared rate-limit error, so errors.Is(err,
// common.StatusCodeError429) and common.RetryAfterFrom read the same on the direct path as
// on the chainlib proxies. Any other status has nothing to unwrap to.
func (e *HTTPStatusError) Unwrap() error {
	if e.StatusCode != http.StatusTooManyRequests {
		return nil
	}
	return &common.RateLimitedError{RetryAfter: e.RetryAfter}
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

	// Set the default Content-Type for requests with a body BEFORE applying
	// per-request headers, so a spec-supplied content-type wins over this default
	// instead of being overwritten by it. Stellar's REST POST collection overrides
	// content-type to application/x-www-form-urlencoded (Horizon rejects anything
	// else on /transactions with 415); with the opposite ordering that override was
	// computed correctly and then discarded here. This matches the ordering the
	// chainlib REST proxy has always used.
	if params.Body != nil && params.ContentType != "" {
		req.Header.Set("Content-Type", params.ContentType)
	}

	// Apply per-request headers with delete semantics
	for _, header := range params.Headers {
		if header.Value == "" {
			req.Header.Del(header.Name) // Empty value = delete header
		} else {
			req.Header.Set(header.Name, header.Value)
		}
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

	reply, err := client.CallContext(ctx, wireID, reqMsg.Method, reqMsg.SendableParams(), w.isJsonRPC, false)
	if err != nil {
		return nil, err
	}

	// Restore the caller's id on a COPY of the reply. The rpcclient dispatch
	// goroutine may still read the returned reply concurrently, so mutating it
	// in place is a data race — we only read it (to copy) and write the id on
	// our own value.
	out := *reply
	out.ID = reqMsg.ID // caller's id (omitted/empty for notifications)

	respBytes, err := out.MarshalReply()
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
	maps.Copy(headers, w.nodeUrl.GetAuthHeaders())
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

	// Hold connMu in read mode across the whole checkout, not just the field read.
	// A bare `g.connector.GetRpc(...)` raced Close()'s write straight into a nil
	// dereference, which takes the whole process down and every chain on the pod
	// with it — but snapshotting the pointer alone is not enough either: it keeps
	// the pointer alive without keeping the pool alive, so Close could still drain
	// the connector in the gap before GetRpc reached it (MAG-2808).
	//
	// Keeping the read lock until GetRpc returns closes that gap. Close blocks on
	// the write lock until the checkout finishes, and by then GRPCConnector has
	// incremented usedClients, which makes its own Close wait for the ReturnRpc
	// below. The borrowed conn is therefore valid for the entire request. GetRpc
	// honours ctx, so this bounds Close by the caller's deadline rather than
	// indefinitely.
	g.connMu.RLock()
	connector := g.connector
	if connector == nil {
		g.connMu.RUnlock()
		return nil, ErrGRPCConnectionClosed
	}
	conn, err := connector.GetRpc(ctx, true)
	g.connMu.RUnlock()

	if err != nil {
		return nil, utils.LavaFormatError("gRPC get connection failed", err,
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	// Return the conn to the same connector that issued it. Go evaluates and saves
	// a deferred call's receiver when the defer statement runs, so this pins the
	// local — the hazard in `defer g.connector.ReturnRpc(conn)` was the second
	// unguarded read of the field here at registration time, not a re-read at exit.
	defer connector.ReturnRpc(conn)

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
	methodDescriptor, err := g.getMethodDescriptor(ctx, svc, methodName)
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
	// A closed connection must never lazily re-initialize itself. Close() does not
	// clear `initialized`, so a connection closed AFTER init short-circuits below
	// and is caught by the nil-connector check in SendRequest; but one closed
	// BEFORE its first request would otherwise build a fresh connector here and
	// silently come back to life (MAG-2808).
	if g.closed.Load() {
		return ErrGRPCConnectionClosed
	}

	if g.initialized.Load() {
		return nil
	}

	g.initMu.Lock()
	defer g.initMu.Unlock()

	// Re-check closed now that we hold initMu. The load above is only a fast path:
	// Close may have run in full while we waited here, and initializing behind it
	// would publish a connector nobody is left to tear down.
	if g.closed.Load() {
		return ErrGRPCConnectionClosed
	}

	// Double-check after acquiring lock
	if g.initialized.Load() {
		return nil
	}

	// This relay may have spent its whole budget queued behind another
	// initializer's dial. Starting a fresh one on a context that is already spent
	// would hold initMu for a full dial budget while every relay behind us waits,
	// so fail this one and leave the attempt to a relay that still has time. This
	// is what keeps the retry below from turning a dead endpoint into a serialized
	// dial storm.
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := g.initialize(ctx); err != nil {
		// Deliberately NOT latched. `initialized` used to be stored regardless of
		// outcome, with the error cached in an initErr field and replayed to every
		// later request — so a node that happened to be down when the FIRST relay
		// arrived poisoned the connection permanently, and endpoint.DirectConnections
		// caches this object until the pairing is rebuilt. Same "dead until the
		// pairing changes" shape as the connector-lifetime bug (MAG-2808).
		//
		// A failed dial is transient, so the next relay retries it. Every error path
		// in initialize() leaves no connector installed — the one that dials
		// successfully and then finds the connection closed tears its own connector
		// down — so retrying cannot leak a pool or publish twice. Taking a genuinely
		// dead endpoint out of rotation is consumer_session_manager's job (endpoint
		// health, connection refusals, probe re-enable), not a latch here.
		return err
	}
	g.initialized.Store(true)
	return nil
}

// initialize performs the actual initialization
func (g *GRPCDirectRPCConnection) initialize(ctx context.Context) error {
	// Validate gRPC URL
	if err := g.validateURL(); err != nil {
		return err
	}

	// Bound reflection-timeout before anything derives a context from it. It budgets
	// every descriptor lookup (lookupService) and, multiplied, the warm-up sweep, so
	// an out-of-bounds value is not cosmetic: a nanosecond fails every lookup on the
	// endpoint for as long as it is configured, and there is no later check to catch
	// it — common.GrpcConfig.Validate defines these bounds but has no caller on any
	// path that reaches here, and the static-provider validation only checks the URL.
	//
	// Only the timeout half of Validate is applied. The rest of it requires a
	// descriptor-set path for "hybrid", which DescriptorSourceForNode deliberately
	// tolerates by degrading to reflection, so running all of Validate here would
	// reject configurations that work today.
	if err := g.nodeUrl.GrpcConfig.ValidateReflectionTimeout(); err != nil {
		return err
	}

	// Resolve the descriptor source before dialing. It needs no connection — it
	// only reads config and the process-wide protoset cache — and failing here
	// keeps a misconfigured endpoint from spending a dial budget it can never use.
	if err := g.initializeDescriptorSource(); err != nil {
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

	newConnector := g.newConnector
	if newConnector == nil {
		newConnector = newGRPCConnector
	}
	// The two contexts are the whole point of MAG-2808's second half:
	//
	//	g.connectorCtx  owns the pool. The relay's context here is what tore the
	//	                connector down the moment that relay returned; Close cancels
	//	                this one instead, so a teardown racing this call aborts it
	//	                rather than waiting it out.
	//	ctx             is this relay's, and bounds only the blocking dial below.
	//	                Without it the caller waits out createConnection's own retry
	//	                budget instead of its own deadline — and because initMu is
	//	                held across this call, every relay queued behind waits too,
	//	                which delays failover for all of them.
	connector, err := newConnector(g.connectorCtx, ctx, 10, connectorNodeUrl)
	if err != nil {
		return utils.LavaFormatError("failed to create gRPC connector", err,
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	// Publish under connMu: this write is serialized against other initializers by
	// initMu, but Close() guards the same field with connMu only — so without this
	// the two had no lock in common and raced on g.connector outright.
	//
	// Constructing the connector can take a while (NewGRPCConnector dials), so
	// re-check closed while holding connMu before installing it. Close waits on
	// initMu and would tear this connector down anyway, but bailing here means a
	// connection closed mid-construction never sets up a descriptor source it will
	// not use, and never leaves a dialed pool alive for the length of that setup.
	g.connMu.Lock()
	if g.closed.Load() {
		g.connMu.Unlock()
		connector.Close()
		return ErrGRPCConnectionClosed
	}
	g.connector = connector
	g.connMu.Unlock()

	// descriptorsCache is set at construction, not here: re-assigning it would be
	// an unsynchronized pointer write to a field getMethodDescriptor and
	// GetCachedMethodDescriptor read without holding initMu.

	utils.LavaFormatInfo("gRPC direct connection initialized",
		utils.LogAttr("url", g.nodeUrl.Url),
		utils.LogAttr("tls", parsedURL.Scheme == "grpcs"))

	// Start the descriptor sweep behind this call. On the prewarmed path Prewarm has
	// already started it and is waiting on it; this is the fallback for a connection
	// that reached its first relay without one — the lazily-initialized paths, and
	// any endpoint whose prewarm ran out of budget at setup.
	//
	// Deliberately after the connector is published (the sweep borrows from that
	// pool), and deliberately not awaited: initMu is held across initialize(), so
	// waiting here would make every relay queued behind sit through a reflection
	// sweep. The boundary that actually removes the cold relay is Prewarm.
	g.startDescriptorWarmup()

	return nil
}

// Prewarm implements DirectRPCPrewarmer: it dials the endpoint and resolves its
// descriptors, so that by the time this connection is reachable by a relay the
// work a relay would otherwise trigger is already done.
//
// This is the boundary MAG-2860 turns on. Detaching descriptor lookups from the
// relay deadline stops a slow endpoint being permanently unusable, but it still
// leaves the FIRST request paying — and cross-validation needs N successes in one
// request, so a first request that fails is the reported bug, not a slow start.
// Calling this before the endpoint is published is what removes the cold relay
// rather than merely making it recoverable.
//
// Idempotent and concurrency-safe: the sweep runs once per connection whoever
// asks, and later callers wait for the running one rather than starting another.
// Bounded by ctx — an expired ctx abandons the WAIT, never the sweep, which
// carries on against its own budget and caches what it finds.
func (g *GRPCDirectRPCConnection) Prewarm(ctx context.Context) error {
	if err := g.ensureInitialized(ctx); err != nil {
		return err
	}

	select {
	case <-g.startDescriptorWarmup():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startDescriptorWarmup starts the one descriptor sweep this connection will ever
// run and returns the channel closed when it finishes.
//
// Once-ness is the point: Prewarm and initialize() both reach here, and a second
// sweep would re-resolve, on a node likely chosen for this treatment because its
// reflection is slow, everything the first one already cached.
func (g *GRPCDirectRPCConnection) startDescriptorWarmup() <-chan struct{} {
	g.warmOnce.Do(func() {
		go func() {
			defer close(g.warmed)
			g.warmDescriptorCache()
		}()
	})
	return g.warmed
}

// descriptorWarmupBudgetFactor scales grpc-config's per-lookup reflection-timeout
// into a budget for the warm-up, which resolves the node's whole service list
// rather than one symbol.
//
// It is a backstop, not a target: nobody waits on the warm, whatever it finishes
// is cached, and anything it misses still resolves on demand. What the bound is
// actually for is the pooled client the sweep holds — without it, one hung
// reflection stream keeps that client checked out until Close.
const descriptorWarmupBudgetFactor = 6

// warmDescriptorCache resolves every service the node serves. It runs at most
// once per connection — see startDescriptorWarmup — and is what Prewarm waits on.
//
// It stays transport-only: the node's own reflection lists what it serves, so
// nothing here has to know which chain it is or which methods a spec will call.
//
// Each service is claimed through the same registry the on-demand path uses, so
// the two never resolve the same service twice, and a relay that arrives mid-sweep
// waits on the sweep's claim for ITS service rather than on the sweep as a whole.
func (g *GRPCDirectRPCConnection) warmDescriptorCache() {
	// Nothing to warm in "file" mode: it never touches the network, and
	// initializeDescriptorSource has already loaded the protoset into the
	// process-wide cache that backs it. Hybrid still warms — reflection is the half
	// it falls back to, and that half is the slow one.
	if g.nodeUrl.GrpcConfig.GetDescriptorSource() == common.GrpcDescriptorSourceFile {
		return
	}

	ctx, cancel := context.WithTimeout(g.connectorCtx,
		descriptorWarmupBudgetFactor*g.nodeUrl.GrpcConfig.GetReflectionTimeout())
	defer cancel()

	g.connMu.RLock()
	connector := g.connector
	if connector == nil {
		g.connMu.RUnlock()
		return
	}
	conn, err := connector.GetRpc(ctx, true)
	g.connMu.RUnlock()
	if err != nil {
		utils.LavaFormatDebug("gRPC descriptor warm-up skipped, no connection",
			utils.LogAttr("error", err.Error()),
			utils.LogAttr("url", g.nodeUrl.Url))
		return
	}
	defer connector.ReturnRpc(conn)

	// One reflection client for the whole sweep. grpcreflect caches the file
	// descriptors it downloads per client, and services on a node share most of
	// their imports, so resolving them together costs roughly one descriptor set
	// rather than one per service — which matters on the gateways that throttle
	// reflection in the first place.
	cl := grpcreflect.NewClientAuto(ctx, conn)
	defer cl.Reset()

	services, err := cl.ListServices()
	if err != nil {
		// Not an error: a node that does not serve reflection is a supported
		// configuration ("file"/"hybrid" with a protoset), and one that is merely
		// slow or unreachable right now still resolves on demand later.
		utils.LavaFormatDebug("gRPC descriptor warm-up could not list services",
			utils.LogAttr("error", err.Error()),
			utils.LogAttr("url", g.nodeUrl.Url))
		return
	}

	descriptorSource, err := rpcInterfaceMessages.DescriptorSourceForGrpcConfig(&g.nodeUrl.GrpcConfig, rpcInterfaceMessages.DescriptorSourceFromServer(cl))
	if err != nil {
		return
	}

	started := time.Now()
	warmed, methods, skipped := 0, 0, 0
	for _, service := range services {
		if ctx.Err() != nil {
			break
		}
		// Reflection's own service is never relayed, so resolving it is pure cost.
		if strings.HasPrefix(service, "grpc.reflection.") {
			continue
		}

		// Claim the service through the same registry the on-demand path uses. A
		// relay that got here first already owns this one and is resolving it on its
		// own client; resolving it again would be exactly the duplicate reflection
		// this sweep exists to avoid, against a node whose reflection is the reason
		// we are sweeping at all. Skip it and move on — the sweep has other services
		// to reach, and waiting would only serialize the two.
		resolution, claimed := g.claimServiceResolution(service)
		if !claimed {
			skipped++
			continue
		}

		serviceDescriptor, err := resolveServiceFrom(descriptorSource, service)
		if err == nil {
			g.cacheServiceMethods(serviceDescriptor)
			warmed++
			methods += len(serviceDescriptor.GetMethods())
		} else {
			// One unresolvable service must not abandon the rest; it falls back to an
			// on-demand lookup like any other cache miss.
			utils.LavaFormatDebug("gRPC descriptor warm-up skipped a service",
				utils.LogAttr("service", service),
				utils.LogAttr("error", err.Error()),
				utils.LogAttr("url", g.nodeUrl.Url))
		}

		// Always publish: relays that joined this claim mid-sweep are waiting on it.
		g.publishServiceResolution(service, resolution, serviceDescriptor, err)
	}

	utils.LavaFormatInfo("gRPC descriptor cache warmed",
		utils.LogAttr("services", warmed),
		utils.LogAttr("methods", methods),
		utils.LogAttr("alreadyResolving", skipped),
		utils.LogAttr("duration", time.Since(started)),
		utils.LogAttr("url", g.nodeUrl.Url))
}

// resolveServiceFrom pulls one service descriptor out of an already-built
// descriptor source. It is the half of lookupService that does not own a
// connection, shared so the sweep and the on-demand path cannot drift on what
// counts as a resolved service.
func resolveServiceFrom(descriptorSource grpcurl.DescriptorSource, service string) (*desc.ServiceDescriptor, error) {
	descriptor, err := descriptorSource.FindSymbol(service)
	if err != nil {
		return nil, err
	}

	serviceDescriptor, ok := descriptor.(*desc.ServiceDescriptor)
	if !ok {
		return nil, utils.LavaFormatError("descriptor is not a ServiceDescriptor", nil,
			utils.LogAttr("service", service))
	}

	return serviceDescriptor, nil
}

// cacheServiceMethods stores every method of a resolved service.
//
// Every method, not only the one that was asked for: resolving the service
// resolved all of them, and the cache is keyed per method — so without this the
// second method on a service pays the full cold cost again.
func (g *GRPCDirectRPCConnection) cacheServiceMethods(serviceDescriptor *desc.ServiceDescriptor) {
	service := serviceDescriptor.GetFullyQualifiedName()
	for _, method := range serviceDescriptor.GetMethods() {
		g.descriptorsCache.Store(service+"."+method.GetName(), method)
	}
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

// initializeDescriptorSource loads and validates the configured descriptor source
// at connect time, so a bad descriptor-set-path surfaces here rather than at the
// first relay.
//
// It deliberately goes through the same loader the request path uses. The previous
// version validated with dyncodec's parser and then threw the result away, while
// resolution went through reflection regardless — so the check proved nothing about
// what would actually be consumed, and in "file" mode nothing was consumed at all
// (MAG-2350). Loading here now also warms the per-path cache, making the first
// relay's descriptor lookup free.
//
// A failure is fatal to initialize(), not advisory. It used to be logged as "will
// use reflection", which was true only while every path resolved through reflection
// anyway: in "file" mode getMethodDescriptor and parseInputMessage now go through
// this same loader and return this same error, and LoadProtoset caches failures, so
// there is no fallback and nothing recovers. Warning and continuing buys one line at
// boot followed by total relay failure on the endpoint — the silent-misconfiguration
// shape MAG-2350 exists to remove. It cannot fire for the reflection default:
// DescriptorSourceForGrpcConfig only errors for "file" and for an unrecognised
// descriptor-source, both of which are operator config.
//
// The load is not retained on the struct: the cache is keyed by path and shared
// process-wide, so re-resolving per request is a map hit.
func (g *GRPCDirectRPCConnection) initializeDescriptorSource() error {
	grpcConfig := &g.nodeUrl.GrpcConfig

	// A nil server source is safe to pass: reflection-mode returns it untouched and
	// no descriptor lookup happens here. Only the file half is being exercised.
	if _, err := rpcInterfaceMessages.DescriptorSourceForGrpcConfig(grpcConfig, nil); err != nil {
		return err
	}

	return nil
}

// getMethodDescriptor retrieves the method descriptor from cache, or waits on a
// lookup against the node's configured descriptor source (reflection, file, or
// hybrid).
//
// The WAIT is bounded by the caller's ctx. The LOOKUP is not (MAG-2860): it runs
// on its own budget and caches what it finds even if this relay has already given
// up, so the cost of a cold cache is paid once for the endpoint rather than once
// per attempt. See resolveService.
func (g *GRPCDirectRPCConnection) getMethodDescriptor(
	ctx context.Context,
	service, methodName string,
) (*desc.MethodDescriptor, error) {
	fullMethodName := service + "." + methodName

	// Check cache first
	if methodDesc, found, _ := g.descriptorsCache.Load(fullMethodName); found {
		return methodDesc, nil
	}

	resolution := g.resolveService(service)

	var finished bool
	select {
	case <-resolution.done:
		finished = true
	case <-ctx.Done():
		// Both cases can be ready at once, and select picks among ready cases at
		// RANDOM — so landing here proves nothing about the lookup. Ask again before
		// deciding anything, or roughly half of the races report whichever outcome
		// the runtime happened to choose.
		select {
		case <-resolution.done:
			finished = true
		default:
		}
	}

	// A lookup that SUCCEEDED is used whatever the caller's context now says. The
	// descriptor is in hand and costs nothing to return; discarding it would fail a
	// relay we can still serve, and the cancellation will be noticed by the caller
	// on its own terms anyway.
	if finished && resolution.err == nil {
		methodDescriptor := resolution.svc.FindMethodByName(methodName)
		if methodDescriptor == nil {
			return nil, utils.LavaFormatError("method not found in service", nil,
				utils.LogAttr("service", service),
				utils.LogAttr("method", methodName))
		}
		return methodDescriptor, nil
	}

	// Some other resolution — the warm-up sweep, or another relay's lookup — may
	// have cached this method while we waited. A descriptor is a descriptor.
	if methodDesc, found, _ := g.descriptorsCache.Load(fullMethodName); found {
		return methodDesc, nil
	}

	// From here the lookup has given us nothing, so the question is only which
	// error to report. The caller's own context wins over the lookup's error
	// whenever both are available.
	//
	// This is not cosmetic. common.IsClientCancellation matches on the
	// context.Canceled SENTINEL, and a relay the router itself abandoned — a loser
	// of a stateful fan-out race, or a client that hung up — must not be scored
	// against the endpoint. Reporting the lookup's generic failure instead drops
	// that sentinel and hands endpoint health a node error that never happened.
	// The risk is asymmetric in the same way handleGRPCError's cancellation branch
	// is: over-reporting cancellation only skips scoring a relay whose result we
	// were discarding, while under-reporting it penalises an endpoint that did
	// nothing wrong.
	//
	// Passing ctx.Err() through rather than flattening it into a message is what
	// keeps the sentinel reachable by errors.Is.
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The lookup itself is NOT abandoned here — it runs on its own budget and
		// caches what it finds, so the next relay to this endpoint is not cold. That
		// is what stops one slow endpoint being permanently unusable (MAG-2860).
		return nil, utils.LavaFormatWarning("gRPC descriptor lookup outlived the relay, continuing in background", ctxErr,
			utils.LogAttr("service", service),
			utils.LogAttr("method", methodName),
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	// Unreachable unless the lookup finished: the select above only returns on one
	// of the two channels, and a live ctx means it was resolution.done.
	return nil, resolution.err
}

// resolveService starts — or joins — the one running lookup of `service` on this
// endpoint, and returns the handle to wait on.
//
// Deduplicating matters here for the same reason detaching does: a cold service is
// cold for every relay at once, and letting each of them open its own reflection
// stream turns one slow lookup into N concurrent ones against a node that is
// already too slow to answer inside a relay timeout.
func (g *GRPCDirectRPCConnection) resolveService(service string) *descriptorResolution {
	resolution, claimed := g.claimServiceResolution(service)
	if claimed {
		go g.runServiceResolution(service, resolution)
	}
	return resolution
}

// claimServiceResolution registers the caller as the one resolving `service`, or
// hands back the resolution already running for it.
//
// The claim is what keeps the warm-up sweep and the on-demand path from resolving
// the same service twice: whichever reaches a service first owns it, and the
// other either waits on it (a relay) or skips it (the sweep, which has other
// services to get on with).
func (g *GRPCDirectRPCConnection) claimServiceResolution(service string) (resolution *descriptorResolution, claimed bool) {
	g.resolveMu.Lock()
	defer g.resolveMu.Unlock()

	if existing, ok := g.inflight[service]; ok {
		return existing, false
	}

	resolution = &descriptorResolution{done: make(chan struct{})}
	g.inflight[service] = resolution
	return resolution, true
}

// publishServiceResolution ends a claimed resolution, waking every waiter.
//
// It must be reached on every path out of a claimed lookup, including failures:
// a claim that is never published is a resolution every future waiter blocks on
// until its own context expires.
func (g *GRPCDirectRPCConnection) publishServiceResolution(
	service string,
	resolution *descriptorResolution,
	serviceDescriptor *desc.ServiceDescriptor,
	err error,
) {
	resolution.svc, resolution.err = serviceDescriptor, err

	// Drop the entry BEFORE publishing, so a relay that arrives after a failed
	// lookup starts a fresh one instead of joining a finished one and being handed
	// its stale error. Waiters already holding this resolution still get the real
	// answer. The cost is a short window in which a second lookup can start while
	// this one is finishing; the cache check in getMethodDescriptor makes that a
	// no-op as soon as one of them succeeds.
	g.resolveMu.Lock()
	if g.inflight[service] == resolution {
		delete(g.inflight, service)
	}
	g.resolveMu.Unlock()

	close(resolution.done)
}

// runServiceResolution performs one claimed service lookup and publishes it.
func (g *GRPCDirectRPCConnection) runServiceResolution(service string, resolution *descriptorResolution) {
	started := time.Now()

	serviceDescriptor, err := g.lookupService(service)
	if err == nil {
		// Cache before publishing: a waiter woken by the close below re-checks the
		// cache, and finding it already filled is one less way to race.
		g.cacheServiceMethods(serviceDescriptor)

		utils.LavaFormatDebug("gRPC service descriptor resolved",
			utils.LogAttr("service", service),
			utils.LogAttr("methods", len(serviceDescriptor.GetMethods())),
			utils.LogAttr("duration", time.Since(started)),
			utils.LogAttr("url", g.nodeUrl.Url))
	}

	g.publishServiceResolution(service, resolution, serviceDescriptor, err)
}

// lookupService resolves one service descriptor on a budget of its own.
//
// The context is rooted at connectorCtx, never at a relay's. A relay's deadline is
// how long the CALLER waits, not how long the lookup may take; bounding the lookup
// by it meant a node whose reflection is slower than one relay could never finish
// one, and so could never cache one. Rooting it at the connection's lifetime keeps
// Close prompt all the same — the cancellation in Close step 2 aborts this and
// releases the pooled client below before the drain in step 5 waits on it.
//
// The budget itself is grpc-config's reflection-timeout (default 5s, validated to
// 100ms..30s). That knob was defined and validated but never applied to anything;
// this is the call it was written for.
func (g *GRPCDirectRPCConnection) lookupService(service string) (*desc.ServiceDescriptor, error) {
	ctx, cancel := context.WithTimeout(g.connectorCtx, g.nodeUrl.GrpcConfig.GetReflectionTimeout())
	defer cancel()

	// Borrow a client of our own rather than reusing the one the triggering relay
	// holds: that relay hands its client back the moment it gives up on us, and a
	// handed-back client can be closed outright (ReturnRpc closes it when the pool
	// is over-full or being torn down). Borrowing keeps the connector's accounting
	// true for exactly as long as we use it.
	g.connMu.RLock()
	connector := g.connector
	if connector == nil {
		g.connMu.RUnlock()
		return nil, ErrGRPCConnectionClosed
	}
	conn, err := connector.GetRpc(ctx, true)
	g.connMu.RUnlock()
	if err != nil {
		return nil, utils.LavaFormatError("gRPC get connection failed for descriptor lookup", err,
			utils.LogAttr("service", service),
			utils.LogAttr("url", g.nodeUrl.Url))
	}
	defer connector.ReturnRpc(conn)

	// Resolve through the node's configured descriptor source. In "reflection" mode
	// (the default) this is exactly the reflection client below; in "file" mode the
	// client is built but never queried.
	cl := grpcreflect.NewClientAuto(ctx, conn)
	defer cl.Reset()

	descriptorSource, err := rpcInterfaceMessages.DescriptorSourceForGrpcConfig(&g.nodeUrl.GrpcConfig, rpcInterfaceMessages.DescriptorSourceFromServer(cl))
	if err != nil {
		return nil, err
	}

	serviceDescriptor, err := resolveServiceFrom(descriptorSource, service)
	if err != nil {
		return nil, utils.LavaFormatError("failed to find service descriptor", err,
			utils.LogAttr("service", service),
			utils.LogAttr("descriptor-source", g.nodeUrl.GrpcConfig.GetDescriptorSource()),
			utils.LogAttr("reflection-timeout", g.nodeUrl.GrpcConfig.GetReflectionTimeout()))
	}

	return serviceDescriptor, nil
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
		// JSON input - use grpcurl parser. The parser resolves any message types the
		// request references, so it needs the same descriptor source as the method
		// lookup did — reflection-only here would defeat "file" mode (MAG-2350).
		cl := grpcreflect.NewClientAuto(ctx, conn)
		defer cl.Reset()
		descriptorSource, err := rpcInterfaceMessages.DescriptorSourceForGrpcConfig(&g.nodeUrl.GrpcConfig, rpcInterfaceMessages.DescriptorSourceFromServer(cl))
		if err != nil {
			return err
		}

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
	// A relay the router itself cancelled — a loser of a stateful fan-out race,
	// or a client that disconnected — is not a node failure and must not be
	// scored as one. grpc-go reports this as status.Error(codes.Canceled, ...),
	// which does NOT wrap context.Canceled, so re-attach the sentinel that
	// common.IsClientCancellation looks for.
	//
	// Returning a NIL response is the load-bearing half: sendGRPCRelay only takes
	// its error-with-response arm when response != nil, so a nil response is what
	// routes cancellation through classifyAndWrap like every other transport
	// instead of recording it as a successful relay with fabricated latency
	// (MAG-2687).
	// Match on context.Canceled specifically rather than `ctx.Err() != nil`: a ctx
	// whose DEADLINE expired is also non-nil, and relabelling a timeout as a client
	// cancellation would let a genuinely slow endpoint escape being blamed.
	//
	// The LOCAL context is the whole test. Pairing it with `status.Code(err) ==
	// codes.Canceled` was a hole of the same class this branch exists to close: when
	// the router cancels and grpc-go surfaces something other than Canceled — most
	// often Unavailable from a connection torn down in the same instant — the
	// conjunct fails, sendGRPCRelay takes its node-error arm and returns (result,
	// nil), and the `err != nil` guard at the relay-race carve-out never fires. A
	// relay we abandoned is then scored against a healthy endpoint. The risk is
	// asymmetric: missing a local cancel penalises an endpoint that did nothing
	// wrong, while over-detecting one only skips scoring for a relay whose result we
	// were going to discard anyway.
	//
	// Both errors are wrapped. context.Canceled is the sentinel
	// common.IsClientCancellation resolves; err keeps the originating gRPC status so
	// logs can still say what the endpoint was doing when we pulled the plug.
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, fmt.Errorf("gRPC relay cancelled: %w: %w", context.Canceled, err)
	}

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

	// A corroborated rate limit — a known rate-limit text, or RESOURCE_EXHAUSTED with a
	// retry delay in the metadata — types the error, so errors.Is(err,
	// common.StatusCodeError429) and common.RetryAfterFrom read the same on gRPC as on
	// every HTTP transport (see GRPCStatusError.Unwrap).
	retryAfter, rateLimited := common.RateLimitFromGRPC(errorCode, errorMessage, respHeaders)

	// Return the error response with metadata
	// The caller can inspect the response to determine if it's an error
	return &DirectRPCResponse{
		Data:       respBytes,
		Metadata:   respHeaders, // Include any headers received before the error
		StatusCode: int(errorCode),
	}, &GRPCStatusError{
		Code:        errorCode,
		Message:     errorMessage,
		cause:       err,
		rateLimited: rateLimited,
		retryAfter:  retryAfter,
	}
}

// GRPCStatusError represents a gRPC status error.
//
// cause preserves the originating error. Without it this type was a dead end for
// errors.Is / errors.As: every sentinel carried by the underlying grpc-go error
// was lost the moment we rebuilt the error from just a code and a message, which
// is what made gRPC cancellations invisible to the relay-race carve-out
// (MAG-2687).
type GRPCStatusError struct {
	Code    uint32
	Message string
	cause   error
	// rateLimited + retryAfter are set by handleGRPCError when the failure is a
	// corroborated upstream rate refusal (common.RateLimitFromGRPC). They scope Unwrap
	// so the typed rate-limit error is visible on the gRPC path too.
	rateLimited bool
	retryAfter  time.Duration
}

func (e *GRPCStatusError) Error() string {
	return fmt.Sprintf("gRPC error %d: %s", e.Code, e.Message)
}

// Unwrap exposes the originating error so errors.Is / errors.As see through this
// wrapper. Note this makes status.Code(err) resolve to the underlying gRPC code
// rather than Unknown; SessionOutOfSyncGRPCCode is 677 and so cannot collide with
// any real code, which grpc_status_error_test.go pins.
//
// A corroborated rate limit unwraps to common.RateLimitedError instead, mirroring
// HTTPStatusError: rate-limit is the terminal classification, and the typed sentinel is
// what every consumer keys on.
func (e *GRPCStatusError) Unwrap() error {
	if e.rateLimited {
		return &common.RateLimitedError{RetryAfter: e.retryAfter}
	}
	return e.cause
}

func (g *GRPCDirectRPCConnection) GetProtocol() DirectRPCProtocol {
	return DirectRPCProtocolGRPC
}

func (g *GRPCDirectRPCConnection) Close() error {
	// 1. Publish the intent first, so ensureInitialized turns away every request
	//    that has not yet started initializing.
	g.closed.Store(true)

	// 2. End the connector's lifetime. This is what closing the connection means
	//    to the pool: connectorLoop is parked on this context and closes the
	//    connector when it ends, which also releases that goroutine. Doing it
	//    before taking initMu keeps teardown prompt — an initializer already
	//    dialing with grpc.WithBlock() aborts on the cancellation instead of making
	//    step 3 wait out a full dial. Cancel is idempotent, so repeated Close calls
	//    are fine.
	g.connectorCancel()

	// 3. Wait out any initialization already in flight. Setting closed does not
	//    stop an initializer that is already past its own check. Taking initMu
	//    means that once Close returns, no initialization is still running: the
	//    initializer either bailed at its closed re-check under connMu, tearing its
	//    own fresh connector down, or published one that we tear down below.
	g.initMu.Lock()
	defer g.initMu.Unlock()

	// 4. Detach under connMu. The write lock waits for every in-flight pool
	//    checkout to finish, so once it is acquired no GetRpc can be running, and
	//    once the field is nil none can start.
	g.connMu.Lock()
	connector := g.connector
	g.connector = nil
	g.connMu.Unlock()

	// 5. Drain outside connMu. GRPCConnector.Close blocks until every borrowed
	//    client has been handed back, which can take as long as the requests
	//    holding them; holding connMu across that would stall new requests instead
	//    of letting them fail fast with ErrGRPCConnectionClosed. Closing here
	//    rather than leaving it to connectorLoop makes the drain synchronous with
	//    this call; the loop's own Close then finds nothing left to do.
	if connector != nil {
		connector.Close()
	}

	return nil
}

func (g *GRPCDirectRPCConnection) GetURL() string {
	return g.nodeUrl.Url
}

func (g *GRPCDirectRPCConnection) GetNodeUrl() *common.NodeUrl {
	return &g.nodeUrl
}
