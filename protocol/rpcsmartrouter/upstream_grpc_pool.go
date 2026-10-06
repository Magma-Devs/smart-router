package rpcsmartrouter

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// grpcChannelLiveness is the part of GRPCStreamingConfig every pooled channel dials with.
// A flow that a NAT or load balancer drops without a reset still looks open to the channel,
// so the idle timeout closes an unused transport before a middlebox can forget it, and
// keepalive pings find a transport that died under live streams. 0 disables each.
type grpcChannelLiveness struct {
	idleTimeout      time.Duration
	keepaliveTime    time.Duration
	keepaliveTimeout time.Duration
}

func newGRPCChannelLiveness(config *GRPCStreamingConfig) grpcChannelLiveness {
	return grpcChannelLiveness{
		idleTimeout:      config.PoolIdleTimeout,
		keepaliveTime:    config.PoolKeepaliveTime,
		keepaliveTimeout: config.PoolKeepaliveTimeout,
	}
}

// keepaliveParams returns the client keepalive, or false when either value is 0.
// PermitWithoutStream stays false because gRPC servers penalise pings on connections without
// streams. A grpc-go server by default also GOAWAYs (too_many_pings) a client that pings more
// often than every 5m while the server writes nothing, and closes the connection with every
// stream on it, so the default interval is 5m. grpc-go doubles a channel's interval after such
// a GOAWAY, but only once its streams have already failed.
func (l grpcChannelLiveness) keepaliveParams() (keepalive.ClientParameters, bool) {
	if l.keepaliveTime <= 0 || l.keepaliveTimeout <= 0 {
		return keepalive.ClientParameters{}, false
	}
	return keepalive.ClientParameters{
		Time:                l.keepaliveTime,
		Timeout:             l.keepaliveTimeout,
		PermitWithoutStream: false,
	}, true
}

// dialOptions always sets the idle timeout, so 0 turns idleness off rather than leaving
// grpc-go's 30-minute default in place; a negative one is clamped to 0, as grpc-go would fire
// it at once. Keepalive is left out when disabled, because grpc-go raises a zero ping
// interval to its 10s minimum instead of treating it as off.
func (l grpcChannelLiveness) dialOptions() []grpc.DialOption {
	opts := []grpc.DialOption{grpc.WithIdleTimeout(max(l.idleTimeout, 0))}
	if params, ok := l.keepaliveParams(); ok {
		opts = append(opts, grpc.WithKeepaliveParams(params))
	}
	return opts
}

// UpstreamGRPCStreamConnection wraps a grpc.ClientConn with health tracking and stream count.
// Unlike the unary GRPCConnector, this is designed for long-lived streaming connections.
type UpstreamGRPCStreamConnection struct {
	conn             *grpc.ClientConn
	endpoint         string
	sanitizedURL     string // For logging (no auth info)
	nodeUrl          *common.NodeUrl
	liveness         grpcChannelLiveness
	healthy          atomic.Bool
	lastError        atomic.Value // stores error
	createdAt        time.Time
	activeStreams    atomic.Int32                                        // Number of active streams on this connection
	descriptorsCache *common.SafeSyncMap[string, *desc.MethodDescriptor] // Cached method descriptors
	lock             sync.RWMutex
	closed           atomic.Bool
}

// NewUpstreamGRPCStreamConnection creates a new upstream gRPC connection for streaming
func NewUpstreamGRPCStreamConnection(ctx context.Context, nodeUrl *common.NodeUrl, timeout time.Duration, liveness grpcChannelLiveness) (*UpstreamGRPCStreamConnection, error) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:         nodeUrl.Url,
		sanitizedURL:     sanitizeEndpointURL(nodeUrl.Url),
		nodeUrl:          nodeUrl,
		liveness:         liveness,
		createdAt:        time.Now(),
		descriptorsCache: &common.SafeSyncMap[string, *desc.MethodDescriptor]{},
	}
	conn.healthy.Store(true)

	if err := conn.connect(ctx, timeout); err != nil {
		return nil, err
	}

	return conn, nil
}

// connect establishes the gRPC connection
func (c *UpstreamGRPCStreamConnection) connect(ctx context.Context, timeout time.Duration) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.closed.Load() {
		return fmt.Errorf("connection is closed")
	}

	// Parse URL to extract host.
	// Bare "host:port" (no "://") must be prefixed with "//" so url.Parse
	// treats it as authority rather than "scheme:opaque" (Go's default for
	// strings without "://", which leaves parsedURL.Host empty).
	rawURL := c.endpoint
	if !strings.Contains(rawURL, "://") {
		rawURL = "//" + rawURL
	}
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("failed to parse gRPC URL %s: %w", c.sanitizedURL, err)
	}

	// Determine target address (host:port)
	target := parsedURL.Host
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		target += parsedURL.Path
	}

	// Determine TLS configuration
	var dialOpts []grpc.DialOption
	scheme := strings.ToLower(parsedURL.Scheme)

	transportIsSecure := scheme == "grpcs" || c.nodeUrl.AuthConfig.UseTLS
	if transportIsSecure {
		// Use TLS
		tlsConfig := &tls.Config{}
		if c.nodeUrl.AuthConfig.AllowInsecure {
			tlsConfig.InsecureSkipVerify = true
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		// Insecure connection (for local/dev)
		if !c.nodeUrl.GrpcConfig.AllowInsecure {
			return fmt.Errorf("insecure gRPC (grpc://) requires allow-insecure: true in config")
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Large responses. The shared constant rather than a repeated literal: this pool and
	// chainproxy.GRPCConnector dial the same upstreams, so a divergence between them would
	// mean the same endpoint accepted a response on one path and rejected it on the other.
	dialOpts = append(dialOpts,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(chainproxy.MaxCallRecvMsgSize)),
	)

	// MAG-2218: attach the endpoint's configured auth-headers to the subscription
	// manager's upstream streams, which this pool backs.
	c.nodeUrl.TokenOverInsecureWarning(transportIsSecure)
	dialOpts = append(dialOpts, c.nodeUrl.GrpcAuthDialOptions()...)

	// MAG-3887: idle timeout and keepalive, so a flow a middlebox dropped is neither
	// reused after an idle spell nor left silently under live streams.
	dialOpts = append(dialOpts, c.liveness.dialOptions()...)

	// grpc.NewClient is lazy — it does not establish a connection here.
	// We force a state transition under the configured timeout so the
	// caller still gets a fast failure for unreachable endpoints, the way
	// the previous grpc.DialContext + WithBlock behaved.
	grpcConn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		c.healthy.Store(false)
		c.lastError.Store(err)
		return fmt.Errorf("failed to construct gRPC client for %s: %w", c.sanitizedURL, err)
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	grpcConn.Connect()
	// WaitForStateChange returns on ANY state transition (including
	// Connecting → TransientFailure), so a single call would accept a
	// channel that's failing. Loop until we either reach Ready or the
	// timeout fires — matching WithBlock semantics.
	for {
		state := grpcConn.GetState()
		if state == connectivity.Ready {
			break
		}
		if state == connectivity.Shutdown {
			_ = grpcConn.Close()
			err := fmt.Errorf("gRPC channel entered Shutdown before Ready")
			c.healthy.Store(false)
			c.lastError.Store(err)
			return fmt.Errorf("failed to dial gRPC %s: %w", c.sanitizedURL, err)
		}
		if !grpcConn.WaitForStateChange(connectCtx, state) {
			_ = grpcConn.Close()
			err := connectCtx.Err()
			c.healthy.Store(false)
			c.lastError.Store(err)
			return fmt.Errorf("failed to dial gRPC %s: %w", c.sanitizedURL, err)
		}
	}

	c.conn = grpcConn
	c.healthy.Store(true)

	utils.LavaFormatDebug("gRPC streaming connection established",
		utils.LogAttr("endpoint", c.sanitizedURL),
	)

	return nil
}

// GetConn returns the underlying grpc.ClientConn for stream operations
func (c *UpstreamGRPCStreamConnection) GetConn() *grpc.ClientConn {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.conn
}

// IsHealthy returns true if the connection is healthy
func (c *UpstreamGRPCStreamConnection) IsHealthy() bool {
	return c.healthy.Load() && !c.closed.Load()
}

// MarkUnhealthy marks the connection as unhealthy
func (c *UpstreamGRPCStreamConnection) MarkUnhealthy(err error) {
	c.healthy.Store(false)
	if err != nil {
		c.lastError.Store(err)
	}
}

// Close closes the gRPC connection
func (c *UpstreamGRPCStreamConnection) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		utils.LavaFormatDebug("gRPC streaming connection closed",
			utils.LogAttr("endpoint", c.sanitizedURL),
		)
		return err
	}

	return nil
}

// GetEndpoint returns the endpoint URL (sanitized for logging)
func (c *UpstreamGRPCStreamConnection) GetEndpoint() string {
	return c.sanitizedURL
}

// GetNodeUrl returns the full nodeUrl configuration
func (c *UpstreamGRPCStreamConnection) GetNodeUrl() *common.NodeUrl {
	return c.nodeUrl
}

// IncrementStreams increments the active stream count and returns the new count
func (c *UpstreamGRPCStreamConnection) IncrementStreams() int32 {
	return c.activeStreams.Add(1)
}

// DecrementStreams decrements the active stream count and returns the new count
func (c *UpstreamGRPCStreamConnection) DecrementStreams() int32 {
	return c.activeStreams.Add(-1)
}

// StreamCount returns the current number of active streams on this connection
func (c *UpstreamGRPCStreamConnection) StreamCount() int32 {
	return c.activeStreams.Load()
}

// GetMethodDescriptor retrieves a method descriptor from cache, or resolves it
// through the node's configured descriptor source (reflection, file, or hybrid).
func (c *UpstreamGRPCStreamConnection) GetMethodDescriptor(
	ctx context.Context,
	service, methodName string,
) (*desc.MethodDescriptor, error) {
	fullMethodName := service + "." + methodName

	// Check cache first
	if methodDesc, found, _ := c.descriptorsCache.Load(fullMethodName); found {
		return methodDesc, nil
	}

	c.lock.RLock()
	conn := c.conn
	c.lock.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("connection not available")
	}

	// Resolve through the node's configured descriptor source. Streaming methods are
	// exactly the ones a partial reflection service tends to omit, so this path needs
	// the file escape hatch as much as the unary one does (MAG-2350).
	cl := grpcreflect.NewClientAuto(ctx, conn)
	defer cl.Reset()

	descriptorSource, err := rpcInterfaceMessages.DescriptorSourceForGrpcConfig(&c.nodeUrl.GrpcConfig, rpcInterfaceMessages.DescriptorSourceFromServer(cl))
	if err != nil {
		return nil, err
	}

	descriptor, err := descriptorSource.FindSymbol(service)
	if err != nil {
		return nil, utils.LavaFormatError("failed to find service descriptor", err,
			utils.LogAttr("service", service),
			utils.LogAttr("descriptor-source", c.nodeUrl.GrpcConfig.GetDescriptorSource()))
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
	c.descriptorsCache.Store(fullMethodName, methodDescriptor)

	return methodDescriptor, nil
}

// UpstreamGRPCPool manages a pool of gRPC connections for streaming.
// The pool automatically scales between minConnections and maxConnections based on
// stream load, targeting approximately streamsPerConn streams per connection.
type UpstreamGRPCPool struct {
	nodeUrl      *common.NodeUrl
	sanitizedURL string
	connections  []*UpstreamGRPCStreamConnection
	backoff      *ExponentialBackoff
	reconnectMu  sync.Mutex // Prevents concurrent reconnection attempts
	lock         sync.RWMutex
	closed       atomic.Bool
	reconnecting atomic.Bool
	// reconnectOutcome holds the result of the last ReconnectWithBackoff attempt that dialed,
	// as a reconnectResult, so a caller that waited for it learns how it went.
	reconnectOutcome atomic.Value
	onReconnect      func() // Callback when reconnected (for stream restoration)

	// Pool configuration
	minConnections int                 // Minimum connections to maintain (default: 1)
	maxConnections int                 // Maximum connections allowed (default: 5)
	streamsPerConn int                 // Target streams per connection (default: 100)
	connectTimeout time.Duration       // Connection establishment timeout
	liveness       grpcChannelLiveness // Idle timeout and keepalive every connection dials with
}

// NewUpstreamGRPCPool creates a new gRPC connection pool for streaming
func NewUpstreamGRPCPool(nodeUrl *common.NodeUrl) *UpstreamGRPCPool {
	return &UpstreamGRPCPool{
		nodeUrl:        nodeUrl,
		sanitizedURL:   sanitizeEndpointURL(nodeUrl.Url),
		connections:    make([]*UpstreamGRPCStreamConnection, 0),
		backoff:        NewWebSocketBackoff(), // Reuse the same backoff logic
		minConnections: 1,
		maxConnections: 5,
		streamsPerConn: 100,
		connectTimeout: 30 * time.Second,
		liveness:       newGRPCChannelLiveness(DefaultGRPCStreamingConfig()),
	}
}

// NewUpstreamGRPCPoolWithConfig creates a pool with custom configuration
func NewUpstreamGRPCPoolWithConfig(nodeUrl *common.NodeUrl, config *GRPCStreamingConfig) *UpstreamGRPCPool {
	pool := NewUpstreamGRPCPool(nodeUrl)
	if config != nil {
		pool.minConnections = config.PoolMinConnections
		pool.maxConnections = config.PoolMaxConnections
		pool.streamsPerConn = config.StreamsPerConnection
		pool.connectTimeout = config.ConnectionTimeout
		pool.liveness = newGRPCChannelLiveness(config)
	}
	return pool
}

// SetReconnectCallback sets a callback to be called after successful reconnection
// This is used to restore streams after reconnection
func (p *UpstreamGRPCPool) SetReconnectCallback(callback func()) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.onReconnect = callback
}

// GetConnectionForStream returns the best connection for a new stream.
// It prefers connections with lower stream counts and will create new connections
// if all existing ones are near capacity (and we haven't hit maxConnections).
func (p *UpstreamGRPCPool) GetConnectionForStream(ctx context.Context) (*UpstreamGRPCStreamConnection, error) {
	return p.connectionForStream(ctx, false)
}

// ReserveConnectionForStream is GetConnectionForStream that also counts the caller's stream on
// the connection it returns, under the pool lock. Counted any later, the connection can look
// unused in the meantime, and maybeScaleDown closes a connection past minConnections that
// carries no counted stream: the stream opened on it then fails at once. The caller releases
// the slot with NotifyStreamRemoved, including when opening the stream fails.
func (p *UpstreamGRPCPool) ReserveConnectionForStream(ctx context.Context) (*UpstreamGRPCStreamConnection, error) {
	return p.connectionForStream(ctx, true)
}

func (p *UpstreamGRPCPool) connectionForStream(ctx context.Context, reserve bool) (*UpstreamGRPCStreamConnection, error) {
	if p.closed.Load() {
		return nil, fmt.Errorf("pool is closed")
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	conn, err := p.pickConnectionLocked(ctx)
	if err == nil && reserve {
		conn.IncrementStreams()
	}
	return conn, err
}

// pickConnectionLocked chooses or creates the connection for a new stream (caller must hold lock).
func (p *UpstreamGRPCPool) pickConnectionLocked(ctx context.Context) (*UpstreamGRPCStreamConnection, error) {
	// Find the best connection (healthy with lowest stream count)
	var bestConn *UpstreamGRPCStreamConnection
	var lowestStreams int32 = int32(p.streamsPerConn + 1)

	for _, conn := range p.connections {
		if conn.IsHealthy() {
			streams := conn.StreamCount()
			if streams < lowestStreams {
				lowestStreams = streams
				bestConn = conn
			}
		}
	}

	// If we have a connection with capacity, use it
	if bestConn != nil && lowestStreams < int32(p.streamsPerConn) {
		return bestConn, nil
	}

	// Need to scale up if possible
	if len(p.connections) < p.maxConnections {
		newConn, err := p.createConnectionLocked(ctx)
		if err != nil {
			// If we can't create a new connection but have an existing one, use it
			if bestConn != nil {
				utils.LavaFormatWarning("gRPC pool: failed to scale up, using existing connection", err,
					utils.LogAttr("endpoint", p.sanitizedURL),
					utils.LogAttr("currentConnections", len(p.connections)),
					utils.LogAttr("existingStreamCount", lowestStreams),
				)
				return bestConn, nil
			}
			return nil, fmt.Errorf("failed to create connection: %w", err)
		}

		utils.LavaFormatInfo("gRPC pool: scaled up",
			utils.LogAttr("endpoint", p.sanitizedURL),
			utils.LogAttr("totalConnections", len(p.connections)),
			utils.LogAttr("reason", "all connections near capacity"),
		)
		return newConn, nil
	}

	// At max connections - use the one with lowest streams even if over target
	if bestConn != nil {
		return bestConn, nil
	}

	// No healthy connections - try to create one
	return p.createConnectionLocked(ctx)
}

// createConnectionLocked creates a new connection (caller must hold lock)
func (p *UpstreamGRPCPool) createConnectionLocked(ctx context.Context) (*UpstreamGRPCStreamConnection, error) {
	conn, err := NewUpstreamGRPCStreamConnection(ctx, p.nodeUrl, p.connectTimeout, p.liveness)
	if err != nil {
		return nil, err
	}

	p.connections = append(p.connections, conn)
	p.backoff.Reset()

	utils.LavaFormatDebug("gRPC pool: connection added",
		utils.LogAttr("endpoint", p.sanitizedURL),
		utils.LogAttr("totalConnections", len(p.connections)),
	)

	return conn, nil
}

// NotifyStreamRemoved should be called when a stream is closed on a connection.
// This allows the pool to potentially scale down if connections are underutilized.
func (p *UpstreamGRPCPool) NotifyStreamRemoved(conn *UpstreamGRPCStreamConnection) {
	if conn == nil {
		return
	}

	conn.DecrementStreams()

	// Consider scaling down if we have excess connections with no streams
	p.maybeScaleDown()
}

// maybeScaleDown removes empty connections if we have more than minConnections
func (p *UpstreamGRPCPool) maybeScaleDown() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.connections) <= p.minConnections {
		return
	}

	// Find connections with no streams (excluding the first minConnections)
	var toRemove []*UpstreamGRPCStreamConnection
	kept := make([]*UpstreamGRPCStreamConnection, 0, len(p.connections))

	for i, conn := range p.connections {
		if i < p.minConnections {
			// Keep minimum connections
			kept = append(kept, conn)
		} else if conn.StreamCount() == 0 && !conn.IsHealthy() {
			// Remove unhealthy empty connections
			toRemove = append(toRemove, conn)
		} else if conn.StreamCount() == 0 {
			// Mark for potential removal (only if we have excess)
			if len(kept)+len(p.connections)-i-1 >= p.minConnections {
				toRemove = append(toRemove, conn)
			} else {
				kept = append(kept, conn)
			}
		} else {
			kept = append(kept, conn)
		}
	}

	if len(toRemove) > 0 {
		p.connections = kept

		// Close removed connections asynchronously
		go func(conns []*UpstreamGRPCStreamConnection) {
			for _, conn := range conns {
				conn.Close()
			}
			utils.LavaFormatDebug("gRPC pool: scaled down",
				utils.LogAttr("endpoint", p.sanitizedURL),
				utils.LogAttr("removedConnections", len(conns)),
				utils.LogAttr("remainingConnections", len(kept)),
			)
		}(toRemove)
	}
}

// reconnectResult is the outcome of one ReconnectWithBackoff attempt.
type reconnectResult struct {
	err error
}

// ReconnectWithBackoff attempts to reconnect the pool with exponential backoff.
//
// A connection that dies fails every stream on it at once, and each of their subscriptions
// restores through here. One caller dials; the others wait for that attempt and get its
// outcome, as UpstreamWSPool's callers do, then open their streams on what the pool holds.
// Refusing them instead tore each of those subscriptions down.
func (p *UpstreamGRPCPool) ReconnectWithBackoff(ctx context.Context) (err error) {
	if !p.reconnecting.CompareAndSwap(false, true) {
		return p.waitForReconnect(ctx)
	}
	defer func() {
		// Record the outcome before clearing the flag, so a waiter that sees the flag
		// drop reads this attempt's result.
		p.reconnectOutcome.Store(reconnectResult{err: err})
		p.reconnecting.Store(false)
	}()

	p.reconnectMu.Lock()
	defer p.reconnectMu.Unlock()

	// Get backoff duration
	backoffDuration, _ := p.backoff.NextBackoff()
	utils.LavaFormatDebug("gRPC pool: waiting before reconnect",
		utils.LogAttr("endpoint", p.sanitizedURL),
		utils.LogAttr("backoff", backoffDuration),
	)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(backoffDuration):
	}

	// Try to create a new connection
	p.lock.Lock()
	newConn, err := NewUpstreamGRPCStreamConnection(ctx, p.nodeUrl, p.connectTimeout, p.liveness)
	if err != nil {
		p.lock.Unlock()
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	p.connections = append(p.connections, newConn)
	p.backoff.Reset()
	callback := p.onReconnect
	p.lock.Unlock()

	utils.LavaFormatInfo("gRPC pool: reconnected successfully",
		utils.LogAttr("endpoint", p.sanitizedURL),
	)

	// Call reconnect callback if set (for stream restoration)
	if callback != nil {
		callback()
	}

	return nil
}

// waitForReconnect returns once the reconnect in flight has ended, with that attempt's
// error, or with ctx's error if ctx ends first.
func (p *UpstreamGRPCPool) waitForReconnect(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for p.reconnecting.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	if outcome, ok := p.reconnectOutcome.Load().(reconnectResult); ok && outcome.err != nil {
		return fmt.Errorf("the reconnect this caller waited for failed: %w", outcome.err)
	}
	return nil
}

// Close closes all connections in the pool
func (p *UpstreamGRPCPool) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	p.lock.Lock()
	conns := p.connections
	p.connections = nil
	p.lock.Unlock()

	for _, conn := range conns {
		conn.Close()
	}

	utils.LavaFormatDebug("gRPC pool: closed",
		utils.LogAttr("endpoint", p.sanitizedURL),
		utils.LogAttr("closedConnections", len(conns)),
	)

	return nil
}

// GetEndpoint returns the sanitized endpoint URL
func (p *UpstreamGRPCPool) GetEndpoint() string {
	return p.sanitizedURL
}

// ConnectionCount returns the current number of connections in the pool
func (p *UpstreamGRPCPool) ConnectionCount() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.connections)
}

// TotalStreamCount returns the total number of active streams across all connections
func (p *UpstreamGRPCPool) TotalStreamCount() int32 {
	p.lock.RLock()
	defer p.lock.RUnlock()

	var total int32
	for _, conn := range p.connections {
		total += conn.StreamCount()
	}
	return total
}
