package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
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

// grpcEndpointDescriptors is one endpoint's method-descriptor cache and the resolver
// that fills it, shared by every channel the pool holds for that endpoint.
//
// The scope is the endpoint because that is what the data describes: a descriptor
// describes the SERVER, so every channel of one endpoint resolves the same service to
// the same descriptor. One cache per channel therefore re-pays a cold service's
// reflection lookup on every channel the pool adds — on scale-up past streamsPerConn,
// with PoolMinConnections above 1, and on every reconnect, which dials a fresh channel
// (MAG-4397). Sharing makes the endpoint's second channel free.
//
// lifetime bounds every lookup and ends with the POOL rather than with a channel. A
// lookup borrows whichever channel is healthy when it runs (borrowAnyChannel), so one
// channel closing is no longer a reason to abandon it.
type grpcEndpointDescriptors struct {
	cache    *common.SafeSyncMap[string, *desc.MethodDescriptor]
	resolver *lavasession.GRPCDescriptorResolver
	endLife  context.CancelFunc
}

// newGRPCEndpointDescriptors builds the shared cache and resolver for one endpoint.
// borrow must lend a channel to THAT endpoint; the pool lends any healthy one of its own.
func newGRPCEndpointDescriptors(
	nodeUrl *common.NodeUrl,
	borrow func(context.Context) (*grpc.ClientConn, func(), error),
) *grpcEndpointDescriptors {
	lifetime, endLife := context.WithCancel(context.Background())
	cache := &common.SafeSyncMap[string, *desc.MethodDescriptor]{}
	return &grpcEndpointDescriptors{
		cache:    cache,
		resolver: lavasession.NewGRPCDescriptorResolver(lifetime, &nodeUrl.GrpcConfig, nodeUrl.Url, cache, borrow),
		endLife:  endLife,
	}
}

// close aborts every descriptor lookup still running for this endpoint.
func (d *grpcEndpointDescriptors) close() {
	if d != nil && d.endLife != nil {
		d.endLife()
	}
}

// UpstreamGRPCStreamConnection wraps a grpc.ClientConn with health tracking and stream count.
// Unlike the unary GRPCConnector, this is designed for long-lived streaming connections.
type UpstreamGRPCStreamConnection struct {
	conn          *grpc.ClientConn
	endpoint      string
	sanitizedURL  string // For logging (no auth info)
	nodeUrl       *common.NodeUrl
	liveness      grpcChannelLiveness
	healthy       atomic.Bool
	lastError     atomic.Value // stores error
	createdAt     time.Time
	activeStreams atomic.Int32 // Number of active streams on this connection
	// descriptors is the ENDPOINT's shared descriptor cache and resolver, owned by the
	// pool and handed to every channel it holds, so a cold service costs one reflection
	// stream for the endpoint however many channels and subscriptions ask (MAG-4397).
	// The resolver is the same single-flight, whole-service, reflection-timeout one the
	// unary connection runs (MAG-4278 #18), over its own instance: that one borrows from
	// the unary connector in another package, so the two are not the same object.
	// Nil only on a connection built as a bare literal in a test.
	descriptors *grpcEndpointDescriptors
	lock        sync.RWMutex
	closed      atomic.Bool
}

// NewUpstreamGRPCStreamConnection creates a new upstream gRPC connection for streaming.
//
// descriptors is the endpoint's shared cache and resolver, which the POOL owns: every
// channel of one endpoint is given the same one, so the endpoint pays a cold service's
// reflection lookup once (MAG-4397). The pool always passes its own.
func NewUpstreamGRPCStreamConnection(ctx context.Context, nodeUrl *common.NodeUrl, timeout time.Duration, liveness grpcChannelLiveness, descriptors *grpcEndpointDescriptors) (*UpstreamGRPCStreamConnection, error) {
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     nodeUrl.Url,
		sanitizedURL: sanitizeEndpointURL(nodeUrl.Url),
		nodeUrl:      nodeUrl,
		liveness:     liveness,
		createdAt:    time.Now(),
		descriptors:  descriptors,
	}
	conn.healthy.Store(true)

	if err := conn.connect(ctx, timeout); err != nil {
		return nil, err
	}

	return conn, nil
}

// lendChannel hands out this connection's one channel; there is nothing to give back, so
// the release is a no-op. The pool's borrowAnyChannel calls it, trying each channel of
// the endpoint in turn. A closed connection answers with
// lavasession.ErrGRPCConnectionClosed, the error the unary connection's borrow gives,
// which the resolver passes through as it is instead of logging a failed lookup.
func (c *UpstreamGRPCStreamConnection) lendChannel(context.Context) (*grpc.ClientConn, func(), error) {
	c.lock.RLock()
	conn := c.conn
	c.lock.RUnlock()
	if conn == nil || c.closed.Load() {
		return nil, nil, lavasession.ErrGRPCConnectionClosed
	}
	return conn, func() {}, nil
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

	// The transport: TLS for grpcs:// or use-tls, built from the endpoint's auth-config the
	// way every other dialer builds it (cacert-pem, cert-pem/key-pem, allow-insecure);
	// plaintext only where the config says so.
	scheme := strings.ToLower(parsedURL.Scheme)
	transportIsSecure := scheme == "grpcs" || c.nodeUrl.AuthConfig.UseTLS
	if !transportIsSecure && !c.nodeUrl.GrpcConfig.AllowInsecure {
		return fmt.Errorf("insecure gRPC (grpc://) requires allow-insecure: true in config")
	}
	c.nodeUrl.TokenOverInsecureWarning(transportIsSecure)

	// The shared options (receive-size cap, MAG-2218 auth-headers) plus what a streaming
	// channel adds: the MAG-3887 idle timeout and keepalive, so a flow a middlebox dropped
	// is neither reused after an idle spell nor left silently under live streams.
	dialOpts := c.nodeUrl.GRPCDialOptions(c.nodeUrl.AuthConfig.GRPCTransportCredentials(transportIsSecure), c.liveness.dialOptions()...)

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

// Close closes the gRPC connection
func (c *UpstreamGRPCStreamConnection) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}
	// A descriptor lookup is NOT aborted here. Its lifetime is the pool's, because the
	// resolver is the endpoint's: a lookup borrows whichever channel is healthy when it
	// runs, so this channel going away is not a reason to throw the lookup away
	// (MAG-4397). The pool ends them all in its own Close. A lookup that happens to be
	// borrowing this channel fails as the channel closes below, drops its in-flight
	// entry, and the next caller retries on a live channel.

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

func (c *UpstreamGRPCStreamConnection) slots() int32 { return c.activeStreams.Load() }

func (c *UpstreamGRPCStreamConnection) addSlots(delta int32) int32 { return c.activeStreams.Add(delta) }

// GetMethodDescriptor retrieves a method descriptor from cache, or resolves it
// through the node's configured descriptor source (reflection, file, or hybrid).
// Streaming methods are exactly the ones a partial reflection service tends to
// omit, so this path needs the file escape hatch as much as the unary one does
// (MAG-2350). The resolver is the ENDPOINT's, shared by every channel the pool holds,
// so a cold service costs one reflection stream however many channels and subscriptions
// ask (MAG-4397), and a lookup outlives the caller that started it (MAG-2860).
func (c *UpstreamGRPCStreamConnection) GetMethodDescriptor(
	ctx context.Context,
	service, methodName string,
) (*desc.MethodDescriptor, error) {
	if c.descriptors == nil {
		// Only reachable from a connection built as a bare literal in a test; the pool
		// always hands its own. An error rather than a panic, naming the cause.
		return nil, utils.LavaFormatError("gRPC connection has no descriptor resolver", nil,
			utils.LogAttr("endpoint", c.sanitizedURL),
			utils.LogAttr("service", service),
			utils.LogAttr("method", methodName))
	}
	return c.descriptors.resolver.MethodDescriptor(ctx, service, methodName)
}

// UpstreamGRPCPool manages a pool of gRPC connections for streaming.
// The pool automatically scales between minConnections and maxConnections based on
// stream load, targeting approximately perConn streams per connection.
type UpstreamGRPCPool struct {
	*connPool[*UpstreamGRPCStreamConnection]
	nodeUrl        *common.NodeUrl
	connectTimeout time.Duration       // Connection establishment timeout
	liveness       grpcChannelLiveness // Idle timeout and keepalive every connection dials with

	// descriptors is this endpoint's one descriptor cache and resolver, handed to every
	// channel the pool dials (MAG-4397).
	descriptors *grpcEndpointDescriptors
}

// grpcReconnectPolicy keeps the gRPC pool's reconnect as it was before the two pools shared
// one implementation; see reconnectPolicy for what each field does.
var grpcReconnectPolicy = reconnectPolicy{}

// NewUpstreamGRPCPool creates a new gRPC connection pool for streaming
func NewUpstreamGRPCPool(nodeUrl *common.NodeUrl) *UpstreamGRPCPool {
	pool := &UpstreamGRPCPool{
		nodeUrl:        nodeUrl,
		connectTimeout: defaultUpstreamDialTimeout,
		liveness:       newGRPCChannelLiveness(DefaultGRPCStreamingConfig()),
	}
	pool.connPool = newConnPool(
		poolVocabulary{name: "gRPC", slotAttr: "existingStreamCount"},
		nodeUrl.Url, pool.dial, grpcReconnectPolicy,
		1,   // Default minimum
		5,   // Default maximum
		100, // Default target
	)
	// Built here, before any channel exists: borrowAnyChannel is a closure over the pool
	// and is only called when a lookup actually runs, by which time there is a channel.
	pool.descriptors = newGRPCEndpointDescriptors(nodeUrl, pool.borrowAnyChannel)
	// Ends every descriptor lookup for this endpoint. The lifetime is the pool's because
	// the resolver is (MAG-4397), so this is the one place that aborts them.
	pool.onClose = pool.descriptors.close
	return pool
}

// borrowAnyChannel lends a descriptor lookup any channel of this endpoint.
//
// The resolver is the endpoint's, so a lookup must not be tied to the channel whose relay
// triggered it: that channel can be scaled down, closed or replaced by a reconnect while
// the lookup runs. Any channel to the same endpoint answers reflection identically.
//
// The connection slice is snapshotted under the pool lock and the channels are asked
// outside it, so a lookup can never hold the pool lock — a caller waiting on a lookup
// while holding that lock would otherwise deadlock.
func (p *UpstreamGRPCPool) borrowAnyChannel(ctx context.Context) (*grpc.ClientConn, func(), error) {
	if p.closed.Load() {
		return nil, nil, lavasession.ErrGRPCConnectionClosed
	}

	p.lock.RLock()
	conns := make([]*UpstreamGRPCStreamConnection, len(p.connections))
	copy(conns, p.connections)
	p.lock.RUnlock()

	// A healthy channel first. An unhealthy one is still worth asking when it is all
	// there is: health is the RELAY path's verdict, and a node that fails relays can
	// still answer reflection.
	for _, healthyOnly := range []bool{true, false} {
		for _, conn := range conns {
			if healthyOnly && !conn.IsHealthy() {
				continue
			}
			if channel, release, err := conn.lendChannel(ctx); err == nil {
				return channel, release, nil
			}
		}
	}

	return nil, nil, lavasession.ErrGRPCConnectionClosed
}

// NewUpstreamGRPCPoolWithConfig creates a pool with custom configuration
func NewUpstreamGRPCPoolWithConfig(nodeUrl *common.NodeUrl, config *GRPCStreamingConfig) *UpstreamGRPCPool {
	pool := NewUpstreamGRPCPool(nodeUrl)
	if config != nil {
		pool.minConnections = config.PoolMinConnections
		pool.maxConnections = config.PoolMaxConnections
		pool.perConn = config.StreamsPerConnection
		pool.connectTimeout = config.ConnectionTimeout
		pool.liveness = newGRPCChannelLiveness(config)
	}
	return pool
}

// dial opens one channel to the endpoint with the pool's timeout, liveness and the
// endpoint's shared descriptor resolver.
func (p *UpstreamGRPCPool) dial(ctx context.Context) (*UpstreamGRPCStreamConnection, error) {
	return NewUpstreamGRPCStreamConnection(ctx, p.nodeUrl, p.connectTimeout, p.liveness, p.descriptors)
}

// ReserveConnectionForStream returns the best connection for a new stream and counts the
// caller's stream on it, under the pool lock. It prefers connections with lower stream counts
// and creates a new connection when all existing ones are near capacity (and the pool is below
// maxConnections). Counted any later, the connection can look unused in the meantime, and
// maybeScaleDown closes a connection past minConnections that carries no counted stream: the
// stream opened on it then fails at once. The caller releases the slot with
// NotifyStreamRemoved, including when opening the stream fails.
func (p *UpstreamGRPCPool) ReserveConnectionForStream(ctx context.Context) (*UpstreamGRPCStreamConnection, error) {
	return p.take(ctx, true)
}

// NotifyStreamRemoved should be called when a stream is closed on a connection.
// This allows the pool to potentially scale down if connections are underutilized.
func (p *UpstreamGRPCPool) NotifyStreamRemoved(conn *UpstreamGRPCStreamConnection) {
	p.release(conn)
}

// TotalStreamCount returns the total number of active streams across all connections
func (p *UpstreamGRPCPool) TotalStreamCount() int32 {
	return p.totalSlots()
}
