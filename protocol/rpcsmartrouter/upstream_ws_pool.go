package rpcsmartrouter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

// UpstreamWSConnection wraps an rpcclient.Client with health tracking and subscription count
type UpstreamWSConnection struct {
	client            *rpcclient.Client
	endpoint          string
	sanitizedURL      string // For logging (no auth)
	nodeUrl           *common.NodeUrl
	healthy           atomic.Bool
	lastError         atomic.Value // stores error
	createdAt         time.Time
	subscriptionCount atomic.Int32 // Number of active subscriptions on this connection
	lock              sync.RWMutex
	closed            atomic.Bool
}

// NewUpstreamWSConnection creates a new upstream WebSocket connection
func NewUpstreamWSConnection(ctx context.Context, nodeUrl *common.NodeUrl) (*UpstreamWSConnection, error) {
	conn := &UpstreamWSConnection{
		endpoint:     nodeUrl.Url,
		sanitizedURL: sanitizeEndpointURL(nodeUrl.Url),
		nodeUrl:      nodeUrl,
		createdAt:    time.Now(),
	}
	conn.healthy.Store(true)

	if err := conn.connect(ctx); err != nil {
		return nil, err
	}

	return conn, nil
}

// connect establishes the WebSocket connection using rpcclient
func (c *UpstreamWSConnection) connect(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.closed.Load() {
		return fmt.Errorf("connection is closed")
	}

	// Build headers from nodeUrl auth config
	headers := make(map[string]string)
	for k, v := range c.nodeUrl.GetAuthHeaders() {
		headers[k] = v
	}

	// Add auth path if configured
	endpoint := c.nodeUrl.AuthConfig.AddAuthPath(c.endpoint)

	// Use rpcclient's WebSocket dialer
	client, err := rpcclient.DialWebsocket(ctx, endpoint, headers)
	if err != nil {
		c.healthy.Store(false)
		c.lastError.Store(err)
		return fmt.Errorf("failed to dial WebSocket %s: %w", c.sanitizedURL, err)
	}

	c.client = client
	c.healthy.Store(true)
	// Note: Don't store nil in lastError - atomic.Value panics on nil
	// The healthy flag indicates success; lastError is only set on failure

	utils.LavaFormatDebug("WebSocket connection established",
		utils.LogAttr("endpoint", c.sanitizedURL),
	)

	return nil
}

// GetClient returns the underlying rpcclient.Client for subscription operations
func (c *UpstreamWSConnection) GetClient() *rpcclient.Client {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client
}

// IsHealthy returns true if the connection is healthy
func (c *UpstreamWSConnection) IsHealthy() bool {
	return c.healthy.Load() && !c.closed.Load()
}

// MarkUnhealthy marks the connection as unhealthy
func (c *UpstreamWSConnection) MarkUnhealthy(err error) {
	c.healthy.Store(false)
	if err != nil {
		c.lastError.Store(err)
	}
}

// Close closes the WebSocket connection
func (c *UpstreamWSConnection) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.client != nil {
		c.client.Close()
		c.client = nil
	}

	utils.LavaFormatDebug("WebSocket connection closed",
		utils.LogAttr("endpoint", c.sanitizedURL),
	)

	return nil
}

// GetEndpoint returns the endpoint URL (sanitized for logging)
func (c *UpstreamWSConnection) GetEndpoint() string {
	return c.sanitizedURL
}

// GetNodeUrl returns the full nodeUrl configuration
func (c *UpstreamWSConnection) GetNodeUrl() *common.NodeUrl {
	return c.nodeUrl
}

// IncrementSubscriptions increments the subscription count and returns the new count
func (c *UpstreamWSConnection) IncrementSubscriptions() int32 {
	return c.subscriptionCount.Add(1)
}

// DecrementSubscriptions decrements the subscription count and returns the new count
func (c *UpstreamWSConnection) DecrementSubscriptions() int32 {
	return c.subscriptionCount.Add(-1)
}

// SubscriptionCount returns the current number of subscriptions on this connection
func (c *UpstreamWSConnection) SubscriptionCount() int32 {
	return c.subscriptionCount.Load()
}

func (c *UpstreamWSConnection) slots() int32 { return c.subscriptionCount.Load() }

func (c *UpstreamWSConnection) addSlots(delta int32) int32 { return c.subscriptionCount.Add(delta) }

// UpstreamWSPool manages a pool of WebSocket connections to upstream endpoints.
// The pool automatically scales between minConnections and maxConnections based on
// subscription load, targeting approximately perConn subscriptions per connection.
type UpstreamWSPool struct {
	*connPool[*UpstreamWSConnection]
	nodeUrl     *common.NodeUrl
	dialTimeout time.Duration // bounds one dial whatever the caller's context allows
}

// wsReconnectPolicy keeps the WebSocket pool's reconnect as it was before the two pools
// shared one implementation; see reconnectPolicy for what each field does.
var wsReconnectPolicy = reconnectPolicy{
	skipWhenHealthy:          true,
	retryOnFreshLadder:       true,
	dropUnhealthyOnReconnect: true,
	waitersJudgeByHealth:     true,
	pruneDeadOnPick:          true,
}

// NewUpstreamWSPool creates a new WebSocket connection pool for an endpoint
func NewUpstreamWSPool(nodeUrl *common.NodeUrl) *UpstreamWSPool {
	pool := &UpstreamWSPool{
		nodeUrl:     nodeUrl,
		dialTimeout: defaultUpstreamDialTimeout,
	}
	pool.connPool = newConnPool(
		poolVocabulary{name: "WebSocket", slotAttr: "existingSubCount"},
		nodeUrl.Url, pool.dial, wsReconnectPolicy,
		1,   // Default minimum
		10,  // Default maximum
		100, // Default target
	)
	return pool
}

// NewUpstreamWSPoolWithConfig creates a pool with custom configuration
func NewUpstreamWSPoolWithConfig(nodeUrl *common.NodeUrl, config *WebsocketConfig) *UpstreamWSPool {
	pool := NewUpstreamWSPool(nodeUrl)
	if config != nil {
		pool.minConnections = config.UpstreamPoolMinConnections
		pool.maxConnections = config.UpstreamPoolMaxConnections
		pool.perConn = config.UpstreamPoolSubscriptionsPerConn
	}
	return pool
}

// dial opens one connection to the endpoint, bounded by dialTimeout. The context only bounds
// the handshake; the connection outlives it.
func (p *UpstreamWSPool) dial(ctx context.Context) (*UpstreamWSConnection, error) {
	dialCtx, cancel := context.WithTimeout(ctx, p.dialTimeout)
	defer cancel()
	return NewUpstreamWSConnection(dialCtx, p.nodeUrl)
}

// ReserveConnectionForSubscription returns the best connection for a new subscription and
// counts the caller's subscription on it, under the pool lock. It prefers connections with
// lower subscription counts and creates a new connection when all existing ones are near
// capacity (and the pool is below maxConnections).
//
// The caller releases the slot with NotifySubscriptionRemoved, including when the subscribe
// itself fails.
func (p *UpstreamWSPool) ReserveConnectionForSubscription(ctx context.Context) (*UpstreamWSConnection, error) {
	return p.take(ctx, true)
}

// NotifySubscriptionRemoved should be called when a subscription is removed from a connection.
// This allows the pool to potentially scale down if connections are underutilized.
func (p *UpstreamWSPool) NotifySubscriptionRemoved(conn *UpstreamWSConnection) {
	p.release(conn)
}

// TotalSubscriptions returns the total number of subscriptions across all connections
func (p *UpstreamWSPool) TotalSubscriptions() int32 {
	return p.totalSlots()
}

// Note: sanitizeEndpointURL is defined in direct_rpc_relay.go
