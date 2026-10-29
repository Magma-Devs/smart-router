package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
)

// Note: Configuration constants are now in WebsocketConfig (websocket_config.go)

// directActiveSubscription holds state for an active upstream subscription
type directActiveSubscription struct {
	// Upstream connection info
	upstreamPool         *UpstreamWSPool
	upstreamConnection   *UpstreamWSConnection // The specific connection used for this subscription
	upstreamSubscription *rpcclient.ClientSubscription
	upstreamID           string // ID returned by upstream node

	// Router-generated info - each client gets their own router ID
	// that maps to the shared upstream ID. This enables proper deduplication
	// where multiple clients can share one upstream subscription while each
	// having their own unique subscription ID for unsubscribe operations.
	clientRouterIDs map[string]string // clientKey -> routerID
	hashedParams    string            // Hash of subscription parameters

	// Subscription params for restoration after reconnect
	subscriptionParams []byte // Original subscription params (JSON)
	subscribeMethod    string // Method name (e.g., "eth_subscribe", "subscribe")

	// unsubscribeMethod is what the node is told to drop this subscription with on the paths
	// where no client message names the method: the teardown that runs when the last client
	// goes, a failed restore's cleanup, and the discard of a subscription a restore re-opened
	// onto an entry that had gone. It is resolved from the spec once, when the subscription is
	// created (resolveTeardownUnsubscribeMethod), rather than derived at each call site by
	// string surgery whose fallback was the literal "unsubscribe" — a method only Tendermint
	// has, so every other non-EVM chain rejected the call and kept publishing into the pooled
	// connection (MAG-4278 item 1, the MAG-3366 leak shape).
	//
	// Empty when the spec declares no unsubscribe this subscription can be dropped with. The
	// node is then told nothing, because a call it does not answer to cannot stop it.
	//
	// A restore keeps it: it re-subscribes with the same method, so the pairing still holds.
	unsubscribeMethod string

	// numericIDs records that this chain numbers its subscriptions rather than naming
	// them, read once from the subscribe response the node actually sent. It is a property
	// of the chain, so it survives a reconnect even though upstreamID does not.
	//
	// It is the ONLY source of this fact. Every site that renders an id for the wire — the
	// subscribe reply, a joining client's reply, the per-frame rewrite, the param sent
	// upstream on teardown — reads it rather than re-sniffing whatever JSON is nearest,
	// because re-deriving invites the sites to disagree: an upstream that numbers the
	// subscribe result but quotes the id in its pushes would otherwise hand the client a
	// number at subscribe time and quoted ids in every frame after.
	numericIDs bool

	// Client tracking - multiple clients can share one upstream subscription
	connectedClients map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]

	// clientDetached is closed for a client the moment it leaves this subscription by
	// any path, so its disconnect watcher can stop waiting on a socket that may stay
	// open for days after the client unsubscribed.
	clientDetached map[string]chan struct{}

	// First reply cached for deduplication
	firstReply *pairingtypes.RelayReply

	// Original result from upstream for Tendermint (stores {"query":"..."} format)
	// Used when joining clients need the original response format
	originalResult json.RawMessage

	// Lifecycle
	ctx          context.Context
	cancel       context.CancelFunc
	closeSubChan chan struct{}
	closeSubOnce sync.Once
	messagesChan chan *rpcclient.JsonrpcMessage

	// Restoration state
	restoring atomic.Bool // Prevents concurrent restoration attempts

	// restoresWithoutProgress counts restores since the last progress: a restored
	// subscription that stayed up for restoreStableAfter. Past maxRestoresWithoutProgress
	// the subscription is given up.
	//
	// Each WS restore spawns a fresh listener, whose own failure spawns the next restore, so
	// without a bound a node that accepts a subscribe and drops it immediately is
	// re-subscribed for the life of the process — one round trip per cycle, on every
	// subscription it does this to. The gRPC manager has carried this bound since MAG-4177;
	// this is the WebSocket side catching up (MAG-4278 item 11).
	restoresWithoutProgress atomic.Int32

	// lastRestoredAt is when the last restore committed, in unix nanoseconds; 0 for none.
	lastRestoredAt atomic.Int64
}

// attachClientLocked registers a client on the subscription. The caller holds dwsm.lock,
// or owns the subscription exclusively as StartSubscription does before publishing it.
func (s *directActiveSubscription) attachClientLocked(clientKey, routerID string, sender *common.SafeChannelSender[*pairingtypes.RelayReply]) {
	s.connectedClients[clientKey] = sender
	s.clientRouterIDs[clientKey] = routerID
	if s.clientDetached == nil {
		s.clientDetached = make(map[string]chan struct{})
	}
	s.clientDetached[clientKey] = make(chan struct{})
}

// releaseClientLocked takes a client off the subscription: it closes the client's reply
// channel so the ingress forwarder goroutine ranging over it ends, drops the client's
// bookkeeping, and wakes the client's disconnect watcher. It is the one place a client
// leaves through, whatever the reason, so no path can forget one of those steps again
// (MAG-3722). Idempotent. Returns the client's router id, "" when it was not attached.
func (s *directActiveSubscription) releaseClientLocked(clientKey string) (routerID string, wasAttached bool) {
	sender, wasAttached := s.connectedClients[clientKey]
	if wasAttached {
		sender.Close()
		delete(s.connectedClients, clientKey)
	}
	routerID = s.clientRouterIDs[clientKey]
	delete(s.clientRouterIDs, clientKey)
	if detached, ok := s.clientDetached[clientKey]; ok {
		close(detached)
		delete(s.clientDetached, clientKey)
	}
	return routerID, wasAttached
}

// detachedSignal returns the channel closed when clientKey leaves this subscription. A nil
// channel never fires, which is the right answer for a client that is not attached.
func (s *directActiveSubscription) detachedSignal(clientKey string) <-chan struct{} {
	return s.clientDetached[clientKey]
}

// stopUpstreamListener ends listenForUpstreamMessages. Safe to call more than once.
func (s *directActiveSubscription) stopUpstreamListener() {
	s.closeSubOnce.Do(func() {
		if s.closeSubChan != nil {
			close(s.closeSubChan)
		}
	})
}

// pendingSubscriptionsBroadcastManager handles synchronization for concurrent subscription requests
type pendingSubscriptionsBroadcastManager struct {
	broadcastChannelList []chan bool
}

func (psbm *pendingSubscriptionsBroadcastManager) broadcastToChannelList(value bool) {
	for _, ch := range psbm.broadcastChannelList {
		ch <- value
	}
}

// DirectWSSubscriptionManager manages WebSocket subscriptions directly to upstream endpoints
// without going through Lava providers. It implements chainlib.WSSubscriptionManager.
//
// The gRPC counterpart is DirectGRPCSubscriptionManager, which follows the same shape:
// one upstream stream per distinct subscription, fanned out to per-client channels.
type DirectWSSubscriptionManager struct {
	// Client-facing (reuse existing patterns)
	connectedClients map[string]map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]
	// First key: clientKey (dappID:ip:wsUID), Second key: hashedParams

	// Active subscriptions keyed by hashedParams
	activeSubscriptions map[string]*directActiveSubscription

	// endpointNames names each endpoint's provider for relay_end (SetEndpointNames).
	endpointNames subscriptionEndpointNames

	// Pending subscriptions for deduplication
	pendingSubscriptions map[string]*pendingSubscriptionsBroadcastManager

	// slowClients holds the connections dropSlowClient dropped a subscription of, keyed by
	// client key, until the ingress reads the drop (SubscriptionEndError) and closes the client's
	// socket, or the connection ends (forgetClientLocked). Guarded by lock.
	slowClients map[string]struct{}

	// beforeRestoreCommit, when set, runs in handleUpstreamDisconnect after the node has
	// answered the re-subscribe and before the result is committed under dwsm.lock. Nil
	// outside tests; tests use it to hold a restore at exactly that point.
	beforeRestoreCommit func()

	// Upstream connection pools keyed by endpoint URL
	upstreamPools map[string]*UpstreamWSPool

	// Subscription ID mapping
	idMapper *SubscriptionIDMapper

	// Dependencies
	metricsManager metrics.ConsumerMetricsManagerInf
	connectionType string
	chainID        string
	apiInterface   string

	// Upstream endpoint tiers, the optimizer, sticky sessions and endpoint selection, shared
	// with the gRPC manager. Two-tier separation matches the HTTP backup model: the backup
	// tier is only consulted when primary is exhausted.
	subscriptionEndpoints

	// Configuration - all configurable parameters
	config *WebsocketConfig

	// Rate limiting - per-client subscription rate limits
	rateLimiter *ClientRateLimiter

	// Total subscription counter for global limit tracking
	totalSubscriptions atomic.Int64

	lock sync.RWMutex
}

// WebSocketEndpointOptimizer is an interface for selecting WebSocket endpoints
// This allows using ProviderOptimizer or a simple round-robin fallback
type WebSocketEndpointOptimizer interface {
	// ChooseUpstream selects the best endpoint(s) from available addresses
	ChooseUpstream(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string)
	// AppendRelayData updates metrics after successful relay
	AppendRelayData(provider string, latency time.Duration, cu, syncBlock uint64)
	// AppendRelayFailure updates metrics after failed relay
	AppendRelayFailure(providerAddress string)
}

// NewDirectWSSubscriptionManager creates a new direct WebSocket subscription manager.
// If config is nil, DefaultWebsocketConfig() will be used.
//
// wsEndpoints is the primary tier — selectEndpoint serves these first.
// wsBackupEndpoints is the backup tier — only consulted when primary is exhausted.
// Either slice may be nil or empty; both must not be empty for selection to succeed.
func NewDirectWSSubscriptionManager(
	metricsManager metrics.ConsumerMetricsManagerInf,
	connectionType string,
	chainID string,
	apiInterface string,
	wsEndpoints []*common.NodeUrl,
	wsBackupEndpoints []*common.NodeUrl,
	optimizer WebSocketEndpointOptimizer,
	config *WebsocketConfig,
) *DirectWSSubscriptionManager {
	// Use default config if none provided
	if config == nil {
		config = DefaultWebsocketConfig()
	}

	return &DirectWSSubscriptionManager{
		connectedClients:     make(map[string]map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		activeSubscriptions:  make(map[string]*directActiveSubscription),
		pendingSubscriptions: make(map[string]*pendingSubscriptionsBroadcastManager),
		slowClients:          make(map[string]struct{}),
		upstreamPools:        make(map[string]*UpstreamWSPool),
		idMapper:             NewSubscriptionIDMapper(),
		metricsManager:       metrics.SafeMetrics(metricsManager),
		connectionType:       connectionType,
		chainID:              chainID,
		apiInterface:         apiInterface,
		subscriptionEndpoints: newSubscriptionEndpoints("DirectWS",
			"no WebSocket endpoints available (primary and backup both exhausted)",
			wsEndpoints, wsBackupEndpoints, optimizer),
		config:      config,
		rateLimiter: NewClientRateLimiter(config),
	}
}

// SetEndpointNames gives the manager its endpoints' provider names, by URL, which relay_end writes
// for the upstream a subscription request went to.
func (dwsm *DirectWSSubscriptionManager) SetEndpointNames(names map[string]string) {
	dwsm.endpointNames.set(names)
}

// upstreamNameOf names the provider of activeSub's upstream.
func (dwsm *DirectWSSubscriptionManager) upstreamNameOf(activeSub *directActiveSubscription) string {
	if activeSub == nil || activeSub.upstreamPool == nil || activeSub.upstreamPool.nodeUrl == nil {
		return ""
	}
	return dwsm.endpointNames.name(activeSub.upstreamPool.nodeUrl.Url)
}

// subscriptionUpstreamName names the provider of the open subscription hashedParams identifies.
func (dwsm *DirectWSSubscriptionManager) subscriptionUpstreamName(hashedParams string) string {
	dwsm.lock.RLock()
	activeSub := dwsm.activeSubscriptions[hashedParams]
	dwsm.lock.RUnlock()
	return dwsm.upstreamNameOf(activeSub)
}

// SetEndpoints replaces both tiers with the currently-serving WebSocket endpoints and
// reports whether anything actually changed. See subscriptionEndpoints.setEndpoints.
func (dwsm *DirectWSSubscriptionManager) SetEndpoints(wsEndpoints, wsBackupEndpoints []*common.NodeUrl) (changed bool) {
	return dwsm.setEndpoints(wsEndpoints, wsBackupEndpoints)
}

// Start starts the background cleanup goroutine for the DirectWSSubscriptionManager.
// This should be called once after creating the manager.
func (dwsm *DirectWSSubscriptionManager) Start(ctx context.Context) {
	go dwsm.cleanupStaleSubscriptions(ctx)
	utils.LavaFormatInfo("DirectWS: started subscription manager",
		utils.LogAttr("chainID", dwsm.chainID),
		utils.LogAttr("cleanupInterval", dwsm.config.CleanupInterval),
		utils.LogAttr("maxSubsPerClient", dwsm.config.MaxSubscriptionsPerClient),
		utils.LogAttr("maxTotalSubscriptions", dwsm.config.MaxTotalSubscriptions),
		utils.LogAttr("subscriptionsPerMinutePerClient", dwsm.config.SubscriptionsPerMinutePerClient),
	)
}

// cleanupStaleSubscriptions periodically removes stale subscriptions with cancelled contexts.
func (dwsm *DirectWSSubscriptionManager) cleanupStaleSubscriptions(ctx context.Context) {
	ticker := time.NewTicker(dwsm.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			utils.LavaFormatDebug("DirectWS: cleanup goroutine stopped")
			return
		case <-ticker.C:
			dwsm.performCleanup()
		}
	}
}

// performCleanup removes subscriptions with cancelled contexts and cleans up orphaned clients
// dropSubscriptionLocked releases every client on the subscription (their reply channels close
// and their watchers wake), forgets its ids and retires it, telling the node. Used by the
// periodic sweep and by cleanupSubscription. Caller holds dwsm.lock.
func (dwsm *DirectWSSubscriptionManager) dropSubscriptionLocked(hashedParams string, activeSub *directActiveSubscription) {
	for clientKey := range activeSub.connectedClients {
		activeSub.releaseClientLocked(clientKey)
		dwsm.forgetSubscriptionForClientLocked(clientKey, hashedParams, false)
	}
	dwsm.idMapper.RemoveAllForUpstream(activeSub.upstreamID)
	dwsm.teardownSubscriptionLocked(hashedParams, activeSub, true)
}

func (dwsm *DirectWSSubscriptionManager) performCleanup() {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	removedSubs := 0
	removedClients := 0

	// Clean up subscriptions with no connected clients or cancelled contexts
	for hashedParams, activeSub := range dwsm.activeSubscriptions {
		// Check if subscription context is done
		select {
		case <-activeSub.ctx.Done():
			// Context cancelled, clean up
			utils.LavaFormatDebug("DirectWS: cleaning up subscription with cancelled context",
				utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
			)
			dwsm.dropSubscriptionLocked(hashedParams, activeSub)
			removedSubs++
			continue
		default:
			// Context still active
		}

		// Remove clients with closed/cancelled contexts
		// Note: We can't check if SafeChannelSender is closed directly,
		// so we rely on the subscription context and client disconnect handlers

		// If no clients left, close the subscription
		if len(activeSub.connectedClients) == 0 {
			utils.LavaFormatDebug("DirectWS: cleaning up subscription with no clients",
				utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
			)
			dwsm.dropSubscriptionLocked(hashedParams, activeSub)
			removedSubs++
		}
	}

	// Rate limiters for clients that never got a subscription registered (a failed
	// subscribe, or a client that only unsubscribed) have no disconnect hook of their
	// own; an entry idle for longer than its refill time is indistinguishable from a
	// fresh one, so it can go.
	dwsm.rateLimiter.SweepIdle(time.Now())

	// Clean up orphaned client entries (clients with no subscriptions)
	for clientKey, subs := range dwsm.connectedClients {
		// Check if subscriptions still exist in activeSubscriptions
		for hashedParams := range subs {
			if _, exists := dwsm.activeSubscriptions[hashedParams]; !exists {
				delete(subs, hashedParams)
			}
		}
		if len(subs) == 0 {
			delete(dwsm.connectedClients, clientKey)
			dwsm.stickyStore.Delete(clientKey)
			removedClients++
		}
	}

	// Log warning if subscription count is high
	totalSubs := len(dwsm.activeSubscriptions)
	if totalSubs > dwsm.config.MaxTotalSubscriptions {
		utils.LavaFormatWarning("DirectWS: subscription count high, potential memory issue", nil,
			utils.LogAttr("current", totalSubs),
			utils.LogAttr("threshold", dwsm.config.MaxTotalSubscriptions),
		)
	}

	if removedSubs > 0 || removedClients > 0 {
		utils.LavaFormatDebug("DirectWS: cleanup completed",
			utils.LogAttr("removedSubscriptions", removedSubs),
			utils.LogAttr("removedClients", removedClients),
			utils.LogAttr("activeSubscriptions", len(dwsm.activeSubscriptions)),
			utils.LogAttr("connectedClients", len(dwsm.connectedClients)),
		)
	}
}

// noteSubscribeFailure applies a failed connect/subscribe attempt's consequences. A
// rate-limited attempt is held off instead of scored — the endpoint is busy, not broken,
// and an availability sample of 0 is the demotion the rate-limit contract forbids
// (docs/RATE-LIMIT-HOLDOFF.md). Anything else penalizes the optimizer as before. The WS
// path works on bare node URLs with no provider name in scope, so the URL keys both
// registry tiers — WS hold-offs are per-URL and do not escalate across a vendor.
func (dwsm *DirectWSSubscriptionManager) noteSubscribeFailure(url string, err error) {
	if errors.Is(err, common.StatusCodeError429) {
		retryAfter, _ := common.RetryAfterFrom(err)
		heldFor := dwsm.holdoffs().RecordRateLimit(url, url, retryAfter)
		utils.LavaFormatDebug("DirectWS: subscribe rate-limited by upstream, holding endpoint off",
			utils.LogAttr("endpoint", sanitizeEndpointURL(url)),
			utils.LogAttr("holdoff", heldFor.String()),
		)
		return
	}
	if dwsm.optimizer != nil {
		dwsm.optimizer.AppendRelayFailure(url)
	}
}

// CreateWebSocketConnectionUniqueKey creates a unique key for a WebSocket connection
func (dwsm *DirectWSSubscriptionManager) CreateWebSocketConnectionUniqueKey(dappID, consumerIp, wsUID string) string {
	return subscriptionClientKey(dappID, consumerIp, wsUID)
}

// GetOrCreatePool gets or creates an upstream WebSocket pool for the given endpoint.
// The pool is configured with WebSocket settings from the manager's config.
func (dwsm *DirectWSSubscriptionManager) GetOrCreatePool(nodeUrl *common.NodeUrl) *UpstreamWSPool {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	pool, exists := dwsm.upstreamPools[nodeUrl.Url]
	if !exists {
		pool = NewUpstreamWSPoolWithConfig(nodeUrl, dwsm.config)
		dwsm.upstreamPools[nodeUrl.Url] = pool
	}
	return pool
}

// StartSubscription implements chainlib.WSSubscriptionManager.
// It starts a new WebSocket subscription or joins an existing one.
func (dwsm *DirectWSSubscriptionManager) StartSubscription(
	ctx context.Context,
	protocolMessage chainlib.ProtocolMessage,
	dappID string,
	consumerIp string,
	webSocketConnectionUniqueId string,
	metricsData *metrics.RelayMetrics,
) (firstReply *pairingtypes.RelayReply, repliesChan <-chan *pairingtypes.RelayReply, err error) {
	// Extract hashed params from protocol message
	hashedParams, subscriptionParams, err := dwsm.getHashedParams(protocolMessage)
	if err != nil {
		return nil, nil, utils.LavaFormatError("could not marshal params", err)
	}

	// Extract the subscription method from the API definition
	// - EVM chains use "eth_subscribe"
	// - Tendermint uses "subscribe"
	subscribeMethod := protocolMessage.GetApi().Name

	// Resolved once, here, while the spec collection this message matched is in hand: the
	// teardown paths have no client message to read the method off, and deriving it there is
	// what sent non-EVM nodes a method they do not have (MAG-4278 item 1).
	unsubscribeMethod, hasUnsubscribeMethod := resolveTeardownUnsubscribeMethod(protocolMessage.GetApiCollection(), subscribeMethod)
	if !hasUnsubscribeMethod {
		utils.LavaFormatWarning("DirectWS: the spec declares no unsubscribe for this subscribe method, so the node cannot be told to stop publishing when the last client goes", nil,
			utils.LogAttr("chainID", dwsm.chainID),
			utils.LogAttr("subscribeMethod", subscribeMethod),
		)
	}

	// Extract the request ID from the client's message - required for JSON-RPC 2.0 compliance
	// The response ID must match the request ID exactly
	requestID := extractRequestID(protocolMessage.RelayPrivateData().Data)

	clientKey := dwsm.CreateWebSocketConnectionUniqueKey(dappID, consumerIp, webSocketConnectionUniqueId)

	utils.LavaFormatTrace("DirectWS: request to start subscription",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		utils.LogAttr("clientKey", clientKey),
		utils.LogAttr("subscribeMethod", subscribeMethod),
		utils.LogAttr("apiInterface", dwsm.apiInterface),
	)

	// Check rate limiting (subscriptions per minute per client)
	if !dwsm.rateLimiter.AllowSubscribe(clientKey) {
		utils.LavaFormatWarning("DirectWS: client rate limit exceeded", nil,
			utils.LogAttr("clientKey", clientKey),
			utils.LogAttr("limit", dwsm.config.SubscriptionsPerMinutePerClient),
		)
		dwsm.metricsManager.SetFailedWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)
		return nil, nil, fmt.Errorf("subscription rate limit exceeded: max %d subscriptions per minute",
			dwsm.config.SubscriptionsPerMinutePerClient)
	}

	// Check per-client subscription limit
	dwsm.lock.RLock()
	clientSubs := dwsm.connectedClients[clientKey]
	currentSubCount := len(clientSubs)
	dwsm.lock.RUnlock()

	if currentSubCount >= dwsm.config.MaxSubscriptionsPerClient {
		if dwsm.config.ShouldRejectOnClientLimit() {
			utils.LavaFormatWarning("DirectWS: client subscription limit exceeded (rejecting)", nil,
				utils.LogAttr("clientKey", clientKey),
				utils.LogAttr("currentCount", currentSubCount),
				utils.LogAttr("limit", dwsm.config.MaxSubscriptionsPerClient),
			)
			dwsm.metricsManager.SetFailedWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)
			return nil, nil, fmt.Errorf("subscription limit exceeded: client has %d subscriptions (max %d)",
				currentSubCount, dwsm.config.MaxSubscriptionsPerClient)
		}
		// Warn but continue (default behavior)
		utils.LavaFormatWarning("DirectWS: client subscription limit exceeded (warn only)", nil,
			utils.LogAttr("clientKey", clientKey),
			utils.LogAttr("currentCount", currentSubCount),
			utils.LogAttr("limit", dwsm.config.MaxSubscriptionsPerClient),
		)
	}

	// Check global subscription limit
	totalSubs := dwsm.totalSubscriptions.Load()
	if int(totalSubs) >= dwsm.config.MaxTotalSubscriptions {
		if dwsm.config.ShouldRejectOnTotalLimit() {
			utils.LavaFormatWarning("DirectWS: global subscription limit exceeded (rejecting)", nil,
				utils.LogAttr("currentTotal", totalSubs),
				utils.LogAttr("limit", dwsm.config.MaxTotalSubscriptions),
			)
			dwsm.metricsManager.SetFailedWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)
			return nil, nil, fmt.Errorf("global subscription limit exceeded: %d subscriptions (max %d)",
				totalSubs, dwsm.config.MaxTotalSubscriptions)
		}
		// Warn but continue (default behavior)
		utils.LavaFormatWarning("DirectWS: global subscription limit exceeded (warn only)", nil,
			utils.LogAttr("currentTotal", totalSubs),
			utils.LogAttr("limit", dwsm.config.MaxTotalSubscriptions),
		)
	}

	// Create reply channel for this client. Buffered for a burst; a client that lets it fill and
	// frees no room within subscriptionClientSendGrace is dropped as a slow consumer (dropSlowClient).
	repliesChannel := make(chan *pairingtypes.RelayReply, subscriptionClientReplyBuffer)
	safeChannelSender := common.NewSafeChannelSender(ctx, repliesChannel)

	// Track metrics
	dwsm.metricsManager.SetWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)

	// Check for existing active subscription
	existingReply, joined := dwsm.checkForActiveSubscriptionAndConnect(ctx, hashedParams, clientKey, safeChannelSender, requestID)
	if existingReply != nil {
		chainlib.SubscriptionUpstreamFrom(ctx).Joined(dwsm.subscriptionUpstreamName(hashedParams))
		if joined {
			return existingReply, repliesChannel, nil
		}
		// Already had this exact subscription
		safeChannelSender.Close()
		return existingReply, nil, nil
	}

	// Handle pending subscriptions (another client is creating the same subscription)
	for {
		pendingChan, foundPending := dwsm.checkAndAddPendingSubscription(hashedParams)
		if foundPending {
			utils.LavaFormatTrace("DirectWS: waiting for pending subscription")
			success := <-pendingChan
			if success {
				existingReply, joined := dwsm.checkForActiveSubscriptionAndConnect(ctx, hashedParams, clientKey, safeChannelSender, requestID)
				if existingReply != nil {
					chainlib.SubscriptionUpstreamFrom(ctx).Joined(dwsm.subscriptionUpstreamName(hashedParams))
					if joined {
						return existingReply, repliesChannel, nil
					}
					safeChannelSender.Close()
					return existingReply, nil, nil
				}
			}
			// Failed, retry
			utils.LavaFormatDebug("DirectWS: pending subscription failed, retrying")
		} else {
			break
		}
	}

	// No active subscription found, create new one
	// Select best endpoint using sticky session, optimizer, or first available
	startTime := time.Now()
	selectedEndpoint, err := dwsm.selectEndpoint(ctx, clientKey, nil)
	if err != nil {
		dwsm.settlePendingSubscription(hashedParams, false)
		dwsm.metricsManager.SetFailedWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)
		return nil, nil, fmt.Errorf("failed to select WebSocket endpoint: %w", err)
	}

	utils.LavaFormatTrace("DirectWS: creating new upstream subscription",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		utils.LogAttr("endpoint", sanitizeEndpointURL(selectedEndpoint.Url)),
	)

	// Get or create upstream pool for selected endpoint
	pool := dwsm.GetOrCreatePool(selectedEndpoint)

	// Get a connection, with this subscription's slot counted on it already (see
	// ReserveConnectionForSubscription); every failure below releases the slot.
	conn, err := pool.ReserveConnectionForSubscription(ctx)
	if err != nil {
		chainlib.SubscriptionUpstreamFrom(ctx).Sent(dwsm.endpointNames.name(selectedEndpoint.Url), startTime, time.Since(startTime), err)
		dwsm.noteSubscribeFailure(selectedEndpoint.Url, err)
		dwsm.settlePendingSubscription(hashedParams, false)
		dwsm.metricsManager.SetFailedWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)
		return nil, nil, fmt.Errorf("failed to get WebSocket connection: %w", err)
	}

	// Create upstream subscription
	upstreamSub, firstMsg, msgChan, err := dwsm.createUpstreamSubscription(ctx, conn, subscriptionParams, subscribeMethod)
	chainlib.SubscriptionUpstreamFrom(ctx).Sent(dwsm.endpointNames.name(selectedEndpoint.Url), startTime, time.Since(startTime), err)
	if err != nil {
		pool.NotifySubscriptionRemoved(conn)
		dwsm.noteSubscribeFailure(selectedEndpoint.Url, err)
		dwsm.settlePendingSubscription(hashedParams, false)
		dwsm.metricsManager.SetFailedWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)
		return nil, nil, fmt.Errorf("failed to create upstream subscription: %w", err)
	}

	// Report success to optimizer. The endpoint answered, so any standing rate-limit
	// hold-off for it is stale.
	dwsm.holdoffs().RecordAnswer(selectedEndpoint.Url, selectedEndpoint.Url)
	if dwsm.optimizer != nil {
		latency := time.Since(startTime)
		dwsm.optimizer.AppendRelayData(selectedEndpoint.Url, latency, 1, 0) // cu=1, syncBlock=0 (unknown)
	}

	// Store sticky session for client affinity
	// Future subscriptions from this client will use the same endpoint
	dwsm.stickyStore.Set(clientKey, &lavasession.StickySession{
		Provider: selectedEndpoint.Url,
		Epoch:    0, // Epoch is not used for direct RPC (no provider rotation)
	})
	utils.LavaFormatTrace("DirectWS: stored sticky session",
		utils.LogAttr("clientKey", clientKey),
		utils.LogAttr("endpoint", sanitizeEndpointURL(selectedEndpoint.Url)),
	)

	// Generate router subscription ID (for EVM deduplication)
	// For Tendermint, the query string is the identifier, but we still generate
	// router IDs for internal tracking (not exposed to clients).
	// The id must be shaped like the one this chain issues, which the node just told us.
	numericIDs := firstMsg != nil && subscriptionIDsAreNumeric(firstMsg.Result)
	routerID := dwsm.generateRouterID(clientKey, numericIDs)

	// Extract upstream subscription ID based on API interface
	// - EVM chains: subscription ID is in the response result (hex string)
	// - Tendermint: subscription ID is the query parameter from the request
	upstreamID := getSubscriptionID(dwsm.apiInterface, firstMsg, subscriptionParams)
	dwsm.idMapper.RegisterMapping(routerID, upstreamID)

	// Store original result for Tendermint (needed when joining clients need the response)
	var originalResult json.RawMessage
	if firstMsg != nil && firstMsg.Result != nil {
		originalResult = firstMsg.Result
	}

	// Create first reply - preserves original format for Tendermint, uses router ID for EVM.
	// requestID is the client's original JSON-RPC id (extracted at the top of this function);
	// passing it explicitly ensures the response echoes the caller's id verbatim, including
	// non-numeric ids like string UUIDs (JSON-RPC 2.0 §4.2).
	firstReplyData, err := createSubscriptionReply(routerID, requestID, firstMsg, dwsm.apiInterface, numericIDs)
	if err != nil {
		upstreamSub.Unsubscribe()
		pool.NotifySubscriptionRemoved(conn)
		dwsm.settlePendingSubscription(hashedParams, false)
		dwsm.metricsManager.SetFailedWsSubscriptionRequestMetric(dwsm.chainID, dwsm.apiInterface)
		return nil, nil, fmt.Errorf("failed to create subscription reply: %w", err)
	}

	firstReply = &pairingtypes.RelayReply{
		Data: firstReplyData,
	}

	// The subscription is shared, so its lifetime belongs to the manager, not to the
	// client that happened to create it: WithoutCancel keeps the request values (GUID)
	// for log correlation but drops the creator's cancellation. Bound to the creator's
	// context, the first subscriber closing its socket ended the upstream listener
	// underneath every client that had joined, and the same rule already holds for
	// the gRPC manager (MAG-3722). Teardown goes through teardownSubscriptionLocked.
	subCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	// Store active subscription
	activeSub := &directActiveSubscription{
		upstreamPool:         pool,
		upstreamConnection:   conn, // Track which connection this subscription uses
		upstreamSubscription: upstreamSub,
		upstreamID:           upstreamID,
		clientRouterIDs:      make(map[string]string),
		hashedParams:         hashedParams,
		subscriptionParams:   subscriptionParams, // Store for restoration after reconnect
		subscribeMethod:      subscribeMethod,    // Store for restoration (eth_subscribe, subscribe, etc.)
		unsubscribeMethod:    unsubscribeMethod,  // Resolved from the spec above, for the teardown paths
		numericIDs:           numericIDs,         // Chain-level, so it outlives any one upstream id
		connectedClients:     make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		firstReply:           firstReply,
		originalResult:       originalResult, // Store for Tendermint joining clients
		ctx:                  subCtx,
		cancel:               cancel,
		closeSubChan:         make(chan struct{}),
		messagesChan:         msgChan, // Use the channel from upstream subscription
	}
	activeSub.attachClientLocked(clientKey, routerID, safeChannelSender)

	dwsm.lock.Lock()
	dwsm.activeSubscriptions[hashedParams] = activeSub
	if dwsm.connectedClients[clientKey] == nil {
		dwsm.connectedClients[clientKey] = make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply])
	}
	dwsm.connectedClients[clientKey][hashedParams] = safeChannelSender
	dwsm.lock.Unlock()

	// Increment global subscription counter
	dwsm.totalSubscriptions.Add(1)

	// Notify pending subscriptions of success
	dwsm.settlePendingSubscription(hashedParams, true)

	// Start listening for upstream messages
	go dwsm.listenForUpstreamMessages(subCtx, hashedParams, activeSub, upstreamSub, msgChan)

	// Handle client disconnect
	go dwsm.handleClientDisconnect(ctx, clientKey, hashedParams)

	utils.LavaFormatInfo("DirectWS: subscription started",
		utils.LogAttr("routerID", routerID),
		utils.LogAttr("upstreamID", upstreamID),
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)

	return firstReply, repliesChannel, nil
}

// Unsubscribe implements chainlib.WSSubscriptionManager.
// It handles an explicit unsubscribe request from a client.
// Returns the node's response bytes (streamed as-is) when successful; nil when no response available.
func (dwsm *DirectWSSubscriptionManager) Unsubscribe(
	ctx context.Context,
	protocolMessage chainlib.ProtocolMessage,
	dappID string,
	consumerIp string,
	webSocketConnectionUniqueId string,
	metricsData *metrics.RelayMetrics,
) ([]byte, error) {
	clientKey := dwsm.CreateWebSocketConnectionUniqueKey(dappID, consumerIp, webSocketConnectionUniqueId)

	// Check unsubscribe rate limiting
	if !dwsm.rateLimiter.AllowUnsubscribe(clientKey) {
		utils.LavaFormatWarning("DirectWS: client unsubscribe rate limit exceeded", nil,
			utils.LogAttr("clientKey", clientKey),
			utils.LogAttr("limit", dwsm.config.UnsubscribesPerMinutePerClient),
		)
		return nil, fmt.Errorf("unsubscribe rate limit exceeded: max %d unsubscribes per minute",
			dwsm.config.UnsubscribesPerMinutePerClient)
	}

	// Extract subscription ID from the unsubscribe message
	// - EVM: Router ID (hex string we generated)
	// - Tendermint: Query string (the actual query used to subscribe)
	subIDFromRequest, err := dwsm.extractSubscriptionIDFromUnsubscribe(protocolMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to extract subscription ID: %w", err)
	}

	// SECURITY: Validate that the subscription belongs to the calling client BEFORE removing it.
	dwsm.lock.Lock()

	// Find the subscription this client is connected to and verify ownership
	var activeSub *directActiveSubscription
	var hashedParams string
	var routerSubID string // The router ID for this client (needed for ID mapper cleanup)
	var clientOwnsSubscription bool

	for hp, sub := range dwsm.activeSubscriptions {
		if ownedRouterID, exists := sub.clientRouterIDs[clientKey]; exists {
			// For Tendermint: client unsubscribes using the query (which is also the upstream ID)
			// For EVM: client unsubscribes using the router ID we gave them, but accept the
			// upstream hex id too — some clients echo back the raw `result` they observed,
			// and (historically) responses leaked the upstream id, so being permissive here
			// is necessary for correctness. Ownership is already enforced by the outer
			// `clientRouterIDs[clientKey]` check, so accepting the upstream id is safe.
			if dwsm.apiInterface == "tendermintrpc" {
				if sub.upstreamID == subIDFromRequest {
					activeSub = sub
					hashedParams = hp
					routerSubID = ownedRouterID
					clientOwnsSubscription = true
					break
				}
			} else {
				if ownedRouterID == subIDFromRequest || sub.upstreamID == subIDFromRequest {
					activeSub = sub
					hashedParams = hp
					routerSubID = ownedRouterID
					clientOwnsSubscription = true
					break
				}
			}
		}
	}

	if !clientOwnsSubscription {
		dwsm.lock.Unlock()
		utils.LavaFormatWarning("DirectWS: unsubscribe rejected - client does not own subscription", nil,
			utils.LogAttr("subIDFromRequest", subIDFromRequest),
			utils.LogAttr("clientKey", clientKey),
		)
		return nil, common.SubscriptionNotFoundError
	}

	// Get upstream connection and params before unlock for the CallContext
	conn := activeSub.upstreamConnection
	upstreamName := dwsm.upstreamNameOf(activeSub)
	upstreamID := activeSub.upstreamID
	numericIDs := activeSub.numericIDs

	// The last client out tells the node. The subscription leaves the active map in this
	// same critical section, before the node is called: from here on it is one the router
	// has decided to drop, and a client that arrives for the same params while that call is
	// in flight creates a subscription of its own instead of joining this one. Joining was
	// a local step with no node call, so the joiner was handed an entry the node had
	// already stopped publishing — no pushes, and a channel nobody closed (MAG-4345). With
	// the entry gone, a restore that reaches its commit point while the call is in flight
	// finds nothing to commit onto and ends the upstream subscription it re-opened, the
	// MAG-2547 discard, so no restored id is left running on the node either. This client
	// stays attached until the local release below, so a push the node queued before it
	// applied the unsubscribe still reaches it.
	retired := len(activeSub.connectedClients) == 1
	if retired {
		dwsm.retireSubscriptionLocked(hashedParams, activeSub)
		dwsm.forgetSubscriptionForClientLocked(clientKey, hashedParams, false)
	}

	// The method to call upstream is the one the client actually invoked, not one
	// derived from the subscribe method. See resolveUnsubscribeMethod.
	unsubscribeMethod := resolveUnsubscribeMethod(protocolMessage, activeSub.unsubscribeMethod)
	requestID := extractRequestID(protocolMessage.RelayPrivateData().Data)

	// The node is told its own id, in its own shape, by the same renderer the teardown paths
	// use: Solana rejects the decimal string the router canonicalises to internally.
	unsubscribeParams := dwsm.unsubscribeParams(upstreamID, numericIDs)

	dwsm.lock.Unlock()

	// Call upstream node to unsubscribe only when last client - stream the actual node response
	var nodeResp []byte
	var callErr error
	if retired && conn != nil && unsubscribeMethod != "" {
		client := conn.GetClient()
		if client == nil || !conn.IsHealthy() {
			// The socket this subscription lived on is gone, and the node lost the subscription
			// with it, so there is nobody to tell. A call would only make rpcclient dial again to
			// say goodbye — into the outage, while the pool is still reconnecting — and fail the
			// client's unsubscribe. The local release below still runs (MAG-4242).
			utils.LavaFormatDebug("DirectWS: no upstream unsubscribe for a subscription that died with its connection",
				utils.LogAttr("upstreamID", upstreamID),
				utils.LogAttr("clientKey", clientKey),
			)
		} else {
			// Use a timeout: some gateways may not return eth_unsubscribe responses.
			// Without timeout, CallContext blocks forever and the client gets no reply.
			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			callStart := time.Now()
			var resp *rpcclient.JsonrpcMessage
			resp, callErr = client.CallContext(callCtx, requestID, unsubscribeMethod, unsubscribeParams, true, false)
			cancel()
			chainlib.SubscriptionUpstreamFrom(ctx).Sent(upstreamName, callStart, time.Since(callStart), callErr)
			if callErr != nil {
				// The client still leaves, and the subscription still ends locally: it was
				// taken out of the map above, and a gateway that never answers an unsubscribe
				// has usually applied it. Keeping the client on it would park it on exactly
				// the dead subscription this function exists to end. The error goes back to
				// the client with its reply.
				utils.LavaFormatWarning("DirectWS: upstream unsubscribe call failed", callErr,
					utils.LogAttr("upstreamID", upstreamID),
					utils.LogAttr("clientKey", clientKey),
				)
				callErr = fmt.Errorf("upstream unsubscribe failed: %w", callErr)
			} else if resp != nil {
				// Forward the node's response directly to the end user - no transformation.
				nodeResp, _ = json.Marshal(resp)
			}
		}
	}

	// Now do local cleanup under lock
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	dwsm.idMapper.RemoveMapping(routerSubID)
	activeSub.releaseClientLocked(clientKey)
	dwsm.forgetSubscriptionForClientLocked(clientKey, hashedParams, false)

	if retired {
		// The node was told above, or lost the subscription with its socket, so the
		// upstream side ends without a second call; the map and the counter were settled
		// in the first critical section.
		dwsm.endUpstreamSubscriptionLocked(activeSub, false)
	} else if len(activeSub.connectedClients) == 0 {
		// Not the last client when this started, but the others left while the lock was
		// released: the node has not been told yet, so this teardown tells it.
		dwsm.teardownSubscriptionLocked(hashedParams, activeSub, true)
	}

	return nodeResp, callErr
}

// resolveUnsubscribeMethod returns the method name to call upstream to tear a
// subscription down.
//
// It is the name the client actually invoked. The spec pairs every SUBSCRIBE
// parse directive with an UNSUBSCRIBE one carrying its own api_name, and the
// caller has already matched this message against those directives by name
// (consumer_websocket_manager.go dispatches here only when the message resolved
// to an UNSUBSCRIBE tag), so the api name on the message IS the spec's
// unsubscribe method. Nothing needs deriving.
//
// MAG-3297: it used to be derived from the subscribe method by string surgery,
// which only ever worked for two shapes. Every Substrate pair fell through those
// to a literal "unsubscribe" fallback — chain_subscribeNewHeads,
// state_subscribeStorage, subscribe_newHead and chainHead_v1_follow are none of
// them "<x>_subscribe" — so the router called a method the node does not have
// and forwarded its -32601 to the client.
//
// The DefaultApiName guard is the one that matters. A method the spec does not
// declare still gets an api, synthesised with Name "Default-<method>"
// (base_chain_parser.go defaultApiContainer), and sending that upstream would be
// a method no node has. It is unreachable from the single current caller — a
// "Default-" name matches no directive's ApiName, so such a message is never
// classified UNSUBSCRIBE and never arrives here — but the guard belongs to this
// function rather than to its caller's invariant, since the doc above invites
// future callers.
//
// resolved is the subscription's own unsubscribeMethod, the one the teardown
// paths use (resolveTeardownUnsubscribeMethod). It stands in for a message that
// names nothing usable, and may itself be empty when the spec declares no
// unsubscribe for the subscription — in which case there is no method to call
// and the caller's node call is skipped, as the teardown paths skip theirs.
func resolveUnsubscribeMethod(protocolMessage chainlib.ProtocolMessage, resolved string) string {
	if protocolMessage != nil {
		if api := protocolMessage.GetApi(); api != nil && api.Name != "" &&
			!strings.HasPrefix(api.Name, chainlib.DefaultApiName) {
			return api.Name
		}
	}
	return resolved
}

// UnsubscribeAll implements chainlib.WSSubscriptionManager.
// It removes all subscriptions for a specific client connection.
func (dwsm *DirectWSSubscriptionManager) UnsubscribeAll(
	ctx context.Context,
	dappID string,
	consumerIp string,
	webSocketConnectionUniqueId string,
	metricsData *metrics.RelayMetrics,
) error {
	clientKey := dwsm.CreateWebSocketConnectionUniqueKey(dappID, consumerIp, webSocketConnectionUniqueId)

	// Note: UnsubscribeAll is NOT rate limited because:
	// 1. It's a cleanup/teardown operation called when a WebSocket connection closes
	// 2. Rate limiting could leave orphaned subscriptions consuming resources
	// 3. It's a single batch operation, not a repeated action like individual unsubscribes
	// The rate limiter's AllowUnsubscribe is designed for individual eth_unsubscribe calls.

	utils.LavaFormatTrace("DirectWS: unsubscribe all request",
		utils.LogAttr("clientKey", clientKey),
	)

	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	clientSubs, ok := dwsm.connectedClients[clientKey]
	if !ok {
		// Nothing left to unsubscribe, but the client can still own per-client state: a
		// client that unsubscribed everything explicitly keeps its sticky endpoint and
		// rate limiters for the rest of its connection, and this is where the connection
		// ending hands them back. Returning early here left them behind (MAG-3722).
		dwsm.forgetClientLocked(clientKey)
		return nil
	}

	// Iterate over all subscriptions for this client
	for hashedParams := range clientSubs {
		activeSub, exists := dwsm.activeSubscriptions[hashedParams]
		if !exists {
			continue
		}

		clientRouterID, _ := activeSub.releaseClientLocked(clientKey)
		if clientRouterID != "" {
			dwsm.idMapper.RemoveMapping(clientRouterID)
		}

		// Last client out tells the node too. This path used to stop only the local
		// forwarding and leave the node publishing to a pooled connection (MAG-3722).
		if len(activeSub.connectedClients) == 0 {
			dwsm.teardownSubscriptionLocked(hashedParams, activeSub, true)
		}
	}

	delete(dwsm.connectedClients, clientKey)
	dwsm.forgetClientLocked(clientKey)

	return nil
}

// generateRouterID issues a router subscription id shaped like the ones this chain uses.
//
// Chains that number their subscriptions get a number, because the client hands the id
// straight back on unsubscribe. The router still issues its OWN id rather than passing the
// upstream one through: an upstream id does not survive a reconnect — handleUpstreamDisconnect
// re-subscribes and the node answers with a different one — whereas a router id is stable for
// the life of the client's subscription, which is the whole reason the indirection exists.
func (dwsm *DirectWSSubscriptionManager) generateRouterID(clientKey string, numeric bool) string {
	if numeric {
		return dwsm.idMapper.GenerateNumericRouterID()
	}
	return dwsm.idMapper.GenerateRouterID(clientKey)
}

// getHashedParams derives the key one subscription is tracked under: the method the client
// invoked, plus its params.
//
// The method has to be in the key. Substrate's subscription methods are overwhelmingly
// parameterless, so chain_subscribeNewHead, state_subscribeRuntimeVersion,
// chain_subscribeAllHeads and the rest all marshal to the same `[]` — hashing params alone
// gave every one of them the same key, and the first subscriber to register it silently
// owned the upstream stream for all the others. A client subscribed to runtime-version
// changes received new-head payloads re-stamped with its own id, with nothing to detect the
// substitution (MAG-3378). EVM never showed this because eth_subscribe carries its channel
// in the params, so its subscriptions hash apart on params alone.
//
// The composite mirrors the gRPC manager's hashSubscriptionParams, which has always keyed on
// (method path, request data). This is the WebSocket side catching up, not a new convention.
//
// Only StartSubscription calls this. Every other consumer of the key — Unsubscribe, the
// pending-subscription broadcast, the joining-client lookup, client disconnect, cleanup —
// receives it or iterates activeSubscriptions, so none of them recompute it and all follow
// automatically.
func (dwsm *DirectWSSubscriptionManager) getHashedParams(protocolMessage chainlib.ProtocolMessage) (hashedParams string, params []byte, err error) {
	params, err = gojson.Marshal(protocolMessage.GetRPCMessage().GetParams())
	if err != nil {
		return "", nil, utils.LavaFormatError("could not marshal params", err)
	}

	method := protocolMessage.GetApi().Name
	hashedParams = rpcclient.CreateHashFromParams([]byte(fmt.Sprintf("%s:%s", method, params)))
	return hashedParams, params, nil
}

// extractSubscriptionIDFromUnsubscribe extracts the subscription ID from an unsubscribe message
// Handles both EVM and Tendermint formats:
// - EVM (eth_unsubscribe): params is ["0x..."] (array with hex subscription ID)
// - Tendermint (unsubscribe): params is {"query": "tm.event='NewBlock'"} (object with query field)
func (dwsm *DirectWSSubscriptionManager) extractSubscriptionIDFromUnsubscribe(protocolMessage chainlib.ProtocolMessage) (string, error) {
	params := protocolMessage.GetRPCMessage().GetParams()
	if params == nil {
		return "", fmt.Errorf("no params in unsubscribe message")
	}

	paramsBytes, err := gojson.Marshal(params)
	if err != nil {
		return "", err
	}

	// For Tendermint: try to extract from object with "query" field
	// Format: {"query": "tm.event='NewBlock'"}
	if dwsm.apiInterface == "tendermintrpc" {
		var paramsMap map[string]any
		if err := gojson.Unmarshal(paramsBytes, &paramsMap); err == nil {
			if query, ok := paramsMap["query"].(string); ok && query != "" {
				return query, nil
			}
		}
	}

	// Everything else: a JSON-RPC array whose first element names the subscription — a
	// string on EVM and Substrate, a number on Solana. Decoding into json.RawMessage keeps
	// both shapes in one branch and hands them to the same canonicaliser the dispatcher
	// uses, rather than type-switching on `any` (MAG-3359).
	//
	// It does NOT rescue precision, despite the obvious temptation to claim so: the caller
	// hands us params that GetRPCMessage already decoded into `any`, so a large integer was
	// rounded to float64 before this function ran and paramsBytes above re-marshals the
	// rounded value. Router ids stay well inside 2^53 (see numericRouterIDBase), so nothing
	// is currently at risk — but the rounding happens upstream of here and a fix belongs
	// there, not in this decode.
	var paramsArray []json.RawMessage
	if err := gojson.Unmarshal(paramsBytes, &paramsArray); err == nil && len(paramsArray) > 0 {
		if id, ok := rpcclient.CanonicalSubscriptionID(paramsArray[0]); ok {
			return id, nil
		}
	}

	// Try as string directly
	if id, ok := params.(string); ok {
		return id, nil
	}

	return "", fmt.Errorf("could not extract subscription ID from params: %v", params)
}

// checkForActiveSubscriptionAndConnect checks if there's an active subscription and connects the client.
// If joining an existing subscription, generates a unique router ID for this client and registers it
// with the ID mapper. This ensures each client has their own subscription ID that maps to the
// shared upstream subscription, enabling proper unsubscribe behavior with deduplication.
// The requestID parameter is the client's original JSON-RPC request ID, which must be included
// in the response per JSON-RPC 2.0 spec.
func (dwsm *DirectWSSubscriptionManager) checkForActiveSubscriptionAndConnect(
	ctx context.Context,
	hashedParams string,
	clientKey string,
	safeChannelSender *common.SafeChannelSender[*pairingtypes.RelayReply],
	requestID json.RawMessage,
) (*pairingtypes.RelayReply, bool) {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	activeSub, found := dwsm.activeSubscriptions[hashedParams]
	if !found {
		return nil, false
	}

	// Check if client is already connected - return their existing router ID's reply
	if existingRouterID, exists := activeSub.clientRouterIDs[clientKey]; exists {
		// Create reply with client's existing router ID and their request ID
		// For Tendermint: uses originalResult to preserve {"query":"..."} format
		replyData, err := createSubscriptionReplyFromRouterID(existingRouterID, requestID, dwsm.apiInterface, activeSub.originalResult, activeSub.numericIDs)
		if err != nil {
			utils.LavaFormatWarning("DirectWS: failed to create reply for existing client", err)
			return activeSub.firstReply, false
		}
		return &pairingtypes.RelayReply{Data: replyData}, false
	}

	// Generate a unique router ID for this joining client (for EVM deduplication)
	// For Tendermint, we still generate IDs for internal tracking but don't expose them.
	// The joining client must be issued an id of the same shape as the one the first
	// client got, which only activeSub still knows.
	clientRouterID := dwsm.generateRouterID(clientKey, activeSub.numericIDs)

	// Register the mapping: this client's router ID -> shared upstream ID
	dwsm.idMapper.RegisterMapping(clientRouterID, activeSub.upstreamID)

	// Add client to existing subscription with their unique router ID
	activeSub.attachClientLocked(clientKey, clientRouterID, safeChannelSender)
	// A joining client leaves the same way a creating one does, usually by closing its
	// socket, so it needs the same watcher. Without one nothing ever released a joined
	// client: its reply channel stayed open and parked the ingress forwarder goroutine,
	// it kept counting as connected, and the upstream subscription outlived every real
	// subscriber (MAG-3722).
	go dwsm.handleClientDisconnect(ctx, clientKey, hashedParams)
	if dwsm.connectedClients[clientKey] == nil {
		dwsm.connectedClients[clientKey] = make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply])
	}
	dwsm.connectedClients[clientKey][hashedParams] = safeChannelSender

	utils.LavaFormatTrace("DirectWS: client joined existing subscription with unique router ID",
		utils.LogAttr("clientKey", clientKey),
		utils.LogAttr("clientRouterID", clientRouterID),
		utils.LogAttr("upstreamID", activeSub.upstreamID),
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)

	// Create first reply with this client's unique router ID and their request ID
	// For Tendermint: uses originalResult to preserve {"query":"..."} format
	replyData, err := createSubscriptionReplyFromRouterID(clientRouterID, requestID, dwsm.apiInterface, activeSub.originalResult, activeSub.numericIDs)
	if err != nil {
		utils.LavaFormatWarning("DirectWS: failed to create reply for joining client", err)
		return activeSub.firstReply, true // Fallback to original reply
	}

	return &pairingtypes.RelayReply{Data: replyData}, true // Joined existing subscription
}

// checkAndAddPendingSubscription checks for pending subscriptions.
//
// It returns (nil, false) when the caller is the first for these params and must create the
// subscription, or (ch, true) when the caller must wait on ch and then join. The caller has
// already looked for an active subscription, but under a separate acquisition of dwsm.lock:
// in the gap between that look and this one a creator can both store its subscription and
// clear its pending entry, so a caller arriving here would see neither and become a second
// creator. That opened a second upstream subscription for the same params, overwrote the
// first map entry (orphaning its listener goroutine and its node-side subscription), and
// incremented totalSubscriptions twice for an entry that is dropped once (MAG-2546). A
// caller that finds the subscription active is therefore handed an already-settled success
// so it takes the join path.
func (dwsm *DirectWSSubscriptionManager) checkAndAddPendingSubscription(hashedParams string) (chan bool, bool) {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	if _, active := dwsm.activeSubscriptions[hashedParams]; active {
		settled := make(chan bool, 1)
		settled <- true
		return settled, true
	}

	pending, found := dwsm.pendingSubscriptions[hashedParams]
	if !found {
		// No pending, we are the first - create pending entry
		dwsm.pendingSubscriptions[hashedParams] = &pendingSubscriptionsBroadcastManager{}
		return nil, false
	}

	// There's a pending subscription, wait for it
	listenChan := make(chan bool, 1)
	pending.broadcastChannelList = append(pending.broadcastChannelList, listenChan)
	return listenChan, true
}

// settlePendingSubscription reports the creator's outcome to every client waiting on it and
// retires the entry.
func (dwsm *DirectWSSubscriptionManager) settlePendingSubscription(hashedParams string, created bool) {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	pending, found := dwsm.pendingSubscriptions[hashedParams]
	if !found {
		return
	}
	pending.broadcastToChannelList(created)
	delete(dwsm.pendingSubscriptions, hashedParams)
}

// createUpstreamSubscription creates a subscription on the upstream WebSocket
// Returns the subscription, first message, message channel, and error
// The subscribeMethod parameter allows using different subscription methods for different protocols:
// - EVM chains: "eth_subscribe"
// - Tendermint: "subscribe"
func (dwsm *DirectWSSubscriptionManager) createUpstreamSubscription(
	ctx context.Context,
	conn *UpstreamWSConnection,
	params []byte,
	subscribeMethod string,
) (*rpcclient.ClientSubscription, *rpcclient.JsonrpcMessage, chan *rpcclient.JsonrpcMessage, error) {
	client := conn.GetClient()
	if client == nil {
		return nil, nil, nil, fmt.Errorf("no WebSocket client available")
	}

	// Create channel for subscription messages - this is where upstream will deliver notifications
	msgChan := make(chan *rpcclient.JsonrpcMessage, 100)

	// Parse params back to interface for the Subscribe call
	var subscriptionParams interface{}
	if err := json.Unmarshal(params, &subscriptionParams); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to unmarshal params: %w", err)
	}

	utils.LavaFormatTrace("DirectWS: creating upstream subscription",
		utils.LogAttr("method", subscribeMethod),
		utils.LogAttr("apiInterface", dwsm.apiInterface),
	)

	// Subscribe using rpcclient with the appropriate method for the protocol
	sub, firstMsg, err := client.Subscribe(ctx, nil, subscribeMethod, msgChan, subscriptionParams)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("upstream subscribe failed: %w", err)
	}

	// Subscribe reports "upstream answered with a JSON-RPC error object" as (nil, resp, nil)
	// — a nil subscription with a NIL error, so the check above does not catch it. That is a
	// failed subscribe, not a successful one: returning it would hand a nil
	// *rpcclient.ClientSubscription to listenForUpstreamMessages, which stores it in an
	// upstreamErrSource interface (non-nil interface, nil pointer) and dereferences it on
	// upstreamSub.Err() — panicking and taking the process down. Surface the node's error
	// to the caller instead. See MAG-2685.
	if sub == nil {
		if firstMsg != nil && firstMsg.Error != nil {
			return nil, nil, nil, fmt.Errorf("upstream rejected subscribe: %w", firstMsg.Error)
		}
		return nil, nil, nil, fmt.Errorf("upstream subscribe returned no subscription")
	}

	return sub, firstMsg, msgChan, nil
}

// upstreamErrSource is the minimal slice of *rpcclient.ClientSubscription that
// listenForUpstreamMessages actually needs. Taking an interface here lets tests drive the
// error-handling branch with a local fake without exposing test-only constructors from the
// rpcclient package. *rpcclient.ClientSubscription satisfies it via its Err() method.
type upstreamErrSource interface {
	Err() <-chan error
}

// listenForUpstreamMessages listens for messages from upstream and routes to clients.
//
// On an upstream error we hand off to handleUpstreamDisconnect, which either restores the
// subscription (spawning a fresh listener) or cleans it up. We must NOT run cleanup from this
// goroutine in that case — handleUpstreamDisconnect mutates activeSub in place to point at the
// new upstream subscription, and a deferred cleanup here would wipe the just-restored entry
// from dwsm.activeSubscriptions, leaving notifications flowing through the new listener (which
// reads activeSub directly) while making the unsubscribe lookup fail with "subscription not
// found". reconnectInFlight is true when ownership has been handed off to that goroutine.
//
// Cleanup ownership: once reconnectInFlight is set, handleUpstreamDisconnect is responsible
// for calling cleanupSubscription on every failure path (reconnect timeout, GetConnection
// failure post-reconnect, re-subscribe failure) so a failed restoration does not leak the
// subscription. See handleUpstreamDisconnect for the corresponding cleanup-on-failure calls.
//
// messagesChan is the channel of the upstream subscription this listener serves,
// handed in rather than read off activeSub on every loop: a reconnect replaces the
// subscription's channel under dwsm.lock and starts a fresh listener for it, so a
// listener reading the field unlocked would race that write. Each listener owns
// one channel for its whole life.
func (dwsm *DirectWSSubscriptionManager) listenForUpstreamMessages(
	ctx context.Context,
	hashedParams string,
	activeSub *directActiveSubscription,
	upstreamSub upstreamErrSource,
	messagesChan <-chan *rpcclient.JsonrpcMessage,
) {
	reconnectInFlight := false
	defer func() {
		if !reconnectInFlight {
			dwsm.cleanupSubscription(hashedParams, activeSub)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			utils.LavaFormatTrace("DirectWS: subscription context done",
				utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
			)
			return

		case <-activeSub.closeSubChan:
			utils.LavaFormatTrace("DirectWS: subscription close requested",
				utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
			)
			return

		case err := <-upstreamSub.Err():
			if err != nil {
				// A JSON-RPC error pushed on the subscription is the node ending it, on a
				// connection that is still healthy. Restoring would resubscribe at once on
				// that same connection, with no backoff, for the node to end it again: clean
				// up instead (the deferred cleanupSubscription releases the clients).
				var nodeErr *rpcclient.JsonError
				if errors.As(err, &nodeErr) {
					utils.LavaFormatWarning("DirectWS: upstream ended the subscription with an error", err,
						utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
					)
					return
				}
				utils.LavaFormatWarning("DirectWS: upstream subscription error",
					err,
					utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
				)
				dwsm.retireConnectionOnUpstreamError(activeSub, err)
				reconnectInFlight = true
				go dwsm.handleUpstreamDisconnect(ctx, hashedParams, activeSub)
			}
			return

		case msg := <-messagesChan:
			if msg == nil {
				continue
			}
			dwsm.routeMessageToClients(activeSub, msg)
		}
	}
}

// routeMessageToClients routes an upstream message to all connected clients.
// Each client receives the message with their unique subscription ID (router ID)
// rewritten in the notification params. This enables proper deduplication where
// clients can unsubscribe individually without affecting other clients.
func (dwsm *DirectWSSubscriptionManager) routeMessageToClients(
	activeSub *directActiveSubscription,
	msg *rpcclient.JsonrpcMessage,
) {
	dwsm.lock.RLock()
	// Copy client channels and their router IDs
	type clientInfo struct {
		sender   *common.SafeChannelSender[*pairingtypes.RelayReply]
		routerID string
	}
	clients := make(map[string]clientInfo)
	for clientKey, sender := range activeSub.connectedClients {
		routerID := activeSub.clientRouterIDs[clientKey]
		clients[clientKey] = clientInfo{sender: sender, routerID: routerID}
	}
	dwsm.lock.RUnlock()

	// Rewrite subscription ID per-client and send
	for clientKey, info := range clients {
		// Rewrite the message with this client's unique router ID
		rewrittenMsg, err := rewriteSubscriptionID(msg, info.routerID, activeSub.numericIDs)
		if err != nil {
			utils.LavaFormatWarning("DirectWS: failed to rewrite subscription ID for client", err,
				utils.LogAttr("clientKey", clientKey),
			)
			continue
		}

		reply := &pairingtypes.RelayReply{
			Data: rewrittenMsg,
		}

		switch info.sender.SendWithin(reply, subscriptionClientSendGrace) {
		case common.TrySendFull:
			dwsm.dropSlowClient(activeSub.hashedParams, activeSub, clientKey)
			continue
		case common.TrySendClosed:
			// The client is leaving by another path, which releases it. It was not slow.
			continue
		}
		utils.LavaFormatTrace("DirectWS: sent message to client with unique router ID",
			utils.LogAttr("clientKey", clientKey),
			utils.LogAttr("routerID", info.routerID),
		)
	}
}

// dropSlowClient releases a client whose reply channel is full: it has fallen
// subscriptionClientReplyBuffer pushes behind and freed no room within
// subscriptionClientSendGrace. Its channel closes, and the connection is marked so that the
// ingress, once it has handed the client what the channel still holds, closes the client's socket
// with 1013 (try again later): the client reconnects and resubscribes, and its other
// subscriptions on that socket end with it. That replaces losing the push silently (a gap in a
// subscription stream is worse than a stream that visibly ends) and waiting for the client on
// every push (which stalled every other client of the shared subscription). The drop is counted,
// and the subscription is torn down when this was its last client. A client already gone by
// another path is left alone.
func (dwsm *DirectWSSubscriptionManager) dropSlowClient(hashedParams string, activeSub *directActiveSubscription, clientKey string) {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	if dwsm.activeSubscriptions[hashedParams] != activeSub {
		return
	}
	if !dwsm.detachClientLocked(hashedParams, activeSub, clientKey, false) {
		return
	}
	// Under the same lock as the channel's close, so the ingress, which reads the mark once it
	// sees the close, cannot miss it.
	dwsm.slowClients[clientKey] = struct{}{}
	if dwsm.metricsManager != nil {
		dwsm.metricsManager.SetSubscriptionClientDroppedMetric(dwsm.chainID, dwsm.apiInterface)
	}
	utils.LavaFormatWarning("DirectWS: client is not keeping up with its subscription, dropping it", nil,
		utils.LogAttr("clientKey", clientKey),
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		utils.LogAttr("pushesBehind", subscriptionClientReplyBuffer),
	)
}

// retireConnectionOnUpstreamError tells the pool that the connection a subscription just failed
// on is dead, so the restore's ReconnectWithBackoff reconnects — with its backoff ladder — instead
// of finding the pool "healthy" and handing the dead socket straight back to the re-subscribe,
// which then had one dial inside rpcclient and no retry (MAG-4242). A subscription's own end
// leaves the socket alone: the forward queue overflowing says nothing about the connection, and
// the restore re-subscribes on it. A JSON-RPC error the node pushed on the subscription never
// gets here: the listener reads that as the node ending the subscription and cleans it up.
//
// A client that redialed on its own is the third error that leaves the connection alone.
// rpcclient replaces a socket itself when a write fails while the old read loop is still
// running, and ends the old socket's subscriptions with ErrClientReconnected — by which time
// it already holds a new, working socket. Retiring the connection there closes that client,
// which is the opposite of a recovery: the pool dials again for no reason, and every OTHER
// subscription sharing the pooled connection is ended too, including one a restore has just
// re-created on the new socket. MAG-4037 reaches this through an ordinary call whose deadline
// has already passed. The restore still runs; it finds the pool healthy and re-subscribes over
// the socket rpcclient opened (MAG-4396).
func (dwsm *DirectWSSubscriptionManager) retireConnectionOnUpstreamError(activeSub *directActiveSubscription, err error) {
	if errors.Is(err, rpcclient.ErrSubscriptionQueueOverflow) || errors.Is(err, rpcclient.ErrClientReconnected) {
		return
	}
	dwsm.lock.RLock()
	conn := activeSub.upstreamConnection
	dwsm.lock.RUnlock()
	if conn == nil {
		return
	}
	conn.MarkUnhealthy(err)
	utils.LavaFormatInfo("DirectWS: upstream connection marked dead, the pool will reconnect with backoff",
		utils.LogAttr("endpoint", conn.sanitizedURL),
		utils.LogAttr("error", err.Error()),
	)
}

// handleUpstreamDisconnect handles upstream connection loss and attempts to restore subscriptions
func (dwsm *DirectWSSubscriptionManager) handleUpstreamDisconnect(
	ctx context.Context,
	hashedParams string,
	activeSub *directActiveSubscription,
) {
	// Prevent concurrent restoration attempts
	if !activeSub.restoring.CompareAndSwap(false, true) {
		utils.LavaFormatDebug("DirectWS: restoration already in progress",
			utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		)
		return
	}
	defer activeSub.restoring.Store(false)

	dwsm.lock.RLock()
	connectedClients := len(activeSub.clientRouterIDs)
	dwsm.lock.RUnlock()
	utils.LavaFormatInfo("DirectWS: handling upstream disconnect",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		utils.LogAttr("connectedClients", connectedClients),
	)

	// Bound the restores. A restored subscription that stayed up for restoreStableAfter was
	// progress, even on a quiet one that delivered nothing since; only restores that keep
	// failing sooner add up. Without this a node that accepts a subscribe and drops it at once
	// is re-subscribed for the life of the process.
	if last := activeSub.lastRestoredAt.Load(); last != 0 && time.Since(time.Unix(0, last)) >= restoreStableAfter {
		activeSub.restoresWithoutProgress.Store(0)
	}
	if attempt := activeSub.restoresWithoutProgress.Add(1); attempt > maxRestoresWithoutProgress {
		utils.LavaFormatWarning("DirectWS: subscription keeps failing, giving it up", nil,
			utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
			utils.LogAttr("restoresWithoutProgress", attempt-1),
			utils.LogAttr("connectedClients", connectedClients),
		)
		dwsm.cleanupSubscription(hashedParams, activeSub)
		return
	}

	// Try to reconnect the WebSocket connection
	err := activeSub.upstreamPool.ReconnectWithBackoff(ctx)
	if err != nil {
		utils.LavaFormatError("DirectWS: reconnection failed, cleaning up subscription", err,
			utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		)
		dwsm.cleanupSubscription(hashedParams, activeSub)
		return
	}

	// Connection restored, now restore the subscription
	utils.LavaFormatInfo("DirectWS: connection restored, restoring subscription",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)

	// Get the restored connection, with this subscription's slot counted on it already, so
	// releasing the old connection below cannot scale the new one down as unused.
	conn, err := activeSub.upstreamPool.ReserveConnectionForSubscription(ctx)
	if err != nil {
		utils.LavaFormatError("DirectWS: failed to get connection after reconnect", err,
			utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		)
		dwsm.cleanupSubscription(hashedParams, activeSub)
		return
	}

	// Re-subscribe using the stored params and method
	newUpstreamSub, firstMsg, newMsgChan, err := dwsm.createUpstreamSubscription(ctx, conn, activeSub.subscriptionParams, activeSub.subscribeMethod)
	if err != nil {
		activeSub.upstreamPool.NotifySubscriptionRemoved(conn)
		utils.LavaFormatError("DirectWS: failed to restore subscription", err,
			utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		)
		dwsm.cleanupSubscription(hashedParams, activeSub)
		return
	}

	// Extract new upstream subscription ID based on API interface
	newUpstreamID := getSubscriptionID(dwsm.apiInterface, firstMsg, activeSub.subscriptionParams)

	if dwsm.beforeRestoreCommit != nil {
		dwsm.beforeRestoreCommit()
	}

	// Update the ID mapping, message channel, and connection bookkeeping
	dwsm.lock.Lock()
	if dwsm.activeSubscriptions[hashedParams] != activeSub {
		// The subscription was torn down while the node was answering the re-subscribe:
		// its last client left, or it was dropped, and the teardown released everything
		// the old upstream subscription held. Nothing owns the one just created, so it
		// ends here — locally, on the node, and in the connection's count — instead of
		// living on as an upstream subscription nobody reads and a count that stops the
		// pool from ever scaling the connection down (MAG-2547).
		dwsm.lock.Unlock()
		utils.LavaFormatInfo("DirectWS: subscription was torn down while being restored, discarding the restored upstream subscription",
			utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
			utils.LogAttr("newUpstreamID", newUpstreamID),
		)
		newUpstreamSub.Unsubscribe()
		// The subscription the discard ends is the one just re-opened, so the id is the new
		// one and the connection the one it was opened on; everything else is the
		// subscription's own, unchanged by the restore.
		discard := teardownOfLocked(activeSub)
		discard.conn = conn
		discard.upstreamID = newUpstreamID
		dwsm.teardownUpstreamSubscription(discard)
		activeSub.upstreamPool.NotifySubscriptionRemoved(conn)
		return
	}
	oldUpstreamID := activeSub.upstreamID
	// What the node has to be told to drop the subscription being replaced, snapshotted
	// under the lock before the fields below are rewritten.
	oldUpstream := teardownOfLocked(activeSub)
	oldConnection := activeSub.upstreamConnection // Keep reference to old connection for cleanup

	activeSub.upstreamSubscription = newUpstreamSub
	activeSub.upstreamID = newUpstreamID
	activeSub.upstreamConnection = conn // Update to new connection
	activeSub.messagesChan = newMsgChan // Update to new channel from upstream

	// Collect all client router IDs to re-register
	clientRouterIDs := make([]string, 0, len(activeSub.clientRouterIDs))
	for _, routerID := range activeSub.clientRouterIDs {
		clientRouterIDs = append(clientRouterIDs, routerID)
	}
	dwsm.lock.Unlock()

	// Tell the node to drop the subscription that was replaced.
	//
	// A restore does not always follow a lost socket. rpcclient ends one subscription on a
	// connection that stays up when its forward queue overflows, and ends the subscriptions
	// of a socket it has itself replaced; the restore then re-subscribes over a live
	// connection the node still holds the old subscription on. Nothing told it to stop, so it
	// kept publishing frames rpcclient read and dropped, and every overflow left one more of
	// those behind (MAG-4396). Where the socket did drop, the node lost the subscription with
	// it and teardownUpstreamSubscription skips a dead connection, so the same call is right
	// on both paths.
	//
	// The local rpcclient subscription is deliberately NOT unsubscribed here. Unsubscribe
	// closes its Err channel, and a listener still parked on that channel reads the close as
	// a nil error, ends, and runs its deferred cleanup — tearing down the subscription this
	// restore has just installed. The listener that handed off has already returned, so
	// nothing is parked there on the restore path, but the margin is one scheduling decision
	// wide and the only thing it buys is dropping rpcclient's handler entry for a replaced
	// subscription, which the connection's own close does anyway.
	//
	// On its own goroutine, because the node call can take upstreamUnsubscribeTimeout and the
	// restored subscription's listener must start now — its channel holds what the node
	// pushes in the meantime. The pool notify goes after the node call, so the connection is
	// still live while it is used.
	go func() {
		dwsm.teardownUpstreamSubscription(oldUpstream)
		// Decrements the subscription count on the stale connection — also when the pool
		// handed back the same connection object, which it counted again for the restored
		// subscription.
		if oldConnection != nil {
			activeSub.upstreamPool.NotifySubscriptionRemoved(oldConnection)
		}
	}()

	// Update the ID mapper - remove old mappings and re-register all client router IDs
	// to the new upstream ID
	dwsm.idMapper.RemoveAllForUpstream(oldUpstreamID)
	for _, routerID := range clientRouterIDs {
		dwsm.idMapper.RegisterMapping(routerID, newUpstreamID)
	}

	activeSub.lastRestoredAt.Store(time.Now().UnixNano())

	utils.LavaFormatInfo("DirectWS: subscription restored successfully",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		utils.LogAttr("oldUpstreamID", oldUpstreamID),
		utils.LogAttr("newUpstreamID", newUpstreamID),
		utils.LogAttr("connectedClients", len(clientRouterIDs)),
	)

	// Start listening for messages on the new subscription
	go dwsm.listenForUpstreamMessages(activeSub.ctx, hashedParams, activeSub, newUpstreamSub, newMsgChan)
}

// handleClientDisconnect releases one client from one subscription when the client's
// connection ends, or returns early once the client has already left by another path.
// It is registered for every attached client, creators and joiners alike.
func (dwsm *DirectWSSubscriptionManager) handleClientDisconnect(
	ctx context.Context,
	clientKey string,
	hashedParams string,
) {
	dwsm.lock.RLock()
	var detached <-chan struct{}
	if activeSub, found := dwsm.activeSubscriptions[hashedParams]; found {
		detached = activeSub.detachedSignal(clientKey)
	}
	dwsm.lock.RUnlock()

	select {
	case <-ctx.Done():
	case <-detached:
		// An explicit unsubscribe or a teardown already released the client. Nothing is
		// left to do, and no reason to stay parked until the socket finally closes.
		return
	}

	utils.LavaFormatTrace("DirectWS: client disconnected",
		utils.LogAttr("clientKey", clientKey),
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)

	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	if activeSub, found := dwsm.activeSubscriptions[hashedParams]; found {
		dwsm.detachClientLocked(hashedParams, activeSub, clientKey, true)
	}
}

// detachClientLocked releases one client from one subscription: its reply channel closes, its
// router id is forgotten, and the subscription is torn down, telling the node, when this was
// its last client. clientGone says the client's connection is over, so its per-client state
// goes too once it holds no other subscription. Reports false, and does nothing, when the
// client was no longer attached. Caller holds dwsm.lock.
func (dwsm *DirectWSSubscriptionManager) detachClientLocked(hashedParams string, activeSub *directActiveSubscription, clientKey string, clientGone bool) bool {
	clientRouterID, wasAttached := activeSub.releaseClientLocked(clientKey)
	if !wasAttached {
		return false
	}
	if clientRouterID != "" {
		dwsm.idMapper.RemoveMapping(clientRouterID)
	}
	dwsm.forgetSubscriptionForClientLocked(clientKey, hashedParams, clientGone)
	if len(activeSub.connectedClients) == 0 {
		dwsm.teardownSubscriptionLocked(hashedParams, activeSub, true)
	}
	return true
}

// forgetSubscriptionForClientLocked drops hashedParams from the client's subscription set.
// When that was the client's last subscription and the client itself is gone, the
// per-client state (sticky endpoint, rate limiters, id counter) goes with it. An explicit
// unsubscribe keeps the rate limiters, otherwise subscribe/unsubscribe cycles would reset
// the subscribe budget. Caller holds dwsm.lock.
func (dwsm *DirectWSSubscriptionManager) forgetSubscriptionForClientLocked(clientKey, hashedParams string, clientGone bool) {
	clientSubs, ok := dwsm.connectedClients[clientKey]
	if !ok {
		return
	}
	delete(clientSubs, hashedParams)
	if len(clientSubs) > 0 {
		return
	}
	delete(dwsm.connectedClients, clientKey)
	if !clientGone {
		return
	}
	dwsm.forgetClientLocked(clientKey)
}

// forgetClientLocked drops every per-client record once the client's connection is gone.
// Caller holds dwsm.lock.
func (dwsm *DirectWSSubscriptionManager) forgetClientLocked(clientKey string) {
	dwsm.stickyStore.Delete(clientKey)
	dwsm.rateLimiter.CleanupClient(clientKey)
	delete(dwsm.slowClients, clientKey)
	utils.LavaFormatTrace("DirectWS: cleared sticky session and rate limiter for a gone client",
		utils.LogAttr("clientKey", clientKey),
	)
}

// teardownSubscriptionLocked retires a subscription that has no clients left. The node is
// told first when tellNode is set, while the connection is still pooled and live, then the
// local subscription ends and the pool may scale the connection down. The network call runs
// off the lock. Caller holds dwsm.lock.
func (dwsm *DirectWSSubscriptionManager) teardownSubscriptionLocked(hashedParams string, activeSub *directActiveSubscription, tellNode bool) {
	dwsm.endUpstreamSubscriptionLocked(activeSub, tellNode)
	dwsm.retireSubscriptionLocked(hashedParams, activeSub)
}

// retireSubscriptionLocked takes activeSub out of the active map, so no client can find and
// join it from here on, and stops counting it. Only the entry that IS activeSub is removed:
// the map is keyed by params alone, so a successor may already hold the key by the time a
// caller that read activeSub earlier gets here. Unsubscribe retires its subscription before it
// calls the node and ends the upstream side afterwards, so the two halves of a teardown are
// separate. Caller holds dwsm.lock.
func (dwsm *DirectWSSubscriptionManager) retireSubscriptionLocked(hashedParams string, activeSub *directActiveSubscription) {
	if dwsm.activeSubscriptions[hashedParams] == activeSub {
		delete(dwsm.activeSubscriptions, hashedParams)
	}
	dwsm.totalSubscriptions.Add(-1)
}

// endUpstreamSubscriptionLocked ends the upstream half of a subscription: the node is told
// when tellNode is set, the local rpcclient subscription and listener end, and the pooled
// connection stops counting it. The network call runs off the lock. Caller holds dwsm.lock.
func (dwsm *DirectWSSubscriptionManager) endUpstreamSubscriptionLocked(activeSub *directActiveSubscription, tellNode bool) {
	// Everything the goroutine needs is read here, under the lock. A restore that is
	// mid-flight rewrites upstreamSubscription, upstreamID and upstreamConnection under
	// this same lock once the node answers, so reading them off the lock raced that
	// write and could tear down whichever half of the restore it happened to see.
	teardown := teardownOfLocked(activeSub)
	upstreamPool := activeSub.upstreamPool
	upstreamSub := activeSub.upstreamSubscription
	go func() {
		if tellNode {
			dwsm.teardownUpstreamSubscription(teardown)
		}
		if upstreamSub != nil {
			upstreamSub.Unsubscribe()
		}
		activeSub.cancel()
		activeSub.stopUpstreamListener()
		if teardown.conn != nil {
			upstreamPool.NotifySubscriptionRemoved(teardown.conn)
		}
	}()
}

// upstreamUnsubscribeTimeout bounds the teardown call below. Some gateways never
// answer an unsubscribe; without a bound the disconnect path would block on one.
const upstreamUnsubscribeTimeout = 10 * time.Second

const (
	// maxRestoresWithoutProgress bounds how many times one subscription is restored without
	// progress before it is given up. Progress is a restored subscription that stayed up for
	// restoreStableAfter. Same bound and the same reasoning as the gRPC manager's
	// defaultMaxRestoresWithoutProgress, which this mirrors.
	maxRestoresWithoutProgress = 5

	// restoreStableAfter is how long a restored subscription must stay up to count as
	// progress. Uptime rather than a delivered push: a quiet subscription can go minutes
	// without one, so the reconnects a load balancer's connection age limit causes now and
	// then must not use up its restores; and a subscription that delivers and then fails
	// each time it is reopened has made no progress, however much it delivered.
	restoreStableAfter = 30 * time.Second
)

// teardownUpstreamSubscription tells the NODE to stop pushing.
//
// This is the half a client disconnect skipped. ClientSubscription.Unsubscribe
// only signals this process's forwarding goroutine — unlike upstream go-ethereum
// it sends nothing on the wire — so the explicit call below is the only thing
// that reaches the node. Without it the node keeps publishing a subscription
// nobody reads, and because the connection is POOLED the next subscription to
// the same query inherits those pushes: every client that closed its socket
// without an explicit unsubscribe adds one more copy of every event, to every
// future subscriber, until the process restarts.
//
// Measured on a Tendermint tenant before the fix: copies per block climbed
// 7 -> 8 -> 9 -> 10 over four sequential subscribe/disconnect cycles, with one
// client connected each time (MAG-3366). The explicit-unsubscribe path always
// did this correctly, which is why it went unnoticed — a well-behaved client
// sends unsubscribe, and a real one just closes the socket.
//
// Best-effort by design: it also runs where the connection may already be gone
// (upstream disconnect, shutdown), so a failure is traced rather than warned.
// Must be called WITHOUT dwsm.lock held — it makes a network call. It takes a snapshot
// rather than reading the fields off the subscription, because a restore in flight rewrites
// them under dwsm.lock while this runs off it (MAG-2547): the caller snapshots while it
// still holds the lock.
func (dwsm *DirectWSSubscriptionManager) teardownUpstreamSubscription(teardown upstreamSubscriptionTeardown) {
	if teardown.conn == nil {
		return
	}
	if teardown.method == "" {
		// The spec declares no unsubscribe this subscription can be dropped with, and the
		// warning naming it was logged when the subscription was created. Calling a method
		// the node does not have cannot stop it publishing; it only costs a round trip and
		// a confusing node error. See resolveTeardownUnsubscribeMethod.
		utils.LavaFormatTrace("DirectWS: no upstream unsubscribe for a subscription the spec gives no unsubscribe method",
			utils.LogAttr("upstreamID", teardown.upstreamID),
			utils.LogAttr("subscribeMethod", teardown.subscribeMethod),
		)
		return
	}
	if !teardown.conn.IsHealthy() {
		// The socket this subscription lived on is gone, and the node lost the subscription
		// with it. A call here would only make rpcclient dial again to say goodbye.
		utils.LavaFormatTrace("DirectWS: no upstream unsubscribe for a subscription that died with its connection",
			utils.LogAttr("upstreamID", teardown.upstreamID),
		)
		return
	}
	client := teardown.conn.GetClient()
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), upstreamUnsubscribeTimeout)
	defer cancel()
	if _, err := client.CallContext(ctx, json.RawMessage("1"),
		teardown.method, dwsm.unsubscribeParams(teardown.upstreamID, teardown.numericIDs), true, false); err != nil {
		utils.LavaFormatTrace("DirectWS: upstream unsubscribe on disconnect failed",
			utils.LogAttr("error", err),
			utils.LogAttr("method", teardown.method),
			utils.LogAttr("upstreamID", teardown.upstreamID),
		)
	}
}

// upstreamSubscriptionTeardown is everything the node has to be told to drop one upstream
// subscription, snapshotted under dwsm.lock by the caller. One type for the three
// router-initiated paths (the last client leaving, a failed restore's cleanup, the discard of
// a re-opened subscription), so none of them can pick a different method or id shape than the
// others again — which is how the teardown path came to send a method only Tendermint has
// while the explicit path sent the spec's (MAG-4278 item 1).
type upstreamSubscriptionTeardown struct {
	conn            *UpstreamWSConnection
	upstreamID      string
	method          string // resolved at subscribe time; empty means the node is told nothing
	subscribeMethod string // for the log line only
	numericIDs      bool
}

// teardownOfLocked snapshots what the node has to be told to drop activeSub's current upstream
// subscription. Caller holds dwsm.lock.
func teardownOfLocked(activeSub *directActiveSubscription) upstreamSubscriptionTeardown {
	return upstreamSubscriptionTeardown{
		conn:            activeSub.upstreamConnection,
		upstreamID:      activeSub.upstreamID,
		method:          activeSub.unsubscribeMethod,
		subscribeMethod: activeSub.subscribeMethod,
		numericIDs:      activeSub.numericIDs,
	}
}

// unsubscribeParams renders the params that name upstreamID to the node, in the shape its
// chain uses. Shared by the explicit-unsubscribe path and the teardown paths: Solana rejects
// the decimal string the router canonicalises to internally, and the teardown path sending the
// string while the explicit path sent the number is half of MAG-4278 item 1.
func (dwsm *DirectWSSubscriptionManager) unsubscribeParams(upstreamID string, numericIDs bool) interface{} {
	if dwsm.apiInterface == "tendermintrpc" {
		return map[string]interface{}{"query": upstreamID}
	}
	return []interface{}{subscriptionIDValue(upstreamID, numericIDs)}
}

// cleanupSubscription cleans up a subscription, but only the one its caller owns. The map
// is keyed by params alone, so once owner has been torn down a new client can register a
// fresh subscription under the same key; a late caller (a listener ending, a restore
// failing) that cleaned up by key would then destroy that newer subscription.
func (dwsm *DirectWSSubscriptionManager) cleanupSubscription(hashedParams string, owner *directActiveSubscription) {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	activeSub, found := dwsm.activeSubscriptions[hashedParams]
	if !found {
		return
	}
	if activeSub != owner {
		utils.LavaFormatTrace("DirectWS: subscription under this key is not the one being cleaned up, leaving it",
			utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
		)
		return
	}

	// Cancelled now, under the lock, as cleanup always has; the teardown below would only
	// cancel it after the node call.
	activeSub.cancel()
	dwsm.dropSubscriptionLocked(hashedParams, activeSub)

	utils.LavaFormatTrace("DirectWS: subscription cleaned up",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)
}

var _ chainlib.WSSubscriptionEndReporter = (*DirectWSSubscriptionManager)(nil)

// SubscriptionEndError implements chainlib.WSSubscriptionEndReporter:
// chainlib.ErrSubscriptionClientTooSlow when dropSlowClient dropped a subscription of this
// connection, nil for every other end of a subscription. Read once: the mark is removed, and the
// ingress closes the connection, which ends the rest of its subscriptions anyway.
func (dwsm *DirectWSSubscriptionManager) SubscriptionEndError(dappID, consumerIp, webSocketConnectionUniqueId string) error {
	clientKey := dwsm.CreateWebSocketConnectionUniqueKey(dappID, consumerIp, webSocketConnectionUniqueId)
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()
	if _, dropped := dwsm.slowClients[clientKey]; !dropped {
		return nil
	}
	delete(dwsm.slowClients, clientKey)
	return chainlib.ErrSubscriptionClientTooSlow
}

// Close closes the subscription manager and all connections
func (dwsm *DirectWSSubscriptionManager) Close() {
	dwsm.lock.Lock()
	defer dwsm.lock.Unlock()

	// Release every client through the same path a disconnect takes, so reply channels
	// close and watchers wake, then end each subscription. The pools closing below is
	// what ends the node side.
	for _, sub := range dwsm.activeSubscriptions {
		for clientKey := range sub.connectedClients {
			sub.releaseClientLocked(clientKey)
		}
		sub.upstreamSubscription.Unsubscribe()
		sub.cancel()
		sub.stopUpstreamListener()
	}
	dwsm.activeSubscriptions = make(map[string]*directActiveSubscription)
	dwsm.connectedClients = make(map[string]map[string]*common.SafeChannelSender[*pairingtypes.RelayReply])
	dwsm.slowClients = make(map[string]struct{})

	// Close all pools
	for _, pool := range dwsm.upstreamPools {
		pool.Close()
	}
	dwsm.upstreamPools = make(map[string]*UpstreamWSPool)

	utils.LavaFormatInfo("DirectWS: subscription manager closed")
}

// Helper functions

// extractRequestID extracts the JSON-RPC request ID from raw request data.
// This is needed because GenericMessage interface doesn't expose GetID().
// Returns the raw JSON representation of the ID to preserve its type (string, number, null).
func extractRequestID(data []byte) json.RawMessage {
	if len(data) == 0 {
		return json.RawMessage("1") // Default fallback
	}

	var req struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(data, &req); err != nil || req.ID == nil {
		return json.RawMessage("1") // Default fallback
	}
	return req.ID
}

// extractSubscriptionID extracts the subscription ID from the first response.
//
// EVM and Substrate name the subscription with a string; Solana numbers it. Both are
// canonicalised to the decimal-string form the rpcclient dispatcher keys subscriptions
// under, using that package's own helper so the two cannot drift apart — they are the
// same identity, and a divergence would strand every push (MAG-3359).
func extractSubscriptionID(msg *rpcclient.JsonrpcMessage) string {
	if msg == nil || msg.Result == nil {
		return ""
	}
	id, _ := rpcclient.CanonicalSubscriptionID(msg.Result)
	return id
}

// subscriptionIDsAreNumeric reports whether a node numbers its subscriptions rather than
// naming them, read from the raw JSON the node sent rather than from the chain id.
//
// Solana is the case this was written for, but the test is on the shape and not the chain,
// so any node answering subscribe with a number takes this path — the repo's own dispatcher
// records a second such implementation in handleResponse ("StarkNet Pathfinder is returning
// an integer instead of a string in the result"). Do not read the Solana references nearby
// as an exhaustive list of who is affected.
//
// The leading-byte check is load-bearing, not defensive: json.Unmarshal decodes `null` into
// an int64 as a no-op and returns a NIL error, so the decode alone reports null as a number.
// A node answering subscribe with {"result":null} would otherwise flip a string-id chain
// onto the numeric path and hand its clients an id type they have never seen.
func subscriptionIDsAreNumeric(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}
	if first := trimmed[0]; first != '-' && (first < '0' || first > '9') {
		return false
	}
	var asNumber int64
	// gojson, not encoding/json: CanonicalSubscriptionID decodes with gojson, and these two
	// have to agree on every input or the router issues an id in a shape whose upstream
	// counterpart it cannot key on. Two implementations of one decision is what produced
	// the null divergence above.
	return gojson.Unmarshal(trimmed, &asNumber) == nil
}

// subscriptionIDValue renders a canonical (decimal-string) subscription id for the wire in
// the shape its chain uses: a JSON number where the node numbers its subscriptions, a
// string everywhere else.
//
// The shape is not cosmetic. A Solana client stores whatever the subscribe response gave it
// and hands that straight back to accountUnsubscribe, so a string there is an id it cannot
// use.
//
// An id that does not parse falls back to the string form, which is the safe direction. A
// router id on a numeric chain always comes from GenerateNumericRouterID and always parses,
// so the fallback is there to stop a future caller turning a bad id into a different
// valid-looking one, not because a current path reaches it.
func subscriptionIDValue(id string, numeric bool) any {
	if numeric {
		if asNumber, err := strconv.ParseInt(id, 10, 64); err == nil {
			return asNumber
		}
	}
	return id
}

// extractTendermintSubscriptionID extracts the subscription ID from Tendermint request params
// For Tendermint: the subscription ID is the "query" parameter from the request
// Example: {"query": "tm.event='NewBlock'"} -> subscription ID is "tm.event='NewBlock'"
func extractTendermintSubscriptionID(params []byte) string {
	if params == nil {
		return ""
	}
	var paramsMap map[string]interface{}
	if err := json.Unmarshal(params, &paramsMap); err != nil {
		return ""
	}
	if query, ok := paramsMap["query"].(string); ok {
		return query
	}
	return ""
}

// getSubscriptionID returns the appropriate subscription ID based on the protocol
// For EVM chains: extracts from response result (hex string)
// For Tendermint: extracts from request params (query field)
func getSubscriptionID(apiInterface string, responseMsg *rpcclient.JsonrpcMessage, requestParams []byte) string {
	if apiInterface == "tendermintrpc" {
		return extractTendermintSubscriptionID(requestParams)
	}
	return extractSubscriptionID(responseMsg)
}

// createSubscriptionReply creates the first reply for a brand-new subscription.
// The response id is taken from the client's request — NOT from originalMsg.ID — because
// originalMsg.ID is the upstream rpcclient's internal counter, which the client never sent
// and must not see (JSON-RPC 2.0 §4.2 requires the response id to match the request id
// exactly, including type: string ids stay strings).
func createSubscriptionReply(routerID string, requestID json.RawMessage, originalMsg *rpcclient.JsonrpcMessage, apiInterface string, numericIDs bool) ([]byte, error) {
	if originalMsg == nil {
		return nil, fmt.Errorf("original message is nil")
	}

	// For Tendermint: preserve the upstream message verbatim (custom extensions,
	// error envelope, anything beyond `result` that a Tendermint client may rely
	// on) and overwrite only the id field with the caller's request id. A naive
	// hand-built {jsonrpc,id,result} envelope would drop everything else that
	// the upstream returned.
	if apiInterface == "tendermintrpc" {
		raw, err := json.Marshal(originalMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal upstream message: %w", err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("failed to round-trip upstream message: %w", err)
		}
		obj["id"] = requestID
		return json.Marshal(obj)
	}

	// Everything else: the response carries the router ID (not the upstream one) and the
	// caller's id, rendered in the shape this chain names subscriptions with. Solana is
	// jsonrpc, so it lands here rather than in the Tendermint branch above.
	response := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      requestID,
		"result":  subscriptionIDValue(routerID, numericIDs),
	}

	return json.Marshal(response)
}

// createSubscriptionReplyFromRouterID creates a subscription reply with the router ID and matching request ID.
// Used when a client joins an existing subscription and needs their unique router ID in the response.
// The requestID must match the client's original eth_subscribe request per JSON-RPC 2.0 spec.
// For Tendermint, originalResult contains the query object that must be preserved.
func createSubscriptionReplyFromRouterID(routerID string, requestID json.RawMessage, apiInterface string, originalResult json.RawMessage, numericIDs bool) ([]byte, error) {
	// For Tendermint: preserve the original result format {"query":"..."}
	// Tendermint clients expect the query object back, not a router ID
	if apiInterface == "tendermintrpc" && originalResult != nil {
		response := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      requestID,
			"result":  originalResult,
		}
		return json.Marshal(response)
	}

	// Everything else: the client's unique router ID and their original request ID, in the
	// shape this chain names subscriptions with. JSON-RPC 2.0 requires the response ID to
	// match the request ID exactly.
	response := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      requestID,
		"result":  subscriptionIDValue(routerID, numericIDs),
	}

	return json.Marshal(response)
}

// rewriteParamsSubscriptionID returns params with its "subscription" field replaced by
// routerID.
//
// It returns (nil, nil) when params is not a subscription envelope at all, which is the
// caller's signal to try the other shapes. It returns an error when params IS one but names
// its subscription in a shape no router id can stand in for: passing that through would
// hand every client the UPSTREAM id — the same id for all of them — quietly defeating the
// per-client indirection that unsubscribe depends on. The pre-MAG-3345 code failed loudly
// there and so does this.
//
// Sibling fields are preserved: the envelope is rebuilt from what upstream actually sent
// rather than from a fixed {subscription, result} pair, so anything a chain adds alongside
// them survives the rewrite.
//
// The router id is written back in the shape the CHAIN uses, per the subscription's
// recorded numericIDs rather than the shape of the frame in hand — the two can disagree,
// and the subscription is the one that decided what the client was given. Requiring a
// string here used to exclude Solana entirely, which was correct while router ids were
// always strings and is wrong now that they follow the chain (MAG-3359).
func rewriteParamsSubscriptionID(params json.RawMessage, routerID string, numericIDs bool) (json.RawMessage, error) {
	if params == nil {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(params, &fields); err != nil {
		// Not a JSON object, so not an envelope this can rewrite — a positional params
		// array lands here, and passing it through is right. Traced rather than silent
		// because the pre-MAG-3345 code returned an error for an eth_subscription frame
		// whose params would not decode, and the caller skipped that client; anything
		// reaching here now is forwarded carrying the UPSTREAM id instead. No delivered
		// frame can reach it today (the dispatcher already decoded these params to find
		// the subscription), so this is the trace that would show that changing.
		utils.LavaFormatTrace("DirectWS: subscription params are not an object, forwarding unrewritten",
			utils.LogAttr("err", err),
		)
		return nil, nil
	}
	raw, ok := fields["subscription"]
	if !ok {
		return nil, nil
	}
	if _, named := rpcclient.CanonicalSubscriptionID(raw); !named {
		return nil, fmt.Errorf("subscription id names no subscription: %s", raw)
	}
	newID, err := json.Marshal(subscriptionIDValue(routerID, numericIDs))
	if err != nil {
		return nil, err
	}
	fields["subscription"] = newID
	rewritten, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return rewritten, nil
}

// rewriteSubscriptionID rewrites the subscription ID in a notification message
// For params-carried subscriptions (EVM, Substrate): rewrites params.subscription with router ID
// For Tendermint: notifications contain query in result, passed through as-is
func rewriteSubscriptionID(msg *rpcclient.JsonrpcMessage, routerID string, numericIDs bool) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}

	// Subscription notifications that carry the id in params:
	// {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x...","result":{...}}}
	//
	// Match the envelope, not the method name. Substrate uses this exact shape but names
	// the frame after the payload (chain_newHead, state_storage, author_extrinsicUpdate),
	// so an == "eth_subscription" test rewrote EVM and let Substrate through carrying the
	// UPSTREAM id — an id the client was never given and cannot match to its subscription
	// (MAG-3345). The method is echoed back rather than hard-coded for the same reason.
	rewritten, err := rewriteParamsSubscriptionID(msg.Params, routerID, numericIDs)
	if err != nil {
		return nil, err
	}
	if rewritten != nil {
		response := map[string]any{
			"jsonrpc": "2.0",
			"method":  msg.Method,
			"params":  rewritten,
		}
		return json.Marshal(response)
	}

	// Tendermint subscription notifications format:
	// {"jsonrpc":"2.0","id":null,"result":{"query":"tm.event='NewBlock'","data":{...}}}
	// The query field identifies the subscription - we pass through as-is since:
	// 1. Clients expect to see their actual query in notifications
	// 2. The router ID is only used for unsubscribe operations
	if msg.Result != nil {
		var result struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(msg.Result, &result); err == nil && result.Query != "" {
			// This is a Tendermint notification - pass through unchanged
			return json.Marshal(msg)
		}
	}

	// For other messages, return as-is
	return json.Marshal(msg)
}

// Compile-time interface compliance check
var _ chainlib.WSSubscriptionManager = (*DirectWSSubscriptionManager)(nil)
