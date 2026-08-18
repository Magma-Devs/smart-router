package rpcsmartrouter

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
)

// grpcActiveSubscription holds state for an active upstream gRPC stream
type grpcActiveSubscription struct {
	// Upstream connection info
	upstreamPool       *UpstreamGRPCPool
	upstreamConnection *UpstreamGRPCStreamConnection
	upstreamStream     grpc.ClientStream
	methodDescriptor   *desc.MethodDescriptor

	// Router-generated unique subscription ID
	routerSubscriptionID string
	hashedParams         string // Hash of method + request params

	// Client tracking - multiple clients can share ONE upstream stream
	clientRouterIDs  map[string]string // clientKey -> routerID
	connectedClients map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]

	// Request info for restoration
	methodPath    string
	requestParams []byte

	// Lifecycle management
	ctx          context.Context
	cancel       context.CancelFunc
	closeSubChan chan struct{}
	closeOnce    sync.Once

	// Restoration state
	restoring atomic.Bool

	// Set once cleanupSubscription has released this subscription's resources. Keyed on
	// the subscription object rather than on its presence in activeSubscriptions, so
	// release runs exactly once no matter who reaches cleanup first.
	cleanedUp atomic.Bool

	// Message sequence counter
	messageSeq atomic.Uint64

	lock sync.RWMutex
}

// signalClose closes closeSubChan exactly once.
//
// Two owners reach it and they race: the last client leaving
// (removeClientFromSubscription) and manager shutdown (Stop). The listener only observes
// the close between receives, so a subscription whose upstream has gone quiet stays
// registered — and visible to Stop — indefinitely after its last client left. A plain
// close in both places panicked the process on shutdown whenever Stop landed in that
// window. Same reasoning as the cleanedUp latch, on the other half of the teardown.
func (sub *grpcActiveSubscription) signalClose() {
	sub.closeOnce.Do(func() {
		close(sub.closeSubChan)
	})
}

// DirectGRPCSubscriptionManager manages gRPC streaming subscriptions directly to upstream endpoints.
// This follows the same pattern as DirectWSSubscriptionManager for consistency.
type DirectGRPCSubscriptionManager struct {
	// Active subscriptions keyed by hashedParams (method + request params)
	activeSubscriptions map[string]*grpcActiveSubscription

	// Pending subscriptions for deduplication (prevent duplicate upstream connections)
	pendingSubscriptions map[string]*pendingSubscriptionsBroadcastManager

	// Upstream connection pools keyed by endpoint URL
	upstreamPools map[string]*UpstreamGRPCPool

	// Subscription ID mapping (reuse from WS manager)
	idMapper *SubscriptionIDMapper

	// Dependencies
	metricsManager metrics.ConsumerMetricsManagerInf
	chainID        string
	apiInterface   string

	// Upstream gRPC endpoints — two-tier separation matches HTTP backup model.
	// Primary tier serves all selections; backup tier is only consulted when
	// primary is exhausted (analogous to ConsumerSessionManager's
	// pairing/backupProviders split).
	//
	// Mutable, same as the WS manager's tiers: SetEndpoints swaps them (copy-on-write)
	// when the live pairing changes. Guarded by `lock` — read via endpointsSnapshot.
	grpcEndpoints       []*common.NodeUrl          // Primary tier — selected first
	grpcBackupEndpoints []*common.NodeUrl          // Backup tier — used only when primary exhausted
	endpointsByURL      map[string]*common.NodeUrl // Lookup across both tiers (sticky-session uniformity)

	// Endpoint selection (can be nil)
	optimizer WebSocketEndpointOptimizer

	// Configuration
	config *GRPCStreamingConfig

	// Rate limiting
	rateLimiter *GRPCClientRateLimiter

	// Sticky sessions (client -> endpoint affinity).
	// Written in createNewSubscription only after the upstream connection is
	// established, so a primary that fails to connect doesn't pin the client
	// and prevent the cascade from reaching the backup tier.
	stickyStore *lavasession.StickySessionStore

	// Total subscription counter
	totalSubscriptions atomic.Int64

	// Per-client subscription tracking
	clientSubscriptions map[string]map[string]struct{} // clientKey -> set of hashedParams

	// Manager state
	ctx    context.Context
	cancel context.CancelFunc

	lock sync.RWMutex
}

// NewDirectGRPCSubscriptionManager creates a new gRPC subscription manager.
//
// grpcEndpoints is the primary tier — selectEndpoint serves these first.
// grpcBackupEndpoints is the backup tier — only consulted when primary is exhausted.
// Either slice may be nil or empty, but at least one must be non-empty for selection to succeed.
func NewDirectGRPCSubscriptionManager(
	metricsManager metrics.ConsumerMetricsManagerInf,
	chainID string,
	apiInterface string,
	grpcEndpoints []*common.NodeUrl,
	grpcBackupEndpoints []*common.NodeUrl,
	optimizer WebSocketEndpointOptimizer,
	config *GRPCStreamingConfig,
) *DirectGRPCSubscriptionManager {
	if config == nil {
		config = DefaultGRPCStreamingConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	manager := &DirectGRPCSubscriptionManager{
		activeSubscriptions:  make(map[string]*grpcActiveSubscription),
		pendingSubscriptions: make(map[string]*pendingSubscriptionsBroadcastManager),
		upstreamPools:        make(map[string]*UpstreamGRPCPool),
		idMapper:             NewSubscriptionIDMapper(),
		metricsManager:       metrics.SafeMetrics(metricsManager),
		chainID:              chainID,
		apiInterface:         apiInterface,
		grpcEndpoints:        grpcEndpoints,
		grpcBackupEndpoints:  grpcBackupEndpoints,
		endpointsByURL:       make(map[string]*common.NodeUrl, len(grpcEndpoints)+len(grpcBackupEndpoints)),
		optimizer:            optimizer,
		config:               config,
		rateLimiter:          NewGRPCClientRateLimiter(config),
		stickyStore:          lavasession.NewStickySessionStore(),
		clientSubscriptions:  make(map[string]map[string]struct{}),
		ctx:                  ctx,
		cancel:               cancel,
	}

	// Build endpoint lookup map across both tiers
	for _, endpoint := range grpcEndpoints {
		manager.endpointsByURL[endpoint.Url] = endpoint
	}

	for _, endpoint := range grpcBackupEndpoints {
		manager.endpointsByURL[endpoint.Url] = endpoint
	}

	return manager
}

// grpcEndpointsSnapshot is an immutable view of both tiers plus the URL index,
// taken under the read lock. See wsEndpointsSnapshot — same contract.
type grpcEndpointsSnapshot struct {
	primary []*common.NodeUrl
	backup  []*common.NodeUrl
	byURL   map[string]*common.NodeUrl
}

func (dgm *DirectGRPCSubscriptionManager) endpointsSnapshot() grpcEndpointsSnapshot {
	dgm.lock.RLock()
	defer dgm.lock.RUnlock()
	return grpcEndpointsSnapshot{
		primary: dgm.grpcEndpoints,
		backup:  dgm.grpcBackupEndpoints,
		byURL:   dgm.endpointsByURL,
	}
}

// SetEndpoints replaces both tiers with the currently-serving gRPC endpoints and
// reports whether anything changed. Counterpart to
// DirectWSSubscriptionManager.SetEndpoints — see there for why the tiers must track
// the live pairing and why only live endpoints may be passed in (MAG-2525).
//
// This also un-sticks gRPC reflection: GetReflectionConnection selects through the
// same cascade, so a chain that booted dark answered reflection with "no endpoints"
// until the tiers were repopulated.
func (dgm *DirectGRPCSubscriptionManager) SetEndpoints(grpcEndpoints, grpcBackupEndpoints []*common.NodeUrl) (changed bool) {
	byURL := make(map[string]*common.NodeUrl, len(grpcEndpoints)+len(grpcBackupEndpoints))
	for _, ep := range grpcEndpoints {
		byURL[ep.Url] = ep
	}
	for _, ep := range grpcBackupEndpoints {
		byURL[ep.Url] = ep
	}

	dgm.lock.Lock()
	defer dgm.lock.Unlock()

	if sameEndpointURLs(dgm.grpcEndpoints, grpcEndpoints) && sameEndpointURLs(dgm.grpcBackupEndpoints, grpcBackupEndpoints) {
		return false
	}

	dgm.grpcEndpoints = grpcEndpoints
	dgm.grpcBackupEndpoints = grpcBackupEndpoints
	dgm.endpointsByURL = byURL

	// Pools for departed URLs are left for the cleanup loop to reap once idle —
	// live streams on a provider that only failed re-verification keep running.
	return true
}

// Start initializes the manager and starts background tasks
func (dgm *DirectGRPCSubscriptionManager) Start(ctx context.Context) {
	utils.LavaFormatInfo("DirectGRPCSubscriptionManager starting",
		utils.LogAttr("chainID", dgm.chainID),
		utils.LogAttr("endpoints", len(dgm.endpointsSnapshot().primary)),
	)

	// Start cleanup goroutine
	go dgm.cleanupLoop(ctx)
}

// Stop gracefully shuts down the manager
func (dgm *DirectGRPCSubscriptionManager) Stop() {
	dgm.cancel()

	dgm.lock.Lock()
	defer dgm.lock.Unlock()

	// Close all active subscriptions
	for _, sub := range dgm.activeSubscriptions {
		sub.cancel()
		sub.signalClose()
	}
	dgm.activeSubscriptions = make(map[string]*grpcActiveSubscription)

	// Close all pools
	for _, pool := range dgm.upstreamPools {
		pool.Close()
	}
	dgm.upstreamPools = make(map[string]*UpstreamGRPCPool)

	utils.LavaFormatInfo("DirectGRPCSubscriptionManager stopped",
		utils.LogAttr("chainID", dgm.chainID),
	)
}

// cleanupLoop periodically cleans up stale subscriptions
func (dgm *DirectGRPCSubscriptionManager) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(dgm.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dgm.ctx.Done():
			return
		case <-ticker.C:
			dgm.cleanupStaleSubscriptions()
		}
	}
}

// cleanupStaleSubscriptions removes subscriptions with cancelled contexts.
//
// It delegates to cleanupSubscription rather than deleting the entry itself. A partial
// reap — map delete plus counter, and nothing else — leaves the client channels open,
// the connection's stream count inflated so the pool cannot scale down, and the idMapper
// entries live, with no one left to release them: the listener's own cleanup arrives to
// find the key already gone (MAG-2540).
//
// The window is not narrow. removeClientFromSubscription cancels the subscription when
// its last client leaves and leaves cleanup to the listener, which may be parked in
// RecvMsg indefinitely — ctx.Done() is only checked between receives — so on a quiet
// stream this sweep routinely gets there first.
func (dgm *DirectGRPCSubscriptionManager) cleanupStaleSubscriptions() {
	dgm.lock.RLock()
	var stale []*grpcActiveSubscription
	for _, sub := range dgm.activeSubscriptions {
		select {
		case <-sub.ctx.Done():
			stale = append(stale, sub)
		default:
			// Still active
		}
	}
	dgm.lock.RUnlock()

	// Called unlocked: cleanupSubscription takes dgm.lock itself.
	for _, sub := range stale {
		dgm.cleanupSubscription(sub.hashedParams, sub)
	}

	if len(stale) > 0 {
		utils.LavaFormatDebug("DirectGRPC: cleaned up stale subscriptions",
			utils.LogAttr("count", len(stale)),
		)
	}
}

// GetReflectionConnection returns a gRPC connection for reflection requests.
// This enables tools like grpcurl to discover services through the smart router.
// The cleanup function should be called when the connection is no longer needed.
func (dgm *DirectGRPCSubscriptionManager) GetReflectionConnection(ctx context.Context) (*grpc.ClientConn, func(), error) {
	// Select an endpoint via the primary→backup cascade. Reflection is read-only
	// metadata so it isn't client-pinned; clientKey is empty.
	endpoint, err := dgm.selectEndpoint(ctx, "", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("no gRPC endpoints available for reflection: %w", err)
	}

	pool, err := dgm.getOrCreatePool(ctx, endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get pool for reflection: %w", err)
	}

	conn, err := pool.GetConnectionForStream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get connection for reflection: %w", err)
	}

	cleanup := func() {}

	return conn.GetConn(), cleanup, nil
}

// IsStreamingMethod checks if a gRPC method is server-streaming
func (dgm *DirectGRPCSubscriptionManager) IsStreamingMethod(ctx context.Context, methodPath string) (bool, *desc.MethodDescriptor, error) {
	// Parse service and method name
	svc, methodName := rpcInterfaceMessages.ParseSymbol(methodPath)

	// Select an endpoint via the primary→backup cascade.
	// clientKey is empty because method-descriptor lookups aren't client-scoped.
	endpoint, err := dgm.selectEndpoint(ctx, "", nil)
	if err != nil {
		return false, nil, err
	}

	pool, err := dgm.getOrCreatePool(ctx, endpoint)
	if err != nil {
		return false, nil, err
	}

	conn, err := pool.GetConnectionForStream(ctx)
	if err != nil {
		return false, nil, err
	}

	methodDesc, err := conn.GetMethodDescriptor(ctx, svc, methodName)
	if err != nil {
		return false, nil, err
	}

	return methodDesc.IsServerStreaming(), methodDesc, nil
}

// StartSubscription starts a new gRPC streaming subscription or joins an existing one
func (dgm *DirectGRPCSubscriptionManager) StartSubscription(
	ctx context.Context,
	chainMessage chainlib.ChainMessage,
	dappID string,
	consumerIp string,
	connectionUniqueId string,
	metricsData *metrics.RelayMetrics,
) (*pairingtypes.RelayReply, <-chan *pairingtypes.RelayReply, error) {
	// Create client key for tracking
	clientKey := dgm.createClientKey(dappID, consumerIp, connectionUniqueId)

	// Rate limiting check
	if !dgm.rateLimiter.AllowSubscribe(clientKey) {
		return nil, nil, fmt.Errorf("subscription rate limit exceeded for client %s", clientKey)
	}

	// Get method path and request data
	grpcMessage, ok := chainMessage.GetRPCMessage().(*rpcInterfaceMessages.GrpcMessage)
	if !ok {
		return nil, nil, fmt.Errorf("expected GrpcMessage, got %T", chainMessage.GetRPCMessage())
	}

	methodPath := grpcMessage.Path
	if methodPath == "" {
		methodPath = chainMessage.GetApi().Name
	}
	requestData := grpcMessage.Msg

	// Create hash for deduplication
	hashedParams := dgm.hashSubscriptionParams(methodPath, requestData)

	// Check for existing subscription to join
	dgm.lock.RLock()
	existingSub, exists := dgm.activeSubscriptions[hashedParams]
	dgm.lock.RUnlock()

	if exists && dgm.config.SubscriptionSharingEnabled {
		return dgm.joinExistingSubscription(ctx, existingSub, clientKey, hashedParams)
	}

	// Check client subscription limit
	if err := dgm.checkClientSubscriptionLimit(clientKey); err != nil {
		return nil, nil, err
	}

	// Check global subscription limit
	if err := dgm.checkGlobalSubscriptionLimit(); err != nil {
		return nil, nil, err
	}

	// Create new subscription
	return dgm.createNewSubscription(ctx, chainMessage, methodPath, requestData, hashedParams, clientKey)
}

// joinExistingSubscription adds a client to an existing subscription
func (dgm *DirectGRPCSubscriptionManager) joinExistingSubscription(
	ctx context.Context,
	sub *grpcActiveSubscription,
	clientKey string,
	hashedParams string,
) (*pairingtypes.RelayReply, <-chan *pairingtypes.RelayReply, error) {
	sub.lock.Lock()
	defer sub.lock.Unlock()

	// Generate unique router ID for this client
	routerID := dgm.idMapper.GenerateRouterID(clientKey)
	dgm.idMapper.RegisterMapping(routerID, sub.routerSubscriptionID)

	// Create channel for this client
	replyChan := make(chan *pairingtypes.RelayReply, 100)
	sender := common.NewSafeChannelSender(ctx, replyChan)

	sub.clientRouterIDs[clientKey] = routerID
	sub.connectedClients[clientKey] = sender

	// Track subscription for this client
	dgm.trackClientSubscription(clientKey, hashedParams)

	utils.LavaFormatDebug("DirectGRPC: client joined existing subscription",
		utils.LogAttr("clientKey", clientKey),
		utils.LogAttr("routerID", routerID),
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)

	// The acknowledgement is minted per client, never shared. It carries the router
	// id this client must quote to unsubscribe, and clientRouterIDs holds a distinct
	// one per client — handing a joiner the creator's copy would tell it to
	// unsubscribe using an id that is not its own.
	return dgm.createStreamAcknowledgement(routerID, sub.methodPath), replyChan, nil
}

// createNewSubscription creates a new upstream subscription
func (dgm *DirectGRPCSubscriptionManager) createNewSubscription(
	ctx context.Context,
	chainMessage chainlib.ChainMessage,
	methodPath string,
	requestData []byte,
	hashedParams string,
	clientKey string,
) (*pairingtypes.RelayReply, <-chan *pairingtypes.RelayReply, error) {
	// Select endpoint
	endpoint, err := dgm.selectEndpoint(ctx, clientKey, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to select endpoint: %w", err)
	}

	// Get or create pool
	pool, err := dgm.getOrCreatePool(ctx, endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get pool: %w", err)
	}

	// Get connection
	conn, err := pool.GetConnectionForStream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Connection established — pin client to this endpoint for future
	// subscriptions. Mirrors DirectWSSubscriptionManager.startUpstreamSubscription
	// (Epoch is unused for direct RPC — there's no provider rotation).
	dgm.stickyStore.Set(clientKey, &lavasession.StickySession{
		Provider: endpoint.Url,
		Epoch:    0,
	})

	// Parse service and method
	svc, methodName := rpcInterfaceMessages.ParseSymbol(methodPath)

	// Get method descriptor
	methodDesc, err := conn.GetMethodDescriptor(ctx, svc, methodName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get method descriptor: %w", err)
	}

	// Verify it's a server-streaming method
	if !methodDesc.IsServerStreaming() {
		return nil, nil, fmt.Errorf("method %s is not a server-streaming method", methodPath)
	}

	// The subscription context is rooted in the manager, NOT in ctx. conn.NewStream
	// binds the upstream stream's whole life to the context it was created with, and
	// with sharing enabled later clients join this same stream — so rooting it in the
	// creating client's request context meant the first client to disconnect killed
	// the stream for every joiner. Rooted here, only the subscription's own teardown
	// ends it: the last client leaving (removeClientFromSubscription) or Stop.
	subCtx, subCancel := context.WithCancel(dgm.ctx)

	// Create the upstream stream
	stream, err := dgm.createUpstreamStream(subCtx, conn.GetConn(), methodPath, requestData, methodDesc)
	if err != nil {
		subCancel()
		return nil, nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Increment stream count
	conn.IncrementStreams()

	// Generate IDs
	routerSubID := dgm.idMapper.GenerateRouterID(clientKey)
	clientRouterID := dgm.idMapper.GenerateRouterID(clientKey)
	dgm.idMapper.RegisterMapping(clientRouterID, routerSubID)

	// Create reply channel for this client
	replyChan := make(chan *pairingtypes.RelayReply, 100)
	sender := common.NewSafeChannelSender(subCtx, replyChan)

	// Create active subscription
	activeSub := &grpcActiveSubscription{
		upstreamPool:         pool,
		upstreamConnection:   conn,
		upstreamStream:       stream,
		methodDescriptor:     methodDesc,
		routerSubscriptionID: routerSubID,
		hashedParams:         hashedParams,
		clientRouterIDs:      map[string]string{clientKey: clientRouterID},
		connectedClients:     map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{clientKey: sender},
		methodPath:           methodPath,
		requestParams:        requestData,
		ctx:                  subCtx,
		cancel:               subCancel,
		closeSubChan:         make(chan struct{}),
	}

	// Store subscription
	dgm.lock.Lock()
	dgm.activeSubscriptions[hashedParams] = activeSub
	dgm.totalSubscriptions.Add(1)
	dgm.lock.Unlock()

	// Track for client
	dgm.trackClientSubscription(clientKey, hashedParams)

	// Start message listener
	go dgm.listenForUpstreamMessages(subCtx, hashedParams, activeSub)

	// Create acknowledgement as first reply
	firstReply := dgm.createStreamAcknowledgement(clientRouterID, methodPath)

	utils.LavaFormatInfo("DirectGRPC: created new subscription",
		utils.LogAttr("methodPath", methodPath),
		utils.LogAttr("clientKey", clientKey),
		utils.LogAttr("routerSubID", routerSubID),
		utils.LogAttr("endpoint", endpoint.Url),
	)

	return firstReply, replyChan, nil
}

// createUpstreamStream creates a gRPC client stream for server-streaming RPC
func (dgm *DirectGRPCSubscriptionManager) createUpstreamStream(
	ctx context.Context,
	conn *grpc.ClientConn,
	methodPath string,
	requestData []byte,
	methodDesc *desc.MethodDescriptor,
) (grpc.ClientStream, error) {
	// Create stream descriptor
	streamDesc := &grpc.StreamDesc{
		StreamName:    methodPath,
		ServerStreams: true,
		ClientStreams: false,
	}

	// Create the stream
	stream, err := conn.NewStream(ctx, streamDesc, "/"+methodPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Parse and send the initial request message
	if len(requestData) > 0 {
		// dynamic.NewMessage, not the message factory: the factory consults the known-type
		// registry and hands back a linked Go type whenever one is registered for this
		// message name, which the JSON branch below then had to reject outright. Chains
		// whose types happen to be linked into the router (anything cosmos-shaped) took
		// that path. A dynamic message parses both encodings for every method.
		inputMsg := dynamic.NewMessage(methodDesc.GetInputType())

		// Detect format and parse request data
		if requestData[0] == '{' || requestData[0] == '[' {
			if err := inputMsg.UnmarshalJSON(requestData); err != nil {
				stream.CloseSend()
				return nil, fmt.Errorf("failed to parse JSON request: %w", err)
			}
		} else {
			// Binary proto input
			if err := proto.Unmarshal(requestData, inputMsg); err != nil {
				stream.CloseSend()
				return nil, fmt.Errorf("failed to parse proto request: %w", err)
			}
		}

		if err := stream.SendMsg(inputMsg); err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
	}

	// Close send direction (server-streaming is receive-only after initial request)
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close send: %w", err)
	}

	return stream, nil
}

// listenForUpstreamMessages listens for messages from upstream gRPC stream.
//
// On an upstream error, ownership of the subscription transfers to
// handleUpstreamDisconnect, which restores it in place or releases it. Cleanup must not
// also run here: it closes every client channel and nils connectedClients, so a
// successful restoration would route into nobody and leak the new stream (MAG-2540).
// Once reconnectInFlight is set, handleUpstreamDisconnect owns cleanup on every failure
// path — the same contract DirectWSSubscriptionManager documents.
func (dgm *DirectGRPCSubscriptionManager) listenForUpstreamMessages(
	ctx context.Context,
	hashedParams string,
	activeSub *grpcActiveSubscription,
) {
	reconnectInFlight := false
	defer func() {
		if !reconnectInFlight {
			dgm.cleanupSubscription(hashedParams, activeSub)
		}
	}()

	msgFactory := dynamic.NewMessageFactoryWithDefaults()

	for {
		select {
		case <-ctx.Done():
			return
		case <-activeSub.closeSubChan:
			return
		default:
			// Create fresh message for each receive
			outputMsg := msgFactory.NewMessage(activeSub.methodDescriptor.GetOutputType())

			// Receive next message
			err := activeSub.upstreamStream.RecvMsg(outputMsg)
			if err == io.EOF {
				utils.LavaFormatInfo("DirectGRPC: stream ended normally",
					utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
				)
				return
			}
			if err != nil {
				utils.LavaFormatWarning("DirectGRPC: stream error",
					err,
					utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
				)
				// Hand ownership of this subscription to the reconnect goroutine —
				// including responsibility for cleanup if restoration fails.
				reconnectInFlight = true
				go dgm.handleUpstreamDisconnect(ctx, hashedParams, activeSub)
				return
			}

			// Marshal to bytes
			msgBytes, err := proto.Marshal(outputMsg)
			if err != nil {
				utils.LavaFormatWarning("DirectGRPC: failed to marshal message", err)
				continue
			}

			// Route to all clients
			dgm.routeMessageToClients(activeSub, msgBytes)
		}
	}
}

// routeMessageToClients routes upstream message to all connected clients
func (dgm *DirectGRPCSubscriptionManager) routeMessageToClients(
	activeSub *grpcActiveSubscription,
	msgData []byte,
) {
	activeSub.lock.RLock()
	clients := make(map[string]struct {
		sender   *common.SafeChannelSender[*pairingtypes.RelayReply]
		routerID string
	})
	for clientKey, sender := range activeSub.connectedClients {
		routerID := activeSub.clientRouterIDs[clientKey]
		clients[clientKey] = struct {
			sender   *common.SafeChannelSender[*pairingtypes.RelayReply]
			routerID string
		}{sender: sender, routerID: routerID}
	}
	activeSub.lock.RUnlock()

	seqNum := activeSub.messageSeq.Add(1)

	for _, info := range clients {
		reply := &pairingtypes.RelayReply{
			Data: msgData,
			Metadata: []pairingtypes.Metadata{
				{Name: MetadataGRPCSubscriptionID, Value: info.routerID},
				{Name: MetadataGRPCStreamSeq, Value: fmt.Sprintf("%d", seqNum)},
			},
		}
		info.sender.Send(reply)
	}
}

// handleUpstreamDisconnect handles upstream stream disconnection
func (dgm *DirectGRPCSubscriptionManager) handleUpstreamDisconnect(
	ctx context.Context,
	hashedParams string,
	activeSub *grpcActiveSubscription,
) {
	// Prevent concurrent restoration.
	//
	// Returning here without cleanup is correct: another handleUpstreamDisconnect holds
	// the latch and owns cleanup for this subscription, so tearing down here would kill a
	// restoration still in progress. That is only true because the latch is released
	// before the restarted listener is spawned (see the end of this function) — if it
	// outlived the hand-off, a handler spawned by that listener would lose the CAS and
	// return, leaving the subscription registered with no listener, nobody reconnecting,
	// and an uncancelled ctx that keeps the stale sweep from collecting it.
	if !activeSub.restoring.CompareAndSwap(false, true) {
		return
	}
	defer activeSub.restoring.Store(false)

	// Cleanup ownership, expressed once rather than at each early return. The listener
	// that handed off skipped its own deferred cleanup, so every way out of this
	// function short of a completed restoration must release the subscription — and a
	// failure path added later gets that for free instead of having to remember it.
	// cleanupSubscription cancels, so the failure paths below do not.
	restored := false
	defer func() {
		if !restored {
			dgm.cleanupSubscription(hashedParams, activeSub)
		}
	}()

	utils.LavaFormatInfo("DirectGRPC: attempting to restore subscription",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)

	// Reconnect pool
	if err := activeSub.upstreamPool.ReconnectWithBackoff(ctx); err != nil {
		utils.LavaFormatWarning("DirectGRPC: failed to reconnect", err)
		return
	}

	// Get new connection
	newConn, err := activeSub.upstreamPool.GetConnectionForStream(ctx)
	if err != nil {
		utils.LavaFormatWarning("DirectGRPC: failed to get new connection", err)
		return
	}

	// Create new stream
	newStream, err := dgm.createUpstreamStream(
		ctx,
		newConn.GetConn(),
		activeSub.methodPath,
		activeSub.requestParams,
		activeSub.methodDescriptor,
	)
	if err != nil {
		utils.LavaFormatWarning("DirectGRPC: failed to create new stream", err)
		return
	}

	// Update subscription
	activeSub.lock.Lock()
	oldConn := activeSub.upstreamConnection
	activeSub.upstreamConnection = newConn
	activeSub.upstreamStream = newStream
	activeSub.lock.Unlock()

	// Decrement old connection stream count
	if oldConn != nil {
		activeSub.upstreamPool.NotifyStreamRemoved(oldConn)
	}
	newConn.IncrementStreams()

	utils.LavaFormatInfo("DirectGRPC: subscription restored",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)

	// Restoration completed — the new listener now owns this subscription's lifecycle,
	// so the cleanup defer above must stand down.
	restored = true

	// Release the latch BEFORE the hand-off, not on the way out via defer: if the new
	// listener errors immediately, its handler has to win the CAS. The trailing defer
	// is then a no-op.
	activeSub.restoring.Store(false)

	// Restart listener
	go dgm.listenForUpstreamMessages(activeSub.ctx, hashedParams, activeSub)
}

// cleanupSubscription releases a subscription: deregisters it, cancels it, closes every
// client channel, returns the stream slot to the pool and drops the per-client tracking
// and ID mappings.
//
// Two guards, answering two different questions (MAG-2540):
//
//   - cleanedUp makes the release run exactly once per subscription. Guarding on the map
//     entry instead would fix the counter and create a leak: a caller arriving after the
//     entry is gone would skip closing the client channels too.
//   - The map entry is only removed when this is still the registered subscription.
//     hashedParams is deterministic — a client re-subscribing to the same method reuses
//     the key — so a handler parked in ReconnectWithBackoff must not come back seconds
//     later and evict its own successor.
//
// Cancellation lives here, not at the call sites, so the stale sweep can never observe a
// cancelled-but-unreleased subscription. Same placement as
// DirectWSSubscriptionManager.cleanupSubscription.
func (dgm *DirectGRPCSubscriptionManager) cleanupSubscription(hashedParams string, activeSub *grpcActiveSubscription) {
	if !activeSub.cleanedUp.CompareAndSwap(false, true) {
		return
	}

	dgm.lock.Lock()
	if current, found := dgm.activeSubscriptions[hashedParams]; found && current == activeSub {
		delete(dgm.activeSubscriptions, hashedParams)
	}
	// Decremented per subscription object, not per map entry: createNewSubscription
	// increments once when it registers, and the cleanedUp latch above makes this the
	// matching decrement. Gating it on the identity check instead would strand the slot
	// of any subscription that left the map by another route.
	dgm.totalSubscriptions.Add(-1)
	dgm.lock.Unlock()

	activeSub.cancel()

	// Close client channels, and snapshot what the release below needs — upstreamConnection
	// is written under this lock by handleUpstreamDisconnect.
	activeSub.lock.Lock()
	clientKeys := make([]string, 0, len(activeSub.connectedClients))
	for clientKey, sender := range activeSub.connectedClients {
		sender.Close()
		clientKeys = append(clientKeys, clientKey)
	}
	activeSub.connectedClients = nil
	routerIDs := make([]string, 0, len(activeSub.clientRouterIDs))
	for _, routerID := range activeSub.clientRouterIDs {
		routerIDs = append(routerIDs, routerID)
	}
	upstreamConnection := activeSub.upstreamConnection
	activeSub.lock.Unlock()

	// Untrack per client, or checkClientSubscriptionLimit keeps counting a dead
	// subscription and ratchets the client toward its cap. Outside activeSub.lock —
	// untrackClientSubscription takes dgm.lock.
	for _, clientKey := range clientKeys {
		dgm.untrackClientSubscription(clientKey, hashedParams)
	}

	// Return connection to pool
	if upstreamConnection != nil {
		activeSub.upstreamPool.NotifyStreamRemoved(upstreamConnection)
	}

	// Clean up ID mappings
	for _, routerID := range routerIDs {
		dgm.idMapper.RemoveMapping(routerID)
	}

	utils.LavaFormatDebug("DirectGRPC: subscription cleaned up",
		utils.LogAttr("hashedParams", utils.ToHexString(hashedParams)),
	)
}

// Unsubscribe handles unsubscribe request from a client
func (dgm *DirectGRPCSubscriptionManager) Unsubscribe(
	ctx context.Context,
	routerSubID string,
	clientKey string,
) error {
	// Rate limiting check
	if !dgm.rateLimiter.AllowUnsubscribe(clientKey) {
		return fmt.Errorf("unsubscribe rate limit exceeded")
	}

	// Find the subscription
	dgm.lock.RLock()
	var targetSub *grpcActiveSubscription
	var targetHashedParams string
	for hashedParams, sub := range dgm.activeSubscriptions {
		sub.lock.RLock()
		if _, exists := sub.clientRouterIDs[clientKey]; exists {
			if sub.clientRouterIDs[clientKey] == routerSubID {
				targetSub = sub
				targetHashedParams = hashedParams
			}
		}
		sub.lock.RUnlock()
		if targetSub != nil {
			break
		}
	}
	dgm.lock.RUnlock()

	if targetSub == nil {
		return fmt.Errorf("subscription not found for router ID %s", routerSubID)
	}

	return dgm.removeClientFromSubscription(targetSub, targetHashedParams, clientKey)
}

// UnsubscribeAll removes all subscriptions for a client
func (dgm *DirectGRPCSubscriptionManager) UnsubscribeAll(
	ctx context.Context,
	clientKey string,
) error {
	dgm.lock.RLock()
	clientSubs, exists := dgm.clientSubscriptions[clientKey]
	if !exists {
		dgm.lock.RUnlock()
		return nil
	}
	// Copy the set
	hashedParamsList := make([]string, 0, len(clientSubs))
	for hp := range clientSubs {
		hashedParamsList = append(hashedParamsList, hp)
	}
	dgm.lock.RUnlock()

	for _, hashedParams := range hashedParamsList {
		dgm.lock.RLock()
		sub, exists := dgm.activeSubscriptions[hashedParams]
		dgm.lock.RUnlock()
		if exists {
			dgm.removeClientFromSubscription(sub, hashedParams, clientKey)
		}
	}

	// Cleanup client tracking
	dgm.lock.Lock()
	delete(dgm.clientSubscriptions, clientKey)
	dgm.stickyStore.Delete(clientKey)
	dgm.lock.Unlock()

	dgm.rateLimiter.CleanupClient(clientKey)

	return nil
}

// removeClientFromSubscription removes a client from a subscription
func (dgm *DirectGRPCSubscriptionManager) removeClientFromSubscription(
	sub *grpcActiveSubscription,
	hashedParams string,
	clientKey string,
) error {
	sub.lock.Lock()
	defer sub.lock.Unlock()

	// Remove client
	if sender, exists := sub.connectedClients[clientKey]; exists {
		sender.Close()
		delete(sub.connectedClients, clientKey)
	}

	routerID := sub.clientRouterIDs[clientKey]
	delete(sub.clientRouterIDs, clientKey)
	dgm.idMapper.RemoveMapping(routerID)

	// Untrack from client
	dgm.untrackClientSubscription(clientKey, hashedParams)

	// If last client, close the subscription
	if len(sub.connectedClients) == 0 {
		sub.cancel()
		sub.signalClose()
	}

	return nil
}

// Helper methods

// ClientKey implements chainlib.GRPCSubscriptionManager. The listener needs the key
// StartSubscription derived internally so that it can release the same client on
// disconnect, without reproducing the key format on its side.
func (dgm *DirectGRPCSubscriptionManager) ClientKey(dappID, consumerIp, connectionUniqueId string) string {
	return dgm.createClientKey(dappID, consumerIp, connectionUniqueId)
}

func (dgm *DirectGRPCSubscriptionManager) createClientKey(dappID, consumerIp, connectionUniqueId string) string {
	return fmt.Sprintf("%s:%s:%s", dappID, consumerIp, connectionUniqueId)
}

func (dgm *DirectGRPCSubscriptionManager) hashSubscriptionParams(methodPath string, requestData []byte) string {
	return utils.ToHexString(fmt.Sprintf("%s:%s", methodPath, string(requestData)))
}

func (dgm *DirectGRPCSubscriptionManager) createStreamAcknowledgement(routerID, methodPath string) *pairingtypes.RelayReply {
	return &pairingtypes.RelayReply{
		Data: []byte(fmt.Sprintf(`{"subscription_id":"%s","method":"%s","status":"STREAMING"}`, routerID, methodPath)),
		Metadata: []pairingtypes.Metadata{
			{Name: MetadataGRPCSubscriptionID, Value: routerID},
			{Name: "content-type", Value: "application/json"},
		},
	}
}

func (dgm *DirectGRPCSubscriptionManager) getOrCreatePool(ctx context.Context, endpoint *common.NodeUrl) (*UpstreamGRPCPool, error) {
	dgm.lock.Lock()
	defer dgm.lock.Unlock()

	pool, exists := dgm.upstreamPools[endpoint.Url]
	if exists {
		return pool, nil
	}

	pool = NewUpstreamGRPCPoolWithConfig(endpoint, dgm.config)
	dgm.upstreamPools[endpoint.Url] = pool

	return pool, nil
}

// selectEndpoint picks a gRPC endpoint for the given client, with primary→backup cascade.
// Selection priority:
//  1. Sticky session (any tier — endpointsByURL spans both)
//  2. Primary tier (optimizer or first-available)
//  3. Backup tier (optimizer or first-available) — only when primary is exhausted
//
// Mirrors DirectWSSubscriptionManager.selectEndpoint and the HTTP backup-fallback model
// (consumer_session_manager.go:820-852). Sticky writes happen in createNewSubscription
// after the upstream connection is verified, not here, so a primary that fails to
// connect doesn't pin the client and block the cascade.
func (dgm *DirectGRPCSubscriptionManager) selectEndpoint(ctx context.Context, clientKey string, ignoredEndpoints map[string]struct{}) (*common.NodeUrl, error) {
	// One snapshot for the whole cascade — see DirectWSSubscriptionManager.selectEndpoint.
	snapshot := dgm.endpointsSnapshot()

	// Tier 0: sticky session for this client (resolves across both tiers).
	if clientKey != "" {
		if stickySession, exists := dgm.stickyStore.Get(clientKey); exists {
			if stickyEndpoint, found := snapshot.byURL[stickySession.Provider]; found {
				if ignoredEndpoints == nil {
					return stickyEndpoint, nil
				}
				if _, ignored := ignoredEndpoints[stickySession.Provider]; !ignored {
					return stickyEndpoint, nil
				}
				// Sticky endpoint is ignored — clear and continue to cascade.
				utils.LavaFormatDebug("DirectGRPC: sticky endpoint ignored, clearing affinity",
					utils.LogAttr("clientKey", clientKey),
					utils.LogAttr("ignoredEndpoint", stickySession.Provider),
				)
				dgm.stickyStore.Delete(clientKey)
			}
		}
	}

	// Tier 1: primary (optimizer-aware).
	if endpoint, err := dgm.selectFromTier(ctx, snapshot.primary, snapshot.byURL, ignoredEndpoints); err == nil {
		return endpoint, nil
	}

	// Tier 2: backup (only when primary is empty/unavailable).
	utils.LavaFormatDebug("DirectGRPC: primary endpoints exhausted, falling back to backup",
		utils.LogAttr("backupCount", len(snapshot.backup)),
	)
	if endpoint, err := dgm.selectFromTier(ctx, snapshot.backup, snapshot.byURL, ignoredEndpoints); err == nil {
		return endpoint, nil
	}

	return nil, fmt.Errorf("no gRPC endpoints available")
}

// selectFromTier picks an endpoint from a single tier using the optimizer when
// available, falling back to first-non-ignored. Tier-agnostic — the cascade
// order is the caller's responsibility.
//
// tier and byURL come from one endpointsSnapshot; see the WS counterpart.
func (dgm *DirectGRPCSubscriptionManager) selectFromTier(ctx context.Context, tier []*common.NodeUrl, byURL map[string]*common.NodeUrl, ignoredEndpoints map[string]struct{}) (*common.NodeUrl, error) {
	if len(tier) == 0 {
		return nil, fmt.Errorf("tier is empty")
	}

	// Single endpoint or no optimizer: first-non-ignored.
	if len(tier) == 1 || dgm.optimizer == nil {
		for _, ep := range tier {
			if ignoredEndpoints == nil {
				return ep, nil
			}
			if _, ignored := ignoredEndpoints[ep.Url]; !ignored {
				return ep, nil
			}
		}
		return nil, fmt.Errorf("all endpoints in tier are ignored/unavailable")
	}

	// Optimizer over this tier.
	allURLs := make([]string, 0, len(tier))
	for _, ep := range tier {
		allURLs = append(allURLs, ep.Url)
	}
	// cu=1 and requestedBlock=LATEST_BLOCK (-2) are sensible defaults for subscriptions.
	selectedURLs := dgm.optimizer.ChooseProvider(ctx, allURLs, ignoredEndpoints, 1, -2)

	if len(selectedURLs) == 0 {
		// Optimizer returned nothing — fall back to first-non-ignored within tier.
		for _, ep := range tier {
			if ignoredEndpoints == nil {
				return ep, nil
			}
			if _, ignored := ignoredEndpoints[ep.Url]; !ignored {
				return ep, nil
			}
		}
		return nil, fmt.Errorf("optimizer returned no endpoints and all fallbacks in tier are ignored")
	}

	selectedURL := selectedURLs[0]
	if endpoint, exists := byURL[selectedURL]; exists {
		return endpoint, nil
	}
	return nil, fmt.Errorf("optimizer selected unknown endpoint: %s", selectedURL)
}

func (dgm *DirectGRPCSubscriptionManager) checkClientSubscriptionLimit(clientKey string) error {
	dgm.lock.RLock()
	count := len(dgm.clientSubscriptions[clientKey])
	dgm.lock.RUnlock()

	if count >= dgm.config.MaxSubscriptionsPerClient {
		if dgm.config.ShouldRejectOnClientLimit() {
			return fmt.Errorf("client subscription limit reached (%d)", dgm.config.MaxSubscriptionsPerClient)
		}
		utils.LavaFormatWarning("DirectGRPC: client near subscription limit",
			nil,
			utils.LogAttr("clientKey", clientKey),
			utils.LogAttr("count", count),
		)
	}
	return nil
}

func (dgm *DirectGRPCSubscriptionManager) checkGlobalSubscriptionLimit() error {
	total := dgm.totalSubscriptions.Load()
	if total >= int64(dgm.config.MaxTotalSubscriptions) {
		if dgm.config.ShouldRejectOnTotalLimit() {
			return fmt.Errorf("global subscription limit reached (%d)", dgm.config.MaxTotalSubscriptions)
		}
		utils.LavaFormatWarning("DirectGRPC: approaching global subscription limit",
			nil,
			utils.LogAttr("total", total),
		)
	}
	return nil
}

func (dgm *DirectGRPCSubscriptionManager) trackClientSubscription(clientKey, hashedParams string) {
	dgm.lock.Lock()
	defer dgm.lock.Unlock()

	if dgm.clientSubscriptions[clientKey] == nil {
		dgm.clientSubscriptions[clientKey] = make(map[string]struct{})
	}
	dgm.clientSubscriptions[clientKey][hashedParams] = struct{}{}
}

func (dgm *DirectGRPCSubscriptionManager) untrackClientSubscription(clientKey, hashedParams string) {
	dgm.lock.Lock()
	defer dgm.lock.Unlock()

	if subs, exists := dgm.clientSubscriptions[clientKey]; exists {
		delete(subs, hashedParams)
		if len(subs) == 0 {
			delete(dgm.clientSubscriptions, clientKey)
		}
	}
}

// GetActiveSubscriptionCount returns the number of active subscriptions
func (dgm *DirectGRPCSubscriptionManager) GetActiveSubscriptionCount() int64 {
	return dgm.totalSubscriptions.Load()
}

// GetClientSubscriptionCount returns the number of subscriptions for a client
func (dgm *DirectGRPCSubscriptionManager) GetClientSubscriptionCount(clientKey string) int {
	dgm.lock.RLock()
	defer dgm.lock.RUnlock()
	return len(dgm.clientSubscriptions[clientKey])
}
