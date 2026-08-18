package chainlib

import (
	"context"

	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// WSSubscriptionManager defines the interface for WebSocket subscription management.
// Implemented by:
//   - DirectWSSubscriptionManager: connects directly to the configured RPC endpoints
//   - NoOpWSSubscriptionManager: refuses, for a chain with no WebSocket endpoint
//
// The gRPC counterpart is GRPCSubscriptionManager.
type WSSubscriptionManager interface {
	// StartSubscription starts a new WebSocket subscription or joins an existing one.
	// If a subscription with the same parameters already exists, the client joins it
	// (subscription deduplication).
	//
	// Returns:
	//   - firstReply: The initial subscription confirmation reply
	//   - repliesChan: Channel for receiving subscription messages (nil if joining existing)
	//   - error: Any error that occurred
	StartSubscription(
		ctx context.Context,
		protocolMessage ProtocolMessage,
		dappID string,
		consumerIp string,
		webSocketConnectionUniqueId string,
		metricsData *metrics.RelayMetrics,
	) (firstReply *pairingtypes.RelayReply, repliesChan <-chan *pairingtypes.RelayReply, err error)

	// Unsubscribe handles an explicit unsubscribe request from a client.
	// The subscription ID is extracted from the protocolMessage.
	// Returns the node's response bytes when available (DirectWS streams the actual node
	// response); returns nil when the implementation has no response to hand back.
	Unsubscribe(
		ctx context.Context,
		protocolMessage ProtocolMessage,
		dappID string,
		consumerIp string,
		webSocketConnectionUniqueId string,
		metricsData *metrics.RelayMetrics,
	) (response []byte, err error)

	// UnsubscribeAll removes all subscriptions for a specific client connection.
	// Called when a WebSocket connection is closed.
	UnsubscribeAll(
		ctx context.Context,
		dappID string,
		consumerIp string,
		webSocketConnectionUniqueId string,
		metricsData *metrics.RelayMetrics,
	) error
}

