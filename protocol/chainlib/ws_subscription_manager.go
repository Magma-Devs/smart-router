package chainlib

import (
	"context"
	"errors"

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

// ErrSubscriptionClientTooSlow is what a WSSubscriptionEndReporter reports for a connection it
// dropped a subscription of because the client did not keep up with the pushes. Its text is the
// reason in the close frame the client gets.
var ErrSubscriptionClientTooSlow = errors.New("subscription client did not keep up")

// ErrSubscriptionUpstreamLost is what a WSSubscriptionEndReporter reports for a connection whose
// subscription the router gave up: the upstream subscription was lost and could not be restored
// (the reconnect or the re-subscribe failed, or the restores without progress ran out). Nothing
// will push on it again. Its text is the reason in the close frame the client gets, the same words
// the gRPC manager ends such a stream with (codes.Unavailable) (MAG-5022).
var ErrSubscriptionUpstreamLost = errors.New("upstream subscription lost")

// WSSubscriptionEndReporter is an optional interface on WSSubscriptionManager, the WebSocket
// counterpart of GRPCSubscriptionEndReporter. A WebSocket subscription has no end of its own to
// carry a reason: its reply channel just closes. A manager that ends a client's subscription for
// a reason the client must hear reports it here, and the ingress tells the client by closing the
// socket. Any other end leaves the socket open, as before.
type WSSubscriptionEndReporter interface {
	// SubscriptionEndError returns why the manager ended a subscription of this connection
	// (ErrSubscriptionClientTooSlow, ErrSubscriptionUpstreamLost), or nil for an ordinary end. The
	// ingress reads it after a reply channel has closed; a reason is read once.
	SubscriptionEndError(dappID string, consumerIp string, webSocketConnectionUniqueId string) error
}
