package rpcsmartrouter

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// relayEndSelectionSubscription is the selection of a subscription request, which a subscription
// manager serves rather than the relay state machine.
const relayEndSelectionSubscription = "subscription"

// The listeners find the reporter by type assertion, so a drifted signature would silently stop it
// being called.
var _ chainlib.SubscriptionRelayReporter = (*RPCSmartRouterServer)(nil)

// ReportSubscriptionRelay writes the relay_end of a subscription request: a WebSocket subscribe,
// unsubscribe or unsubscribe-all frame, or a gRPC stream's open (chainlib.SubscriptionRelayReporter).
// A subscription can carry a transaction (Substrate's submit-and-watch), so these are relays too.
func (rpcss *RPCSmartRouterServer) ReportSubscriptionRelay(ctx context.Context, protocolMessage chainlib.ProtocolMessage, start time.Time, err error) {
	utils.LogEvent(relayEndEvent, rpcss.buildSubscriptionRelayEnd(ctx, protocolMessage, start, err))
}

// buildSubscriptionRelayEnd reads the manager's record of what it did upstream: a request it sent is
// the event's one attempt, one answered from a subscription already open has none and names that
// subscription's upstream, and one the router answered itself names no upstream.
func (rpcss *RPCSmartRouterServer) buildSubscriptionRelayEnd(ctx context.Context, protocolMessage chainlib.ProtocolMessage, start time.Time, err error) *relayEnd {
	end := rpcss.newRelayEnd(ctx, protocolMessage, start, err)
	end.selection = relayEndSelectionSubscription
	upstream := chainlib.SubscriptionUpstreamFrom(ctx).Record()
	if upstream.Sent {
		end.attempts = relayEndAttempts{rpcss.subscriptionAttempt(upstream, start, err == nil)}
	}
	if err == nil {
		end.outcome = relayOutcomeOK
		end.hasReply = true
		end.servedBy = upstream.Upstream
		return end
	}
	end.errorText = relayEndText(err.Error())
	switch {
	case chainlib.IsFunctionTagOfType(protocolMessage, spectypes.FUNCTION_TAG_UNSUBSCRIBE_ALL):
		// The router tears a client's subscriptions down itself.
		end.outcome, end.errorCode = relayOutcomeRouterError, common.LavaErrorSubscriptionCleanupFailed.Name
	case upstream.Sent && upstream.Err == nil:
		// The upstream answered, so what failed was the router's own setup of the subscription.
		end.outcome, end.errorCode = relayOutcomeRouterError, common.LavaErrorSubscriptionInitFailed.Name
	case upstream.Sent:
		end.outcome, end.errorCode = relayOutcomeNoAnswer, relayEndErrorCode(err, nil, end.attempts)
	default:
		end.outcome, end.errorCode = relayOutcomeRejectedBeforeDispatch, relayEndErrorCode(err, nil, nil)
	}
	return end
}

// subscriptionAttempt is the one attempt of a subscription request the manager sent upstream. A node
// that answers with a JSON-RPC error refused the request; any other failure is the transport's.
func (rpcss *RPCSmartRouterServer) subscriptionAttempt(upstream chainlib.SubscriptionUpstreamRecord, start time.Time, returned bool) relayEndAttempt {
	attempt := relayEndAttempt{
		upstream:         upstream.Upstream,
		outcome:          attemptOutcomeOK,
		latency:          upstream.Latency,
		returnedToClient: returned,
		sent:             true,
		sentAfter:        max(upstream.SentAt.Sub(start), 0),
	}
	if upstream.Err == nil {
		return attempt
	}
	family, transport := common.ChainFamilyUnknown, common.TransportJsonRPC
	if rpcss.listenEndpoint != nil {
		family = common.GetChainFamilyOrDefault(rpcss.listenEndpoint.ChainID)
		if rpcss.listenEndpoint.ApiInterface == spectypes.APIInterfaceGrpc {
			transport = common.TransportGRPC
		}
	}
	var nodeError *rpcclient.JsonError
	if errors.As(upstream.Err, &nodeError) {
		attempt.outcome = attemptOutcomeNodeError
		attempt.lavaError = common.ClassifyError(nil, family, transport, nodeError.Code, nodeError.Message)
		attempt.message = relayEndText(nodeError.Message)
	} else {
		attempt.transport = true
		attempt.lavaError = common.ClassifyError(common.DetectConnectionError(upstream.Err), family, transport, 0, upstream.Err.Error())
		attempt.outcome = subscriptionTransportOutcome(attempt.lavaError)
		attempt.message = relayEndText(upstream.Err.Error())
	}
	attempt.errorCode = relayEndCode(attempt.lavaError)
	return attempt
}

func subscriptionTransportOutcome(lavaError *common.LavaError) string {
	switch {
	case lavaError.IsRateLimited():
		return attemptOutcomeRateLimited
	case lavaError == common.LavaErrorContextCanceled:
		return attemptOutcomeCancelled
	case lavaError == common.LavaErrorContextDeadline, lavaError == common.LavaErrorConnectionTimeout, lavaError == common.LavaErrorNoResponseTimeout:
		return attemptOutcomeTimeout
	}
	return attemptOutcomeTransportError
}

// subscriptionEndpointNames maps a subscription manager's node URLs to their providers' names. A
// manager picks an upstream by URL, which may carry a credential; relay_end names it as every other
// relay does, by provider.
type subscriptionEndpointNames struct {
	names atomic.Pointer[map[string]string]
}

func (n *subscriptionEndpointNames) set(names map[string]string) {
	n.names.Store(&names)
}

// name is url's provider name, or the URL's host when the manager was given no names.
func (n *subscriptionEndpointNames) name(url string) string {
	if names := n.names.Load(); names != nil {
		if name, found := (*names)[url]; found {
			return name
		}
	}
	return sanitizeEndpointURL(url)
}

// providerNamesByURL maps every node URL of the providers to its provider's name.
func providerNamesByURL(lists ...[]*lavasession.RPCStaticProviderEndpoint) map[string]string {
	names := map[string]string{}
	for _, providers := range lists {
		for _, provider := range providers {
			for i := range provider.NodeUrls {
				names[provider.NodeUrls[i].Url] = provider.Name
			}
		}
	}
	return names
}
