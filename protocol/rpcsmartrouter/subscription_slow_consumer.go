package rpcsmartrouter

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// subscriptionClientReplyBuffer is how many pushes a client may fall behind on a shared
// subscription before it is dropped as a slow consumer. The fan-out never waits for a client:
// a push that finds the client's channel full ends that client's subscription instead, which
// is counted (smartrouter_subscription_clients_dropped_total) and visible to the client, where
// the previous poll-and-drop lost the push silently and stalled every other client of the
// subscription for the duration of the poll (MAG-4242). It is the depth the gRPC manager has
// always given its per-client channel, now shared with the WebSocket manager.
const subscriptionClientReplyBuffer = 100

// errSlowConsumer is the status a gRPC client's stream ends with when it was dropped for not
// keeping up with the subscription.
var errSlowConsumer = status.Error(codes.Unavailable, "subscription ended: the client did not keep up with its pushes")
