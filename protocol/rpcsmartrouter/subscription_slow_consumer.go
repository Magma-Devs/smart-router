package rpcsmartrouter

import (
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// subscriptionClientReplyBuffer is how many pushes a client may fall behind on a shared
// subscription before the fan-out has to wait for it. It is the depth the gRPC manager has
// always given its per-client channel, now shared with the WebSocket manager.
const subscriptionClientReplyBuffer = 100

// subscriptionClientSendGrace is how long a push waits for room in a client's full reply channel
// before the client is dropped as a slow consumer: counted
// (smartrouter_subscription_clients_dropped_total) and its subscription ended, where the previous
// poll-and-drop lost the push silently and kept the client (MAG-4242).
//
// A drop on the first full channel took healthy clients with it: a node publishes a block's logs
// back to back, and a client reading every push only a little slower than they arrive is a buffer
// behind within microseconds of the burst's start. Such a client frees a slot well within the
// grace, so the fan-out paces itself to it, as Send's retries always did, and drops only a client
// that frees nothing for the whole grace. The bound is the ~10 ms those retries allowed every push,
// so the other clients of a subscription never wait longer than they did before, and only while a
// client is a full buffer behind rather than on every push. Zero restores drop-at-once.
const subscriptionClientSendGrace = 10 * time.Millisecond

// errSlowConsumer is the status a gRPC client's stream ends with when it was dropped for not
// keeping up with the subscription.
var errSlowConsumer = status.Error(codes.Unavailable, "subscription ended: the client did not keep up with its pushes")
