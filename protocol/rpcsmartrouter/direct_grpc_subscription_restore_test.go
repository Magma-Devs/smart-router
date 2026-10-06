package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
)

// restoreUpstream serves the streaming method to any number of subscriptions, told apart by
// the request's host field. A stream gets one message carrying its host and is then held
// open, the shape of a quiet subscription. Each test switches on the behaviour it needs.
type restoreUpstream struct {
	addr string

	mu           sync.Mutex
	opened       map[string]int
	failFirst    map[string]chan struct{} // ends that host's first stream with Internal
	rejected     map[string]codes.Code    // answers every stream of that host with this code
	pushback     map[string]string        // grpc-retry-pushback-ms trailer sent with that host's rejection
	quietReopens bool                     // a reopened stream sends nothing
	failReopens  bool                     // a reopened stream fails at once with Internal
	flapReopens  bool                     // a reopened stream sends its message, then fails with Internal

	refuse   chan struct{} // closed by refuseAll
	refusing atomic.Bool
}

// restoreKeyText stands in for an upstream refusal that names the router's own API key.
const restoreKeyText = "api key sk-test-0001 has expired"

func startRestoreUpstream(t *testing.T) *restoreUpstream {
	t.Helper()
	upstream := &restoreUpstream{
		opened:    map[string]int{},
		failFirst: map[string]chan struct{}{},
		rejected:  map[string]codes.Code{},
		pushback:  map[string]string{},
		refuse:    make(chan struct{}),
	}

	handler := func(_ any, stream grpc.ServerStream) error {
		var request []byte
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		decoded := &grpc_reflection_v1.ServerReflectionRequest{}
		if err := proto.Unmarshal(request, decoded); err != nil {
			return err
		}
		host := decoded.GetHost()

		upstream.mu.Lock()
		upstream.opened[host]++
		opened := upstream.opened[host]
		var fail chan struct{}
		if opened == 1 {
			fail = upstream.failFirst[host]
		}
		code, rejected := upstream.rejected[host]
		pushback := upstream.pushback[host]
		quiet := upstream.quietReopens && opened > 1
		failNow := upstream.failReopens && opened > 1
		flap := upstream.flapReopens && opened > 1
		upstream.mu.Unlock()

		switch {
		case rejected:
			if pushback != "" {
				stream.SetTrailer(metadata.Pairs("grpc-retry-pushback-ms", pushback))
			}
			return status.Error(code, "the upstream rejected "+host)
		case upstream.refusing.Load():
			return status.Error(codes.PermissionDenied, restoreKeyText)
		case failNow:
			return status.Error(codes.Internal, "stream reset")
		}
		if !quiet {
			payload, err := proto.Marshal(&grpc_reflection_v1.ServerReflectionResponse{ValidHost: host})
			if err != nil {
				return err
			}
			if err := stream.SendMsg(payload); err != nil {
				return err
			}
			if flap {
				return status.Error(codes.Internal, "stream reset")
			}
		}
		select {
		case <-fail: // a nil channel never fires
			return status.Error(codes.Internal, "the upstream ended this stream")
		case <-upstream.refuse:
			return status.Error(codes.PermissionDenied, restoreKeyText)
		case <-stream.Context().Done():
			return nil
		}
	}

	server := grpc.NewServer(grpc.UnknownServiceHandler(handler), grpc.ForceServerCodec(grpcproxy.RawBytesCodec{}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	upstream.addr = listener.Addr().String()
	return upstream
}

// endFirstStreamOf arranges for host's first stream to end with Internal when the returned
// function is called. Register it before subscribing.
func (u *restoreUpstream) endFirstStreamOf(host string) func() {
	ch := make(chan struct{})
	u.mu.Lock()
	u.failFirst[host] = ch
	u.mu.Unlock()
	return func() { close(ch) }
}

func (u *restoreUpstream) reject(host string, code codes.Code) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rejected[host] = code
}

// rateLimit answers every stream of host with RESOURCE_EXHAUSTED and, unless pushback is 0, the
// grpc-retry-pushback-ms trailer a rate-limiting server sends with it.
func (u *restoreUpstream) rateLimit(host string, pushback time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rejected[host] = codes.ResourceExhausted
	if pushback > 0 {
		u.pushback[host] = strconv.FormatInt(pushback.Milliseconds(), 10)
	}
}

func (u *restoreUpstream) setReopens(quiet, fail bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.quietReopens, u.failReopens = quiet, fail
}

// setFlappingReopens makes every reopened stream deliver its message and then fail.
func (u *restoreUpstream) setFlappingReopens() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.flapReopens = true
}

// refuseAll ends every open stream, and answers every new one, with PermissionDenied: an
// expired key or a spent quota.
func (u *restoreUpstream) refuseAll() {
	u.refusing.Store(true)
	close(u.refuse)
}

func (u *restoreUpstream) streamsOpened(host string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.opened[host]
}

func (u *restoreUpstream) totalOpened() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	total := 0
	for _, opened := range u.opened {
		total += opened
	}
	return total
}

// subscribeHost opens a subscription whose request names host.
func subscribeHost(t *testing.T, manager *DirectGRPCSubscriptionManager, host string) <-chan *pairingtypes.RelayReply {
	t.Helper()
	return subscribeHostFrom(t, manager, host, "conn-"+host)
}

// subscribeHostFrom is subscribeHost from the client stream connectionID.
func subscribeHostFrom(t *testing.T, manager *DirectGRPCSubscriptionManager, host, connectionID string) <-chan *pairingtypes.RelayReply {
	t.Helper()
	message := newGrpcSubscriptionMessageWithRequest(t, []byte(`{"host":"`+host+`"}`))
	_, replies, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", connectionID, nil)
	require.NoError(t, err)
	return replies
}

func clientKeyOf(manager *DirectGRPCSubscriptionManager, host string) string {
	return manager.ClientKey("dapp", "1.1.1.1", "conn-"+host)
}

// awaitClosed drains replies until the manager closes it, failing after within.
func awaitClosed(t *testing.T, replies <-chan *pairingtypes.RelayReply, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case _, open := <-replies:
			if !open {
				return
			}
		case <-deadline:
			t.Fatalf("the client's channel was still open after %v", within)
		}
	}
}

// requireStillOpen fails if replies has been closed; messages waiting on it are drained.
func requireStillOpen(t *testing.T, replies <-chan *pairingtypes.RelayReply) {
	t.Helper()
	for {
		select {
		case _, open := <-replies:
			require.True(t, open, "the client's channel was closed")
		default:
			return
		}
	}
}

// TestGRPCSubscriptionRestore_EveryStreamOnADeadConnectionComesBack covers what keepalive, a
// GOAWAY or a middlebox reset does: the pooled connection dies and every stream on it fails at
// once. Each of their restores reaches ReconnectWithBackoff together, and all but the first
// used to be refused there, which tore those subscriptions down and closed their clients.
func TestGRPCSubscriptionRestore_EveryStreamOnADeadConnectionComesBack(t *testing.T) {
	upstream := startRestoreUpstream(t)
	proxy := startBlackholeProxy(t, upstream.addr)
	manager := newManagerAgainstUpstream(t, proxy.addr())
	defer manager.Stop()

	aReplies := subscribeHost(t, manager, "a")
	require.Equal(t, "a", awaitStreamPayload(t, aReplies))
	bReplies := subscribeHost(t, manager, "b")
	require.Equal(t, "b", awaitStreamPayload(t, bReplies))

	proxy.dropFlows()

	// A restored stream answers with its host again; a torn-down one closes the channel.
	require.Equal(t, "a", awaitStreamPayload(t, aReplies), "subscription a must be restored")
	require.Equal(t, "b", awaitStreamPayload(t, bReplies), "subscription b must be restored")
	require.Equal(t, int64(2), manager.GetActiveSubscriptionCount())
}

// TestGRPCSubscriptionRestore_OneFailedStreamIsReopenedOnce covers a stream that fails alone
// while its pooled connection keeps serving another. Its restore moves it to a new connection,
// which used to be counted only after the old one was released; the release's scale-down
// closed it as unused, and the restored stream failed at once and went round again.
func TestGRPCSubscriptionRestore_OneFailedStreamIsReopenedOnce(t *testing.T) {
	upstream := startRestoreUpstream(t)
	endY := upstream.endFirstStreamOf("y")
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	xReplies := subscribeHost(t, manager, "x")
	require.Equal(t, "x", awaitStreamPayload(t, xReplies))
	yReplies := subscribeHost(t, manager, "y")
	require.Equal(t, "y", awaitStreamPayload(t, yReplies))

	endY()

	require.Equal(t, "y", awaitStreamPayload(t, yReplies), "subscription y must be restored")
	// A restore that keeps going round shows up as more streams within this second.
	time.Sleep(time.Second)
	require.Equal(t, 2, upstream.streamsOpened("y"), "y must be reopened exactly once")
	require.Equal(t, 1, upstream.streamsOpened("x"), "x must be left alone")
	requireStillOpen(t, xReplies)
}

// TestGRPCSubscriptionRestore_ARefusedRequestEndsWithItsOwnStatus: an upstream that refuses
// the request itself would refuse it again on every restore. The subscription ends at once,
// its client gets the upstream's status, the request is not sent again, and a subscription
// sharing the connection is untouched.
func TestGRPCSubscriptionRestore_ARefusedRequestEndsWithItsOwnStatus(t *testing.T) {
	upstream := startRestoreUpstream(t)
	upstream.reject("bad", codes.InvalidArgument)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	goodReplies := subscribeHost(t, manager, "good")
	require.Equal(t, "good", awaitStreamPayload(t, goodReplies))

	badReplies := subscribeHost(t, manager, "bad")
	awaitClosed(t, badReplies, time.Second)

	endErr := manager.SubscriptionEndError(clientKeyOf(manager, "bad"))
	require.Equal(t, codes.InvalidArgument, status.Code(endErr), "got %v", endErr)
	require.Equal(t, "the upstream rejected bad", status.Convert(endErr).Message())

	// A restore, had one started, would have sent the request again well within this.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, upstream.streamsOpened("bad"), "a refused request must not be sent again")
	require.Equal(t, int64(1), manager.GetActiveSubscriptionCount(), "the other subscription is untouched")
	requireStillOpen(t, goodReplies)
}

// TestGRPCSubscriptionRestore_ACredentialRefusalEndsEveryClientWithoutItsText: an upstream
// refusing every stream for the router's own credentials or quota ends every subscription at
// once, with Unavailable. The client can do nothing about the router's key, and the upstream's
// text about it must not reach the client.
func TestGRPCSubscriptionRestore_ACredentialRefusalEndsEveryClientWithoutItsText(t *testing.T) {
	upstream := startRestoreUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	hosts := []string{"a", "b", "c"}
	replies := map[string]<-chan *pairingtypes.RelayReply{}
	for _, host := range hosts {
		replies[host] = subscribeHost(t, manager, host)
		require.Equal(t, host, awaitStreamPayload(t, replies[host]))
	}

	upstream.refuseAll()

	for _, host := range hosts {
		awaitClosed(t, replies[host], 3*time.Second)
		endErr := manager.SubscriptionEndError(clientKeyOf(manager, host))
		require.Equal(t, codes.Unavailable, status.Code(endErr), "%s got %v", host, endErr)
		require.NotContains(t, status.Convert(endErr).Message(), "sk-test", "the upstream's text about the router's key reached a client")
	}
	require.Equal(t, len(hosts), upstream.totalOpened(), "a refusal must not be restored")
	require.Equal(t, int64(0), manager.GetActiveSubscriptionCount())
}

// newManagerAgainstUpstreams is newManagerAgainstUpstream over a primary tier of several
// upstreams. With no optimizer, selection takes the first one not held off.
func newManagerAgainstUpstreams(t *testing.T, addrs ...string) *DirectGRPCSubscriptionManager {
	t.Helper()
	endpoints := make([]*common.NodeUrl, 0, len(addrs))
	for _, addr := range addrs {
		endpoints = append(endpoints, &common.NodeUrl{
			Url:        "grpc://" + addr,
			GrpcConfig: common.GrpcConfig{AllowInsecure: true},
		})
	}
	manager := NewDirectGRPCSubscriptionManager(nil, "SUI", spectypes.APIInterfaceGrpc, endpoints, nil, nil, nil)

	ctx := context.Background()
	for _, endpoint := range endpoints {
		pool, err := manager.getOrCreatePool(ctx, endpoint)
		require.NoError(t, err)
		conn, err := pool.GetConnectionForStream(ctx)
		require.NoError(t, err)
		conn.descriptorsCache.Store("grpc.reflection.v1.ServerReflection.ServerReflectionInfo", streamingMethodDescriptor(t))
	}
	return manager
}

// TestGRPCSubscriptionRestore_ARateLimitedEndpointIsHeldOff: an upstream that refuses a stream
// for rate ends the subscription with Unavailable, and the client resubscribes at once.
// Selection only reads hold-offs, so the refusal must record one, or the retry goes straight
// back to the endpoint that refused it. A bare RESOURCE_EXHAUSTED is not a recognised rate
// limit, since grpc-go mints the same code for an oversized message, so it holds nothing off.
func TestGRPCSubscriptionRestore_ARateLimitedEndpointIsHeldOff(t *testing.T) {
	for _, tc := range []struct {
		name        string
		pushback    time.Duration
		wantHeldOff bool
	}{
		{name: "with a retry delay", pushback: time.Minute, wantHeldOff: true},
		{name: "bare", wantHeldOff: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limited := startRestoreUpstream(t)
			limited.rateLimit("r", tc.pushback)
			other := startRestoreUpstream(t)
			manager := newManagerAgainstUpstreams(t, limited.addr, other.addr)
			manager.rateLimitHoldoff = holdoff.NewRegistry()
			defer manager.Stop()
			limitedURL := "grpc://" + limited.addr

			// The first endpoint in the tier refuses the subscription.
			first := subscribeHostFrom(t, manager, "r", "first")
			awaitClosed(t, first, 3*time.Second)
			firstKey := manager.ClientKey("dapp", "1.1.1.1", "first")
			endErr := manager.SubscriptionEndError(firstKey)
			require.Equal(t, codes.Unavailable, status.Code(endErr), "got %v", endErr)
			require.Equal(t, tc.wantHeldOff, manager.rateLimitHoldoff.HeldOff(limitedURL, limitedURL))
			// The listener releases a stream once it has ended.
			require.NoError(t, manager.UnsubscribeAll(context.Background(), firstKey))

			// The client resubscribes on a new stream.
			retry := subscribeHostFrom(t, manager, "r", "retry")
			if tc.wantHeldOff {
				require.Equal(t, "r", awaitStreamPayload(t, retry), "the retry must go to the endpoint not held off")
				require.Equal(t, 1, limited.streamsOpened("r"), "the held-off endpoint must not be asked again")
			} else {
				awaitClosed(t, retry, 3*time.Second)
				require.Equal(t, 2, limited.streamsOpened("r"))
				require.Zero(t, other.streamsOpened("r"))
			}
		})
	}
}

// TestGRPCSubscriptionRestore_AStreamThatAnswersClearsTheHoldoff: any answer clears a rate-limit
// hold-off (docs/RATE-LIMIT-HOLDOFF.md). A held-off endpoint still serves when nothing else is
// ready, and once its stream delivers, the hold-off and the strikes behind it are stale.
func TestGRPCSubscriptionRestore_AStreamThatAnswersClearsTheHoldoff(t *testing.T) {
	upstream := startRestoreUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	manager.rateLimitHoldoff = holdoff.NewRegistry()
	defer manager.Stop()
	url := "grpc://" + upstream.addr
	manager.rateLimitHoldoff.RecordRateLimit(url, url, time.Minute)

	replies := subscribeHost(t, manager, "a")
	require.Equal(t, "a", awaitStreamPayload(t, replies))
	require.False(t, manager.rateLimitHoldoff.HeldOff(url, url), "the endpoint answered, so its hold-off is stale")
}

// TestGRPCSubscriptionRestore_GivesUpAfterRepeatedFailures: a stream that fails with an error
// that is not a refusal is restored, but a restored stream that keeps failing before it
// delivers anything is given up after defaultMaxRestoresWithoutProgress restores, and its
// client gets Unavailable.
func TestGRPCSubscriptionRestore_GivesUpAfterRepeatedFailures(t *testing.T) {
	upstream := startRestoreUpstream(t)
	endFirst := upstream.endFirstStreamOf("z")
	upstream.setReopens(false, true)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	replies := subscribeHost(t, manager, "z")
	require.Equal(t, "z", awaitStreamPayload(t, replies))

	endFirst()
	awaitClosed(t, replies, 10*time.Second)

	endErr := manager.SubscriptionEndError(clientKeyOf(manager, "z"))
	require.Equal(t, codes.Unavailable, status.Code(endErr), "got %v", endErr)
	require.Equal(t, 1+defaultMaxRestoresWithoutProgress, upstream.streamsOpened("z"),
		"the first stream plus one per allowed restore")
	require.Equal(t, int64(0), manager.GetActiveSubscriptionCount())
}

// TestGRPCSubscriptionRestore_GivesUpWhenRestoredStreamsDeliverThenFail: a delivered message is
// not progress on its own. A restored stream that delivers and then fails before
// restoreStableAfter counts toward the bound like one that delivers nothing, so a subscription
// that keeps failing that way is given up instead of restored without end.
func TestGRPCSubscriptionRestore_GivesUpWhenRestoredStreamsDeliverThenFail(t *testing.T) {
	upstream := startRestoreUpstream(t)
	endFirst := upstream.endFirstStreamOf("f")
	upstream.setFlappingReopens()
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	replies := subscribeHost(t, manager, "f")
	require.Equal(t, "f", awaitStreamPayload(t, replies))

	endFirst()
	awaitClosed(t, replies, 10*time.Second)

	endErr := manager.SubscriptionEndError(clientKeyOf(manager, "f"))
	require.Equal(t, codes.Unavailable, status.Code(endErr), "got %v", endErr)
	require.Equal(t, 1+defaultMaxRestoresWithoutProgress, upstream.streamsOpened("f"),
		"the first stream plus one per allowed restore")
	require.Equal(t, int64(0), manager.GetActiveSubscriptionCount())
}

// onlyRegisteredSubscription returns the one subscription manager holds.
func onlyRegisteredSubscription(t *testing.T, manager *DirectGRPCSubscriptionManager) *grpcActiveSubscription {
	t.Helper()
	hashedParams := onlyRegisteredSubscriptionKey(t, manager)
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.activeSubscriptions[hashedParams]
}

// TestGRPCSubscriptionRestore_ALeavingClientRunsNoRestore: the last client leaving cancels the
// subscription, and its stream then fails with that cancellation. That is the subscription
// closing, not the upstream failing: no restore starts, so nothing is reopened and no
// reconnect runs on the cancelled context, where it would be abandoned and a restore waiting
// on it would have to dial again.
func TestGRPCSubscriptionRestore_ALeavingClientRunsNoRestore(t *testing.T) {
	upstream := startRestoreUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	replies := subscribeHost(t, manager, "l")
	require.Equal(t, "l", awaitStreamPayload(t, replies))
	sub := onlyRegisteredSubscription(t, manager)

	require.NoError(t, manager.UnsubscribeAll(context.Background(), clientKeyOf(manager, "l")))
	require.Eventually(t, func() bool { return manager.GetActiveSubscriptionCount() == 0 },
		5*time.Second, 10*time.Millisecond)

	// A restore, had one started, would have counted itself well within this.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, upstream.streamsOpened("l"), "a leaving client must not reopen the stream")
	require.Zero(t, sub.restoresWithoutProgress.Load(), "a leaving client must not start a restore")
}

// TestGRPCSubscriptionRestore_AClientLeavingDuringCleanupStrandsNoEndStatus: a client can leave
// while the router gives its subscription up. UnsubscribeAll is the last call the listener makes
// for a stream and drops any end status it left unread, so a status recorded after it would stay
// in the manager for good. Cleanup is held where it releases the clients, and the client leaves
// in that window.
func TestGRPCSubscriptionRestore_AClientLeavingDuringCleanupStrandsNoEndStatus(t *testing.T) {
	upstream := startRestoreUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	replies := subscribeHost(t, manager, "s")
	require.Equal(t, "s", awaitStreamPayload(t, replies))
	sub := onlyRegisteredSubscription(t, manager)

	// Cleanup cancels the subscription and then waits here for its lock.
	sub.lock.Lock()
	sub.giveUp(errSubscriptionLost)
	cleaned := make(chan struct{})
	go func() {
		defer close(cleaned)
		manager.cleanupSubscription(sub.hashedParams, sub)
	}()
	require.Eventually(t, func() bool { return sub.ctx.Err() != nil }, 5*time.Second, time.Millisecond)

	left := make(chan struct{})
	go func() {
		defer close(left)
		_ = manager.UnsubscribeAll(context.Background(), clientKeyOf(manager, "s"))
	}()
	select {
	case <-left:
	case <-time.After(200 * time.Millisecond):
		// Still registered, so the client waits for the subscription's lock too
	}
	sub.lock.Unlock()
	<-cleaned
	<-left

	_, stranded := manager.endErrors.Load(clientKeyOf(manager, "s"))
	require.False(t, stranded, "an end status recorded after its client left is never read or dropped")
}

// TestGRPCSubscriptionRestore_AQuietStreamSurvivesSpacedReconnects: a quiet subscription can go
// minutes without a message, while a load balancer's connection age limit or keepalive drops
// its connection now and then. Each restore works, so none of them may count toward giving it
// up: a restored stream that stays up for restoreStableAfter is progress too.
func TestGRPCSubscriptionRestore_AQuietStreamSurvivesSpacedReconnects(t *testing.T) {
	upstream := startRestoreUpstream(t)
	upstream.setReopens(true, false)
	proxy := startBlackholeProxy(t, upstream.addr)
	manager := newManagerAgainstUpstream(t, proxy.addr())
	manager.restoreStableAfter = 200 * time.Millisecond
	defer manager.Stop()

	replies := subscribeHost(t, manager, "q")
	require.Equal(t, "q", awaitStreamPayload(t, replies))

	drops := defaultMaxRestoresWithoutProgress + 2
	for i := 1; i <= drops; i++ {
		proxy.dropFlows()
		require.Eventually(t, func() bool { return upstream.streamsOpened("q") == i+1 },
			5*time.Second, 10*time.Millisecond, "restore %d never reopened the stream", i)
		// The restored stream stays up long enough to count as progress.
		time.Sleep(2 * manager.restoreStableAfter)
	}

	require.Equal(t, int64(1), manager.GetActiveSubscriptionCount(),
		"a quiet subscription whose restores keep working must not be given up")
	requireStillOpen(t, replies)
}

// restoreFeedMethod is the streaming method the listener-level test subscribes to. Not the
// reflection method the manager-level tests use: the listener answers reflection calls itself,
// so those never reach the subscription manager.
const restoreFeedMethod = "test.v1.Feed/Subscribe"

// restoreRawCodec carries messages as their wire bytes, named "proto" so the client's calls
// go out as application/grpc+proto.
type restoreRawCodec struct{}

func (restoreRawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("cannot marshal %T", v)
	}
	return b, nil
}

func (restoreRawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("cannot unmarshal into %T", v)
	}
	*b = append([]byte(nil), data...)
	return nil
}

func (restoreRawCodec) Name() string { return "proto" }

// restoreRelaySender stands in for the router behind the gRPC listener: it parses with the
// router's own ParseRelay and serves subscriptions from the real manager.
type restoreRelaySender struct {
	rpcss   *RPCSmartRouterServer
	manager *DirectGRPCSubscriptionManager
}

func (s *restoreRelaySender) SendRelay(context.Context, string, string, string, string, string, *metrics.RelayMetrics, []pairingtypes.Metadata) (*common.RelayResult, error) {
	return nil, errors.New("unary relays are not used here")
}

func (s *restoreRelaySender) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadataValues []pairingtypes.Metadata) (chainlib.ProtocolMessage, error) {
	return s.rpcss.ParseRelay(ctx, url, req, connectionType, dappID, consumerIp, metadataValues)
}

func (s *restoreRelaySender) SendParsedRelay(context.Context, *metrics.RelayMetrics, chainlib.ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("unary relays are not used here")
}

func (s *restoreRelaySender) CancelSubscriptionContext(string) {}

func (s *restoreRelaySender) GetGRPCSubscriptionManager() chainlib.GRPCSubscriptionManager {
	return s.manager
}

type restoreHealthyReporter struct{}

func (restoreHealthyReporter) IsHealthy() bool { return true }

// dialRestoreListener starts a real gRPC listener whose subscriptions run through manager,
// with a spec that declares the streaming method a subscription, and returns a client
// connection to it.
func dialRestoreListener(t *testing.T, manager *DirectGRPCSubscriptionManager) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	parser, err := chainlib.NewChainParser(spectypes.APIInterfaceGrpc)
	require.NoError(t, err)
	parser.SetSpec(spectypes.Spec{
		Index:            "SUI",
		Enabled:          true,
		AverageBlockTime: 1000,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGrpc},
			Apis: []*spectypes.Api{{
				Name:         restoreFeedMethod,
				Enabled:      true,
				ComputeUnits: 10,
				Category:     spectypes.SpecCategory{HangingApi: true},
			}},
			ParseDirectives: []*spectypes.ParseDirective{{
				FunctionTag: spectypes.FUNCTION_TAG_SUBSCRIBE,
				ApiName:     restoreFeedMethod,
			}},
		}},
	})

	// The fake upstream serves any method, so the feed reuses the streaming descriptor the
	// harness has: a server-streaming method over the reflection request and response types.
	connectionForUpstream(t, manager).descriptorsCache.Store("test.v1.Feed.Subscribe", streamingMethodDescriptor(t))

	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGrpc}
	sender := &restoreRelaySender{rpcss: &RPCSmartRouterServer{chainParser: parser, listenEndpoint: endpoint}, manager: manager}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	listener := chainlib.NewGrpcChainListener(ctx, endpoint, sender, restoreHealthyReporter{}, logger, parser)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
		select {
		case <-listenerDone:
		case <-shutdownCtx.Done():
			t.Error("gRPC listener did not stop")
		}
	})

	var addr string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && addr == "" {
		addr = listener.GetListeningAddress()
		time.Sleep(20 * time.Millisecond)
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// openClientStream subscribes through conn the way a gRPC client does, with a binary request
// naming host.
func openClientStream(t *testing.T, conn *grpc.ClientConn, host string) grpc.ClientStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/"+restoreFeedMethod, grpc.ForceCodec(restoreRawCodec{}))
	require.NoError(t, err)
	request, err := proto.Marshal(&grpc_reflection_v1.ServerReflectionRequest{Host: host})
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(request))
	require.NoError(t, stream.CloseSend())
	return stream
}

// TestGRPCSubscriptionRestore_TheClientSeesTheRefusalStatus drives a real gRPC client through
// the real listener: when the router ends a subscription because the upstream refused it, the
// client's stream must end with that status. Ending it with OK reads as a finished stream, so
// the client neither fixes its request nor resubscribes.
func TestGRPCSubscriptionRestore_TheClientSeesTheRefusalStatus(t *testing.T) {
	upstream := startRestoreUpstream(t)
	upstream.reject("bad", codes.InvalidArgument)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()
	conn := dialRestoreListener(t, manager)

	// Control: a good request streams through the same listener.
	good := openClientStream(t, conn, "good")
	var payload []byte
	require.NoError(t, good.RecvMsg(&payload))
	decoded := &grpc_reflection_v1.ServerReflectionResponse{}
	require.NoError(t, proto.Unmarshal(payload, decoded))
	require.Equal(t, "good", decoded.ValidHost)

	bad := openClientStream(t, conn, "bad")
	received := make(chan error, 1)
	go func() {
		var reply []byte
		received <- bad.RecvMsg(&reply)
	}()
	select {
	case err := <-received:
		require.Equal(t, codes.InvalidArgument, status.Code(err), "the client must see the refusal, not a normal end: %v", err)
		require.Equal(t, "the upstream rejected bad", status.Convert(err).Message())
	case <-time.After(time.Second):
		t.Fatal("the client's stream did not end within 1s")
	}
}

// TestGRPCSubscriptionRestore_TheClientSeesWhyItWasGivenUp drives each way the router gives a
// subscription up through the real listener, as TestGRPCSubscriptionRestore_TheClientSeesTheRefusalStatus
// does for a refused request. The client's stream must end with the router's own Unavailable
// status, not OK, so the client resubscribes instead of taking the end for a finished stream;
// and with that exact text, so none of the upstream's text about the router's key reaches it.
func TestGRPCSubscriptionRestore_TheClientSeesWhyItWasGivenUp(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arm sets the upstream up before the client subscribes, and returns what gives the
		// subscription up once the client has had its first message.
		arm  func(*restoreUpstream, *DirectGRPCSubscriptionManager) func()
		want error
	}{
		{
			name: "the upstream refuses the router's credentials",
			arm:  func(u *restoreUpstream, _ *DirectGRPCSubscriptionManager) func() { return u.refuseAll },
			want: errUpstreamRefused,
		},
		{
			name: "restores make no progress",
			arm: func(u *restoreUpstream, _ *DirectGRPCSubscriptionManager) func() {
				endFirst := u.endFirstStreamOf("g")
				u.setReopens(false, true)
				return endFirst
			},
			want: errSubscriptionLost,
		},
		{
			name: "the router stops",
			arm:  func(_ *restoreUpstream, m *DirectGRPCSubscriptionManager) func() { return m.Stop },
			want: errRouterStopping,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := startRestoreUpstream(t)
			manager := newManagerAgainstUpstream(t, upstream.addr)
			defer manager.Stop()
			giveUp := tc.arm(upstream, manager)
			conn := dialRestoreListener(t, manager)

			stream := openClientStream(t, conn, "g")
			var payload []byte
			require.NoError(t, stream.RecvMsg(&payload), "the subscription must stream before it is given up")

			giveUp()

			received := make(chan error, 1)
			go func() {
				var reply []byte
				received <- stream.RecvMsg(&reply)
			}()
			select {
			case err := <-received:
				require.Equal(t, codes.Unavailable, status.Code(err), "the client must see why, not a normal end: %v", err)
				require.Equal(t, status.Convert(tc.want).Message(), status.Convert(err).Message())
			case <-time.After(10 * time.Second):
				t.Fatal("the client's stream did not end within 10s")
			}
		})
	}
}
