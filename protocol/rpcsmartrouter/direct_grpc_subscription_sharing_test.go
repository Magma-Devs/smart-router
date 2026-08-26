package rpcsmartrouter

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
)

// streamingMethodPath is the ServerReflectionInfo method, borrowed purely as a
// server-streaming method descriptor that is already linked into the binary — building
// one from scratch would mean generating a proto for the test alone. Nothing here
// performs reflection; the descriptor is seeded into the connection cache directly.
const streamingMethodPath = "grpc.reflection.v1.ServerReflection/ServerReflectionInfo"

// TestGRPCManagerImplementsListenerInterface pins the contract the gRPC listener
// consumes (MAG-2643). StartSubscription had zero production callers before that
// ticket, so nothing would otherwise catch a signature drift here.
func TestGRPCManagerImplementsListenerInterface(t *testing.T) {
	var _ chainlib.GRPCSubscriptionManager = (*DirectGRPCSubscriptionManager)(nil)
}

// TestGRPCClientKeyMatchesInternalKey guards the seam that makes disconnect cleanup
// work: the listener releases a client by the key it gets from ClientKey, and
// StartSubscription tracks it under createClientKey. If those two ever diverge,
// UnsubscribeAll silently releases nothing and every disconnect leaks a subscription.
func TestGRPCClientKeyMatchesInternalKey(t *testing.T) {
	manager := NewDirectGRPCSubscriptionManager(nil, "SUI", "grpc", nil, nil, nil, nil)

	require.Equal(t,
		manager.createClientKey("dapp", "1.2.3.4", "conn-7"),
		manager.ClientKey("dapp", "1.2.3.4", "conn-7"))
}

// TestGRPCJoinExistingSubscription_MintsPerClientAck covers subscription sharing, which
// is on by default. A joiner used to be handed the creator's cached acknowledgement,
// so it was told to quote a subscription id registered to somebody else — Unsubscribe
// matches on the caller's own entry in clientRouterIDs and would never have found it.
func TestGRPCJoinExistingSubscription_MintsPerClientAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fixture := newTestGRPCManagerWithSub(t, ctx, cancel)
	fixture.sub.methodPath = streamingMethodPath

	const joinerKey = "dapp:5.6.7.8:conn-2"
	ack, replies, err := fixture.manager.joinExistingSubscription(ctx, fixture.sub, joinerKey, fixture.hashedParams)
	require.NoError(t, err)
	require.NotNil(t, replies, "a joining client needs its own channel")

	fixture.sub.lock.RLock()
	joinerRouterID := fixture.sub.clientRouterIDs[joinerKey]
	creatorRouterID := fixture.sub.clientRouterIDs[testGRPCClientKey]
	fixture.sub.lock.RUnlock()

	require.NotEmpty(t, joinerRouterID)
	require.NotEqual(t, creatorRouterID, joinerRouterID, "each client gets a distinct router id")

	var ackSubscriptionID string
	for _, entry := range ack.GetMetadata() {
		if entry.Name == MetadataGRPCSubscriptionID {
			ackSubscriptionID = entry.Value
		}
	}
	require.Equal(t, joinerRouterID, ackSubscriptionID,
		"the acknowledgement must carry the joiner's own id, not the creator's")
	require.Contains(t, string(ack.GetData()), joinerRouterID)
}

// TestGRPCJoinExistingSubscription_RefusesSubscriptionBeingTornDown covers the join
// side of the sharing hot path: same params means a departing client and an arriving one
// meet on the same subscription object. StartSubscription reads it out of
// activeSubscriptions under dgm.lock and drops that lock before joining, so the whole
// teardown can happen in the gap.
//
// Both outcomes were bad. Against a subscription cleanup had finished with,
// connectedClients is nil and the join assigned to a nil map — an unrecoverable panic
// that kills the router. Against one whose last client had just left, the join succeeded
// and the cleanup still in flight closed the joiner's brand-new channel: a clean EOF
// one message into a subscription, which is the silent truncation this path exists to
// remove.
func TestGRPCJoinExistingSubscription_RefusesSubscriptionBeingTornDown(t *testing.T) {
	tests := []struct {
		name string
		// tearDown puts the fixture's subscription into one of the states a joiner can
		// find it in, each reached through the production path that produces it.
		tearDown func(t *testing.T, fixture grpcSubFixture)
	}{
		{
			name: "cleanup already released it",
			tearDown: func(t *testing.T, fixture grpcSubFixture) {
				fixture.manager.cleanupSubscription(fixture.hashedParams, fixture.sub)
			},
		},
		{
			name: "last client left, cleanup still pending",
			tearDown: func(t *testing.T, fixture grpcSubFixture) {
				require.NoError(t, fixture.manager.removeClientFromSubscription(fixture.sub, fixture.hashedParams, testGRPCClientKey))
				require.True(t, fixture.isRegistered(t),
					"the subscription must still be registered, or this is not the window a joiner sees")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fixture := newTestGRPCManagerWithSub(t, ctx, cancel)
			fixture.sub.methodPath = streamingMethodPath
			tt.tearDown(t, fixture)

			var (
				ack     *pairingtypes.RelayReply
				replies <-chan *pairingtypes.RelayReply
				err     error
			)
			require.NotPanics(t, func() {
				ack, replies, err = fixture.manager.joinExistingSubscription(ctx, fixture.sub, "dapp:5.6.7.8:conn-2", fixture.hashedParams)
			}, "joining a subscription being torn down must not write to a nil map")

			require.ErrorIs(t, err, errSubscriptionUnavailable,
				"the joiner must be sent to the create-new path, not handed a dying subscription")
			require.Nil(t, ack)
			require.Nil(t, replies)

			fixture.sub.lock.RLock()
			_, joined := fixture.sub.connectedClients["dapp:5.6.7.8:conn-2"]
			fixture.sub.lock.RUnlock()
			require.False(t, joined, "a refused joiner must leave nothing behind on the subscription")
		})
	}
}

// TestGRPCManager_JoinAndUnsubscribeRespectLockOrder drives the manager's two locks from
// both sides at once. Unsubscribe scans under dgm.lock and reaches for sub.lock inside
// it; join and remove used to hold sub.lock and reach for dgm.lock through
// track/untrackClientSubscription. That is a plain AB-BA inversion, and dgm.lock is an
// RWMutex — a single pending writer blocks every later reader, so it takes the whole
// manager with it rather than the two goroutines involved.
//
// A deadlock has no assertion of its own, so the workload runs in its own goroutine and
// the timeout is the failure: the fixed ordering finishes this in milliseconds.
func TestGRPCManager_JoinAndUnsubscribeRespectLockOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fixture := newTestGRPCManagerWithSub(t, ctx, cancel)
	fixture.sub.methodPath = streamingMethodPath

	const (
		workers = 8
		rounds  = 100
	)

	finished := make(chan struct{})
	go func() {
		defer close(finished)

		var wg sync.WaitGroup
		for worker := range workers {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for round := range rounds {
					// A fresh key per round, as gRPC mints one per stream — so the
					// rate limiter admits every call and the loop keeps exercising
					// both lock paths rather than short-circuiting.
					clientKey := fmt.Sprintf("dapp:1.2.3.4:worker-%d-round-%d", worker, round)

					ack, _, err := fixture.manager.joinExistingSubscription(ctx, fixture.sub, clientKey, fixture.hashedParams)
					if err != nil {
						return
					}
					_ = fixture.manager.Unsubscribe(context.Background(), ackSubscriptionID(ack), clientKey)
				}
			}(worker)
		}
		wg.Wait()
	}()

	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("join and Unsubscribe deadlocked: dgm.lock and sub.lock were taken in both orders")
	}

	// The fixture's original client never leaves, so the subscription must have survived
	// — otherwise the workers stopped early and proved nothing.
	require.True(t, fixture.isRegistered(t), "the workload must have run against a live subscription")
	fixture.sub.lock.RLock()
	remaining := len(fixture.sub.connectedClients)
	fixture.sub.lock.RUnlock()
	require.Equal(t, 1, remaining, "every joiner unsubscribed; only the fixture's own client remains")
}

func ackSubscriptionID(ack *pairingtypes.RelayReply) string {
	for _, entry := range ack.GetMetadata() {
		if entry.Name == MetadataGRPCSubscriptionID {
			return entry.Value
		}
	}
	return ""
}

// TestGRPCStartSubscription_ReplacesSubscriptionThatDiedBeforeJoin is the other half:
// being refused by joinExistingSubscription must cost the client nothing. It opens its
// own stream instead, so the race resolves into a working subscription rather than an
// error the caller never asked for.
func TestGRPCStartSubscription_ReplacesSubscriptionThatDiedBeforeJoin(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	message := newGrpcSubscriptionMessage(t)

	// One client comes and goes, purely to learn the key these params share under —
	// hashSubscriptionParams hashes the parsed request, not the raw bytes handed in.
	_, _, err := manager.StartSubscription(context.Background(), message, "dapp", "0.0.0.0", "probe-conn", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)
	hashedParams := onlyRegisteredSubscriptionKey(t, manager)
	require.NoError(t, manager.UnsubscribeAll(context.Background(), manager.ClientKey("dapp", "0.0.0.0", "probe-conn")))
	<-upstream.streamEnded

	// Now leave a released subscription registered under that key. That is the state
	// between a last client leaving and the listener — parked in RecvMsg, which only
	// checks ctx.Done() between receives — getting round to releasing it. On a quiet
	// stream it is not a narrow window.
	deadCtx, deadCancel := context.WithCancel(context.Background())
	deadCancel()
	dead, _ := newTestGRPCSub(deadCtx, deadCancel, "dapp:9.9.9.9:departed")
	dead.hashedParams = hashedParams
	dead.cleanedUp.Store(true)
	dead.connectedClients = nil

	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams] = dead
	manager.lock.Unlock()

	_, replies, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", "joiner-conn", nil)
	require.NoError(t, err, "finding a dead subscription must not fail the subscribe")
	requireUpstreamStreamOpened(t, upstream)

	upstream.messages <- marshalStreamPayload(t, "checkpoint-1")
	require.Equal(t, "checkpoint-1", awaitStreamPayload(t, replies),
		"the client must land on a live stream of its own, not the released one")
}

// TestGRPCStartSubscription_ReleasesClientStateOnFailure bounds two maps that nothing
// else reaps. Client keys are minted per gRPC stream and never seen again, and the only
// other release path — UnsubscribeAll, from the listener's stream Close — runs solely
// for a stream that was actually established. So every *failed* subscribe used to strand
// a rate-limiter entry, plus a sticky-session entry once it got past the connect. With
// endpoints down and a client retrying, both grew without bound.
//
// The failure is staged at the descriptor check on purpose: it is the last thing
// createNewSubscription does before the stream exists, and it is past the point where
// the sticky entry has already been written.
func TestGRPCStartSubscription_ReleasesClientStateOnFailure(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	// Same method, described as unary — createNewSubscription refuses it after it has
	// connected and pinned the client to the endpoint.
	connectionForUpstream(t, manager).descriptorsCache.Store(
		"grpc.reflection.v1.ServerReflection.ServerReflectionInfo", unaryMethodDescriptor(t))

	message := newGrpcSubscriptionMessage(t)
	const retries = 5
	clientKeys := make([]string, 0, retries)

	for attempt := range retries {
		connectionUniqueId := fmt.Sprintf("conn-%d", attempt)
		clientKeys = append(clientKeys, manager.ClientKey("dapp", "1.1.1.1", connectionUniqueId))

		_, _, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", connectionUniqueId, nil)
		require.Error(t, err, "a non-streaming method must not open a subscription")
	}

	manager.rateLimiter.lock.Lock()
	strandedLimiters := len(manager.rateLimiter.subscribeLimiters)
	manager.rateLimiter.lock.Unlock()
	require.Zero(t, strandedLimiters,
		"a failed subscribe must not strand a rate-limiter entry under a key that will never be seen again")

	for _, clientKey := range clientKeys {
		_, pinned := manager.stickyStore.Get(clientKey)
		require.False(t, pinned, "a failed subscribe must not leave the client pinned to an endpoint")
	}
}

// TestGRPCSubscription_RoutesUpstreamMessagesToClient is the end-to-end proof that
// StartSubscription actually works against a live server-streaming upstream. It had zero
// production callers before MAG-2643 and zero tests that drove a real stream through it.
func TestGRPCSubscription_RoutesUpstreamMessagesToClient(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	_, replies, err := manager.StartSubscription(context.Background(), newGrpcSubscriptionMessage(t), "dapp", "1.1.1.1", "conn-1", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)

	upstream.messages <- marshalStreamPayload(t, "checkpoint-1")
	upstream.messages <- marshalStreamPayload(t, "checkpoint-2")
	require.Equal(t, "checkpoint-1", awaitStreamPayload(t, replies))
	require.Equal(t, "checkpoint-2", awaitStreamPayload(t, replies))
}

// TestGRPCSubscription_SurvivesCreatorDisconnect is the ownership rule that makes
// subscription sharing safe, and it is on by default.
//
// conn.NewStream binds an upstream stream to the context it was created with. That used
// to be the creating client's request context, so the moment the first subscriber
// disconnected the shared upstream stream was cancelled underneath every client that had
// joined it — they saw their subscription die for no reason of their own.
func TestGRPCSubscription_SurvivesCreatorDisconnect(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	message := newGrpcSubscriptionMessage(t)

	creatorCtx, disconnectCreator := context.WithCancel(context.Background())
	_, creatorReplies, err := manager.StartSubscription(creatorCtx, message, "dapp", "1.1.1.1", "creator-conn", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)

	// Identical parameters, so this client joins rather than opening a second stream.
	_, joinerReplies, err := manager.StartSubscription(context.Background(), message, "dapp", "2.2.2.2", "joiner-conn", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), manager.GetActiveSubscriptionCount(), "identical params must share one upstream stream")

	upstream.messages <- marshalStreamPayload(t, "before-disconnect")
	require.Equal(t, "before-disconnect", awaitStreamPayload(t, creatorReplies))
	require.Equal(t, "before-disconnect", awaitStreamPayload(t, joinerReplies))

	// The creator goes away: its stream context is cancelled and the listener releases
	// it, exactly as grpcproxy.StreamResponse.Close does.
	disconnectCreator()
	require.NoError(t, manager.UnsubscribeAll(context.Background(), manager.ClientKey("dapp", "1.1.1.1", "creator-conn")))

	upstream.messages <- marshalStreamPayload(t, "after-disconnect")
	require.Equal(t, "after-disconnect", awaitStreamPayload(t, joinerReplies),
		"the shared upstream stream must outlive the client that opened it")
}

// TestGRPCSubscription_LastClientLeavingClosesUpstream is the other side of that rule:
// decoupling the stream from the caller's context must not leave it running forever.
func TestGRPCSubscription_LastClientLeavingClosesUpstream(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	_, replies, err := manager.StartSubscription(context.Background(), newGrpcSubscriptionMessage(t), "dapp", "1.1.1.1", "only-conn", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)

	require.NoError(t, manager.UnsubscribeAll(context.Background(), manager.ClientKey("dapp", "1.1.1.1", "only-conn")))

	select {
	case <-upstream.streamEnded:
	case <-time.After(10 * time.Second):
		t.Fatal("the upstream stream is still running after its last client left")
	}

	require.Eventually(t, func() bool {
		select {
		case _, open := <-replies:
			return !open
		default:
			return false
		}
	}, 10*time.Second, 20*time.Millisecond, "the client channel must be closed once the subscription is released")
}

// TestGRPCManagerStop_AfterLastClientLeft covers a shutdown crash. Both the last client
// leaving and Stop closed closeSubChan, and the listener only notices the close between
// receives — so a subscription whose upstream has gone quiet stays registered long after
// its last client left, and a Stop landing in that window closed an already-closed
// channel and panicked the process.
func TestGRPCManagerStop_AfterLastClientLeft(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fixture := newTestGRPCManagerWithSub(t, ctx, cancel)

	// Last client leaves. The listener has not run cleanup yet, so the subscription is
	// still registered — exactly the state Stop must tolerate.
	require.NoError(t, fixture.manager.removeClientFromSubscription(fixture.sub, fixture.hashedParams, testGRPCClientKey))
	require.True(t, fixture.isRegistered(t), "the fixture must still be registered for this to be the real window")

	require.NotPanics(t, fixture.manager.Stop, "shutdown must tolerate a subscription whose last client already left")
}

// TestGRPCSubscription_SendsRequestEvenWhenEmpty covers a bug found by running this
// against a live Sui testnet node: SubscribeCheckpoints with no options is an
// all-default request, which marshals to zero bytes. The initial send was skipped on
// that basis, so the stream half-closed without ever sending a request message and the
// upstream failed the whole call with "Internal: Missing request message".
//
// Empty is a valid message, not an absent one. The fake upstream only signals once its
// RecvMsg succeeds, so a skipped send shows up here as a stream that never opens.
func TestGRPCSubscription_SendsRequestEvenWhenEmpty(t *testing.T) {
	upstream := startFakeStreamingUpstream(t)
	manager := newManagerAgainstUpstream(t, upstream.addr)
	defer manager.Stop()

	message := newGrpcSubscriptionMessageWithRequest(t, nil)

	_, replies, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", "conn-1", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, upstream)

	upstream.messages <- marshalStreamPayload(t, "checkpoint-1")
	require.Equal(t, "checkpoint-1", awaitStreamPayload(t, replies),
		"an empty request must still open a working stream")
}

// --- harness ----------------------------------------------------------------

// fakeStreamingUpstream serves any method as a server stream, pushing whatever the test
// writes to it. RawBytesCodec means it never has to know the method's proto types.
type fakeStreamingUpstream struct {
	addr        string
	messages    chan []byte
	streamsOpen chan struct{}
	streamEnded chan struct{}
}

func startFakeStreamingUpstream(t *testing.T) *fakeStreamingUpstream {
	t.Helper()

	upstream := &fakeStreamingUpstream{
		messages:    make(chan []byte, 8),
		streamsOpen: make(chan struct{}, 4),
		streamEnded: make(chan struct{}, 4),
	}

	handler := func(srv any, stream grpc.ServerStream) error {
		var request []byte
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		upstream.streamsOpen <- struct{}{}
		defer func() { upstream.streamEnded <- struct{}{} }()

		for {
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case payload, open := <-upstream.messages:
				if !open {
					return nil
				}
				if err := stream.SendMsg(payload); err != nil {
					return err
				}
			}
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

// newManagerAgainstUpstream points a manager at the fake upstream and seeds the method
// descriptor into the connection cache, so GetMethodDescriptor never calls reflection.
func newManagerAgainstUpstream(t *testing.T, addr string) *DirectGRPCSubscriptionManager {
	t.Helper()

	endpoint := &common.NodeUrl{
		Url:        "grpc://" + addr,
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	}
	manager := NewDirectGRPCSubscriptionManager(nil, "SUI", spectypes.APIInterfaceGrpc,
		[]*common.NodeUrl{endpoint}, nil, nil, nil)

	ctx := context.Background()
	pool, err := manager.getOrCreatePool(ctx, endpoint)
	require.NoError(t, err)
	conn, err := pool.GetConnectionForStream(ctx)
	require.NoError(t, err)
	conn.descriptorsCache.Store("grpc.reflection.v1.ServerReflection.ServerReflectionInfo", streamingMethodDescriptor(t))

	return manager
}

// connectionForUpstream returns the single pooled connection newManagerAgainstUpstream
// created, so a test can re-seed its descriptor cache. GetConnectionForStream reuses a
// healthy connection until streamsPerConn is reached, so this is the same one the
// manager will pick up.
func connectionForUpstream(t *testing.T, manager *DirectGRPCSubscriptionManager) *UpstreamGRPCStreamConnection {
	t.Helper()

	manager.lock.RLock()
	require.Len(t, manager.upstreamPools, 1, "the harness points the manager at exactly one endpoint")
	var pool *UpstreamGRPCPool
	for _, only := range manager.upstreamPools {
		pool = only
	}
	manager.lock.RUnlock()

	conn, err := pool.GetConnectionForStream(context.Background())
	require.NoError(t, err)
	return conn
}

// onlyRegisteredSubscriptionKey reads back the hashedParams a subscribe registered
// under, so a test never has to reproduce hashSubscriptionParams' inputs by hand.
func onlyRegisteredSubscriptionKey(t *testing.T, manager *DirectGRPCSubscriptionManager) string {
	t.Helper()

	manager.lock.RLock()
	defer manager.lock.RUnlock()

	require.Len(t, manager.activeSubscriptions, 1)
	for hashedParams := range manager.activeSubscriptions {
		return hashedParams
	}
	return ""
}

func streamingMethodDescriptor(t *testing.T) *desc.MethodDescriptor {
	t.Helper()

	messageDescriptor, err := desc.LoadMessageDescriptorForMessage(&grpc_reflection_v1.ServerReflectionRequest{})
	require.NoError(t, err)
	service := messageDescriptor.GetFile().FindService("grpc.reflection.v1.ServerReflection")
	require.NotNil(t, service)
	method := service.FindMethodByName("ServerReflectionInfo")
	require.NotNil(t, method)
	require.True(t, method.IsServerStreaming(), "the harness relies on this being a server-streaming method")
	return method
}

// unaryMethodDescriptor is any linked non-streaming method, borrowed the same way
// streamingMethodDescriptor borrows a streaming one — it exists so a test can make
// createNewSubscription fail its IsServerStreaming check without going near the network.
func unaryMethodDescriptor(t *testing.T) *desc.MethodDescriptor {
	t.Helper()

	messageDescriptor, err := desc.LoadMessageDescriptorForMessage(&grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	service := messageDescriptor.GetFile().FindService("grpc.health.v1.Health")
	require.NotNil(t, service)
	method := service.FindMethodByName("Check")
	require.NotNil(t, method)
	require.False(t, method.IsServerStreaming(), "the harness relies on this NOT being a server-streaming method")
	return method
}

// newGrpcSubscriptionMessage builds a real gRPC chain message through the production
// parser, with the API carrying a SUBSCRIBE parse directive the way a spec would.
func newGrpcSubscriptionMessage(t *testing.T) chainlib.ChainMessage {
	return newGrpcSubscriptionMessageWithRequest(t, []byte("{}"))
}

// newGrpcSubscriptionMessageWithRequest is the same, with control over the request
// payload — an empty one is the ordinary case for a no-options subscribe.
func newGrpcSubscriptionMessageWithRequest(t *testing.T, requestData []byte) chainlib.ChainMessage {
	t.Helper()

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
				Name:         streamingMethodPath,
				Enabled:      true,
				ComputeUnits: 10,
				Category:     spectypes.SpecCategory{HangingApi: true},
			}},
			ParseDirectives: []*spectypes.ParseDirective{{
				FunctionTag: spectypes.FUNCTION_TAG_SUBSCRIBE,
				ApiName:     streamingMethodPath,
			}},
		}},
	})

	message, err := parser.ParseMsg(streamingMethodPath, requestData, "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	require.True(t, chainlib.IsGrpcSubscription(message), "the fixture must classify as a gRPC subscription")
	return message
}

// marshalStreamPayload encodes a distinguishable message of the method's output type.
func marshalStreamPayload(t *testing.T, marker string) []byte {
	t.Helper()
	payload, err := proto.Marshal(&grpc_reflection_v1.ServerReflectionResponse{ValidHost: marker})
	require.NoError(t, err)
	return payload
}

func awaitStreamPayload(t *testing.T, replies <-chan *pairingtypes.RelayReply) string {
	t.Helper()
	select {
	case reply := <-replies:
		require.NotNil(t, reply, "the channel closed instead of delivering a message")
		decoded := &grpc_reflection_v1.ServerReflectionResponse{}
		require.NoError(t, proto.Unmarshal(reply.GetData(), decoded))
		return decoded.ValidHost
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an upstream message to reach the client")
		return ""
	}
}

func requireUpstreamStreamOpened(t *testing.T, upstream *fakeStreamingUpstream) {
	t.Helper()
	select {
	case <-upstream.streamsOpen:
	case <-time.After(10 * time.Second):
		t.Fatal("the manager never opened an upstream stream")
	}
}
