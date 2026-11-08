package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MAG-4730: a new subscription went to one endpoint, and the client got that endpoint's error.
// A primary that refused connections but was still in the tier failed new subscriptions while
// a healthy second primary and the backup tier went untried, until re-verification demoted it
// about two epochs later; HTTP relays had failed over long before. Within one subscribe the
// router now skips every endpoint it already tried, cascades primary → backup, and answers
// with an error only once every endpoint failed.
//
// The upstreams are real servers dialed for real. The tiers are handed to the manager
// directly, which is the runtime state between two re-verify ticks.

// failoverSubscribes is how many subscriptions a scenario opens, each from its own client
// connection on its own params, so none joins another's upstream subscription or inherits its
// sticky endpoint: every one runs the whole selection.
const failoverSubscribes = 10

// failoverOptimizer is how a scenario's tiers are chosen from.
type failoverOptimizer struct {
	name  string
	build func() WebSocketEndpointOptimizer
	// inOrder: first-available tries a tier in configured order, so every subscribe meets the
	// first primary first.
	inOrder bool
}

func failoverOptimizers() []failoverOptimizer {
	return []failoverOptimizer{
		// An untyped nil, so the manager sees no optimizer at all.
		{name: "first available", build: func() WebSocketEndpointOptimizer { return nil }, inOrder: true},
		// The optimizer the router hands both managers. It learns from every failure, so on its
		// own it steered most subscribes off the refusing primary, but not the first (1 of 10
		// failed in the ticket's run).
		{name: "router optimizer", build: func() WebSocketEndpointOptimizer {
			rand.InitRandomSeed() // the optimizer's weighted pick panics on an unseeded protocol rand
			return provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, 1, nil, "ETH")
		}},
	}
}

// wsFailoverChainID is the chain the WebSocket scenarios run on; the error registry reads a
// node's refusal by its family.
const wsFailoverChainID = "ETH"

// wsFailoverNodes are one scenario's WebSocket nodes: two primaries and a backup. A refusing
// node answers every new dial with HTTP 503.
type wsFailoverNodes struct {
	primary1, primary2, backup *mockSubscriptionServer
}

func newWSFailoverManager(t *testing.T, optimizer WebSocketEndpointOptimizer) (*DirectWSSubscriptionManager, wsFailoverNodes) {
	t.Helper()
	newNode := func() *mockSubscriptionServer {
		node := newMockSubscriptionServer()
		// Nothing here reads notifications. A node that pushed them would fill each client's
		// buffer and have it dropped as a slow consumer, which is a different test.
		node.messageInterval = time.Hour
		t.Cleanup(node.Close)
		return node
	}
	nodes := wsFailoverNodes{primary1: newNode(), primary2: newNode(), backup: newNode()}
	manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", wsFailoverChainID, "jsonrpc",
		[]*common.NodeUrl{{Url: nodes.primary1.URL()}, {Url: nodes.primary2.URL()}},
		[]*common.NodeUrl{{Url: nodes.backup.URL()}},
		optimizer, nil)
	// Closed before the nodes: a subscription still open when a node goes away would keep its
	// pool reconnecting with backoff into the next test.
	t.Cleanup(manager.Close)
	return manager, nodes
}

// wsSubscribeLogs subscribes client connection uid to the logs of address. Another address is
// another upstream subscription.
func wsSubscribeLogs(t *testing.T, manager *DirectWSSubscriptionManager, uid, address string) error {
	t.Helper()
	params := []interface{}{"logs", map[string]interface{}{"address": address}}
	body, err := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "method": "eth_subscribe", "params": params, "id": 1})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: params},
		rawData:               body,
	}
	// The client stays connected until the test ends: a cancelled context is a client that
	// left, and its subscription would be torn down under the assertions.
	ctx, disconnect := context.WithCancel(context.Background())
	t.Cleanup(disconnect)
	_, _, err = manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", uid, nil)
	return err
}

// wsOpenFailoverSubscribes opens failoverSubscribes subscriptions and returns how many failed.
func wsOpenFailoverSubscribes(t *testing.T, manager *DirectWSSubscriptionManager) (failed int) {
	t.Helper()
	for i := range failoverSubscribes {
		if err := wsSubscribeLogs(t, manager, fmt.Sprintf("ws-%d", i), fmt.Sprintf("0x%040x", i)); err != nil {
			t.Logf("subscribe %d failed: %v", i, err)
			failed++
		}
	}
	return failed
}

// TestWSSubscribe_FailsOverPastRefusingEndpoints is MAG-4730's A2 and A3 on WebSocket.
func TestWSSubscribe_FailsOverPastRefusingEndpoints(t *testing.T) {
	for _, optimizer := range failoverOptimizers() {
		t.Run(optimizer.name, func(t *testing.T) {
			// A2: the second primary takes every subscription the first refuses, and the backup
			// tier stays untouched while a primary works.
			t.Run("one primary refusing: the other primary serves", func(t *testing.T) {
				manager, nodes := newWSFailoverManager(t, optimizer.build())
				nodes.primary1.setRefusing(true)

				require.Zero(t, wsOpenFailoverSubscribes(t, manager),
					"subscribes failed while the second primary and the backup were healthy")
				refused := int(nodes.primary1.refusedDials.Load())
				require.LessOrEqual(t, refused, failoverSubscribes, "the refusing primary is tried at most once per subscribe")
				if optimizer.inOrder {
					require.Equal(t, failoverSubscribes, refused, "first-available meets the refusing primary on every subscribe")
				}
				require.Equal(t, failoverSubscribes, nodes.primary2.liveSubscriptions(), "the second primary serves every subscription")
				require.Zero(t, nodes.backup.liveSubscriptions(), "the backup tier is for when every primary failed")
			})

			// A3: the backup takes every subscription once both primaries refused it, each
			// primary tried exactly once per subscribe, before the backup.
			t.Run("every primary refusing: the backup serves", func(t *testing.T) {
				manager, nodes := newWSFailoverManager(t, optimizer.build())
				nodes.primary1.setRefusing(true)
				nodes.primary2.setRefusing(true)

				require.Zero(t, wsOpenFailoverSubscribes(t, manager), "subscribes failed while the backup was healthy")
				require.Equal(t, failoverSubscribes, nodes.backup.liveSubscriptions(), "the backup serves every subscription")
				require.Equal(t, int32(failoverSubscribes), nodes.primary1.refusedDials.Load(), "each primary is tried once per subscribe")
				require.Equal(t, int32(failoverSubscribes), nodes.primary2.refusedDials.Load(), "each primary is tried once per subscribe")
			})
		})
	}
}

// TestWSSubscribe_DeadAffinityEndpointIsSkipped is MAG-4730's A1. A client connection keeps
// the endpoint of its first subscription as its affinity. When that endpoint died, every later
// subscribe on the connection went straight back to it and failed, while the other endpoints
// were never contacted. Now the dead endpoint costs one refused dial, and the subscription and
// the connection's affinity move to the next endpoint.
func TestWSSubscribe_DeadAffinityEndpointIsSkipped(t *testing.T) {
	manager, nodes := newWSFailoverManager(t, nil)
	const uid = "ws-1"
	require.NoError(t, wsSubscribeLogs(t, manager, uid, "0x01"))
	require.Equal(t, 1, nodes.primary1.liveSubscriptions(), "first-available puts the first subscription on the first primary")

	// The first primary goes down: its sockets drop and every new dial is refused. Waiting for
	// the router to redial it means the pool has retired the dead socket rather than handing it
	// to the next subscribe.
	nodes.primary1.setRefusing(true)
	nodes.primary1.dropConnections()
	require.Eventually(t, func() bool { return nodes.primary1.refusedDials.Load() >= 1 },
		5*time.Second, 10*time.Millisecond, "the router must notice the first primary went down")

	for _, address := range []string{"0x02", "0x03"} {
		require.NoError(t, wsSubscribeLogs(t, manager, uid, address),
			"a later subscribe on the connection must not fail on its dead affinity endpoint")
	}
	require.Equal(t, 2, nodes.primary2.liveSubscriptions(), "the later subscriptions go to the second primary")
	affinity, pinned := manager.stickyStore.Get(manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", uid))
	require.True(t, pinned)
	require.Equal(t, nodes.primary2.URL(), affinity.Provider, "the connection's affinity moves to the endpoint that took the subscription")
}

// chargingOptimizer chooses nothing, so a tier falls back to first-available, and counts the
// failures charged against each endpoint.
type chargingOptimizer struct {
	lock    sync.Mutex
	charged map[string]int
}

func (o *chargingOptimizer) ChooseUpstream(context.Context, []string, map[string]struct{}, uint64, int64) []string {
	return nil
}

func (o *chargingOptimizer) AppendRelayData(string, time.Duration, uint64, uint64) {}

func (o *chargingOptimizer) AppendRelayFailure(endpoint string) {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.charged[endpoint]++
}

func (o *chargingOptimizer) failures() map[string]int {
	o.lock.Lock()
	defer o.lock.Unlock()
	failures := make(map[string]int, len(o.charged))
	for endpoint, count := range o.charged {
		failures[endpoint] = count
	}
	return failures
}

// subscribesSeen is how many eth_subscribe calls node was asked.
func subscribesSeen(node *mockSubscriptionServer) int {
	seen := 0
	for _, method := range node.ObservedMethods() {
		if method == "eth_subscribe" {
			seen++
		}
	}
	return seen
}

// A node's refusal ends the failover when every endpoint would repeat it. An HTTP relay stops
// retrying when the error registry rates the node's answer non-retryable, and so does a
// subscribe: the client gets the answer of the one endpoint asked, and no other endpoint is
// subscribed, or charged a failure for a request that was the client's mistake. A refusal the
// registry rates retryable, a method this node does not offer or a rate limit, still moves on
// to the next endpoint, which serves it (MAG-4730).
func TestWSSubscribe_FailsOverOnlyPastRefusalsAnotherEndpointCouldServe(t *testing.T) {
	scenarios := []struct {
		name    string
		refusal rpcclient.JsonError
		// endsFailover is the registry's verdict on the refusal: non-retryable.
		endsFailover bool
	}{
		{
			name:         "invalid params: every endpoint refuses them",
			refusal:      rpcclient.JsonError{Code: -32602, Message: "invalid argument 1: json: cannot unmarshal hex string without 0x prefix"},
			endsFailover: true,
		},
		{
			name:    "a method this node does not offer: another may",
			refusal: rpcclient.JsonError{Code: -32601, Message: "the method eth_subscribe does not exist/is not available"},
		},
		{
			name:    "rate limited: another endpoint is not",
			refusal: rpcclient.JsonError{Code: 429, Message: "Too Many Requests"},
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			verdict := common.ClassifyNodeErrorForRetry(common.GetChainFamilyOrDefault(wsFailoverChainID), common.TransportJsonRPC, sc.refusal.Code, sc.refusal.Message)
			require.Equal(t, sc.endsFailover, verdict.IsNonRetryable, "precondition: the registry's verdict, the one an HTTP relay stops on")

			optimizer := &chargingOptimizer{charged: map[string]int{}}
			manager, nodes := newWSFailoverManager(t, optimizer)
			// A refusal the registry rates non-retryable is one every node gives; a retryable one
			// is the first primary's alone.
			refusing := []*mockSubscriptionServer{nodes.primary1}
			if sc.endsFailover {
				refusing = append(refusing, nodes.primary2, nodes.backup)
			}
			for _, node := range refusing {
				refusal := sc.refusal
				node.subscribeRefusal.Store(&refusal)
			}

			err := wsSubscribeLogs(t, manager, "ws-1", "0x01")
			require.Equal(t, 1, subscribesSeen(nodes.primary1), "the first primary is asked once")
			if sc.endsFailover {
				var answer *rpcclient.JsonError
				require.ErrorAs(t, err, &answer, "the client gets the node's own answer")
				require.Equal(t, sc.refusal, *answer)
				require.Zero(t, subscribesSeen(nodes.primary2)+subscribesSeen(nodes.backup), "no other endpoint is asked")
				require.Equal(t, map[string]int{nodes.primary1.URL(): 1}, optimizer.failures(), "only the endpoint that answered is charged")
				return
			}
			require.NoError(t, err, "another endpoint serves what this one refused")
			require.Equal(t, 1, nodes.primary2.liveSubscriptions(), "the second primary takes the subscription")
			require.Zero(t, subscribesSeen(nodes.backup), "the backup tier is for when every primary failed")
		})
	}
}

// The decision reads the error registry's rows for the answer's transport, the rows an HTTP
// relay's retries stop on, through whatever the manager wrapped the answer in; an error no node
// sent never ends the failover. A gRPC node's status reaches a subscribe only from the
// reflection lookup and the stream's opening: its refusal of the request itself arrives later,
// on the stream, where the listener ends the client's stream with it.
func TestEveryEndpointWouldRefuse_ReadsTheErrorRegistry(t *testing.T) {
	endpoints := newSubscriptionEndpoints("test", "no endpoints", wsFailoverChainID, nil, nil, nil)
	jsonRPCAnswer := func(code int, message string) error {
		return fmt.Errorf("failed to create upstream subscription: %w",
			fmt.Errorf("upstream rejected subscribe: %w", &rpcclient.JsonError{Code: code, Message: message}))
	}
	grpcAnswer := func(code codes.Code, message string) error {
		return fmt.Errorf("failed to create stream: %w", status.Error(code, message))
	}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"JSON-RPC invalid params", jsonRPCAnswer(-32602, "invalid params"), true},
		{"JSON-RPC invalid request", jsonRPCAnswer(-32600, "invalid request"), true},
		{"JSON-RPC method this node does not offer", jsonRPCAnswer(-32601, "the method eth_subscribe does not exist/is not available"), false},
		{"JSON-RPC rate limit", jsonRPCAnswer(429, "Too Many Requests"), false},
		{"JSON-RPC answer the registry does not know", jsonRPCAnswer(-31999, "the node says something new"), false},
		{"gRPC InvalidArgument", grpcAnswer(codes.InvalidArgument, "invalid request"), true},
		{"gRPC Aborted", grpcAnswer(codes.Aborted, "transaction aborted"), true},
		{"gRPC Unavailable", grpcAnswer(codes.Unavailable, "connection refused"), false},
		{"gRPC NotFound", grpcAnswer(codes.NotFound, "service not found"), false},
		{"gRPC Unimplemented", grpcAnswer(codes.Unimplemented, "unknown service grpc.reflection.v1.ServerReflection"), false},
		{"refused dial", fmt.Errorf("failed to get WebSocket connection: %w", errors.New("websocket: bad handshake (HTTP status 503 Service Unavailable)")), false},
		{"dial timeout", fmt.Errorf("failed to get connection: %w", context.DeadlineExceeded), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, endpoints.everyEndpointWouldRefuse(tc.err))
		})
	}
}

// grpcFailoverDialTimeout bounds each gRPC dial in these tests. A dial to an endpoint that never
// reaches READY waits the whole connect timeout out (NewUpstreamGRPCStreamConnection keeps the
// WithBlock semantics of the dial it replaced), so every failover off a dead gRPC endpoint costs
// one of these: the 30s default would make each scenario minutes long, and a loopback endpoint
// is READY in a few milliseconds.
const grpcFailoverDialTimeout = 250 * time.Millisecond

// refusingGRPCEndpoint accepts every TCP connection and closes it before the HTTP/2 handshake,
// as a node whose gRPC server is down behind its load balancer does: a dial to it never reaches
// READY. dials counts the connections it turned away.
type refusingGRPCEndpoint struct {
	addr  string
	dials atomic.Int32
}

func startRefusingGRPCEndpoint(t *testing.T) *refusingGRPCEndpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	endpoint := &refusingGRPCEndpoint{addr: listener.Addr().String()}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // the listener closed
			}
			endpoint.dials.Add(1)
			_ = conn.Close()
		}
	}()
	return endpoint
}

// newGRPCFailoverManager points a manager at the given tiers of host:port endpoints.
func newGRPCFailoverManager(t *testing.T, primary, backup []string) *DirectGRPCSubscriptionManager {
	t.Helper()
	tier := func(addrs []string) []*common.NodeUrl {
		endpoints := make([]*common.NodeUrl, 0, len(addrs))
		for _, addr := range addrs {
			endpoints = append(endpoints, &common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}})
		}
		return endpoints
	}
	primaryTier, backupTier := tier(primary), tier(backup)
	config := DefaultGRPCStreamingConfig()
	config.ConnectionTimeout = grpcFailoverDialTimeout
	manager := NewDirectGRPCSubscriptionManager(nil, "SUI", spectypes.APIInterfaceGrpc, primaryTier, backupTier, nil, config)

	// The method descriptor is seeded into each endpoint's cache, so no subscribe asks
	// reflection, and without a dial: an endpoint's first dial is a subscribe's.
	for _, endpoint := range append(primaryTier, backupTier...) {
		pool, err := manager.getOrCreatePool(endpoint)
		require.NoError(t, err)
		pool.descriptors.cache.Store("grpc.reflection.v1.ServerReflection.ServerReflectionInfo", streamingMethodDescriptor(t))
	}
	// Stopped before the upstreams, which were started first.
	t.Cleanup(manager.Stop)
	return manager
}

// grpcOpenFailoverSubscribes opens subscribes streams, each from its own client stream on its
// own request, and returns how many failed.
func grpcOpenFailoverSubscribes(t *testing.T, manager *DirectGRPCSubscriptionManager, subscribes int) (failed int) {
	t.Helper()
	for i := range subscribes {
		message := newGrpcSubscriptionMessageWithRequest(t, []byte(fmt.Sprintf(`{"host":"host-%d"}`, i)))
		if _, _, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", fmt.Sprintf("conn-%d", i), nil); err != nil {
			t.Logf("subscribe %d failed: %v", i, err)
			failed++
		}
	}
	return failed
}

// TestGRPCSubscribe_FailsOverPastRefusingEndpoints is MAG-4730's A2 and A3 on gRPC streaming,
// with first-available selection, which meets the refusing primary first on every subscribe.
func TestGRPCSubscribe_FailsOverPastRefusingEndpoints(t *testing.T) {
	// A2: the second primary opens every stream the first refuses, and the backup tier stays
	// untouched while a primary works.
	t.Run("one primary refusing: the other primary serves", func(t *testing.T) {
		refusing := startRefusingGRPCEndpoint(t)
		primary2, backup := startRestoreUpstream(t), startRestoreUpstream(t)
		manager := newGRPCFailoverManager(t, []string{refusing.addr, primary2.addr}, []string{backup.addr})

		require.Zero(t, grpcOpenFailoverSubscribes(t, manager, failoverSubscribes),
			"subscribes failed while the second primary and the backup were healthy")
		require.LessOrEqual(t, int(refusing.dials.Load()), failoverSubscribes, "the refusing primary is tried at most once per subscribe")
		require.Eventually(t, func() bool { return primary2.totalOpened() == failoverSubscribes },
			5*time.Second, 10*time.Millisecond, "the second primary serves every stream")
		require.Zero(t, backup.totalOpened(), "the backup tier is for when every primary failed")
	})

	// A3: the backup opens the stream once both primaries refused it. Each subscribe here waits
	// out two dead dials, so it opens fewer.
	t.Run("every primary refusing: the backup serves", func(t *testing.T) {
		const subscribes = 3
		refusing1, refusing2 := startRefusingGRPCEndpoint(t), startRefusingGRPCEndpoint(t)
		backup := startRestoreUpstream(t)
		manager := newGRPCFailoverManager(t, []string{refusing1.addr, refusing2.addr}, []string{backup.addr})

		require.Zero(t, grpcOpenFailoverSubscribes(t, manager, subscribes), "subscribes failed while the backup was healthy")
		require.Eventually(t, func() bool { return backup.totalOpened() == subscribes },
			5*time.Second, 10*time.Millisecond, "the backup serves every stream")
		require.LessOrEqual(t, int(refusing1.dials.Load()), subscribes, "each primary is tried at most once per subscribe")
		require.LessOrEqual(t, int(refusing2.dials.Load()), subscribes, "each primary is tried at most once per subscribe")
	})
}

// A rate-limit hold-off passes an endpoint over only while a ready one is left to try. Once the
// ready primary failed this subscribe, the held-off primary is next: not the backup tier, and
// not an error with a primary never tried (MAG-4730).
func TestSelectEndpoint_HeldOffEndpointIsTriedOnceTheReadyOneFailed(t *testing.T) {
	for _, manager := range subscriptionManagerCases() {
		t.Run(manager.name, func(t *testing.T) {
			registry := withFreshRelayHoldoff(t) // before the build: the gRPC manager keeps the registry it was built with
			heldOff, ready, backup := manager.url("primary-1"), manager.url("primary-2"), manager.url("backup-1")
			cascade := manager.build([]*common.NodeUrl{{Url: heldOff}, {Url: ready}}, []*common.NodeUrl{{Url: backup}}, nil)
			registry.RecordRateLimit(heldOff, heldOff, time.Minute)

			ep, err := cascade.selectEndpoint(context.Background(), "client-1", nil)
			require.NoError(t, err)
			require.Equal(t, ready, ep.Url, "the ready primary goes first")

			ep, err = cascade.selectEndpoint(context.Background(), "client-1", map[string]struct{}{ready: {}})
			require.NoError(t, err)
			require.Equal(t, heldOff, ep.Url, "with the ready primary tried, the held-off primary is next")
		})
	}
}
