package rpcsmartrouter

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// MAG-5697: a gRPC subscription that cannot start. The client's stream ends with the router's own
// status and the request id, as a subscription the router gives up does; the reason stays in the
// router's log and in relay_end, with the node's address as [address].

// stoppableUpstream starts a gRPC node and returns its address and a function that stops it. A
// manager pointed at it before the stop holds a connection set up while the node was up, as for a
// node that goes down mid-outage; its next stream finds the port closed.
func stoppableUpstream(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), srv.Stop
}

// stopUpstream stops the node and waits until the manager's connection to it has seen it go, so
// the next stream finds the port closed rather than opening on a transport that is still closing.
func stopUpstream(t *testing.T, manager *DirectGRPCSubscriptionManager, stop func()) {
	t.Helper()
	conn := connectionForUpstream(t, manager).GetConn()
	stop()
	require.Eventually(t, func() bool { return conn.GetState() != connectivity.Ready }, 5*time.Second, 10*time.Millisecond,
		"the router's connection must see the node go")
}

// closedPort is a local port nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	require.NoError(t, lis.Close())
	return port
}

func TestGRPCSubscriptionStart_TheClientGetsTheRoutersStatus(t *testing.T) {
	utils.EnableDebugLogBuffer(200000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	addr, stop := stoppableUpstream(t)
	manager := newManagerAgainstUpstream(t, addr)
	defer manager.Stop()
	conn := dialRestoreListener(t, manager)
	stopUpstream(t, manager, stop)

	stream := openClientStream(t, conn, "g")
	received := make(chan error, 1)
	go func() {
		var reply []byte
		received <- stream.RecvMsg(&reply)
	}()
	var err error
	select {
	case err = <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("the client's stream did not end within 10s")
	}

	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	require.Equal(t, "subscription could not be started", status.Convert(err).Message())
	guids := stream.Trailer().Get(common.GUID_HEADER_NAME)
	require.Len(t, guids, 1, "the request id, under which the router logged the reason")

	// The reason is logged under that id, and the log line names no address.
	mine := `"GUID":"` + guids[0] + `"`
	var logged []string
	for _, record := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 200000) {
		if line := string(record); strings.Contains(line, mine) {
			logged = append(logged, line)
		}
	}
	require.NotEmpty(t, logged, "the router logs why the subscription did not start")
	for _, line := range logged {
		require.NotContains(t, line, addr, "the reason the router logged: %s", line)
	}
}

// The reason itself: what the listener logs and relay_end records names the node by neither its
// address nor its configured host, and still reads as the fault it is.
func TestGRPCSubscriptionStart_TheReasonCarriesNoNodeAddress(t *testing.T) {
	for _, tc := range []struct {
		name string
		// upstream returns a manager whose next stream fails, and what of the node the failure
		// would name.
		upstream func(t *testing.T) (*DirectGRPCSubscriptionManager, []string)
		check    func(t *testing.T, err error)
	}{
		{
			name: "a node that went down after the router connected",
			upstream: func(t *testing.T) (*DirectGRPCSubscriptionManager, []string) {
				addr, stop := stoppableUpstream(t)
				manager := newManagerAgainstUpstream(t, addr)
				stopUpstream(t, manager, stop)
				return manager, []string{addr}
			},
			check: func(t *testing.T, err error) {
				require.Equal(t, common.RouterErrorConnectionRefused, common.DetectConnectionError(err), "%v", err)
			},
		},
		{
			name: "a node the router never reached, configured by name",
			upstream: func(t *testing.T) (*DirectGRPCSubscriptionManager, []string) {
				port := closedPort(t)
				endpoint := &common.NodeUrl{Url: "grpc://localhost:" + port + "/sub-key", GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
				manager := NewDirectGRPCSubscriptionManager(nil, "SUI", spectypes.APIInterfaceGrpc, []*common.NodeUrl{endpoint}, nil, nil, nil)
				manager.config.ConnectionTimeout = 300 * time.Millisecond
				return manager, []string{"localhost", ":" + port, "sub-key"}
			},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, context.DeadlineExceeded, "the cause is still there to classify")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, nodeNames := tc.upstream(t)
			defer manager.Stop()

			message := newGrpcSubscriptionMessageWithRequest(t, []byte(`{"host":"g"}`))
			ctx := chainlib.WithSubscriptionUpstream(utils.WithUniqueIdentifier(context.Background(), 1))
			_, _, err := manager.StartSubscription(ctx, message, "dapp", "1.1.1.1", "conn-g", nil)
			require.Error(t, err)
			tc.check(t, err)

			server := &RPCSmartRouterServer{listenEndpoint: &routersession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGrpc}}
			end := server.buildSubscriptionRelayEnd(ctx, chainlib.NewProtocolMessage(message, nil, &pairingtypes.RelayPrivateData{}, "dapp", "1.1.1.1"), time.Now(), err)
			require.Equal(t, relayOutcomeNoAnswer, end.outcome, "the request went to the node")
			require.Len(t, end.attempts, 1)
			for _, name := range nodeNames {
				require.NotContains(t, err.Error(), name, "the reason the listener logs")
				require.NotContains(t, end.errorText, name, "relay_end's error")
				require.NotContains(t, end.attempts[0].message, name, "relay_end's attempt")
			}
			require.Contains(t, end.attempts[0].message, "[address]")
		})
	}
}

// A subscription turned away at the router's limit: the client learns it hit a limit, with the
// router's own RESOURCE_EXHAUSTED, and the log keeps the limit's figure.
func TestGRPCSubscriptionStart_AtTheLimitTheClientGetsResourceExhausted(t *testing.T) {
	addr, _ := stoppableUpstream(t)
	manager := newManagerAgainstUpstream(t, addr)
	defer manager.Stop()
	manager.config.MaxTotalSubscriptions = 0
	manager.config.TotalLimitEnforcement = string(EnforcementModeReject)

	message := newGrpcSubscriptionMessageWithRequest(t, []byte(`{"host":"g"}`))
	_, _, err := manager.StartSubscription(context.Background(), message, "dapp", "1.1.1.1", "conn-g", nil)
	require.ErrorIs(t, err, chainlib.ErrSubscriptionLimitReached)
	require.EqualError(t, err, "global subscription limit reached (0)", "the text the log has always had")

	stream := openClientStream(t, dialRestoreListener(t, manager), "g")
	var reply []byte
	err = stream.RecvMsg(&reply)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
	require.Equal(t, "subscription limit reached", status.Convert(err).Message())
	require.Len(t, stream.Trailer().Get(common.GUID_HEADER_NAME), 1)
}
