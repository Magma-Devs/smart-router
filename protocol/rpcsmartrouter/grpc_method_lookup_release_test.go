package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// A JSON gRPC request this node's lookup gave no definition for cannot be converted, so it is never
// sent. That is not the endpoint's failure: it is NODE_UNIMPLEMENTED, retried on a provider that may
// describe the method, and the endpoint is neither marked unhealthy nor held off (MAG-5675). As an
// unrecognised error it marked the endpoint unhealthy.
func TestSendGRPCRelay_NoMethodDescriptorIsNotTheEndpointsFault(t *testing.T) {
	lookupErr := fmt.Errorf("%w: %w", routersession.ErrNoMethodDescriptor, errors.New("Symbol not found: unknown.v1.Service"))
	sender := &DirectRPCRelaySender{
		directConnection: &fakeDirectConn{err: fmt.Errorf("failed to get method descriptor: %w", lookupErr)},
		endpointName:     "grpc-endpoint",
		chainFamily:      common.ChainFamily(-1),
	}
	chainMessage := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: "unknown.v1.Service/Call", Msg: []byte(`{}`)},
	}}

	_, err := sender.sendGRPCRelay(context.Background(), chainMessage, time.Second)
	require.ErrorIs(t, err, routersession.ErrNoMethodDescriptor)

	verdict := healthVerdict(err, common.ChainFamily(-1), common.TransportGRPC)
	require.Equal(t, common.RouterErrorNodeUnimplemented, verdict)
	require.True(t, verdict.Retryable, "another provider may describe the method")
	unhealthy, backoff := classifyEndpointHealth(verdict, false)
	require.False(t, unhealthy, "the endpoint is not marked unhealthy")
	require.False(t, backoff, "nor held off")
}

// The methods the router serves in the tests below. No node in them describes these.
const (
	undescribedReadMethod  = "routertest.undescribed.v1.Query/Get"
	undescribedWriteMethod = "routertest.undescribed.v1.Tx/Broadcast"
)

// startStatusGRPCNode starts a gRPC node that serves no reflection and answers every call with st.
func startStatusGRPCNode(t *testing.T, st *status.Status) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		if method, _ := grpc.MethodFromServerStream(stream); strings.Contains(method, "ServerReflection") {
			return status.Error(codes.Unimplemented, "no reflection")
		}
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		return st.Err()
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// providerReports is the optimizer's reputation report of each of the router's providers, named
// grpc-node-<i> by startProtobufCheckRouterServing: availability, latency and sync, which the
// selection score is made of.
func providerReports(server *RPCSmartRouterServer, providers int) []string {
	reports := make([]string, 0, providers)
	for i := range providers {
		report, _ := server.sessionManager.GetProviderOptimizer().GetReputationReportForProvider(fmt.Sprintf("grpc-node-%d", i))
		reports = append(reports, fmt.Sprintf("availability=%v latency=%v sync=%v", report.GetAvailability(), report.GetLatency(), report.GetSync()))
	}
	return reports
}

// MAG-5675's done-when, through the router's gRPC listener: a burst of JSON requests for a method no
// provider describes leaves every provider's availability, score and health as it was, at a
// --bench-after of 3. Each request fails on every provider it is tried on before anything is sent,
// and the session is released as one that was selected and not used. Two kinds of node give no
// definition: one that serves no reflection, and one whose reflection does not describe the method.
func TestGRPCListener_JSONRequestsForAMethodNoProviderDescribesLeaveEveryProviderAsItWas(t *testing.T) {
	const providers, burst = 3, 10
	for _, tc := range []struct {
		name  string
		start func(t *testing.T) (string, *atomic.Int32)
	}{
		{"nodes that serve no reflection", startDecodingNode},
		{"nodes whose reflection does not describe the method", startReflectingNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			benchAfter(t, 3)
			var addrs []string
			var calls []*atomic.Int32
			for range providers {
				addr, count := tc.start(t)
				addrs = append(addrs, addr)
				calls = append(calls, count)
			}
			client, endpoints, server := startProtobufCheckRouterServing(t, undescribedReadMethod, undescribedWriteMethod, addrs...)
			reports := providerReports(server, providers)

			for range burst {
				for _, method := range []string{undescribedReadMethod, undescribedWriteMethod} {
					_, _, err := invokeRaw(t, client, method, []byte(`{"id":"7"}`))
					require.Error(t, err, "%s: no provider can convert the request", method)
				}
			}
			time.Sleep(200 * time.Millisecond) // samples are handed to the optimizer asynchronously

			for i, endpoint := range endpoints {
				require.Zero(t, calls[i].Load(), "node %d received a request", i)
				require.True(t, endpoint.IsEnabled(), "endpoint %d switched off", i)
				require.Zero(t, endpoint.ConnectionRefusals, "endpoint %d marked unhealthy", i)
			}
			require.Equal(t, reports, providerReports(server, providers), "a provider's availability or score moved")
			snapshot := server.sessionManager.ProviderRoutingSnapshot()
			require.Empty(t, snapshot.Blocked, "a provider was blocked")
			require.Len(t, snapshot.ValidAddresses, providers)
		})
	}

	// The harness sees a scored node error: a node that answers UNAVAILABLE is scored and counted.
	t.Run("control: a node error is scored and counted", func(t *testing.T) {
		benchAfter(t, 3)
		client, endpoints, server := startProtobufCheckRouterServing(t, undescribedReadMethod, undescribedWriteMethod,
			startStatusGRPCNode(t, status.New(codes.Unavailable, "node down")))
		reports := providerReports(server, 1)

		for range 3 {
			_, _, _ = invokeRaw(t, client, undescribedReadMethod, []byte{0x08, 0x01})
		}

		require.Eventually(t, func() bool { return providerReports(server, 1)[0] != reports[0] }, 5*time.Second, 20*time.Millisecond,
			"a scored node error moves the provider's report")
		require.Positive(t, endpoints[0].ConnectionRefusals, "and counts against the endpoint")
	})
}

// endpointRelayCount sums the per-endpoint relay counter name (rpc_endpoint_total_errored or
// rpc_endpoint_total_cancelled) over the test chain's endpoints for method. The default registry
// keeps counting across tests, so callers compare a before and an after.
func endpointRelayCount(t *testing.T, name, method string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var total float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["spec"] == "GRPCTEST" && labels["function"] == method {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
}

// The per-endpoint relay metric agrees with the release (MAG-5675): a JSON request no provider
// describes was never sent, so each attempt counts as cancelled, as a race loser's does, and not in
// rpc_endpoint_total_errored or a provider's failed requests. A node's own error still counts.
func TestGRPCListener_JSONRequestsForAMethodNoProviderDescribesAreNotCountedAsEndpointErrors(t *testing.T) {
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	withMetrics := func(server *RPCSmartRouterServer) { server.smartRouterEndpointMetrics = mm }

	for _, tc := range []struct {
		name  string
		start func(t *testing.T) (string, *atomic.Int32)
	}{
		{"nodes that serve no reflection", startDecodingNode},
		{"nodes whose reflection does not describe the method", startReflectingNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var addrs []string
			for range 3 {
				addr, _ := tc.start(t)
				addrs = append(addrs, addr)
			}
			client, _, _ := startProtobufCheckRouterConfigured(t, withMetrics, undescribedReadMethod, undescribedWriteMethod, addrs...)

			for _, method := range []string{undescribedReadMethod, undescribedWriteMethod} {
				errored := endpointRelayCount(t, "rpc_endpoint_total_errored", method)
				cancelled := endpointRelayCount(t, "rpc_endpoint_total_cancelled", method)
				for range 3 {
					_, _, err := invokeRaw(t, client, method, []byte(`{"id":"7"}`))
					require.Error(t, err)
				}
				require.Eventually(t, func() bool {
					return endpointRelayCount(t, "rpc_endpoint_total_cancelled", method) >= cancelled+3
				}, 5*time.Second, 20*time.Millisecond, "%s: each attempt counts as cancelled", method)
				require.Equal(t, errored, endpointRelayCount(t, "rpc_endpoint_total_errored", method), "%s: no attempt counts as an endpoint error", method)
			}
		})
	}

	// The harness sees an endpoint error: once the node has gone away, a binary request fails with
	// no reply, and that counts.
	t.Run("control: a node that went away counts as an endpoint error", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		node := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			if method, _ := grpc.MethodFromServerStream(stream); strings.Contains(method, "ServerReflection") {
				return status.Error(codes.Unimplemented, "no reflection")
			}
			var body []byte
			if err := stream.RecvMsg(&body); err != nil {
				return err
			}
			return stream.SendMsg([]byte{0x08, 0x01})
		}))
		go func() { _ = node.Serve(lis) }()
		t.Cleanup(node.Stop)
		client, _, _ := startProtobufCheckRouterConfigured(t, withMetrics, undescribedReadMethod, undescribedWriteMethod, lis.Addr().String())
		_, _, err = invokeRaw(t, client, undescribedReadMethod, []byte{0x08, 0x01})
		require.NoError(t, err, "the router reaches the node")
		node.Stop()

		errored := endpointRelayCount(t, "rpc_endpoint_total_errored", undescribedReadMethod)
		_, _, err = invokeRaw(t, client, undescribedReadMethod, []byte{0x08, 0x01})
		require.Error(t, err)
		require.Eventually(t, func() bool {
			return endpointRelayCount(t, "rpc_endpoint_total_errored", undescribedReadMethod) > errored
		}, 5*time.Second, 20*time.Millisecond)
	})
}
