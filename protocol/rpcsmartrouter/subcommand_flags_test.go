package rpcsmartrouter

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// The test command read --use-tls and --allow-insecure-connection through viper, which only the
// root command binds to its flag set, so both were always false; and neither it nor health accepted
// the spec repository tokens the root command does (MAG-4278).
func TestTestAndHealthCommandsReadTheirOwnFlags(t *testing.T) {
	testCmd := CreateTestRPCSmartRouterCobraCommand()
	require.NoError(t, testCmd.Flags().Parse([]string{"--use-tls", "--allow-insecure-connection", "--github-token=gh", "--gitlab-token=gl"}))
	auth := grpcAuthConfigFromFlags(testCmd.Flags())
	require.True(t, auth.UseTLS)
	require.True(t, auth.AllowInsecure)
	gh, gl := specTokensFromFlags(testCmd.Flags())
	require.Equal(t, "gh", gh)
	require.Equal(t, "gl", gl)

	// With no flags the command dials as it effectively did before: plaintext first, then TLS.
	defaults := grpcAuthConfigFromFlags(CreateTestRPCSmartRouterCobraCommand().Flags())
	require.False(t, defaults.UseTLS, "--use-tls defaults to false: plaintext first, then TLS")
	require.False(t, defaults.AllowInsecure)

	health := CreateHealthCobraCommand()
	require.NotNil(t, health.Flags().Lookup(common.GitHubTokenFlag))
	require.NotNil(t, health.Flags().Lookup(common.GitLabTokenFlag))
}

// The command's own Example dials a router's gRPC listener (127.0.0.1:3334 LAV1 grpc), which is
// plaintext unless the listen endpoint enables TLS. With no flags the test command must still reach
// one: a --use-tls default of true would dial TLS only and never try plaintext (MAG-4278).
func TestTestCommandDefaultFlagsReachAPlaintextGRPCListener(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()

	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()

	defaults := grpcAuthConfigFromFlags(CreateTestRPCSmartRouterCobraCommand().Flags())
	connector, err := chainproxy.NewGRPCConnector(lifetime, dialCtx, 1, common.NodeUrl{Url: lis.Addr().String(), AuthConfig: defaults})
	require.NoError(t, err, "the test command's default flags must reach a plaintext router listener")
	connector.Close()
}
