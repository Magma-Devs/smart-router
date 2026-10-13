package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// The test command read --use-tls and --allow-insecure-connection through viper, which only the
// root command binds to its flag set, so both were always false; and neither it nor health accepted
// the spec repository tokens the root command does (MAG-4278).
func TestTestAndHealthCommandsReadTheirOwnFlags(t *testing.T) {
	testCmd := CreateTestRPCSmartRouterCobraCommand()
	require.NoError(t, testCmd.Flags().Parse([]string{"--use-tls=false", "--allow-insecure-connection", "--github-token=gh", "--gitlab-token=gl"}))
	auth := grpcAuthConfigFromFlags(testCmd.Flags())
	require.False(t, auth.UseTLS)
	require.True(t, auth.AllowInsecure)
	gh, gl := specTokensFromFlags(testCmd.Flags())
	require.Equal(t, "gh", gh)
	require.Equal(t, "gl", gl)

	defaults := grpcAuthConfigFromFlags(CreateTestRPCSmartRouterCobraCommand().Flags())
	require.True(t, defaults.UseTLS, "the registered default is true; viper used to read false")

	health := CreateHealthCobraCommand()
	require.NotNil(t, health.Flags().Lookup(common.GitHubTokenFlag))
	require.NotNil(t, health.Flags().Lookup(common.GitLabTokenFlag))
}
