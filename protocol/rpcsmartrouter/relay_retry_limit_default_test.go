package rpcsmartrouter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// The default retry limit is 3, the most primary nodes a deployment runs today: when
// three primaries fail fast, the request still reaches a backup (MAG-5182).
//
// The test reads the constant, not relaycore.RelayRetryLimit: building the command
// (here, or in any earlier test of this package) writes the flag default into that
// variable, so a check of the variable would pass whatever it was declared as.
func TestRelayRetryLimitDefaultIsThree(t *testing.T) {
	require.Equal(t, 3, relaycore.DefaultRelayRetryLimit)

	original := relaycore.RelayRetryLimit
	t.Cleanup(func() { relaycore.RelayRetryLimit = original })

	cmd := CreateRPCSmartRouterCobraCommand()
	flag := cmd.Flags().Lookup(common.SetRelayRetryLimitFlag)
	require.NotNil(t, flag, "the shipped command registers --%s", common.SetRelayRetryLimitFlag)
	require.Equal(t, "3", flag.DefValue)
}
