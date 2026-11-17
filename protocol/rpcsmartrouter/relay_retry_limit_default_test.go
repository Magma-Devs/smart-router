package rpcsmartrouter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// The default retry limit is 3, the most primary nodes a deployment runs today: when
// three primaries fail fast, the request still reaches a backup (MAG-5182). The default
// lives in two places, the flag and the package variable a test or an embedder reads
// without parsing flags, so both are pinned here.
func TestRelayRetryLimitDefaultIsThree(t *testing.T) {
	original := relaycore.RelayRetryLimit
	t.Cleanup(func() { relaycore.RelayRetryLimit = original })

	require.Equal(t, 3, original, "relaycore.RelayRetryLimit starts at the shipped default")

	cmd := CreateRPCSmartRouterCobraCommand()
	flag := cmd.Flags().Lookup(common.SetRelayRetryLimitFlag)
	require.NotNil(t, flag, "the shipped command registers --%s", common.SetRelayRetryLimitFlag)
	require.Equal(t, "3", flag.DefValue)
}
