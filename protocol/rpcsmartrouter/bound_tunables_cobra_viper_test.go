package rpcsmartrouter_test

// Acceptance for the twelve keys that used to carry their own viper.BindPFlag when the command
// was built, repeating the viper.BindPFlags(cmd.Flags()) RunE runs before it reads any of them
// (MAG-4876). RunE's bind is now the only one, so each key must still resolve through it: to its
// registered default when nothing is set, since zero is not neutral for several of them
// (bench-after rejects 0, a zero weight drops a dimension from selection), and to the operator's
// value from the command line or the YAML file.
//
// Same sequence and same caveats as resp_cobra_viper_test.go.

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/rpcsmartrouter"
	"github.com/magma-Devs/smart-router/protocol/tracing"
)

func TestRealCommand_TunablesResolveThroughRunEBind(t *testing.T) {
	// Each value differs from the key's default, so reading the default instead would fail.
	set := []struct{ key, value string }{
		{"debug-address", "localhost:9999"},
		{common.ProviderOptimizerAvailabilityWeight, "0.7"},
		{common.ProviderOptimizerLatencyWeight, "0.6"},
		{common.ProviderOptimizerSyncWeight, "0.5"},
		{common.ProviderOptimizerStakeWeight, "0.4"},
		{common.ProviderOptimizerMinSelectionChance, "0.05"},
		{common.ProviderOptimizerSelectionMode, "best"},
		{common.ProviderOptimizerSelectionPriority, "fastest"},
		{common.StatefulToBackupFlag, "true"},
		{common.ProbeUpdateWeightFlagName, "2.5"},
		{common.BenchAfterFlagName, "7"},
		{tracing.TraceBodyFlag, "true"},
	}

	t.Run("default when unset", func(t *testing.T) {
		wireLikeRunE(t, "")
		cmd := rpcsmartrouter.CreateRPCSmartRouterCobraCommand()
		for _, s := range set {
			f := cmd.Flags().Lookup(s.key)
			require.NotNil(t, f, "--%s must be registered on the real command", s.key)
			require.NotEqual(t, s.value, f.DefValue, "--%s: the test value must differ from the default", s.key)
			require.Equal(t, f.DefValue, viper.GetString(s.key), "--%s must fall back to its registered default", s.key)
		}
	})

	t.Run("from flags", func(t *testing.T) {
		var args []string
		for _, s := range set {
			args = append(args, "--"+s.key+"="+s.value)
		}
		wireLikeRunE(t, "", args...)
		for _, s := range set {
			require.Equal(t, s.value, viper.GetString(s.key), "--%s", s.key)
		}
	})

	t.Run("from YAML", func(t *testing.T) {
		var yaml string
		for _, s := range set {
			yaml += s.key + ": " + s.value + "\n"
		}
		wireLikeRunE(t, yaml)
		for _, s := range set {
			require.Equal(t, s.value, viper.GetString(s.key), "%s in the config file", s.key)
		}
		require.Equal(t, uint64(7), viper.GetUint64(common.BenchAfterFlagName), "RunE reads bench-after as a uint64")
	})
}
