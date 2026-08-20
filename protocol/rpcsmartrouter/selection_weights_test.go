package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// newWeightFlagSet mirrors the four weight flags as rpcsmartrouter registers them, so
// Changed() behaves exactly as it does for the real command.
func newWeightFlagSet(t *testing.T) *pflag.FlagSet {
	t.Helper()
	def := provideroptimizer.DefaultUpstreamSelectorConfig()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Float64(common.ProviderOptimizerAvailabilityWeight, def.AvailabilityWeight, "")
	fs.Float64(common.ProviderOptimizerLatencyWeight, def.LatencyWeight, "")
	fs.Float64(common.ProviderOptimizerSyncWeight, def.SyncWeight, "")
	fs.Float64(common.ProviderOptimizerStakeWeight, def.StakeWeight, "")
	return fs
}

// withCleanViper isolates each case from viper's package-level state, which the router
// shares process-wide.
func withCleanViper(t *testing.T) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
}

// TestResolveSelectionWeights_PresetApplies verifies the preset reaches the weights when
// nothing is overridden.
func TestResolveSelectionWeights_PresetApplies(t *testing.T) {
	withCleanViper(t)
	viper.Set(common.ProviderOptimizerSelectionPriority, "fastest")

	config, err := resolveSelectionWeights(newWeightFlagSet(t))
	require.NoError(t, err)

	want := provideroptimizer.SelectionPriorityFastest.ApplyTo(provideroptimizer.DefaultUpstreamSelectorConfig())
	require.Equal(t, want.LatencyWeight, config.LatencyWeight)
	require.Equal(t, want.AvailabilityWeight, config.AvailabilityWeight)
	require.Equal(t, want.SyncWeight, config.SyncWeight)
	require.Equal(t, want.StakeWeight, config.StakeWeight)
}

// TestResolveSelectionWeights_CLIFlagBeatsPreset is the headline precedence rule: a weight
// typed on the command line wins over the preset, and the untouched axes keep the
// preset's values.
func TestResolveSelectionWeights_CLIFlagBeatsPreset(t *testing.T) {
	withCleanViper(t)
	viper.Set(common.ProviderOptimizerSelectionPriority, "fastest")

	fs := newWeightFlagSet(t)
	require.NoError(t, fs.Set(common.ProviderOptimizerLatencyWeight, "0.5"))
	viper.Set(common.ProviderOptimizerLatencyWeight, 0.5)

	config, err := resolveSelectionWeights(fs)
	require.NoError(t, err)

	require.Equal(t, 0.5, config.LatencyWeight, "hand-set weight must beat the preset")
	// fastest's other axes survive untouched
	require.Equal(t, 0.20, config.AvailabilityWeight)
	require.Equal(t, 0.10, config.SyncWeight)
	require.Equal(t, 0.00, config.StakeWeight)
}

// TestResolveSelectionWeights_ConfigFileBeatsPreset is the half that pflag.Changed()
// cannot see. A weight set only in config.yml must still override the preset — using
// Changed() alone would let the preset silently win here.
func TestResolveSelectionWeights_ConfigFileBeatsPreset(t *testing.T) {
	withCleanViper(t)
	viper.SetConfigType("yaml")
	require.NoError(t, viper.ReadConfig(strings.NewReader(
		common.ProviderOptimizerSelectionPriority+": fastest\n"+
			common.ProviderOptimizerLatencyWeight+": 0.42\n",
	)))

	// Flags exist but were never typed, so Changed() is false for every one of them.
	fs := newWeightFlagSet(t)
	for _, name := range []string{
		common.ProviderOptimizerAvailabilityWeight,
		common.ProviderOptimizerLatencyWeight,
		common.ProviderOptimizerSyncWeight,
		common.ProviderOptimizerStakeWeight,
	} {
		require.False(t, fs.Changed(name), "precondition: %s must not be marked as CLI-set", name)
	}

	config, err := resolveSelectionWeights(fs)
	require.NoError(t, err)

	require.Equal(t, 0.42, config.LatencyWeight, "config-file weight must beat the preset")
	require.Equal(t, 0.20, config.AvailabilityWeight, "untouched axis keeps the preset value")
}

// TestResolveSelectionWeights_DefaultIsUnchanged pins that a deployment which sets nothing
// keeps exactly today's weights.
func TestResolveSelectionWeights_DefaultIsUnchanged(t *testing.T) {
	withCleanViper(t)

	config, err := resolveSelectionWeights(newWeightFlagSet(t))
	require.NoError(t, err)

	def := provideroptimizer.DefaultUpstreamSelectorConfig()
	require.Equal(t, def.AvailabilityWeight, config.AvailabilityWeight)
	require.Equal(t, def.LatencyWeight, config.LatencyWeight)
	require.Equal(t, def.SyncWeight, config.SyncWeight)
	require.Equal(t, def.StakeWeight, config.StakeWeight)
}

// TestResolveSelectionWeights_InvalidPriorityIsRejected verifies a typo aborts instead of
// silently running the default weights.
func TestResolveSelectionWeights_InvalidPriorityIsRejected(t *testing.T) {
	withCleanViper(t)
	viper.Set(common.ProviderOptimizerSelectionPriority, "quickest")

	_, err := resolveSelectionWeights(newWeightFlagSet(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid selection priority")
}
