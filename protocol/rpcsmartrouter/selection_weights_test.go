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

	// Asserted on the EFFECTIVE weights, not on the typed ones. The typed 0.5 leaves the
	// set summing to 0.80, so it reaches the selector as 0.625 — the precedence rule is
	// about which value wins, and the winning value is then normalised with the rest.
	effective := effectiveWeights(t, fs)
	require.InDelta(t, 0.6250, effective.LatencyWeight, 1e-9, "hand-set weight must beat the preset")
	require.InDelta(t, 0.2500, effective.AvailabilityWeight, 1e-9)
	require.InDelta(t, 0.1250, effective.SyncWeight, 1e-9)
	require.InDelta(t, 0.0000, effective.StakeWeight, 1e-9)

	// The ratio is what actually proves the override won: had the preset survived, latency
	// would sit at 0.70 against availability's 0.20, a ratio of 3.5 rather than 2.5.
	require.InDelta(t, 0.5/0.2, effective.LatencyWeight/effective.AvailabilityWeight, 1e-9,
		"the typed 0.5 must be what got weighted, not the preset's 0.70")
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

	// 0.20/0.42/0.10/0.00 sums to 0.72, so the effective set is that divided by 0.72.
	effective := effectiveWeights(t, fs)
	require.InDelta(t, 0.42/0.72, effective.LatencyWeight, 1e-9, "config-file weight must beat the preset")
	require.InDelta(t, 0.20/0.72, effective.AvailabilityWeight, 1e-9, "untouched axis keeps the preset value")
	require.InDelta(t, 0.42/0.20, effective.LatencyWeight/effective.AvailabilityWeight, 1e-9,
		"the config-file 0.42 must be what got weighted, not the preset's 0.70")
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

// effectiveWeights runs the full production path — resolveSelectionWeights, then the
// selector that actually scores on the result. Every assertion in this file's normalisation
// tests ends here on purpose: the previous tests stopped at the struct resolveSelectionWeights
// returns, which is the number the operator TYPED, not the number the router runs. That gap
// is exactly what let the rescale go unnoticed.
func effectiveWeights(t *testing.T, fs *pflag.FlagSet) provideroptimizer.UpstreamSelectorConfig {
	t.Helper()
	config, err := resolveSelectionWeights(fs)
	require.NoError(t, err)

	sum := config.AvailabilityWeight + config.LatencyWeight + config.SyncWeight + config.StakeWeight
	require.InDelta(t, 1.0, sum, 0.001,
		"resolveSelectionWeights must hand the selector a normalised set — this delta is NewUpstreamSelector's own tolerance, so satisfying it is what proves the 'weights do not sum to 1.0' warning cannot fire")

	return provideroptimizer.NewUpstreamSelector(config).GetConfig()
}

// TestResolveSelectionWeights_PartialOverrideIsPreNormalized is the fix for the case
// @avitenzer reported: a partial override of a preset used to reach NewUpstreamSelector
// summing to 0.80, which warned at startup and rescaled the operator's number behind their
// back. The arithmetic is unchanged — it just happens here now, quietly, and the Info line
// reports the result.
func TestResolveSelectionWeights_PartialOverrideIsPreNormalized(t *testing.T) {
	withCleanViper(t)
	viper.Set(common.ProviderOptimizerSelectionPriority, "fastest")

	fs := newWeightFlagSet(t)
	require.NoError(t, fs.Set(common.ProviderOptimizerLatencyWeight, "0.5"))
	viper.Set(common.ProviderOptimizerLatencyWeight, 0.5)

	// fastest is 0.20/0.70/0.10/0.00; overriding latency to 0.5 sums to 0.80, so every
	// weight is divided by 0.80.
	effective := effectiveWeights(t, fs)
	require.InDelta(t, 0.2500, effective.AvailabilityWeight, 1e-9)
	require.InDelta(t, 0.6250, effective.LatencyWeight, 1e-9)
	require.InDelta(t, 0.1250, effective.SyncWeight, 1e-9)
	require.InDelta(t, 0.0000, effective.StakeWeight, 1e-9)
}

// TestResolveSelectionWeights_NormalizationPreservesRatios pins WHY option (b) was chosen
// over honouring the typed number literally: scaling changes the absolute figures but not
// their relationship, so no upstream's ranking moves. If this ever fails, the rescale has
// stopped being a presentation concern and become a scoring change.
func TestResolveSelectionWeights_NormalizationPreservesRatios(t *testing.T) {
	withCleanViper(t)
	viper.Set(common.ProviderOptimizerSelectionPriority, "fastest")

	fs := newWeightFlagSet(t)
	require.NoError(t, fs.Set(common.ProviderOptimizerLatencyWeight, "0.5"))
	viper.Set(common.ProviderOptimizerLatencyWeight, 0.5)

	effective := effectiveWeights(t, fs)
	require.InDelta(t, 0.5/0.2, effective.LatencyWeight/effective.AvailabilityWeight, 1e-9,
		"latency:availability must survive the rescale")
	require.InDelta(t, 0.5/0.1, effective.LatencyWeight/effective.SyncWeight, 1e-9,
		"latency:sync must survive the rescale")
}

// TestResolveSelectionWeights_NoPresetOverrideIsUnchanged is the backward-compatibility pin,
// and the reason option (a) was rejected. A deployment that sets ONE weight by hand today,
// with no preset involved, sums to 1.20 and is rescaled by NewUpstreamSelector. Normalising
// earlier must produce the byte-identical result — honouring the typed 0.5 literally would
// have moved these deployments on upgrade without anyone asking.
func TestResolveSelectionWeights_NoPresetOverrideIsUnchanged(t *testing.T) {
	withCleanViper(t)

	fs := newWeightFlagSet(t)
	require.NoError(t, fs.Set(common.ProviderOptimizerLatencyWeight, "0.5"))
	viper.Set(common.ProviderOptimizerLatencyWeight, 0.5)

	// Defaults 0.30/0.30/0.20/0.20 with latency -> 0.5 sums to 1.20.
	effective := effectiveWeights(t, fs)
	require.InDelta(t, 0.30/1.2, effective.AvailabilityWeight, 1e-9)
	require.InDelta(t, 0.50/1.2, effective.LatencyWeight, 1e-9)
	require.InDelta(t, 0.20/1.2, effective.SyncWeight, 1e-9)
	require.InDelta(t, 0.20/1.2, effective.StakeWeight, 1e-9)

	// The same set handed straight to the selector, exactly as it was before this change.
	legacy := provideroptimizer.DefaultUpstreamSelectorConfig()
	legacy.LatencyWeight = 0.5
	before := provideroptimizer.NewUpstreamSelector(legacy).GetConfig()
	require.InDelta(t, before.AvailabilityWeight, effective.AvailabilityWeight, 1e-9)
	require.InDelta(t, before.LatencyWeight, effective.LatencyWeight, 1e-9)
	require.InDelta(t, before.SyncWeight, effective.SyncWeight, 1e-9)
	require.InDelta(t, before.StakeWeight, effective.StakeWeight, 1e-9)
}

// TestResolveSelectionWeights_UntouchedDeploymentIsBitIdentical guards against the
// normalisation introducing float drift on the path almost every deployment takes. The
// defaults already sum to 1.0, so nothing may be divided at all — Equal, not InDelta.
func TestResolveSelectionWeights_UntouchedDeploymentIsBitIdentical(t *testing.T) {
	withCleanViper(t)

	config, err := resolveSelectionWeights(newWeightFlagSet(t))
	require.NoError(t, err)

	def := provideroptimizer.DefaultUpstreamSelectorConfig()
	require.Equal(t, def.AvailabilityWeight, config.AvailabilityWeight)
	require.Equal(t, def.LatencyWeight, config.LatencyWeight)
	require.Equal(t, def.SyncWeight, config.SyncWeight)
	require.Equal(t, def.StakeWeight, config.StakeWeight)
}

// TestResolveSelectionWeights_InvalidWeightsStillReachTheSelector verifies the normaliser
// stays out of the way of genuinely broken input. A negative weight is a config error, not
// an unscaled set: it must arrive at NewUpstreamSelector untouched so the existing
// validation rejects it and falls back to the defaults, rather than being silently rescaled
// into something that looks plausible.
func TestResolveSelectionWeights_InvalidWeightsStillReachTheSelector(t *testing.T) {
	withCleanViper(t)

	fs := newWeightFlagSet(t)
	require.NoError(t, fs.Set(common.ProviderOptimizerLatencyWeight, "-0.4"))
	viper.Set(common.ProviderOptimizerLatencyWeight, -0.4)

	config, err := resolveSelectionWeights(fs)
	require.NoError(t, err)
	require.Equal(t, -0.4, config.LatencyWeight, "an invalid weight must pass through unscaled")

	def := provideroptimizer.DefaultUpstreamSelectorConfig()
	effective := provideroptimizer.NewUpstreamSelector(config).GetConfig()
	require.Equal(t, def.LatencyWeight, effective.LatencyWeight, "the selector must still fall back to defaults")
	require.Equal(t, def.AvailabilityWeight, effective.AvailabilityWeight)
}
