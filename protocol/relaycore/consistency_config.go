package relaycore

import (
	"time"

	"github.com/magma-Devs/smart-router/utils"
)

// ConsistencyBlockGapFactorFlagName is the polling-relief flag that widens the
// endpoint-lag consistency gate (the blockLagForQosSync multiplier; default 2).
const ConsistencyBlockGapFactorFlagName = "consistency-block-gap-factor"

// ConsistencyBlockGapFactorOverride is the polling-relief process-wide override of
// the blockLagForQosSync multiplier inside EndpointLagThreshold (default 2). 0 = no
// relief. A larger value tolerates staler endpoints — the companion to slower polling.
// Set once at startup from the flag and passed into NewConsistencyValidationConfig.
var ConsistencyBlockGapFactorOverride int64 = 0

// ConsistencyValidationConfig holds configuration for consistency validation
// with chain-specific thresholds derived from chain spec values.
// Only pre-request validation is used (post-response validation removed).
type ConsistencyValidationConfig struct {
	// EndpointLagThreshold is the maximum number of blocks an endpoint can be behind
	// the seen block before being deprioritized or skipped (for pre-request validation).
	// This is typically more lenient than StalenessThreshold.
	EndpointLagThreshold int64

	// EnableWaitForCatchup determines whether to wait for endpoints to catch up
	// during pre-request validation. If false, endpoints that are too far behind
	// are simply skipped.
	EnableWaitForCatchup bool

	// MaxWaitTime is the maximum time to wait for an endpoint to catch up
	// when EnableWaitForCatchup is true.
	MaxWaitTime time.Duration
}

// DefaultConsistencyValidationConfig returns a default configuration
// suitable for most chains. Uses conservative defaults.
func DefaultConsistencyValidationConfig() *ConsistencyValidationConfig {
	return &ConsistencyValidationConfig{
		EndpointLagThreshold: 10,                     // Allow endpoints up to 10 blocks behind for pre-request
		EnableWaitForCatchup: false,                  // Don't wait by default, just skip
		MaxWaitTime:          500 * time.Millisecond, // Default max wait if enabled
	}
}

// NewConsistencyValidationConfig creates a new ConsistencyValidationConfig
// with thresholds derived from chain spec values.
//
// Parameters:
//   - blockLagForQosSync: The block lag threshold used for QoS sync calculations
//   - blockDistanceToFinalization: The number of blocks until finalization
//   - averageBlockTime: The average time between blocks for this chain
//   - blockGapFactor: polling-relief multiplier on blockLagForQosSync (0 = default 2)
//
// Derivation logic:
//   - EndpointLagThreshold: max(blockLagForQosSync*blockGapFactor, blockDistanceToFinalization), minimum 10
//   - MaxWaitTime: averageBlockTime * 2 - wait up to 2 average block times
func NewConsistencyValidationConfig(
	blockLagForQosSync int64,
	blockDistanceToFinalization uint32,
	averageBlockTime time.Duration,
	blockGapFactor int64,
) *ConsistencyValidationConfig {
	// Calculate endpoint lag threshold: use the larger of double QoS sync lag
	// or the finalization distance, to ensure endpoints can serve finalized blocks.
	gapFactor := int64(2)
	if blockGapFactor != 0 {
		gapFactor = blockGapFactor
	}
	endpointLagThreshold := blockLagForQosSync * gapFactor
	if finalizationThreshold := int64(blockDistanceToFinalization); finalizationThreshold > endpointLagThreshold {
		endpointLagThreshold = finalizationThreshold
	}
	// Apply a minimum of 10 blocks
	if endpointLagThreshold < 10 {
		endpointLagThreshold = 10
	}

	// Calculate max wait time: up to 2 average block times
	maxWaitTime := averageBlockTime * 2
	// Ensure minimum of 500ms and maximum of 5s
	if maxWaitTime < 500*time.Millisecond {
		maxWaitTime = 500 * time.Millisecond
	}
	if maxWaitTime > 5*time.Second {
		maxWaitTime = 5 * time.Second
	}

	config := &ConsistencyValidationConfig{
		EndpointLagThreshold: endpointLagThreshold,
		EnableWaitForCatchup: false, // Disabled by default, can be enabled later
		MaxWaitTime:          maxWaitTime,
	}

	utils.LavaFormatDebug("created consistency validation config",
		utils.LogAttr("endpointLagThreshold", config.EndpointLagThreshold),
		utils.LogAttr("maxWaitTime", config.MaxWaitTime),
		utils.LogAttr("blockLagForQosSync", blockLagForQosSync),
		utils.LogAttr("blockDistanceToFinalization", blockDistanceToFinalization),
		utils.LogAttr("averageBlockTime", averageBlockTime),
	)

	return config
}

// IsEndpointTooFarBehind checks if the given lag exceeds the endpoint threshold.
// lag = seenBlock - endpointLatestBlock
func (c *ConsistencyValidationConfig) IsEndpointTooFarBehind(lag int64) bool {
	if c == nil {
		return false // No config means no validation
	}
	return lag > c.EndpointLagThreshold
}
