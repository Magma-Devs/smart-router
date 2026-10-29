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
}

// DefaultConsistencyValidationConfig returns a default configuration
// suitable for most chains. Uses conservative defaults.
func DefaultConsistencyValidationConfig() *ConsistencyValidationConfig {
	return &ConsistencyValidationConfig{
		EndpointLagThreshold: 10, // Allow endpoints up to 10 blocks behind for pre-request
	}
}

// NewConsistencyValidationConfig creates a new ConsistencyValidationConfig
// with thresholds derived from chain spec values.
//
// Parameters:
//   - blockLagForQosSync: The block lag threshold used for QoS sync calculations
//   - averageBlockTime: The average time between blocks for this chain. Logged for context
//     only; no threshold derives from it. It used to size MaxWaitTime for a wait-for-catchup
//     mode that was never enabled, removed with that mode (MAG-4243).
//   - blockGapFactor: polling-relief multiplier on blockLagForQosSync (0 = default 2)
//
// Derivation logic:
//   - EndpointLagThreshold: max(blockLagForQosSync*blockGapFactor, 10)
//
// EndpointLagThreshold bounds how stale an endpoint may be before we stop routing to it, so it
// derives from blockLagForQosSync alone: this gate only ever sees LATEST_BLOCK requests, since
// ShouldSkipConsistencyValidation skips concrete blocks and the finalized/safe/earliest/pending
// tags. A chain's finality distance bounds something else — when data stops changing — and belongs
// to cache-write finalization. Raise allowed_block_lag_for_qos_sync, or set
// --consistency-block-gap-factor, when a chain needs more tolerance.
func NewConsistencyValidationConfig(
	blockLagForQosSync int64,
	averageBlockTime time.Duration,
	blockGapFactor int64,
) *ConsistencyValidationConfig {
	// Calculate endpoint lag threshold from the QoS sync lag.
	gapFactor := int64(2)
	if blockGapFactor != 0 {
		gapFactor = blockGapFactor
	}
	endpointLagThreshold := blockLagForQosSync * gapFactor
	// Apply a minimum of 10 blocks
	if endpointLagThreshold < 10 {
		endpointLagThreshold = 10
	}

	config := &ConsistencyValidationConfig{
		EndpointLagThreshold: endpointLagThreshold,
	}

	utils.LavaFormatInfo("created consistency validation config",
		utils.LogAttr("endpointLagThreshold", config.EndpointLagThreshold),
		utils.LogAttr("blockLagForQosSync", blockLagForQosSync),
		utils.LogAttr("blockGapFactor", gapFactor),
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
