package relaycore

import (
	"fmt"

	"github.com/magma-Devs/smart-router/protocol/lavaprotocol/protocolerrors"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// ShouldSkipConsistencyValidation returns true when consistency validation
// should not be applied to the request. Validation is skipped for:
// - NOT_APPLICABLE requests (no block parsing)
// - Historical block requests (specific block number > 0)
// - Special block tags (EARLIEST, FINALIZED, SAFE, PENDING)
//
// Validation is only applied for LATEST_BLOCK requests where we expect
// the most recent state.
func ShouldSkipConsistencyValidation(requestedBlock int64) bool {
	switch requestedBlock {
	case spectypes.NOT_APPLICABLE:
		// Block parsing failed or API doesn't use blocks
		return true
	case spectypes.EARLIEST_BLOCK:
		// Historical request for genesis/earliest block
		return true
	case spectypes.FINALIZED_BLOCK:
		// Tag-based finalized block request
		return true
	case spectypes.SAFE_BLOCK:
		// Tag-based safe block request
		return true
	case spectypes.PENDING_BLOCK:
		// Pending block request (future state)
		return true
	case spectypes.LATEST_BLOCK:
		// LATEST_BLOCK should be validated
		return false
	default:
		// requestedBlock >= 0 means specific historical block (including genesis block 0)
		// requestedBlock < -6 would be unknown/invalid
		if requestedBlock >= 0 {
			return true // Historical block request (including genesis)
		}
		// Unknown negative value, validate to be safe
		return false
	}
}

// ValidateEndpointCapability checks if an endpoint can serve a request by comparing the endpoint's
// own latest block against the global chain tip. This is the pre-request validation used for
// endpoint selection/prioritization.
//
// The reference is the CHAIN TIP (chainstate.ChainState.GetLatestBlock), not a per-user seen block
// (Topic C C-G). The per-user seenBlock it replaced was fed straight from the served provider's
// Reply.LatestBlock with only a monotonic-increase guard and no anti-lie cross-check, so a provider
// reporting a fake-high block poisoned the reference: honest endpoints then measured as "behind"
// and were filtered out here, handing the liar all traffic on a multi-provider pod and driving a
// single-provider pod to No pairings until manual reset (F14, confirmed in production). The tip is
// anti-lie-guarded on both sides — SetLatestBlock rejects implausible jumps and Recompute snaps it
// back into the consensus band — so a rejected block can no longer poison consistency.
//
// Parameters:
//   - endpointLatestBlock: The endpoint's own tracked latest block (per-endpoint operand)
//   - chainTip: The global chain tip to measure against
//   - requestedBlock: The block requested in the original request
//   - config: Validation configuration with thresholds
//
// Returns:
//   - nil if the endpoint is capable or validation should be skipped
//   - ConsistencyError if the endpoint is too far behind
func ValidateEndpointCapability(
	endpointLatestBlock int64,
	chainTip int64,
	requestedBlock int64,
	config *ConsistencyValidationConfig,
) error {
	// Skip if no config (validation disabled)
	if config == nil {
		return nil
	}

	// Skip if the tip is unknown (no reference to measure against)
	if chainTip <= 0 {
		return nil
	}

	// Skip if endpoint's latest block is unknown
	if endpointLatestBlock <= 0 {
		// Unknown state - allow the request to proceed
		// Post-response validation will catch any issues
		return nil
	}

	// Skip for requests that shouldn't be validated
	if ShouldSkipConsistencyValidation(requestedBlock) {
		return nil
	}

	// If endpoint is at or ahead of the tip, it's capable
	if endpointLatestBlock >= chainTip {
		return nil
	}

	// Calculate lag: how many blocks behind the tip is the endpoint?
	lag := chainTip - endpointLatestBlock

	// Check if lag exceeds threshold
	if config.IsEndpointTooFarBehind(lag) {
		utils.LavaFormatDebug("endpoint failed capability validation: too far behind",
			utils.LogAttr("endpointLatestBlock", endpointLatestBlock),
			utils.LogAttr("chainTip", chainTip),
			utils.LogAttr("lag", lag),
			utils.LogAttr("threshold", config.EndpointLagThreshold),
			utils.LogAttr("requestedBlock", requestedBlock),
		)
		return fmt.Errorf("endpoint block %d is too far behind (chain tip: %d, lag: %d blocks, threshold: %d): %w",
			endpointLatestBlock, chainTip, lag, config.EndpointLagThreshold, protocolerrors.ConsistencyError)
	}

	// Lag is within acceptable threshold
	utils.LavaFormatDebug("endpoint within lag threshold",
		utils.LogAttr("endpointLatestBlock", endpointLatestBlock),
		utils.LogAttr("chainTip", chainTip),
		utils.LogAttr("lag", lag),
		utils.LogAttr("threshold", config.EndpointLagThreshold),
	)
	return nil
}
