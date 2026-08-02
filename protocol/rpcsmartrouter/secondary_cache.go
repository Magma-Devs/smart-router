package rpcsmartrouter

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/internal/chainqueries"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/tracing"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/protocopy"
)

// secondaryCacheActive reports whether a secondary cache is configured and worth
// querying. Nil-safe for both the unconfigured case (interface nil) and the concrete
// client's own disconnected states (CacheActive is nil-receiver-safe).
func (rpcss *RPCSmartRouterServer) secondaryCacheActive() bool {
	return rpcss.secondaryCache != nil && rpcss.secondaryCache.CacheActive()
}

// trySecondaryCacheLookup queries the read-only secondary cache tier
// (docs/SECONDARY-CACHE-DESIGN.md §5). It runs only when the primary produced no hit
// (miss, error, timeout, or primary inactive/unconfigured). On a hit it sanitizes a
// private copy of the entry (§4 — the secondary is a cross-zone trust boundary),
// serves it exactly like a primary hit, and backfills the primary through the
// populator with the exact block that hit (§6) — unless the entry is a cached node
// error (§7), which is served but never re-written as a success. Every error,
// timeout, or malformed entry degrades to a miss; this tier can never fail a request.
//
// Returns served=true when the response was set on the relay processor, plus the raw
// reply so a miss can still contribute block-hash→height data to the §8 merge.
//
// Deliberate differences from the primary lookup:
//   - SharedStateId is empty and the reply's SeenBlock is never fed to
//     adoptSharedStateTip: shared-state tip exchange is fleet-scoped, and a
//     foreign-zone cache is not this router's fleet (§5, T13).
//   - The lookup runs under the operator-configured secondary-cache-timeout rather
//     than the primary's fixed budget.
//   - Per-tier cache metrics land with the observability slice (design §12);
//     recording this lookup into today's tier-less series would corrupt them. Router
//     request counters (RecordCacheHitRequest, provider_address="Cached") fire for a
//     hit on either tier, unchanged in shape.
func (rpcss *RPCSmartRouterServer) trySecondaryCacheLookup(
	ctx context.Context,
	protocolMessage chainlib.ProtocolMessage,
	localRelayData *pairingtypes.RelayPrivateData,
	relayProcessor *relaycore.RelayProcessor,
	analytics *metrics.RelayMetrics,
	hashKey []byte,
	outputFormatter func([]byte) []byte,
	requestedBlockForCache int64,
) (served bool, secondaryReply *pairingtypes.CacheRelayReply) {
	chainId, apiInterface := rpcss.GetChainIdAndApiInterface()

	cacheCtx, cancel := context.WithTimeout(ctx, rpcss.secondaryCacheTimeout)
	_, cacheSpan := tracing.StartInternalSpan(ctx, tracing.SpanCacheLookup)
	cacheStart := time.Now()
	cacheReply, cacheError := rpcss.secondaryCache.GetEntry(cacheCtx, &pairingtypes.RelayCacheGet{
		RequestHash:           hashKey,
		RequestedBlock:        requestedBlockForCache,
		ChainId:               chainId,
		BlockHash:             nil,
		Finalized:             false, // same as the primary: the server searches both stores
		SharedStateId:         "",    // never join a foreign fleet's shared state (§5)
		SeenBlock:             localRelayData.SeenBlock,
		BlocksHashesToHeights: rpcss.newBlocksHashesToHeightsSliceFromRequestedBlockHashes(protocolMessage.GetRequestedBlocksHashes()),
	})
	cancel()
	latencyMs := float64(time.Since(cacheStart).Milliseconds())
	hit := cacheError == nil && cacheReply.GetReply() != nil
	tracing.RecordCacheResult(ctx, cacheSpan, hit, latencyMs)
	cacheSpan.End()
	if !hit {
		// miss, error, and timeout all degrade to a miss (§5); the reply may still
		// carry block-hash→height data worth merging (§8)
		utils.LavaFormatDebug("secondary cache lookup produced no hit",
			utils.LogAttr("GUID", ctx),
			utils.LogAttr("requestedBlockForCache", requestedBlockForCache),
			utils.LogAttr("cacheError", cacheError),
			utils.LogAttr("latencyMs", latencyMs),
		)
		return false, cacheReply
	}

	// Trust boundary (§4): work on a private copy and strip identity-bearing data
	// before the entry is served or backfilled. The sanitized copy is the ONLY copy
	// used from here on.
	copyReply := &pairingtypes.RelayReply{}
	if copyErr := protocopy.DeepCopyProtoObject(cacheReply.GetReply(), copyReply); copyErr != nil {
		utils.LavaFormatWarning("secondary cache hit dropped: failed to copy reply", copyErr,
			utils.LogAttr("GUID", ctx),
		)
		return false, cacheReply
	}
	performance.SanitizeForeignCacheReply(copyReply)
	copyReply.Data = outputFormatter(copyReply.Data)

	// Entry kind must be decided BEFORE the GUID substitution below erases the legacy
	// placeholder (§7): explicit flag from newer backends, placeholder heuristic for
	// older ones.
	replyDataStr := string(copyReply.Data)
	isNodeError := cacheReply.GetIsNodeError() || strings.Contains(replyDataStr, common.CachedErrorGUIDPlaceholder)
	if strings.Contains(replyDataStr, common.CachedErrorGUIDPlaceholder) {
		if guid, guidOk := utils.GetUniqueIdentifier(ctx); guidOk {
			guidStr := strconv.FormatUint(guid, 10)
			copyReply.Data = []byte(strings.Replace(replyDataStr, common.CachedErrorGUIDPlaceholder, common.CachedErrorGUIDKeyPrefix+guidStr+`"`, 1))
		}
	}

	relayResult := common.RelayResult{
		Reply: copyReply,
		Request: &pairingtypes.RelayRequest{
			RelayData: localRelayData,
		},
		Finalized:    false, // cache responses are not considered finalized
		StatusCode:   200,
		ProviderInfo: common.ProviderInfo{ProviderAddress: ""}, // rendered as "Cached", same as a primary hit
	}

	// Backfill (§6): populator-governed, keyed by the exact block that produced this
	// hit. Runs BEFORE SetResponse so the populator's synchronous deep copy cannot
	// race the response path's later header appends; the actual SET stays async
	// inside the populator. Cached node errors are served but never re-written as
	// successes (§7); when the primary is inactive the populator skips itself (§5
	// secondary-only topology).
	if !isNodeError {
		rpcss.tryCacheWriteResolved(ctx, protocolMessage, &relayResult, &requestedBlockForCache)
	}

	// Same MAG-2160 rule as primary hits: a cached reply's LatestBlock is the block
	// that was current when the entry was written, not a fresh chain-head
	// observation — it never feeds tip state. The reply's SeenBlock is likewise
	// dropped here (T13), not adopted.
	relayProcessor.SetResponse(&relaycore.RelayResponse{
		RelayResult: relayResult,
		Err:         nil,
	})

	if analytics == nil {
		analytics = &metrics.RelayMetrics{}
	}
	analytics.IsWrite = chainlib.GetStateful(protocolMessage) != 0
	analytics.IsArchive = chainqueries.IsArchiveRequest(protocolMessage)
	analytics.IsDebugTrace = chainqueries.IsDebugOrTraceRequest(protocolMessage)
	analytics.IsBatch = chainqueries.IsBatchRequest(protocolMessage)
	go rpcss.smartRouterEndpointMetrics.RecordCacheHitRequest(chainId, apiInterface, protocolMessage.GetApi().GetName(), analytics)

	utils.LavaFormatDebug("secondary cache hit",
		utils.LogAttr("chainId", chainId),
		utils.LogAttr("requestedBlock", requestedBlockForCache),
		utils.LogAttr("isNodeError", isNodeError),
		utils.LogAttr("GUID", ctx),
	)
	return true, cacheReply
}

// mergeBlockHashHeights merges the block-hash→height scalars harvested from the
// primary and secondary miss replies (docs/SECONDARY-CACHE-DESIGN.md §8):
// information is merged, never overwritten. The earliest is the minimum valid
// height, the latest the maximum, and a reply without mappings (NOT_APPLICABLE,
// negative) cannot erase the other tier's values. Conflicting heights for the same
// hash both enter the fold deterministically.
func mergeBlockHashHeights(latestA, earliestA, latestB, earliestB int64) (latest, earliest int64) {
	latest = latestA
	if latestB >= 0 && (latest < 0 || latestB > latest) {
		latest = latestB
	}
	earliest = earliestA
	if earliestB >= 0 && (earliest < 0 || earliestB < earliest) {
		earliest = earliestB
	}
	return latest, earliest
}
