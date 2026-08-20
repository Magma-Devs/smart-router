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
// (docs/SECONDARY-CACHE.md). It runs only when the primary produced no hit
// (miss, error, timeout, or primary inactive/unconfigured). On a hit it sanitizes a
// private copy of the entry (the secondary is a cross-zone trust boundary),
// serves it exactly like a primary hit, and backfills the primary through the
// populator with the exact block that hit — unless the entry is a cached node
// error, which is served but never re-written as a success. Every error,
// timeout, or malformed entry degrades to a miss; this tier can never fail a request.
//
// Returns served=true when the response was set on the relay processor, plus the raw
// reply so a miss can still contribute block-hash→height data to the
// mergeBlockHashHeights fold.
//
// Deliberate differences from the primary lookup:
//   - SharedStateId is empty and the reply's SeenBlock is never fed to
//     adoptSharedStateTip: shared-state tip exchange is fleet-scoped, and a
//     foreign-zone cache is not this router's fleet.
//   - The lookup runs under the operator-configured secondary-cache-timeout rather
//     than the primary's fixed budget.
//   - Every attempted lookup is recorded with cache_tier=secondary and its outcome
//     (hit|miss|error|timeout) in the smartrouter_cache_* series and on its own
//     smartrouter.CacheLookup span. Router request counters
//     (RecordCacheHitRequest, provider_address="Cached") fire for a hit on either
//     tier, unchanged in shape.
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
		SharedStateId:         "",    // never join a foreign fleet's shared state
		SeenBlock:             localRelayData.SeenBlock,
		BlocksHashesToHeights: rpcss.newBlocksHashesToHeightsSliceFromRequestedBlockHashes(protocolMessage.GetRequestedBlocksHashes()),
	})
	cancel()
	latencyMs := float64(time.Since(cacheStart).Milliseconds())
	hit := cacheError == nil && cacheReply.GetReply() != nil
	outcome := metrics.ClassifyCacheLookupOutcome(cacheError, hit)
	tracing.RecordCacheResult(ctx, cacheSpan, metrics.CacheTierSecondary, outcome, hit, latencyMs)
	cacheSpan.End()
	go rpcss.smartRouterEndpointMetrics.RecordCacheResult(chainId, apiInterface, protocolMessage.GetApi().GetName(), metrics.CacheTierSecondary, outcome, latencyMs)
	if !hit {
		// miss, error, and timeout all degrade to a miss; the reply may still
		// carry block-hash→height data worth merging
		utils.LavaFormatDebug("secondary cache lookup produced no hit",
			utils.LogAttr("GUID", ctx),
			utils.LogAttr("requestedBlockForCache", requestedBlockForCache),
			utils.LogAttr("cacheError", cacheError),
			utils.LogAttr("latencyMs", latencyMs),
		)
		return false, cacheReply
	}

	// Trust boundary: work on a private copy and strip identity-bearing data
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
	// placeholder: explicit flag from newer backends, placeholder heuristic for
	// older ones.
	replyDataStr := string(copyReply.Data)
	isNodeError := cacheReply.GetIsNodeError() || strings.Contains(replyDataStr, common.CachedErrorGUIDPlaceholder)
	if strings.Contains(replyDataStr, common.CachedErrorGUIDPlaceholder) {
		if guid, guidOk := utils.GetUniqueIdentifier(ctx); guidOk {
			guidStr := strconv.FormatUint(guid, 10)
			copyReply.Data = []byte(strings.Replace(replyDataStr, common.CachedErrorGUIDPlaceholder, common.CachedErrorGUIDKeyPrefix+guidStr+`"`, 1))
		}
	}

	// Original upstream status, when the entry's writer recorded it. Zero is the
	// legacy "unknown" and keeps today's assume-success serving; a real stored status
	// flows through RelayResult so the populator's own 429/504/non-2xx checks decide
	// backfill eligibility instead of a hardcoded 200 making them unreachable.
	statusCode := cacheReply.GetStatusCode()
	if statusCode == 0 {
		statusCode = 200
	}

	relayResult := common.RelayResult{
		Reply: copyReply,
		Request: &pairingtypes.RelayRequest{
			RelayData: localRelayData,
		},
		Finalized:  false, // cache responses are not considered finalized
		StatusCode: statusCode,
		// Served truthfully: a cached node error carries the
		// lava-identified-node-error header (appendHeadersToRelayResult), and the
		// populator's node-error check is what rejects its backfill.
		IsNodeError:  isNodeError,
		ProviderInfo: common.ProviderInfo{ProviderAddress: ""}, // rendered as "Cached", same as a primary hit
	}

	// Backfill: the populator owns ALL eligibility — node-error, status-code,
	// stateful, NOT_APPLICABLE, finalization — so this call is unconditional and the
	// checks see the entry's real state. It runs BEFORE SetResponse so the
	// populator's synchronous deep copy cannot race the response path's later header
	// appends; the actual SET stays async inside the populator. When the primary is
	// inactive the populator skips itself (secondary-only topology).
	rpcss.tryCacheWriteResolved(ctx, protocolMessage, &relayResult, &requestedBlockForCache)

	// Same MAG-2160 rule as primary hits: a cached reply's LatestBlock is the block
	// that was current when the entry was written, not a fresh chain-head
	// observation — it never feeds tip state. The reply's SeenBlock is likewise
	// dropped here, not adopted.
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
// primary and secondary miss replies:
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
