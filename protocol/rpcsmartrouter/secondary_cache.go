package rpcsmartrouter

import (
	"context"
	"net/http"
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
// Returns served=true when the response was set on the relay processor.
//
// Deliberate differences from the primary lookup:
//   - SharedStateId is empty and the reply's SeenBlock is never fed to
//     adoptSharedStateTip: shared-state tip exchange is fleet-scoped, and a
//     foreign-zone cache is not this router's fleet.
//   - The reply's BlocksHashesToHeights is discarded rather than folded into
//     latestBlockHashRequested/earliestBlockHashRequested. Those two scalars steer
//     local decisions — resolveRequestedBlock raises reqBlock to the latest, which
//     gates endpoint sync and optimizer selection, and the earliest drives
//     UpdateEarliestAndValidateExtensionRules into archive routing — and the fold
//     took max-for-latest / min-for-earliest, so the more extreme value always won
//     and a foreign tier beat this router's own primary by construction. That is the
//     same class SanitizeForeignCacheReply exists to close for LatestBlock: a foreign
//     scalar must not reach local chain-scoped state. The cost is that hash-keyed
//     archive detection falls back to the primary's mappings alone (and, in a
//     secondary-only topology, to none) — the same position a router with no cache is
//     already in.
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
) (served bool) {
	chainId, apiInterface := rpcss.GetChainIdAndApiInterface()

	cacheCtx, cancel := context.WithTimeout(ctx, rpcss.secondaryCacheTimeout)
	_, cacheSpan := tracing.StartInternalSpan(ctx, tracing.SpanCacheLookup)
	cacheStart := time.Now()
	cacheReply, cacheError := rpcss.secondaryCache.GetEntry(cacheCtx, &pairingtypes.RelayCacheGet{
		RequestHash:    hashKey,
		RequestedBlock: requestedBlockForCache,
		ChainId:        chainId,
		BlockHash:      nil,
		Finalized:      false, // same as the primary: the server searches both stores
		SharedStateId:  "",    // never join a foreign fleet's shared state
		SeenBlock:      localRelayData.SeenBlock,
		// Not asked for, because the answer would not be used: foreign block-hash→height
		// mappings are chain-scoped state this router must not adopt (see the doc comment
		// above). Leaving the field nil keeps the request and the trust boundary in
		// agreement instead of relying on the caller to drop the reply.
		BlocksHashesToHeights: nil,
	})
	cancel()
	latencyMs := float64(time.Since(cacheStart).Milliseconds())
	hit := cacheError == nil && cacheReply.GetReply() != nil

	// Trust boundary: work on a private copy and strip identity-bearing data
	// before the entry is served or backfilled. The sanitized copy is the ONLY copy
	// used from here on.
	//
	// The copy is attempted BEFORE the outcome is recorded, because a copy failure
	// falls through to a provider: classifying this as a hit first would count the
	// request in smartrouter_cache_success_total{cache_tier="secondary"} while the
	// secondary served nothing, and that series is documented as requests served from
	// the tier. A reply the secondary did return but that this router could not handle
	// is recorded as an error rather than a miss — the distinction is the point of the
	// outcome label, and a miss would hide a local defect as normal cache behaviour.
	var copyReply *pairingtypes.RelayReply
	copyFailed := false
	if hit {
		copyReply = &pairingtypes.RelayReply{}
		if copyErr := protocopy.DeepCopyProtoObject(cacheReply.GetReply(), copyReply); copyErr != nil {
			utils.LavaFormatWarning("secondary cache hit dropped: failed to copy reply", copyErr,
				utils.LogAttr("GUID", ctx),
			)
			hit = false
			copyFailed = true
		}
	}

	outcome := metrics.ClassifyCacheLookupOutcome(cacheError, hit)
	if copyFailed {
		outcome = metrics.CacheOutcomeError
	}
	tracing.RecordCacheResult(ctx, cacheSpan, metrics.CacheTierSecondary, outcome, hit, latencyMs)
	cacheSpan.End()
	go rpcss.smartRouterEndpointMetrics.RecordCacheResult(chainId, apiInterface, protocolMessage.GetApi().GetName(), metrics.CacheTierSecondary, outcome, latencyMs)
	if !hit {
		// miss, error, and timeout all degrade to a miss; nothing from the reply is
		// carried forward, so the request proceeds exactly as it would with no
		// secondary configured
		utils.LavaFormatDebug("secondary cache lookup produced no hit",
			utils.LogAttr("GUID", ctx),
			utils.LogAttr("requestedBlockForCache", requestedBlockForCache),
			utils.LogAttr("cacheError", cacheError),
			utils.LogAttr("outcome", outcome),
			utils.LogAttr("latencyMs", latencyMs),
		)
		return false
	}

	performance.SanitizeForeignCacheReply(copyReply)
	// Re-stamp the zeroed LatestBlock from this router's own GATED tip, so the two tiers
	// serve the same header set. LatestBlock's only serving-side reader is
	// appendHeadersToRelayResult, which emits Provider-Latest-Block when the value is
	// positive: leaving the sanitizer's zero in place would make a secondary hit the one
	// response in the system that omits it, breaking the tier symmetry operators are told
	// to rely on. The value written here is locally sourced and anti-lie-guarded, so none
	// of the three backfill problems the zeroing closed can reopen —
	// isFinalizedForCacheWrite sees max(localTip, localTip) and SetRelay stores
	// max(localTip, locally derived SeenBlock), both local. A router with no tip yet
	// leaves the field at zero and simply omits the header, as it already does elsewhere.
	if localTip := int64(rpcss.getLatestBlock()); localTip > 0 {
		copyReply.LatestBlock = localTip
	}
	copyReply.Data = outputFormatter(copyReply.Data)

	// Entry kind + legacy GUID placeholder substitution, shared with the primary tier
	// (resolveCachedEntryKind) so both label a replayed node error identically.
	isNodeError, resolvedData := resolveCachedEntryKind(ctx, cacheReply, copyReply.Data)
	copyReply.Data = resolvedData

	relayResult := common.RelayResult{
		Reply: copyReply,
		Request: &pairingtypes.RelayRequest{
			RelayData: localRelayData,
		},
		Finalized: false, // cache responses are not considered finalized
		// The status the ENTRY's writer recorded, carried verbatim — including zero,
		// which means "the writer recorded none" (legacy backend). It is deliberately
		// NOT fabricated into a 200 here: the backfill below persists this value, and
		// stamping an assumed 200 would launder "unknown" into "this router observed a
		// 200" in the primary store, destroying the distinction
		// CacheRelayReply.StatusCode exists to preserve. A real stored status (429,
		// 504, non-2xx) flows through so the populator's own checks reject the backfill.
		// The serving default is applied after the backfill, below.
		StatusCode: cacheReply.GetStatusCode(),
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

	// Serving default, applied only AFTER the backfill has captured the true stored
	// value: an entry whose writer recorded no status is served as an assumed 200,
	// matching a primary hit. (The chainlib serving paths would reach the same result
	// on their own — each guards with `if relayResult.GetStatusCode() != 0` before
	// setting an explicit status — but RelayResult also feeds RoundTrip, which copies
	// StatusCode straight into an http.Response where a zero is not a valid status.)
	if relayResult.StatusCode == 0 {
		relayResult.StatusCode = http.StatusOK
	}

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
	return true
}
