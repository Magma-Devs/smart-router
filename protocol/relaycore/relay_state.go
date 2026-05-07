package relaycore

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	common "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	slices "github.com/magma-Devs/smart-router/utils/lavaslices"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

type RetryHashCacheInf interface {
	CheckHashInCache(hash string) bool
	AddHashToCache(hash string)
}

type RelayParserInf interface {
	ParseRelay(
		ctx context.Context,
		url string,
		req string,
		connectionType string,
		dappID string,
		consumerIp string,
		metadata []pairingtypes.Metadata,
	) (protocolMessage chainlib.ProtocolMessage, err error)
}

type ArchiveStatus struct {
	isArchive      atomic.Bool
	isUpgraded     atomic.Bool
	isHashCached   atomic.Bool
	isEarliestUsed atomic.Bool
}

func (as *ArchiveStatus) IsArchive() bool {
	return as.isArchive.Load()
}

func (as *ArchiveStatus) SetArchive(v bool) {
	as.isArchive.Store(v)
}

func (as *ArchiveStatus) IsUpgraded() bool {
	return as.isUpgraded.Load()
}

func (as *ArchiveStatus) SetUpgraded(v bool) {
	as.isUpgraded.Store(v)
}

func (as *ArchiveStatus) Copy() *ArchiveStatus {
	archiveStatus := &ArchiveStatus{}
	archiveStatus.isArchive.Store(as.isArchive.Load())
	archiveStatus.isUpgraded.Store(as.isUpgraded.Load())
	archiveStatus.isHashCached.Store(as.isHashCached.Load())
	archiveStatus.isEarliestUsed.Store(as.isEarliestUsed.Load())
	return archiveStatus
}

type RelayState struct {
	archiveStatus   *ArchiveStatus
	stateNumber     int
	protocolMessage chainlib.ProtocolMessage
	cache           RetryHashCacheInf
	relayParser     RelayParserInf
	ctx             context.Context
	lock            sync.RWMutex
}

func GetEmptyRelayState(ctx context.Context, protocolMessage chainlib.ProtocolMessage) *RelayState {
	archiveStatus := &ArchiveStatus{}
	archiveStatus.isEarliestUsed.Store(true)
	return &RelayState{
		ctx:             ctx,
		protocolMessage: protocolMessage,
		archiveStatus:   archiveStatus,
	}
}

func NewRelayState(ctx context.Context, protocolMessage chainlib.ProtocolMessage, stateNumber int, cache RetryHashCacheInf, relayParser RelayParserInf, archiveStatus *ArchiveStatus) *RelayState {
	relayRequestData := protocolMessage.RelayPrivateData()
	if archiveStatus == nil {
		utils.LavaFormatError("misuse detected archiveStatus is nil", nil, utils.Attribute{Key: "protocolMessage.GetApi", Value: protocolMessage.GetApi()})
		archiveStatus = &ArchiveStatus{}
	}
	rs := &RelayState{
		ctx:             ctx,
		protocolMessage: protocolMessage,
		stateNumber:     stateNumber,
		cache:           cache,
		relayParser:     relayParser,
		archiveStatus:   archiveStatus,
	}
	rs.archiveStatus.isArchive.Store(rs.CheckIsArchive(relayRequestData))
	return rs
}

func (rs *RelayState) CheckIsArchive(relayRequestData *pairingtypes.RelayPrivateData) bool {
	return relayRequestData != nil && slices.Contains(relayRequestData.Extensions, extensionslib.ArchiveExtension)
}

func (rs *RelayState) GetIsEarliestUsed() bool {
	if rs == nil || rs.archiveStatus == nil {
		return true
	}
	return rs.archiveStatus.isEarliestUsed.Load()
}

func (rs *RelayState) GetIsArchive() bool {
	if rs == nil {
		return false
	}
	return rs.archiveStatus.isArchive.Load()
}

func (rs *RelayState) GetIsUpgraded() bool {
	if rs == nil {
		return false
	}
	return rs.archiveStatus.isUpgraded.Load()
}

func (rs *RelayState) SetIsEarliestUsed() {
	if rs == nil || rs.archiveStatus == nil {
		return
	}
	rs.archiveStatus.isEarliestUsed.Store(true)
}

func (rs *RelayState) SetIsArchive(isArchive bool) {
	if rs == nil || rs.archiveStatus == nil {
		return
	}
	rs.archiveStatus.isArchive.Store(isArchive)
}

func (rs *RelayState) GetStateNumber() int {
	if rs == nil {
		return 0
	}
	return rs.stateNumber
}

func (rs *RelayState) GetProtocolMessage() chainlib.ProtocolMessage {
	if rs == nil {
		return nil
	}
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.protocolMessage
}

func (rs *RelayState) GetArchiveStatus() *ArchiveStatus {
	if rs == nil || rs.archiveStatus == nil {
		return nil
	}
	return rs.archiveStatus.Copy()
}

func (rs *RelayState) SetProtocolMessage(protocolMessage chainlib.ProtocolMessage) {
	if rs == nil {
		return
	}
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.protocolMessage = protocolMessage
}

// Static function to determine if archive upgrade is needed and return the appropriate protocol message
// addArchiveExtension adds the archive extension to the protocol message and
// updates archiveStatus. Returns the original message on failure.
func addArchiveExtension(ctx context.Context, protocolMessage chainlib.ProtocolMessage, archiveStatus *ArchiveStatus, relayParser RelayParserInf) chainlib.ProtocolMessage {
	relayRequestData := protocolMessage.RelayPrivateData()
	if relayRequestData == nil {
		utils.LavaFormatError("Relay request data is nil", nil, utils.LogAttr("GUID", ctx))
		return protocolMessage
	}
	if archiveStatus.isArchive.Load() {
		return protocolMessage // already archive
	}
	userData := protocolMessage.GetUserData()
	existingExtensionsPlusArchive := strings.Join(append(relayRequestData.Extensions, extensionslib.ArchiveExtension), ",")
	metaDataForArchive := []pairingtypes.Metadata{{Name: common.EXTENSION_OVERRIDE_HEADER_NAME, Value: existingExtensionsPlusArchive}}
	utils.LavaFormatTrace("[Archive] Adding archive extension", utils.LogAttr("extensions", existingExtensionsPlusArchive), utils.LogAttr("GUID", ctx))
	newProtocolMessage, err := relayParser.ParseRelay(ctx, relayRequestData.ApiUrl, string(relayRequestData.Data), relayRequestData.ConnectionType, userData.DappId, userData.ConsumerIp, metaDataForArchive)
	if err != nil {
		utils.LavaFormatError("Failed adding archive extension", err, utils.LogAttr("apiUrl", relayRequestData.ApiUrl))
		return protocolMessage
	}
	preserveRetrySafeDirectives(protocolMessage, newProtocolMessage)
	archiveStatus.isUpgraded.Store(true)
	archiveStatus.isArchive.Store(true)
	return newProtocolMessage
}

// removeArchiveExtension removes the archive extension from the protocol message
// and updates archiveStatus. Returns the original message on failure.
func removeArchiveExtension(ctx context.Context, protocolMessage chainlib.ProtocolMessage, archiveStatus *ArchiveStatus, relayParser RelayParserInf) chainlib.ProtocolMessage {
	if !archiveStatus.isUpgraded.Load() {
		return protocolMessage // nothing to remove
	}
	relayRequestData := protocolMessage.RelayPrivateData()
	if relayRequestData == nil {
		utils.LavaFormatError("Relay request data is nil", nil, utils.LogAttr("GUID", ctx))
		return protocolMessage
	}
	userData := protocolMessage.GetUserData()
	filteredExtensions := make([]string, 0, len(relayRequestData.Extensions))
	for _, ext := range relayRequestData.Extensions {
		if ext != extensionslib.ArchiveExtension {
			filteredExtensions = append(filteredExtensions, ext)
		}
	}
	existingExtensions := strings.Join(filteredExtensions, ",")
	metaDataForArchive := []pairingtypes.Metadata{{Name: common.EXTENSION_OVERRIDE_HEADER_NAME, Value: existingExtensions}}
	utils.LavaFormatTrace("[Archive] Removing archive extension", utils.LogAttr("GUID", ctx))
	newProtocolMessage, err := relayParser.ParseRelay(ctx, relayRequestData.ApiUrl, string(relayRequestData.Data), relayRequestData.ConnectionType, userData.DappId, userData.ConsumerIp, metaDataForArchive)
	if err != nil {
		utils.LavaFormatError("Failed removing archive extension", err, utils.LogAttr("apiUrl", relayRequestData.ApiUrl))
		return protocolMessage
	}
	preserveRetrySafeDirectives(protocolMessage, newProtocolMessage)
	archiveStatus.isArchive.Store(false)
	return newProtocolMessage
}

// preserveRetrySafeDirectives copies consumer-intent directives from src onto
// dst after an archive add/remove rebuilds the protocolMessage. The rebuild
// passes only the new extension override into ParseRelay, so every other
// directive on dst defaults to its zero value — without re-copying, retries
// silently drop client-set directives (MAG-1653).
//
// Both the chainMessage state (runtime source of truth for force-cache-refresh
// and timeout-override) and the directiveHeaders map (parse-time input surface,
// where lava-debug-relay lives) are updated so the two stay consistent —
// otherwise a future caller reaching for the map would silently see the wrong
// answer.
//
// Provider-selection directives (lava-select-provider, lava-stickiness) are
// deliberately NOT copied: the failover path may need to fall through to a
// different provider on retry. The block-list (lava-providers-block) is also
// not handled here because it's already preserved via usedProviders, captured
// once at the top of ProcessRelaySend from the original protocolMessage.
func preserveRetrySafeDirectives(src, dst chainlib.ProtocolMessage) {
	if src == nil || dst == nil {
		return
	}
	preserveForceCacheRefresh(src, dst)
	preserveRelayTimeout(src, dst)
	preserveDebugRelay(src, dst)
}

func preserveForceCacheRefresh(src, dst chainlib.ProtocolMessage) {
	if !src.GetForceCacheRefresh() {
		return
	}
	dst.SetForceCacheRefresh(true)
	if hdrs := dst.GetDirectiveHeaders(); hdrs != nil {
		hdrs[common.FORCE_CACHE_REFRESH_HEADER_NAME] = "true"
	}
}

func preserveRelayTimeout(src, dst chainlib.ProtocolMessage) {
	timeout := src.TimeoutOverride()
	if timeout == 0 {
		return
	}
	dst.TimeoutOverride(timeout)
	if hdrs := dst.GetDirectiveHeaders(); hdrs != nil {
		hdrs[common.RELAY_TIMEOUT_HEADER_NAME] = timeout.String()
	}
}

func preserveDebugRelay(src, dst chainlib.ProtocolMessage) {
	srcHdrs := src.GetDirectiveHeaders()
	if srcHdrs == nil {
		return
	}
	v, ok := srcHdrs[common.LAVA_DEBUG_RELAY]
	if !ok {
		return
	}
	if hdrs := dst.GetDirectiveHeaders(); hdrs != nil {
		hdrs[common.LAVA_DEBUG_RELAY] = v
	}
}

// cacheBlockHashes marks all requested block hashes as irrelevant for future queries.
func cacheBlockHashes(protocolMessage chainlib.ProtocolMessage, archiveStatus *ArchiveStatus, cache RetryHashCacheInf) {
	hashes := protocolMessage.GetRequestedBlocksHashes()
	if archiveStatus.isHashCached.CompareAndSwap(false, true) {
		for _, hash := range hashes {
			cache.AddHashToCache(hash)
		}
	}
}

// UpgradeToArchiveIfNeeded manages the archive extension lifecycle based on retry count.
// On first retry (attempt #1): adds archive extension.
// On second retry (attempt #2): removes archive extension if previously upgraded.
// If upgraded and 2+ node errors: caches hashes and returns original message (archive failed).
func UpgradeToArchiveIfNeeded(ctx context.Context, protocolMessage chainlib.ProtocolMessage, archiveStatus *ArchiveStatus, relayParser RelayParserInf, cache RetryHashCacheInf, numberOfRetriesLaunched int, numberOfNodeErrors uint64) chainlib.ProtocolMessage {
	if archiveStatus == nil {
		return protocolMessage
	}

	select {
	case <-ctx.Done():
		utils.LavaFormatTrace("Context cancelled at start of archive upgrade", utils.LogAttr("GUID", ctx))
		return protocolMessage
	default:
	}

	// Archive tried and failed — cache hashes and bail
	if archiveStatus.isUpgraded.Load() && numberOfNodeErrors >= 2 {
		cacheBlockHashes(protocolMessage, archiveStatus, cache)
		return protocolMessage
	}

	if !archiveStatus.isArchive.Load() && numberOfRetriesLaunched == 1 {
		return addArchiveExtension(ctx, protocolMessage, archiveStatus, relayParser)
	} else if archiveStatus.isUpgraded.Load() && numberOfRetriesLaunched == 2 {
		return removeArchiveExtension(ctx, protocolMessage, archiveStatus, relayParser)
	}
	return protocolMessage
}

// Legacy method wrapper for backward compatibility
func (rs *RelayState) UpgradeToArchiveIfNeeded(numberOfRetriesLaunched int, numberOfNodeErrors uint64) {
	if rs == nil || rs.archiveStatus == nil {
		return
	}

	// Use the static function to get the upgraded protocol message
	upgradedProtocolMessage := UpgradeToArchiveIfNeeded(rs.ctx, rs.GetProtocolMessage(), rs.archiveStatus, rs.relayParser, rs.cache, numberOfRetriesLaunched, numberOfNodeErrors)

	// Update the RelayState with the new protocol message
	rs.SetProtocolMessage(upgradedProtocolMessage)
}
