package lavasession

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	metrics "github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	BlockedProviderSessionUsedStatus   = uint32(1)
	BlockedProviderSessionUnusedStatus = uint32(0)
)

// debugProbes is an atomic toggle: it is read by probe goroutines (probeProvider) while the CLI
// layer sets it from the parsed --debug-probes flag. A plain bool here raced the flag write against
// those reads under -race (a leaked probe goroutine from one test vs another test building the cobra
// command). Use SetDebugProbes/DebugProbesEnabled rather than touching it directly.
var debugProbes atomic.Bool

// SetDebugProbes applies the --debug-probes flag value (called once at startup from the CLI layer).
func SetDebugProbes(enabled bool) { debugProbes.Store(enabled) }

// DebugProbesEnabled reports whether verbose probe logging is on.
func DebugProbesEnabled() bool { return debugProbes.Load() }

var (
	retrySecondChanceAfter = time.Minute * 3
	// ProbeLoopInterval is the configurable cadence (MAG-2161 D5) of the proactive health prober
	// (rpcsmartrouter.runProbeLoop) — the single source of truth for direct-RPC endpoint health and
	// probe-fed QoS. Default 5s; validated at startup (a non-positive value is rejected back to the
	// default).
	ProbeLoopInterval = 5 * time.Second
)

// created with NewConsumerSessionManager
type ConsumerSessionManager struct {
	rpcEndpoint    *RPCEndpoint // used to filter out endpoints
	lock           sync.RWMutex
	pairing        map[string]*ConsumerSessionsWithProvider // key == provider address
	stickySessions *StickySessionStore
	currentEpoch   uint64
	numberOfResets uint64

	// original pairingAddresses for current epoch
	// contains all addresses from the initial pairing. and the keys are the indexes of the pairing query (these indexes are used for data reliability)
	pairingAddresses       map[uint64]string
	pairingAddressesLength uint64

	// pairingGeneration increments on every UpdateAllProviders. It is the key the pool-empty
	// report throttles on: an empty pool is a property of the pairing, not of the request that
	// tripped over it, so it is worth one WARN per pairing rather than one per relay.
	pairingGeneration uint64

	// poolEmptyReportMu guards the two fields below. A dedicated mutex rather than csm.lock: this
	// is taken on the failing-relay path, and that path must not contend the manager's central
	// lock just to decide whether a line has already been printed.
	poolEmptyReportMu     sync.Mutex
	poolEmptyReportedGen  uint64
	poolEmptyReportedWhy  string
	poolEmptyReportedEver bool

	// lastPairingUpdate is when UpdateAllProviders last rebuilt the pairing. It is reported on the
	// pool-empty line, where it separates a pool that was never populated (the provider failed its
	// startup verification and was never added) from one that has drained since the last epoch —
	// two causes that look identical once the pool is empty.
	lastPairingUpdate time.Time

	// contains all provider addresses that are currently valid
	validAddresses []string
	// provider addresses that were given a second chance instead of reporting them immediately
	secondChanceGivenToAddresses map[string]struct{}

	// rateLimitHoldoff is consulted at provider selection so requests prefer providers
	// that are not currently held off after a 429 (docs/RATE-LIMIT-HOLDOFF.md).
	// Production uses the process-wide holdoff.Shared; tests inject a clock-pinned one.
	rateLimitHoldoff *holdoff.Registry

	// contains a sorted list of blocked addresses, sorted by their cu used this epoch for higher chance of response
	currentlyBlockedProviderAddresses []string

	// blockedProviderRecords answers "why is this provider out?" for every currently blocked
	// address — regular (currentlyBlockedProviderAddresses) and backup (blockedBackupProviders)
	// alike (MAG-2599). Kept as a side map rather than folded into the slice above because that
	// slice is ordered by CU served (sortBlockedProviderListByCuServed) and the blocked-provider
	// walk depends on that order.
	//
	// Invariant: an address is present here if and only if it is blocked in one of those two
	// stores. Every write to either must keep this in step.
	//
	// The RELEASE half is centralised — releaseBlockRecordLocked owns it, and knows that the two
	// stores share one record. The BLOCK half is not: four sites write this map directly
	// (the epoch re-block pass, removeAddressFromValidAddresses, blockProvider's backup branch and
	// its second-chance fixup), so that half is still enforced by convention. Every bug found in
	// review so far has been a store mutation that forgot this map, so a new write site is the
	// thing to look at first. Folding all three stores into one owning type is the real answer and
	// is planned as stage 1 of the block/unblock consolidation.
	blockedProviderRecords map[string]BlockRecord

	// History of blocked providers from previous epoch to prevent known-bad providers
	// from getting a clean slate at epoch transitions. Carries the full record, not just the
	// address, so a block that survives an epoch keeps its original reason and Since instead of
	// resetting to "blocked because it was blocked".
	previousEpochBlockedProviders map[string]BlockRecord

	// backup providers - emergency fallback providers when no regular providers are available
	backupProviders map[string]*ConsumerSessionsWithProvider // key == provider address

	// blocked backup providers - backup providers blocked this epoch due to failures.
	// Separate from currentlyBlockedProviderAddresses because backup providers are not in validAddresses.
	blockedBackupProviders map[string]struct{}

	addonAddresses    map[string][]string // key is RouterKey.String()
	reportedProviders *ReportedProviders

	// Latest selection stats for debugging provider selection (thread-safe access)
	selectionStatsLock   sync.RWMutex
	latestSelectionStats *provideroptimizer.SelectionStats
	// pairingPurge - contains all pairings that are unwanted this epoch, keeps them in memory in order to avoid release.
	// (if a consumer session still uses one of them or we want to report it.)
	pairingPurge                       map[string]*ConsumerSessionsWithProvider
	providerOptimizer                  ProviderOptimizer
	consumerMetricsManager             metrics.ConsumerMetricsManagerInf
	consumerPublicAddress              string
	activeSubscriptionProvidersStorage *ActiveSubscriptionProvidersStorage

	qosManager *qos.QoSManager

	// getLavaBlockHeight returns the current Lava blockchain block height
	// This is NOT used for RelaySession.Epoch (which must be the pairing epoch start block)
	getLavaBlockHeight func() int64

	// consensusBaselineGetter resolves THIS interface's consensus baseline (block, the time it was
	// computed, and whether a fresh majority exists) for the relay sync dimension (Topic E / F4). It
	// is per-CSM (one CSM per chain+interface) so the relay path measures sync lag against its own
	// interface's ChainState — never another interface's, the bug of the former shared-optimizer
	// getter. nil means no consensus integration is wired (legacy max-across-providers reference).
	consensusBaselineGetterLock sync.RWMutex
	consensusBaselineGetter     func() (block uint64, at time.Time, fresh bool)
}

// SetConsensusBaselineGetter installs the per-interface consensus baseline source the relay sync
// dimension measures lag against (Topic E / F4). Read-only toward the data plane (reads ChainState).
func (csm *ConsumerSessionManager) SetConsensusBaselineGetter(getter func() (block uint64, at time.Time, fresh bool)) {
	csm.consensusBaselineGetterLock.Lock()
	defer csm.consensusBaselineGetterLock.Unlock()
	csm.consensusBaselineGetter = getter
}

// resolveSyncReference builds the SyncReference for a relay sample from this interface's consensus
// getter. No getter → ConsensusConfigured=false (legacy reference); getter present but no fresh
// majority → ConsensusConfigured=true, Fresh=false (the optimizer then OMITS the sync update rather
// than poisoning it with max-across-providers, F5).
func (csm *ConsumerSessionManager) resolveSyncReference() provideroptimizer.SyncReference {
	csm.consensusBaselineGetterLock.RLock()
	getter := csm.consensusBaselineGetter
	csm.consensusBaselineGetterLock.RUnlock()
	if getter == nil {
		return provideroptimizer.SyncReference{}
	}
	ref := provideroptimizer.SyncReference{ConsensusConfigured: true}
	if block, at, fresh := getter(); fresh && block > 0 {
		ref.Block, ref.Time, ref.Fresh = block, at, true
	}
	return ref
}

func (csm *ConsumerSessionManager) GetQoSManager() *qos.QoSManager {
	return csm.qosManager
}

// GetProviderOptimizer exposes the provider optimizer so callers can wire optional capabilities
// onto it — e.g. installing the Topic E sync-reference getter (the consensus baseline the QoS sync
// dimension measures lag against). Returns the interface; callers type-assert for the capability.
func (csm *ConsumerSessionManager) GetProviderOptimizer() ProviderOptimizer {
	return csm.providerOptimizer
}

// ProviderRoutingSnapshot is a consistent copy of the CSM's routing-pool state for read-only debug
// introspection (MAG-2202 /debug/provider-routing): the addresses currently eligible to route, the
// primary providers blocked this epoch, and the blocked backup providers. Every slice is a copy, so
// the caller never aliases CSM-internal state; backup providers are sorted for deterministic output.
// All slices are non-nil (empty rather than nil) so the JSON encodes as [] rather than null.
type ProviderRoutingSnapshot struct {
	ValidAddresses                    []string
	CurrentlyBlockedProviderAddresses []string
	BlockedBackupProviders            []string
	// Blocked is the same set as the two blocked lists above, with the reason each provider is out
	// (MAG-2599). The plain address lists are kept alongside it because the MAG-2202 integration
	// suite reads them; this is additive.
	Blocked []BlockedProviderInfo
	// HeldOff is a DIFFERENT thing from blocked, and the distinction matters: a provider held off
	// after a 429 is still listed in ValidAddresses and is still perfectly healthy — it is simply
	// not being asked until its deadline passes. Without this an operator sees an eligible provider
	// receiving no traffic and goes looking for a bug that is not there.
	HeldOff []HeldOffProviderInfo
}

// BlockedProviderInfo is one blocked provider and why, for /debug/provider-routing.
//
// Reported and SecondChanceGranted are included because they determine which recovery routes can
// fire — Reported=false means the 30-second reconnect loop will never look at it, and
// SecondChanceGranted=true means it returns on its own — which is the question that follows "why".
type BlockedProviderInfo struct {
	Address string
	Reason  BlockReason
	// Since is when the block was decided, preserved across epoch carry-over.
	Since time.Time
	// BlockedForSeconds is Since rendered as an age, so a reader does not have to subtract.
	BlockedForSeconds   float64
	Detail              string
	Reported            bool
	SecondChanceGranted bool
	// Carries is how many epoch transitions this block has survived.
	Carries uint32
	// Scope is "primary" or "backup". For a provider blocked in both pools it names the block that
	// is still standing, so it stays truthful after one of the two is released.
	Scope string
}

// HeldOffProviderInfo is one provider currently skipped for rate (BLOCKING-TODAY.md layer 6).
//
// Derived at read time from the shared hold-off registry rather than from any stored state, so it
// disappears with no migration once hold-off folds into the same gate as everything else.
type HeldOffProviderInfo struct {
	Address string
	ReadyAt time.Time
	// SecondsRemaining is clamped at 0 rather than going negative on a deadline that has just
	// passed but has not yet been observed as expired.
	SecondsRemaining float64
}

// ProviderRoutingSnapshot returns a copy of this CSM's valid / blocked / blocked-backup provider
// addresses under csm.lock. Read-only — it never mutates routing state.
func (csm *ConsumerSessionManager) ProviderRoutingSnapshot() ProviderRoutingSnapshot {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	backups := make([]string, 0, len(csm.blockedBackupProviders))
	for addr := range csm.blockedBackupProviders {
		backups = append(backups, addr)
	}
	sort.Strings(backups)
	now := time.Now()
	return ProviderRoutingSnapshot{
		ValidAddresses:                    append([]string{}, csm.validAddresses...),
		CurrentlyBlockedProviderAddresses: append([]string{}, csm.currentlyBlockedProviderAddresses...),
		BlockedBackupProviders:            backups,
		Blocked:                           csm.blockedProviderInfoLocked(now),
		HeldOff:                           csm.heldOffProviderInfoLocked(now),
	}
}

// blockedProviderInfoLocked renders every current block with its reason, newest first so the most
// recent decision — usually the one being investigated — is at the top. csm.lock must be held.
func (csm *ConsumerSessionManager) blockedProviderInfoLocked(now time.Time) []BlockedProviderInfo {
	blocked := make([]BlockedProviderInfo, 0, len(csm.blockedProviderRecords))
	for address, record := range csm.blockedProviderRecords {
		blocked = append(blocked, BlockedProviderInfo{
			Address:             address,
			Reason:              record.Reason,
			Since:               record.Since,
			BlockedForSeconds:   record.blockedFor(now).Seconds(),
			Detail:              record.Detail,
			Reported:            record.Reported,
			SecondChanceGranted: record.SecondChanceGranted,
			Carries:             record.Carries,
			Scope:               blockScope(record.Backup),
		})
	}
	sort.Slice(blocked, func(i, j int) bool {
		if blocked[i].Since.Equal(blocked[j].Since) {
			return blocked[i].Address < blocked[j].Address // stable output for equal timestamps
		}
		return blocked[i].Since.After(blocked[j].Since)
	})
	return blocked
}

// heldOffProviderInfoLocked reports which providers the rate-limit registry is currently holding
// off, across both the regular pairing and the backup pool. csm.lock must be held.
//
// Lock order: csm.lock then the registry's own lock, which is the order the selection path already
// takes (getValidProviderAddresses runs under csm.lock and calls straight through to
// ProviderReadyAt), and protocol/holdoff has no dependency on this package, so there is no cycle.
func (csm *ConsumerSessionManager) heldOffProviderInfoLocked(now time.Time) []HeldOffProviderInfo {
	held := []HeldOffProviderInfo{}
	if csm.rateLimitHoldoff == nil {
		return held
	}
	seen := make(map[string]struct{}, len(csm.pairingAddresses)+len(csm.backupProviders))
	consider := func(address string) {
		if _, done := seen[address]; done {
			return // a provider configured as both regular and backup must appear once
		}
		seen[address] = struct{}{}
		readyAt, isHeld := csm.rateLimitHoldoff.ProviderReadyAt(address)
		if !isHeld {
			return
		}
		remaining := readyAt.Sub(now).Seconds()
		if remaining < 0 {
			remaining = 0
		}
		held = append(held, HeldOffProviderInfo{Address: address, ReadyAt: readyAt, SecondsRemaining: remaining})
	}
	for _, address := range csm.pairingAddresses {
		consider(address)
	}
	for address := range csm.backupProviders {
		consider(address)
	}
	sort.Slice(held, func(i, j int) bool { return held[i].Address < held[j].Address })
	return held
}

func (csm *ConsumerSessionManager) GetNumberOfValidProviders() int {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	return len(csm.validAddresses)
}

// countDistinctGroups counts distinct cross-validation group labels across the given provider addresses,
// folding an empty GroupLabel into the implicit common.DefaultProviderGroup. Assumes csm.lock is held.
func (csm *ConsumerSessionManager) countDistinctGroups(addresses []string) int {
	groups := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		label := common.DefaultProviderGroup
		if cswp, ok := csm.pairing[addr]; ok && cswp.GroupLabel != "" {
			label = cswp.GroupLabel
		}
		groups[label] = struct{}{}
	}
	return len(groups)
}

// countByGroup returns the per-group provider count for the given addresses (empty label folded into
// common.DefaultProviderGroup). Assumes csm.lock is held.
func (csm *ConsumerSessionManager) countByGroup(addresses []string) map[string]int {
	counts := make(map[string]int, len(addresses))
	for _, addr := range addresses {
		label := common.DefaultProviderGroup
		if cswp, ok := csm.pairing[addr]; ok && cswp.GroupLabel != "" {
			label = cswp.GroupLabel
		}
		counts[label]++
	}
	return counts
}

// NumberOfValidProviderGroups returns the count of distinct cross-validation group labels across ALL
// currently valid providers (ignoring addon/extension filtering). Used by the startup capacity check as
// the upper bound on how many distinct groups a request could ever draw from.
func (csm *ConsumerSessionManager) NumberOfValidProviderGroups() int {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	return csm.countDistinctGroups(csm.validAddresses)
}

// ProviderGroupAssignments returns a snapshot of how the currently valid providers map onto
// cross-validation group labels (label -> sorted provider addresses), folding an empty label into
// common.DefaultProviderGroup. It is meant for one-shot startup/diagnostic logging so operators can see the diversity
// their config actually yields; it is not on any hot path.
func (csm *ConsumerSessionManager) ProviderGroupAssignments() map[string][]string {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	assignments := make(map[string][]string)
	for _, addr := range csm.validAddresses {
		label := common.DefaultProviderGroup
		if cswp, ok := csm.pairing[addr]; ok && cswp.GroupLabel != "" {
			label = cswp.GroupLabel
		}
		assignments[label] = append(assignments[label], addr)
	}
	for label := range assignments {
		sort.Strings(assignments[label])
	}
	return assignments
}

// ProviderAndGroupCountsForRequest returns the number of providers and the number of distinct group
// labels among the providers that actually support the request's addon + extensions — i.e. the concrete
// candidate set, not all valid providers. Used by the per-request cross-validation capacity check so a
// min-groups / max-participants policy is validated against what the request can really reach.
func (csm *ConsumerSessionManager) ProviderAndGroupCountsForRequest(addon string, extensions []string, ctx context.Context) (providers, groups int) {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	addresses := csm.CalculateAddonValidAddresses(addon, extensions, ctx)
	return len(addresses), csm.countDistinctGroups(addresses)
}

// GroupCountsForRequest returns the per-group provider count among the candidate set that supports the
// request's addon + extensions (group label -> count, empty label folded into common.DefaultProviderGroup). Used by the
// per-group-quorum capacity check, which needs to know not just how many distinct groups exist but how many
// of them have enough providers to each reach the agreement threshold.
func (csm *ConsumerSessionManager) GroupCountsForRequest(addon string, extensions []string, ctx context.Context) map[string]int {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	return csm.countByGroup(csm.CalculateAddonValidAddresses(addon, extensions, ctx))
}

// IsStaticProvider returns true when the given provider address belongs to a
// static provider in the current pairing (including backup providers and
// purged providers that may still be serving active subscriptions across an
// epoch boundary).
//
// This is used by higher-level flows (e.g. WS subscriptions) to decide whether
// to skip reply signature verification, matching the behavior of regular RPC
// calls for static providers.
func (csm *ConsumerSessionManager) IsStaticProvider(providerAddr string) bool {
	if csm == nil || providerAddr == "" {
		return false
	}

	csm.lock.RLock()
	defer csm.lock.RUnlock()

	if cswp, ok := csm.pairing[providerAddr]; ok && cswp != nil {
		cswp.Lock.RLock()
		defer cswp.Lock.RUnlock()
		return cswp.StaticProvider
	}

	if cswp, ok := csm.backupProviders[providerAddr]; ok && cswp != nil {
		cswp.Lock.RLock()
		defer cswp.Lock.RUnlock()
		return cswp.StaticProvider
	}

	// pairingPurge holds providers from the previous epoch that may still be
	// actively serving subscriptions. Check it so static providers are not
	// misclassified after an epoch handover.
	if cswp, ok := csm.pairingPurge[providerAddr]; ok && cswp != nil {
		cswp.Lock.RLock()
		defer cswp.Lock.RUnlock()
		return cswp.StaticProvider
	}

	return false
}

// this is being read in multiple locations and but never changes so no need to lock.
func (csm *ConsumerSessionManager) RPCEndpoint() RPCEndpoint {
	return *csm.rpcEndpoint
}

func (csm *ConsumerSessionManager) UpdateAllProviders(epoch uint64, pairingList map[uint64]*ConsumerSessionsWithProvider, backupProviderList map[uint64]*ConsumerSessionsWithProvider) error {
	utils.LavaFormatDebug("UpdateAllProviders", utils.Attribute{Key: "epoch", Value: epoch}, utils.Attribute{Key: "pairingListLen", Value: len(pairingList)})
	pairingListLength := len(pairingList)
	// TODO: we can block updating until some of the probing is done, this can prevent failed attempts on epoch change when we have no information on the providers,
	// and all of them are new (less effective on big pairing lists or a process that runs for a few epochs)

	defer func() {
		// run this after done updating pairing
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) // sleep up to 500ms in order to scatter different chains probe triggers
		go func() {
			ctx := context.Background()
			// Check re-blocked providers from previous epoch and unblock if healthy
			csm.checkAndUnblockHealthyReBlockedProviders(ctx, epoch)
		}()
	}()
	previousEpoch := csm.atomicReadCurrentEpoch()
	// clean qos manager purged epochs.
	csm.qosManager.CleanPurgedEpochs(previousEpoch)

	csm.lock.Lock()         // start by locking the class lock.
	defer csm.lock.Unlock() // we defer here so in case we return an error it will unlock automatically.

	if epoch < previousEpoch { // sentry shouldn't update an old epoch
		return utils.LavaFormatError("trying to update provider list for older epoch", nil, utils.Attribute{Key: "epoch", Value: epoch}, utils.Attribute{Key: "currentEpoch", Value: csm.atomicReadCurrentEpoch()})
	}

	// For same-epoch updates, we still need to proceed with the update
	// because each ConsumerSessionManager has its own state (validAddresses, reportedProviders, etc.)
	// that needs to be reset. We just skip the epoch write to avoid redundant atomic operations.
	skipEpochWrite := (epoch == previousEpoch)
	if skipEpochWrite {
		utils.LavaFormatDebug("UpdateAllProviders called with same epoch, updating state anyway",
			utils.Attribute{Key: "epoch", Value: epoch},
			utils.Attribute{Key: "spec", Value: csm.rpcEndpoint.Key()})
	}

	// Update Epoch (only if it's different)
	if !skipEpochWrite {
		csm.atomicWriteCurrentEpoch(epoch)
	}

	// Reset States - MUST run even for same-epoch updates because each CSM has its own state
	// csm.validAddresses length is reset in setValidAddressesToDefaultValue
	csm.pairingAddresses = make(map[uint64]string, pairingListLength)

	// Save blocking history from previous epoch before clearing
	// This prevents known-bad providers from getting a clean slate at epoch transition
	csm.previousEpochBlockedProviders = make(map[string]BlockRecord)
	for _, blockedAddr := range csm.currentlyBlockedProviderAddresses {
		csm.previousEpochBlockedProviders[blockedAddr] = csm.blockRecordOrUnknownLocked(blockedAddr)
		utils.LavaFormatDebug("UpdateAllProviders: Preserving blocked provider from previous epoch",
			utils.Attribute{Key: "provider", Value: blockedAddr},
			utils.Attribute{Key: "fromEpoch", Value: previousEpoch},
			utils.Attribute{Key: "toEpoch", Value: epoch},
		)
	}
	for blockedAddr := range csm.blockedBackupProviders {
		csm.previousEpochBlockedProviders[blockedAddr] = csm.blockRecordOrUnknownLocked(blockedAddr)
		utils.LavaFormatDebug("UpdateAllProviders: Preserving blocked backup provider from previous epoch",
			utils.Attribute{Key: "provider", Value: blockedAddr},
			utils.Attribute{Key: "fromEpoch", Value: previousEpoch},
			utils.Attribute{Key: "toEpoch", Value: epoch},
		)
	}
	// Drop the records for the backup blocks being torn down here. Without this a backup that was
	// blocked and is then absent from the new backup list leaks its record permanently: the re-block
	// loop below only re-seeds records for backups still present, and no other path deletes them.
	// The per-reason gauge is level-triggered, so a leaked record is a phantom block that never
	// self-corrects — the same shape as the MAG-3106 bug this package just fixed.
	//
	// Cleared first so releaseBlockRecordLocked sees the store as it now is; a provider also blocked
	// as regular keeps its record, re-pointed at that block.
	previouslyBlockedBackups := csm.blockedBackupProviders
	csm.blockedBackupProviders = make(map[string]struct{})
	for blockedAddr := range previouslyBlockedBackups {
		csm.releaseBlockRecordLocked(blockedAddr, true)
	}

	csm.secondChanceGivenToAddresses = make(map[string]struct{})

	csm.reportedProviders.Reset()
	csm.pairingAddressesLength = uint64(pairingListLength)
	csm.numberOfResets = 0

	providerAddressToEndpoint := map[string]string{}

	csm.RemoveAddonAddresses("", nil)
	// Reset the pairingPurge.
	// This happens only after an entire epoch. so its impossible to have session connected to the old purged list
	// Save reference to old pairingPurge BEFORE updating, so we can close connections outside the lock
	oldPairingPurge := csm.pairingPurge
	csm.pairingPurge = csm.pairing
	// Membership of the outgoing pairing, captured before the map is replaced, so the line at the
	// end of this function can report what actually changed rather than only the new size. A
	// provider silently dropping out between epochs is the failure this makes visible.
	previousPairingAddresses := make([]string, 0, len(csm.pairing))
	for address := range csm.pairing {
		previousPairingAddresses = append(previousPairingAddresses, address)
	}
	csm.pairing = make(map[string]*ConsumerSessionsWithProvider, pairingListLength)
	for idx, provider := range pairingList {
		csm.pairingAddresses[idx] = provider.PublicLavaAddress
		csm.pairing[provider.PublicLavaAddress] = provider
		if len(provider.Endpoints) > 0 {
			providerAddressToEndpoint[provider.PublicLavaAddress] = provider.Endpoints[0].NetworkAddress
		}
	}
	csm.setValidAddressesToDefaultValue("", nil, context.Background(), ReleaseEpochRebuild) // the starting point is that valid addresses are equal to pairing addresses.

	// Re-block providers that were blocked in previous epoch and still exist in new pairing
	// This prevents users from hitting known-bad providers at epoch transition
	for blockedAddr, carried := range csm.previousEpochBlockedProviders {
		if _, exists := csm.pairing[blockedAddr]; exists {
			record := carried.withCarryOver()
			// This loop is the REGULAR pool. A record carried from a backup block still says
			// Backup: true, and nothing else resets it, so /debug/provider-routing would report
			// Scope="backup" for a provider whose only standing block is the primary one.
			record.Backup = false
			utils.LavaFormatDebug("UpdateAllProviders: Re-blocking provider from previous epoch",
				utils.Attribute{Key: "provider", Value: blockedAddr},
				utils.Attribute{Key: "epoch", Value: epoch},
				utils.Attribute{Key: "block_reason", Value: record.Reason},
			)
			// Remove from valid addresses to keep it blocked, carrying the ORIGINAL reason and
			// Since forward: the provider has been out since it was first blocked, and replacing
			// that with "blocked at the epoch tick" would erase the only useful answer.
			csm.removeAddressFromValidAddresses(blockedAddr, record)
		}
	}

	// reset session related metrics
	go csm.consumerMetricsManager.ResetSessionRelatedMetrics()
	// UpdateWeights is required for stake-weighted selection; run it synchronously so early relays/probes
	// (which may start immediately after UpdateAllProviders) see correct stake values.
	// Both static and backup providers are registered via CalcWeightsByStake so the optimizer can rank
	// among providers of each tier by QoS. Statics and backups are never in the same candidate list
	// (backups only appear in the backup fallback path), so their weights only affect within-tier ranking.
	weights := CalcWeightsByStake(pairingList)
	backupWeights := CalcWeightsByStake(backupProviderList)
	for addr, w := range backupWeights {
		weights[addr] = w
	}
	csm.providerOptimizer.UpdateWeights(weights, epoch)
	go csm.consumerMetricsManager.ResetBlockedProvidersMetrics(csm.rpcEndpoint.ChainID, csm.rpcEndpoint.ApiInterface, providerAddressToEndpoint)

	// Membership of the outgoing backup pool, captured before the map is replaced, for the same
	// reason the pairing membership is: since MAG-2525 a chain can serve on backups alone, so a
	// backup silently leaving the pool is the same invisible failure with the same blast radius.
	previousBackupAddresses := make([]string, 0, len(csm.backupProviders))
	for address := range csm.backupProviders {
		previousBackupAddresses = append(previousBackupAddresses, address)
	}

	// Store backup providers separately from main pairing list for emergency fallback scenarios
	csm.backupProviders = make(map[string]*ConsumerSessionsWithProvider, len(backupProviderList))
	for _, provider := range backupProviderList {
		csm.backupProviders[provider.PublicLavaAddress] = provider
	}

	// Re-block backup providers that were blocked in previous epoch and still exist in new backup list
	for blockedAddr, carried := range csm.previousEpochBlockedProviders {
		if _, exists := csm.backupProviders[blockedAddr]; exists {
			csm.blockedBackupProviders[blockedAddr] = struct{}{}
			record := carried.withCarryOver()
			record.Backup = true
			csm.blockedProviderRecords[blockedAddr] = record
			utils.LavaFormatDebug("UpdateAllProviders: Re-blocking backup provider from previous epoch",
				utils.Attribute{Key: "provider", Value: blockedAddr},
				utils.Attribute{Key: "epoch", Value: epoch},
				utils.Attribute{Key: "block_reason", Value: record.Reason},
			)
		}
	}

	// Clean up expired sticky sessions
	csm.stickySessions.DeleteOldSessions(previousEpoch)

	csm.lastPairingUpdate = time.Now()
	csm.pairingGeneration++

	// The pairing inventory is the answer to "why was the primary not even a candidate?". A pool
	// that never contained a provider and one that dropped it look identical from the selection
	// path — every line there can only report what IS in the pool, never what left it.
	//
	// Emitted on every provider-set push, not strictly once per epoch: readmitRecoveredProviders
	// pushes a rebuilt set from its retry backoff timer too, which is a feature — a provider coming
	// back shows up as `added` the moment it is readmitted. Either way it is nowhere near the relay
	// path, so INFO costs nothing per request.
	currentPairingAddresses := make([]string, 0, len(csm.pairing))
	for address := range csm.pairing {
		currentPairingAddresses = append(currentPairingAddresses, address)
	}
	sort.Strings(currentPairingAddresses)
	currentBackupAddresses := make([]string, 0, len(csm.backupProviders))
	for address := range csm.backupProviders {
		currentBackupAddresses = append(currentBackupAddresses, address)
	}
	sort.Strings(currentBackupAddresses)
	added, removed, carriedOver := diffAddressSets(previousPairingAddresses, currentPairingAddresses)
	backupAdded, backupRemoved, _ := diffAddressSets(previousBackupAddresses, currentBackupAddresses)
	utils.LavaFormatInfo("pairing updated",
		utils.LogAttr("epoch", epoch),
		utils.LogAttr("spec", csm.rpcEndpoint.Key()),
		utils.LogAttr("size", len(currentPairingAddresses)),
		utils.LogAttr("providers", currentPairingAddresses),
		utils.LogAttr("added", added),
		utils.LogAttr("removed", removed),
		utils.LogAttr("carried_over", carriedOver),
		utils.LogAttr("backup_pool_size", len(currentBackupAddresses)),
		utils.LogAttr("backup_providers", currentBackupAddresses),
		utils.LogAttr("backup_added", backupAdded),
		utils.LogAttr("backup_removed", backupRemoved),
	)

	// Close old connections OUTSIDE the lock to prevent blocking other operations
	// This is safe because after an entire epoch, it's impossible to have sessions connected to the old purged list
	go csm.closePurgedUnusedPairingsConnections(oldPairingPurge)

	return nil
}

func (csm *ConsumerSessionManager) Initialized() bool {
	csm.lock.RLock()         // start by locking the class lock.
	defer csm.lock.RUnlock() // we defer here so in case we return an error it will unlock automatically.
	return len(csm.pairingAddresses) != 0
}

// EndpointWithDirectConnection holds an endpoint and its direct RPC connection.
// Used by smart router for pre-warming ChainTrackers.
type EndpointWithDirectConnection struct {
	Endpoint         *Endpoint
	DirectConnection DirectRPCConnection
	ProviderAddress  string
}

// GetAllDirectRPCEndpoints returns all endpoints with direct RPC connections from both
// the primary pairing and the backup provider list. This is used by the smart router for
// initializing ChainTrackers on startup — excluding backups left their endpoints without a
// tracker until a relay happened to hit them (rare, because backups are fallback-only),
// which meant dedicated-URL backups like base.lava.build had no block data on the dashboard.
// Returns empty slice if no direct RPC endpoints are configured.
//
// NOT a silent filter on reachability (MAG-2622 chased this as a suspect and it was not the
// cause): IsDirectRPC() IS len(DirectConnections) > 0, and an endpoint only ever reaches the
// pairing with a connection attached — convertProvidersToSessions drops a URL whose
// NewDirectRPCConnection failed, and drops the whole provider when every URL failed. So the
// set returned here is "every configured direct-RPC endpoint currently paired", reachable or
// not; a DOWN endpoint is present and must be given a tracker (see initializeChainTrackers).
func (csm *ConsumerSessionManager) GetAllDirectRPCEndpoints() []*EndpointWithDirectConnection {
	csm.lock.RLock()
	defer csm.lock.RUnlock()

	var results []*EndpointWithDirectConnection

	collect := func(providers map[string]*ConsumerSessionsWithProvider) {
		for providerAddr, cswp := range providers {
			for _, endpoint := range cswp.Endpoints {
				// The length check is redundant today (IsDirectRPC IS len(DirectConnections) > 0)
				// but it is what makes the [0] index below safe, so it stays: a future change to
				// IsDirectRPC must not silently turn this into a panic.
				if endpoint.IsDirectRPC() && len(endpoint.DirectConnections) > 0 {
					results = append(results, &EndpointWithDirectConnection{
						Endpoint:         endpoint,
						DirectConnection: endpoint.DirectConnections[0],
						ProviderAddress:  providerAddr,
					})
				}
			}
		}
	}

	collect(csm.pairing)
	collect(csm.backupProviders)

	return results
}

func (csm *ConsumerSessionManager) RemoveAddonAddresses(addon string, extensions []string) {
	if addon == "" && len(extensions) == 0 {
		// purge all
		csm.addonAddresses = make(map[string][]string)
	} else {
		routerKey := NewRouterKey(append(extensions, addon))
		if csm.addonAddresses == nil {
			csm.addonAddresses = make(map[string][]string)
		}
		csm.addonAddresses[routerKey.String()] = []string{}
	}
}

// csm is Rlocked
func (csm *ConsumerSessionManager) CalculateAddonValidAddresses(addon string, extensions []string, ctx context.Context) (supportingProviderAddresses []string) {
	utils.LavaFormatInfo("🔎 CALCULATING VALID ADDRESSES", utils.LogAttr("addon", addon), utils.LogAttr("extensions", extensions), utils.LogAttr("totalValidAddresses", len(csm.validAddresses)), utils.LogAttr("currentlyBlockedCount", len(csm.currentlyBlockedProviderAddresses)), utils.LogAttr("GUID", ctx))
	for _, providerAdress := range csm.validAddresses {
		providerEntry := csm.pairing[providerAdress]
		supportsAddon := providerEntry.IsSupportingAddon(addon)
		supportsExtensions := providerEntry.IsSupportingExtensions(extensions, ctx)
		utils.LavaFormatTrace("[Archive Debug] Provider extension check",
			utils.LogAttr("providerAddress", providerAdress),
			utils.LogAttr("supportsAddon", supportsAddon),
			utils.LogAttr("supportsExtensions", supportsExtensions),
			utils.LogAttr("GUID", ctx))
		if supportsAddon && supportsExtensions {
			supportingProviderAddresses = append(supportingProviderAddresses, providerAdress)
			utils.LavaFormatTrace("[Archive Debug] Provider added to supporting list",
				utils.LogAttr("providerAddress", providerAdress),
				utils.LogAttr("GUID", ctx))
		} else {
			utils.LavaFormatTrace("[Archive Debug] Provider filtered out",
				utils.LogAttr("providerAddress", providerAdress),
				utils.LogAttr("reason", "does not support addon or extensions"),
				utils.LogAttr("GUID", ctx))
		}
	}
	utils.LavaFormatInfo("CALCULATION RESULT", utils.LogAttr("addon", addon), utils.LogAttr("extensions", extensions), utils.LogAttr("supportingProviderCount", len(supportingProviderAddresses)), utils.LogAttr("supportingProviders", supportingProviderAddresses), utils.LogAttr("GUID", ctx))
	return supportingProviderAddresses
}

// assuming csm is Rlocked
func (csm *ConsumerSessionManager) getValidAddresses(addon string, extensions []string, ctx context.Context) (addresses []string) {
	utils.LavaFormatTrace("[Archive Debug] getValidAddresses called",
		utils.LogAttr("addon", addon),
		utils.LogAttr("extensions", extensions),
		utils.LogAttr("GUID", ctx))
	routerKey := NewRouterKey(append(extensions, addon))
	routerKeyString := routerKey.String()
	if csm.addonAddresses == nil || csm.addonAddresses[routerKeyString] == nil {
		utils.LavaFormatTrace("[Archive Debug] Calling CalculateAddonValidAddresses",
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("GUID", ctx))
		return csm.CalculateAddonValidAddresses(addon, extensions, ctx)
	}
	utils.LavaFormatTrace("[Archive Debug] Using cached addonAddresses",
		utils.LogAttr("routerKeyString", routerKeyString),
		utils.LogAttr("cachedAddresses", csm.addonAddresses[routerKeyString]),
		utils.LogAttr("GUID", ctx))
	return csm.addonAddresses[routerKeyString]
}

// After 2 epochs we need to close all open connections.
// otherwise golang garbage collector is not closing network connections and they
// will remain open forever.
// This function is now called asynchronously to avoid blocking UpdateAllProviders while closing connections.
func (csm *ConsumerSessionManager) closePurgedUnusedPairingsConnections(pairingPurge map[string]*ConsumerSessionsWithProvider) {
	if pairingPurge == nil {
		return
	}

	for providerAddr, purgedPairing := range pairingPurge {
		callbackPurge := func() {
			for _, endpoint := range purgedPairing.Endpoints {
				for _, endpointConnection := range endpoint.Connections {
					if endpointConnection.connection != nil {
						utils.LavaFormatTrace("purging connection",
							utils.LogAttr("providerAddr", providerAddr),
							utils.LogAttr("endpoint", endpoint.NetworkAddress),
						)
						endpointConnection.connection.Close()
					}
				}
			}
		}
		// on cases where there is still an active subscription over the epoch handover, we purge the connection when subscription ends.
		if csm.activeSubscriptionProvidersStorage.IsProviderCurrentlyUsed(providerAddr) {
			utils.LavaFormatTrace("skipping purge for provider, as its currently used in a subscription",
				utils.LogAttr("providerAddr", providerAddr),
			)
			csm.activeSubscriptionProvidersStorage.addToPurgeWhenDone(providerAddr, callbackPurge)
			continue
		}
		callbackPurge()
	}
}

// this code needs to be thread safe
func (csm *ConsumerSessionManager) probeProvider(ctx context.Context, consumerSessionsWithProvider *ConsumerSessionsWithProvider, epoch uint64, tryReconnectToDisabledEndpoints bool) (latency time.Duration, providerAddress string, err error) {
	// Static providers (direct RPC in smart router mode) use HTTP/WebSocket connections,
	// not gRPC. Skip fetchEndpointConnectionFromConsumerSessionWithProvider entirely —
	// it returns endpoints with nil chosenEndpointConnection for direct RPC, which causes
	// the gRPC probe loop below to fail with "returned nil client in endpoint", resulting
	// in success=false, latency=0s for every probe.
	if consumerSessionsWithProvider.StaticProvider {
		return csm.probeDirectRPCEndpoints(ctx, consumerSessionsWithProvider, consumerSessionsWithProvider.PublicLavaAddress)
	}

	// A probe measures reachability of the provider, not of one api collection —
	// no internal path to match on, so every endpoint stays eligible.
	connected, endpoints, providerAddress, err := consumerSessionsWithProvider.fetchEndpointConnectionFromConsumerSessionWithProvider(ctx, tryReconnectToDisabledEndpoints, true, "", nil, nil)
	if err != nil || !connected {
		if errors.Is(err, AllProviderEndpointsDisabledError) {
			csm.blockProvider(ctx, providerAddress, BlockReasonAllEndpointsDisabled, true, epoch, MaxConsecutiveConnectionAttempts, 0, false, csm.GenerateReconnectCallback(consumerSessionsWithProvider)) // reporting and blocking provider this epoch
		}
		return 0, providerAddress, err
	}

	var endpointInfos []EndpointInfo
	lastError := fmt.Errorf("endpoints list is empty") // this error will happen if we had 0 endpoints
	for _, endpointAndConnection := range endpoints {
		err := func() error {
			connectCtx, cancel := context.WithTimeout(ctx, common.AverageWorldLatency)
			defer cancel()
			guid, found := utils.GetUniqueIdentifier(connectCtx)
			if !found {
				return utils.LavaFormatError("probeProvider failed fetching unique identifier from context when it's set", nil)
			}
			if endpointAndConnection == nil ||
				endpointAndConnection.chosenEndpointConnection == nil ||
				endpointAndConnection.chosenEndpointConnection.Client == nil {
				// returned nil client in endpoint - this shouldn't happen for provider-relay endpoints
				// For direct RPC, we handle this case above
				consumerSessionsWithProvider.Lock.Lock()
				defer consumerSessionsWithProvider.Lock.Unlock()
				return utils.LavaFormatError("returned nil client in endpoint", nil, utils.Attribute{Key: "consumerSessionWithProvider", Value: consumerSessionsWithProvider})
			}
			client := endpointAndConnection.chosenEndpointConnection.Client
			probeReq := &pairingtypes.ProbeRequest{
				Guid:         guid,
				SpecId:       csm.rpcEndpoint.ChainID,
				ApiInterface: csm.rpcEndpoint.ApiInterface,
			}
			var trailer metadata.MD
			relaySentTime := time.Now()
			metadataAdd := metadata.New(map[string]string{common.LAVA_LB_UNIQUE_ID_HEADER: endpointAndConnection.chosenEndpointConnection.GetLbUniqueId()})
			connectCtx = metadata.NewOutgoingContext(connectCtx, metadataAdd)

			probeResp, err := client.Probe(connectCtx, probeReq, grpc.Trailer(&trailer))

			relayLatency := time.Since(relaySentTime)
			if err != nil {
				return utils.LavaFormatError("probe call error", err, utils.Attribute{Key: "provider", Value: providerAddress})
			}
			providerGuid := probeResp.GetGuid()
			if providerGuid != guid {
				return utils.LavaFormatWarning("mismatch probe response", nil, utils.Attribute{Key: "provider", Value: providerAddress}, utils.Attribute{Key: "provider Guid", Value: providerGuid}, utils.Attribute{Key: "sent guid", Value: guid})
			}
			if probeResp.LatestBlock == 0 {
				return utils.LavaFormatWarning("provider returned 0 latest block", nil, utils.Attribute{Key: "provider", Value: providerAddress}, utils.Attribute{Key: "sent guid", Value: guid})
			}

			endpointInfos = append(endpointInfos, EndpointInfo{
				Latency:  relayLatency,
				Endpoint: endpointAndConnection.endpoint,
			})
			// public lava address is a value that is not changing, so it's thread safe
			if DebugProbesEnabled() {
				utils.LavaFormatDebug("Probed provider successfully", utils.Attribute{Key: "latency", Value: relayLatency}, utils.Attribute{Key: "provider", Value: consumerSessionsWithProvider.PublicLavaAddress})
			}
			return nil
		}()
		if err != nil {
			lastError = err
		}
	}

	if len(endpointInfos) == 0 {
		// no endpoints.
		return 0, providerAddress, lastError
	}
	sort.Sort(EndpointInfoList(endpointInfos))
	consumerSessionsWithProvider.sortEndpointsByLatency(endpointInfos)
	return endpointInfos[0].Latency, providerAddress, nil
}

// probeDirectRPCEndpoints handles health checking for direct RPC endpoints (smart router mode).
// Unlike provider-relay endpoints which use gRPC Probe() calls, direct RPC endpoints
// are probed by checking the health status of their DirectRPCConnections.
// This avoids the "nil client" errors that occur when trying to use provider gRPC clients
// for endpoints that don't have them.
func (csm *ConsumerSessionManager) probeDirectRPCEndpoints(
	ctx context.Context,
	consumerSessionsWithProvider *ConsumerSessionsWithProvider,
	providerAddress string,
) (latency time.Duration, address string, err error) {
	consumerSessionsWithProvider.Lock.RLock()
	defer consumerSessionsWithProvider.Lock.RUnlock()

	var usableEndpoints int
	var totalEndpoints int

	for _, endpoint := range consumerSessionsWithProvider.Endpoints {
		if !endpoint.IsDirectRPC() {
			continue
		}

		totalEndpoints++

		// The relay path no longer gates on a per-socket health bit (a dead socket
		// fails the real relay, which feeds QoS via OnSessionFailure and trips the
		// endpoint.Enabled consecutive-failure backoff — both self-healing). So the
		// probe no longer reads connection health either. We still honour
		// endpoint.Enabled: a backup that hit MaxConsecutiveConnectionAttempts
		// consecutive refusals stays backed off and must not be optimistically
		// unblocked at epoch transition. Active
		// per-endpoint probing/recovery is handled by the chain tracker (Step 3).
		// Read Enabled through the synchronized accessor: the real prober (Topic D) writes this bit
		// under e.mu (RecordProbeVerdict), and an unsynchronized read here raced with it (F3). This
		// path no longer emits liveness metrics or QoS — it is only a race-free routability gate for
		// the epoch-transition unblock/reconnect callers; the prober owns direct-RPC liveness.
		if !endpoint.IsEnabled() {
			utils.LavaFormatDebug("Direct RPC endpoint is disabled, skipping probe",
				utils.LogAttr("provider", providerAddress),
				utils.LogAttr("endpoint", endpoint.NetworkAddress),
			)
			continue
		}

		for _, conn := range endpoint.DirectConnections {
			if conn == nil {
				continue
			}

			usableEndpoints++

			if DebugProbesEnabled() {
				utils.LavaFormatDebug("Direct RPC endpoint probe (no health gate)",
					utils.LogAttr("provider", providerAddress),
					utils.LogAttr("url", conn.GetURL()),
					utils.LogAttr("protocol", conn.GetProtocol()),
				)
			}
		}
	}

	if totalEndpoints == 0 {
		return 0, providerAddress, fmt.Errorf("no direct RPC endpoints found for provider %s", providerAddress)
	}

	if usableEndpoints == 0 {
		return 0, providerAddress, fmt.Errorf("no enabled direct RPC endpoints for provider %s", providerAddress)
	}

	// No per-connection latency is measured anymore (the old "latency" was just the
	// time to read an atomic bool). Report a nominal minimal latency for parity with
	// the provider-relay probe API.
	minLatency := time.Millisecond

	utils.LavaFormatTrace("Direct RPC endpoints probe completed",
		utils.LogAttr("provider", providerAddress),
		utils.LogAttr("usableEndpoints", usableEndpoints),
		utils.LogAttr("totalEndpoints", totalEndpoints),
		utils.LogAttr("latency", minLatency),
	)

	return minLatency, providerAddress, nil
}

// csm needs to be locked here
// setValidAddressesToDefaultValue refills validAddresses from the pairing and drops the whole
// regular blocked list at once.
//
// route names who is doing it — the pool-empty last resort, an operator reset, or an epoch rebuild.
// This is the bulk counterpart to logProviderReleased: one line naming every address released
// rather than one line each, because all three callers can release the entire pool in a single
// call (MAG-2599).
//
// Backup blocks are NOT touched here, so their reason records deliberately survive.
//
// One caveat on the line it logs: with a non-empty addon the branch below APPENDS only the providers
// that support that addon rather than replacing the list, so a released provider that does not
// support it is out of the blocked list but not back in validAddresses. The line names it as
// released, which is true of the blocked list and overstates it as routing. Pre-existing limbo, not
// introduced here.
func (csm *ConsumerSessionManager) setValidAddressesToDefaultValue(addon string, extensions []string, ctx context.Context, route ReleaseRoute) {
	released := csm.currentlyBlockedProviderAddresses
	csm.currentlyBlockedProviderAddresses = make([]string, 0) // reset currently blocked provider addresses
	// Emptied first, then the records: releaseBlockRecordLocked answers "is this provider still
	// blocked anywhere", and it has to see the store as it now is. A provider whose BACKUP block
	// still stands keeps its record — deleting it here would leave a genuinely blocked provider with
	// no reason, absent from the gauge, and eventually released with no log line.
	backInRotation := make([]string, 0, len(released))
	for _, address := range released {
		if _, released := csm.releaseBlockRecordLocked(address, false); released {
			backInRotation = append(backInRotation, address)
		}
	}
	// ReleaseEpochRebuild is not a decision about any of these providers: UpdateAllProviders drains
	// the list and then immediately re-blocks whatever carried over. Announcing a release there
	// claims a recovery that did not happen, on the field that exists to make recovery diagnosable.
	// The epoch's genuine releases are logged per provider by checkAndUnblockHealthyReBlockedProviders.
	if len(backInRotation) > 0 && route != ReleaseEpochRebuild {
		utils.LavaFormatInfo("blocked provider list released",
			utils.LogAttr("released_by", route),
			utils.LogAttr("released", backInRotation),
			utils.LogAttr("count", len(backInRotation)),
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("GUID", ctx),
		)
	}
	if addon == "" && len(extensions) == 0 {
		csm.validAddresses = make([]string, len(csm.pairingAddresses))
		index := 0
		for _, provider := range csm.pairingAddresses {
			csm.validAddresses[index] = provider
			index++
		}
	} else {
		// check if one of the pairing addresses supports the addon
	addingToValidAddresses:
		for _, provider := range csm.pairingAddresses {
			supportsAddon := csm.pairing[provider].IsSupportingAddon(addon)
			supportsExtensions := csm.pairing[provider].IsSupportingExtensions(extensions, ctx)

			utils.LavaFormatTrace("[Archive Debug] Provider filtering check",
				utils.LogAttr("providerAddress", provider),
				utils.LogAttr("addon", addon),
				utils.LogAttr("extensions", extensions),
				utils.LogAttr("supportsAddon", supportsAddon),
				utils.LogAttr("supportsExtensions", supportsExtensions),
				utils.LogAttr("GUID", ctx))

			if supportsAddon && supportsExtensions {
				for _, validAddress := range csm.validAddresses {
					if validAddress == provider {
						// it exists, no need to add it again
						continue addingToValidAddresses
					}
				}
				// get here only it found a supporting provider that is not valid
				utils.LavaFormatTrace("[Archive Debug] Adding provider to valid addresses",
					utils.LogAttr("providerAddress", provider),
					utils.LogAttr("GUID", ctx))
				csm.validAddresses = append(csm.validAddresses, provider)
			} else {
				utils.LavaFormatTrace("[Archive Debug] Provider filtered out",
					utils.LogAttr("providerAddress", provider),
					utils.LogAttr("reason", "does not support addon or extensions"),
					utils.LogAttr("GUID", ctx))
			}
		}
		csm.RemoveAddonAddresses(addon, extensions) // refresh the list
		csm.addonAddresses[NewRouterKey(append(extensions, addon)).String()] = csm.CalculateAddonValidAddresses(addon, extensions, ctx)
	}
}

// poolInventory is a snapshot of the state that explains an empty selectable pool, taken once
// under a single RLock so every line reporting it agrees. Reading the fields one at a time across
// separate locks would let a concurrent UpdateAllProviders land between them, and the resulting
// line would describe a state that never existed.
type poolInventory struct {
	pairingSize       int
	validCount        int
	blockedCount      int
	backupPoolSize    int
	lastPairingUpdate time.Time

	// capable counts pairing members that could serve THIS request — they pass the same
	// addon/extension predicates the selectable set is built from — and capableBlocked how many of
	// those are currently blocked.
	//
	// These exist because validCount cannot answer the question. validCount is the DEFAULT
	// collection's size, while the emptiness that brings us here is addon-scoped, so comparing them
	// is apples to oranges: a pairing of five with one archive provider that is blocked and four
	// that never served archive has validCount=4 and reports "the addon filtered everything out",
	// when the truth is that the single archive-capable provider is blocked. Scoping the counts to
	// the request is what makes the reason survive contact with an addon.
	capable        int
	capableBlocked int

	// generation is the pairing generation this snapshot was taken at, carried here rather than
	// read separately so the throttle key and the counts it guards describe the same instant.
	generation uint64
}

// snapshotPoolInventory reads the pool state for the diagnostic lines, scoped to the request that
// found the pool empty. Callers MUST NOT hold csm.lock — this takes the read lock itself, and takes
// it once so every field describes the same instant.
func (csm *ConsumerSessionManager) snapshotPoolInventory(addon string, extensions []string, ctx context.Context) poolInventory {
	csm.lock.RLock()
	defer csm.lock.RUnlock()

	blocked := make(map[string]struct{}, len(csm.currentlyBlockedProviderAddresses))
	for _, address := range csm.currentlyBlockedProviderAddresses {
		blocked[address] = struct{}{}
	}

	inventory := poolInventory{
		pairingSize:       len(csm.pairingAddresses),
		validCount:        len(csm.validAddresses),
		blockedCount:      len(csm.currentlyBlockedProviderAddresses),
		backupPoolSize:    len(csm.backupProviders),
		lastPairingUpdate: csm.lastPairingUpdate,
		generation:        csm.pairingGeneration,
	}

	// Same predicates CalculateAddonValidAddresses filters on, so "capable" means exactly "would
	// have been selectable had it not been blocked".
	for _, address := range csm.pairingAddresses {
		provider, ok := csm.pairing[address]
		if !ok || provider == nil {
			continue
		}
		if !provider.IsSupportingAddon(addon) || !provider.IsSupportingExtensions(extensions, ctx) {
			continue
		}
		inventory.capable++
		if _, isBlocked := blocked[address]; isBlocked {
			inventory.capableBlocked++
		}
	}
	return inventory
}

// reason names WHY the selectable pool is empty, in the same kebab-case vocabulary as
// BlockReason and EndpointDisableReason. The distinction is the whole point of the line: the causes
// below are investigated in different places, and the empty pool they produce looks identical.
//
//	pairing-empty        nothing was ever registered for this chain — the provider failed startup
//	                     verification, or the epoch rebuilt with an empty list. Not a routing problem.
//	addon-filtered       members are registered, and not one of them serves this addon/extension.
//	                     A spec or config problem; no amount of routing recovers it.
//	no-usable-endpoints  the request asks for the default collection, which every provider serves by
//	                     definition, yet nothing is capable — the only predicate left is having at
//	                     least one endpoint, so these providers registered with none.
//	all-blocked          every member that COULD serve this request is blocked. A routing problem,
//	                     and the block reasons (MAG-2599) say which.
//
// Every branch keys on capable/capableBlocked, which are scoped to this request. Keying on
// validCount instead — as this did before — compares the default collection's size against an
// addon-scoped emptiness, and the two mislabels that produces are exact mirror images: testing
// blockedCount first called a filtered pool "all-blocked", and testing validCount first called a
// blocked pool "addon-filtered". Neither is fixed by reordering; both are fixed by counting the
// providers that could actually have served.
func (inv poolInventory) reason(addon string, extensions []string) string {
	switch {
	case inv.pairingSize == 0:
		return "pairing-empty"
	case inv.capable == 0 && (addon != "" || len(extensions) > 0):
		return "addon-filtered"
	case inv.capable == 0:
		return "no-usable-endpoints"
	case inv.capableBlocked >= inv.capable:
		return "all-blocked"
	default:
		// Providers that can serve this request exist and are not all blocked, yet nothing was
		// selectable. Not a shape we model — name it rather than borrow one of the above.
		return "unspecified"
	}
}

// logPoolEmpty reports that nothing is selectable, and why. It is the one place the pool-empty
// vocabulary is rendered, so the call sites in releaseBlockedProvidersIfPoolEmpty cannot drift into
// describing the same state differently.
//
// The first report for a given pairing generation and reason goes to WARN; every repeat until one
// of those changes goes to DEBUG. An empty pool is a property of the pairing, and the pairing does
// not change between relays — a chain whose providers all failed startup verification stays empty
// indefinitely, and this runs on every relay (more than once per relay, per GetSessions' retry
// cascade). Reporting it per-relay at WARN would turn a permanent state into a sustained alert
// stream, which is the log-flood this whole change set exists to reduce, only at a level alerts
// fire on. Throttling on the generation means a genuinely new outage still warns immediately.
//
// Callers MUST NOT hold csm.lock: the inventory is snapshotted separately, and this takes
// poolEmptyReportMu.
func (csm *ConsumerSessionManager) logPoolEmpty(ctx context.Context, inventory poolInventory, addon string, extensions []string) {
	why := inventory.reason(addon, extensions)

	csm.poolEmptyReportMu.Lock()
	generation := inventory.generation
	firstForThisPairing := !csm.poolEmptyReportedEver ||
		csm.poolEmptyReportedGen != generation ||
		csm.poolEmptyReportedWhy != why
	if firstForThisPairing {
		csm.poolEmptyReportedEver = true
		csm.poolEmptyReportedGen = generation
		csm.poolEmptyReportedWhy = why
	}
	csm.poolEmptyReportMu.Unlock()

	report := utils.LavaFormatDebug
	if firstForThisPairing {
		// LavaFormatWarning takes an error argument the debug form does not, so the two cannot share
		// a function value the way the level-conditional sites elsewhere do.
		report = func(description string, attributes ...utils.Attribute) error {
			return utils.LavaFormatWarning(description, nil, attributes...)
		}
	}

	report("provider pool empty",
		utils.LogAttr("reason", why),
		utils.LogAttr("repeat", !firstForThisPairing),
		utils.LogAttr("pairing_generation", generation),
		utils.LogAttr("capable", inventory.capable),
		utils.LogAttr("capable_blocked", inventory.capableBlocked),
		utils.LogAttr("spec", csm.rpcEndpoint.Key()),
		utils.LogAttr("pairing_size", inventory.pairingSize),
		utils.LogAttr("valid", inventory.validCount),
		utils.LogAttr("blocked", inventory.blockedCount),
		utils.LogAttr("backup_pool", inventory.backupPoolSize),
		utils.LogAttr("last_pairing_update", formatPairingUpdateTime(inventory.lastPairingUpdate)),
		utils.LogAttr("addon", addon),
		utils.LogAttr("extensions", extensions),
		utils.LogAttr("GUID", ctx),
	)
}

// formatPairingUpdateTime renders the last pairing rebuild for the log. The zero value means
// UpdateAllProviders has not run yet, which is a genuinely different state from "rebuilt long ago"
// and must not render as a zero timestamp that reads like 1 January year 1.
func formatPairingUpdateTime(at time.Time) string {
	if at.IsZero() {
		return "never"
	}
	return at.UTC().Format(time.RFC3339)
}

// diffAddressSets reports how membership changed between two provider-address sets. Every result
// is sorted so the log line is stable across epochs — the inputs come from Go map iteration, whose
// order is deliberately randomised, and an unsorted line would read as churn on every epoch even
// when nothing moved.
func diffAddressSets(previous, current []string) (added, removed, carriedOver []string) {
	previousSet := make(map[string]struct{}, len(previous))
	for _, address := range previous {
		previousSet[address] = struct{}{}
	}
	currentSet := make(map[string]struct{}, len(current))
	for _, address := range current {
		currentSet[address] = struct{}{}
	}
	// Non-nil empties: a nil slice renders as "" in the log, which reads as a missing field rather
	// than as "nothing changed".
	added, removed, carriedOver = []string{}, []string{}, []string{}
	for _, address := range current {
		if _, existed := previousSet[address]; existed {
			carriedOver = append(carriedOver, address)
		} else {
			added = append(added, address)
		}
	}
	for _, address := range previous {
		if _, kept := currentSet[address]; !kept {
			removed = append(removed, address)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(carriedOver)
	return added, removed, carriedOver
}

// reads cs.currentEpoch atomically
func (csm *ConsumerSessionManager) atomicWriteCurrentEpoch(epoch uint64) {
	atomic.StoreUint64(&csm.currentEpoch, epoch)
}

// reads cs.currentEpoch atomically
func (csm *ConsumerSessionManager) atomicReadCurrentEpoch() (epoch uint64) {
	return atomic.LoadUint64(&csm.currentEpoch)
}

func (csm *ConsumerSessionManager) atomicReadNumberOfResets() (resets uint64) {
	return atomic.LoadUint64(&csm.numberOfResets)
}

// reset the valid addresses list and increase numberOfResets
//
// The outcome is deliberately NOT logged here. It used to be, in three lines, and the last of them
// blamed an expired or unpurchased subscription for an empty pool. The smart router has neither: it
// has no subscription and no on-chain pairing, its provider set comes from the endpoint config and
// from the epoch refresh that rebuilds it, and consumerPublicAddress is a locally generated
// smart-router-<rand> identifier rather than an account. That line sent every reader of a real
// customer capture to look at billing, and it fired roughly twice a second while doing it.
//
// The single caller reports the outcome instead — see releaseBlockedProvidersIfPoolEmpty — because
// it is the frame that holds the pool inventory and the request GUID, so it can name the actual
// cause and attribute it to a chain and a relay. Logging here as well only reproduced the same
// event three more times, once at ERROR.
// resetValidAddresses releases the blocked list and refills validAddresses from the pairing.
//
// didReset reports whether this call actually did that. The re-verify below can find the pool
// already refilled — an epoch tick landing while we waited for the write lock — in which case this
// is a no-op, and a caller that assumed otherwise would credit that epoch's recovery to a reset
// that never ran. Returning the fact is what lets the caller describe what it actually did.
func (csm *ConsumerSessionManager) resetValidAddresses(addon string, extensions []string) (numberOfResets uint64, didReset bool) {
	csm.lock.Lock() // lock write
	defer csm.lock.Unlock()
	if len(csm.getValidAddresses(addon, extensions, context.Background())) == 0 { // re verify it didn't change while waiting for lock.
		csm.setValidAddressesToDefaultValue(addon, extensions, context.Background(), ReleasePoolEmpty)
		csm.numberOfResets += 1
		didReset = true
	}
	// if len(csm.validAddresses) != 0 meaning we had a reset (or an epoch change), so we need to return the numberOfResets which is currently in csm
	return csm.numberOfResets, didReset
}

func (csm *ConsumerSessionManager) cacheAddonAddresses(addon string, extensions []string, ctx context.Context) []string {
	// Clone extensions to avoid mutating / aliasing the caller's backing array via append.
	routerKey := NewRouterKey(append(slices.Clone(extensions), addon))
	routerKeyString := routerKey.String()

	// OPTIMIZATION: Double-check locking pattern to reduce contention
	// First, try with read lock (allows concurrent readers)
	csm.lock.RLock()
	if csm.addonAddresses != nil {
		if cached, ok := csm.addonAddresses[routerKeyString]; ok && cached != nil {
			// Cache hit - return immediately with read lock (fast path)
			csm.lock.RUnlock()
			return cached
		}
	}
	csm.lock.RUnlock()

	// Cache miss - need to write, acquire write lock
	csm.lock.Lock()
	defer csm.lock.Unlock()

	// Double-check: re-verify after acquiring write lock
	// Another goroutine may have populated the cache while we waited
	if csm.addonAddresses != nil {
		if cached, ok := csm.addonAddresses[routerKeyString]; ok && cached != nil {
			return cached
		}
	} else {
		csm.addonAddresses = make(map[string][]string)
	}

	// Actually need to populate the cache.
	// Note: CalculateAddonValidAddresses assumes the CSM is at least RLocked; holding the write lock is fine.
	result := csm.CalculateAddonValidAddresses(addon, extensions, ctx)
	csm.addonAddresses[routerKeyString] = result
	return result
}

// releaseCouldServeThisRequest reports whether releasing the blocked list could hand THIS request a
// provider it has not already tried. A release refills validAddresses from the whole pairing pool,
// so the question is whether any of those providers is both absent from this request's ignored set
// and able to serve the addon and extensions asked for.
//
// When the answer is no, releasing cannot rescue the request and only destroys standing state that
// every other relay depends on. That happens whenever the pool empties *during* a request rather
// than before it: each provider is blocked as it fails, and by the time the last one goes the
// request has already tried them all.
func (csm *ConsumerSessionManager) releaseCouldServeThisRequest(ignored map[string]struct{}, addon string, extensions []string, ctx context.Context) bool {
	csm.lock.RLock()
	defer csm.lock.RUnlock()

	for _, address := range csm.pairingAddresses {
		if _, alreadyTried := ignored[address]; alreadyTried {
			continue
		}
		provider, ok := csm.pairing[address]
		if !ok || provider == nil {
			continue
		}
		if provider.IsSupportingAddon(addon) && provider.IsSupportingExtensions(extensions, ctx) {
			return true
		}
	}
	return false
}

// releaseBlockedProvidersIfPoolEmpty releases the standing blocked list and retries selection once,
// as the last resort of the failover cascade.
//
// Two guards, and both matter. The pool must be genuinely empty: the errors that bring us here also
// cover "every valid address is already ignored by this request", which is ordinary retry
// exhaustion, and releasing on that would let one retried relay wipe the blocked list for every
// other relay in the process. And the release must be able to help the request making it — see
// releaseCouldServeThisRequest — because GetSessions runs this chain more than once per relay, so a
// request whose providers are blocked one by one arrives here a second time with nothing left to
// try. Releasing then leaves the pool looking healthy while nothing in it can serve.
//
// Returns ok=false when nothing was released, or when the retry after a release still found nothing.
func (csm *ConsumerSessionManager) releaseBlockedProvidersIfPoolEmpty(ctx context.Context, wantedProviderNumber int, tempIgnoredProviders *ignoredProviders, cuNeededForSession uint64, requestedBlock int64, addon string, extensionNames []string, stateful uint32, virtualEpoch uint64, stickiness string, selectedProvider string, minGroups, perGroupTarget int) (SessionWithProviderMap, bool) {
	if len(csm.cacheAddonAddresses(addon, extensionNames, ctx)) != 0 {
		return nil, false
	}

	// Inventory of the pool, taken BEFORE the guard below rather than after it. The guard declines
	// the release for an empty pairing too — its loop over pairingAddresses never executes, so it
	// returns false — and that return is the one shape this whole line exists to report. Taking the
	// snapshot after the guard put the diagnosis on the far side of the branch that swallows it.
	inventory := csm.snapshotPoolInventory(addon, extensionNames, ctx)

	if !csm.releaseCouldServeThisRequest(tempIgnoredProviders.providers, addon, extensionNames, ctx) {
		// A declined release is usually ordinary retry exhaustion: this request has already tried
		// every provider, releasing rescues nothing, and that stays at DEBUG.
		//
		// It is not always that, and the discriminator is whether this request tried anything — NOT
		// whether the pairing is empty. releaseCouldServeThisRequest returns false for every
		// structural emptiness too: an empty pairing, a pairing where nothing serves the requested
		// addon, and a pairing whose members registered with no endpoints (IsSupportingExtensions
		// is false for a zero-endpoint provider, so it fails the capability test even for the
		// default collection). In all three nothing was tried, and "every provider has already been
		// tried" is not merely unhelpful, it is false — the request tried nothing. Keying on
		// pairingSize rescued only the first of the three and left the other two on the false line.
		if len(tempIgnoredProviders.providers) == 0 {
			csm.logPoolEmpty(ctx, inventory, addon, extensionNames)
			return nil, false
		}
		utils.LavaFormatDebug("every provider has already been tried by this request, leaving the blocked list standing",
			utils.LogAttr("addon", addon), utils.LogAttr("extensions", extensionNames), utils.LogAttr("GUID", ctx))
		return nil, false
	}

	csm.logPoolEmpty(ctx, inventory, addon, extensionNames)

	_, didReset := csm.resetValidAddresses(addon, extensionNames)

	// The reset refills validAddresses straight from the pairing, so it can only restore what the
	// pairing holds. With an empty pairing it is a no-op — and the line that used to report this
	// said "RESET COMPLETED" at INFO either way, which reads as recovery precisely when none
	// happened. Report the two outcomes as the different events they are.
	restored := len(csm.cacheAddonAddresses(addon, extensionNames, ctx))
	switch {
	case !didReset:
		// The re-verify inside resetValidAddresses found the pool already refilled, so this call
		// released nothing. Whatever repopulated it — an epoch tick that landed while we waited for
		// the write lock — is not ours to claim, and "pool reset restored providers" would credit
		// this request with a recovery it did not perform.
		utils.LavaFormatDebug("pool refilled before the reset ran, nothing released",
			utils.LogAttr("selectable", restored),
			utils.LogAttr("spec", csm.rpcEndpoint.Key()),
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensionNames),
			utils.LogAttr("GUID", ctx),
		)
	case restored == 0:
		// Re-snapshot. The reset cleared the blocked list and refilled validAddresses, so the
		// pre-reset reason no longer describes the state we are in — reusing it would report
		// all-blocked for a pool whose blocked list was just released and which still came up
		// empty, naming the one cause the reset has already ruled out.
		afterReset := csm.snapshotPoolInventory(addon, extensionNames, ctx)
		utils.LavaFormatWarning("pool reset recovered no providers", nil,
			utils.LogAttr("reason", afterReset.reason(addon, extensionNames)),
			utils.LogAttr("spec", csm.rpcEndpoint.Key()),
			utils.LogAttr("pairing_size", afterReset.pairingSize),
			// Named for its provenance: this count comes from the pre-reset snapshot, while the
			// reason and size beside it come from the post-reset one. Two snapshots on one line is
			// unavoidable here — the whole point is to contrast before with after — but a reader
			// must not take them for one instant.
			utils.LogAttr("blocked_before_reset", inventory.blockedCount),
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensionNames),
			utils.LogAttr("GUID", ctx),
		)
	default:
		utils.LavaFormatInfo("pool reset restored providers",
			utils.LogAttr("restored", restored),
			utils.LogAttr("blocked_before_reset", inventory.blockedCount),
			utils.LogAttr("spec", csm.rpcEndpoint.Key()),
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensionNames),
			utils.LogAttr("GUID", ctx),
		)
	}

	sessionWithProviderMap, err := csm.getValidConsumerSessionsWithProvider(ctx, wantedProviderNumber, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, stickiness, selectedProvider, minGroups, perGroupTarget)
	if err != nil {
		return nil, false
	}

	utils.LavaFormatDebug("Successfully got session after releasing the blocked provider list", utils.LogAttr("GUID", ctx))
	return sessionWithProviderMap, true
}

func (csm *ConsumerSessionManager) getSessionWithProviderOrError(ctx context.Context, wantedProviderNumber int, usedProviders UsedProvidersInf, tempIgnoredProviders *ignoredProviders, cuNeededForSession uint64, requestedBlock int64, addon string, extensionNames []string, stateful uint32, virtualEpoch uint64, stickiness string, selectedProvider string, minGroups, perGroupTarget int) (sessionWithProviderMap SessionWithProviderMap, err error) {
	sessionWithProviderMap, err = csm.getValidConsumerSessionsWithProvider(ctx, wantedProviderNumber, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, stickiness, selectedProvider, minGroups, perGroupTarget)
	if err != nil {
		if errors.Is(err, PairingListEmptyError) {
			// Emergency fallback chain, in order: backup providers, then releasing the standing
			// blocked list, then the blocked providers themselves, for maximum availability.
			if len(csm.backupProviders) > 0 {
				utils.LavaFormatDebug("No regular providers available, trying backup providers", utils.LogAttr("GUID", ctx))
				// try to get a session from the backup providers
				sessionWithProviderMap, err = csm.getValidConsumerSessionsWithProviderFromBackupProviderList(ctx, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, usedProviders)
				if err == nil {
					// backup providers succeeded, return the session
					utils.LavaFormatDebug("Successfully got session from backup providers", utils.LogAttr("GUID", ctx))
					return sessionWithProviderMap, nil
				}
				// backup providers failed, continue to the standing-bench release
				utils.LavaFormatDebug("Backup providers failed, releasing the blocked provider list", utils.LogAttr("error", err.Error()), utils.LogAttr("GUID", ctx))
			}

			// Every primary is blocked and backup could not serve either, so release the standing
			// blocked list and give the primaries one more chance. This used to run at the top of
			// every GetSessions, which refilled validAddresses before the backup tier was ever
			// consulted — so backup was unreachable via this path, and the block never held for
			// longer than a single request. Running it here is what lets "every primary is blocked"
			// actually mean "serve backup".
			if released, ok := csm.releaseBlockedProvidersIfPoolEmpty(ctx, wantedProviderNumber, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, stickiness, selectedProvider, minGroups, perGroupTarget); ok {
				return released, nil
			}

			// try to recover a session from the currently blocked providers
			var errOnRetry error
			sessionWithProviderMap, errOnRetry = csm.tryGetConsumerSessionWithProviderFromBlockedProviderList(ctx, wantedProviderNumber, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, usedProviders)
			if errOnRetry != nil {
				utils.LavaFormatDebug("All providers failed (regular, backup, and blocked)", utils.LogAttr("GUID", ctx))
				return nil, errOnRetry
			}
			utils.LavaFormatDebug("Successfully got session from blocked providers", utils.LogAttr("GUID", ctx))
		} else if errors.Is(err, SelectedProviderUnavailableError) {
			// A pinned request must never fall through to a provider the caller did not ask for, so
			// neither the backup tier nor the blocked-provider walk applies here. But an empty pool
			// still has to release the blocked list, exactly as the old top-of-GetSessions reset
			// did: without this, lava-select-provider stops resolving the moment every provider is
			// blocked, even though the pinned provider is sitting in that blocked list.
			if released, ok := csm.releaseBlockedProvidersIfPoolEmpty(ctx, wantedProviderNumber, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, stickiness, selectedProvider, minGroups, perGroupTarget); ok {
				return released, nil
			}
			return nil, err
		} else {
			return nil, err
		}
		// if we got here we managed to get a sessionWithProviderMap
	}
	return sessionWithProviderMap, nil
}

// GetSessions will return a ConsumerSession, given cu needed for that session.
// The user can also request specific providers to not be included in the search for a session.
// selectedProvider allows forcing selection of a specific provider by address (smartrouter only).
// GetSessionsOptions carries optional cross-validation selection hints for GetSessions.
const (
	// groupBlindMinGroups / groupBlindPerGroupTarget request provider selection with NO cross-validation
	// group-diversity fan-out: providers are chosen purely by QoS. This is the default for the many
	// non-cross-validation callers, and the deliberate mode for the emergency blocked-provider fallback —
	// the downstream diversity / per-group gate still enforces any real requirement, so a non-diverse
	// fallback fails rather than under-validates.
	groupBlindMinGroups      = 1
	groupBlindPerGroupTarget = 1
)

type GetSessionsOptions struct {
	// MinGroups > 1 makes selection fan out across at least this many distinct provider groups so a
	// group-diversity cross-validation policy can be satisfied. Default 0/1 means group-blind selection.
	MinGroups int
	// PerGroupTarget > 1 makes selection front-load up to this many providers from EACH of the MinGroups
	// groups (highest-QoS within the group) before filling the rest by QoS. Set to the per-group quorum's
	// agreement threshold so each group can independently reach its internal quorum; default 0/1 keeps the
	// one-provider-per-group behavior (group-blind fill).
	PerGroupTarget int
	// InternalPath is the internal path of the api collection this relay
	// resolved to ("/v2", "/P", or "" for a spec's root collection). Endpoint
	// selection matches on it EXACTLY, because in direct mode the path lives in
	// the upstream URL: a chain serving two API versions over two node-urls
	// (TON's toncenter v2 + tonindex v3) has one endpoint per version, and
	// dialing the wrong one returns that vendor's 404 as if it were an answer.
	//
	// nil — not "" — is the "no collection to match on" case (probes, tests),
	// which leaves selection unfiltered. "" is a real path: a chain that
	// enables a root collection alongside versioned ones (STRK) must keep its
	// root traffic on the root url.
	InternalPath *string
}

func (csm *ConsumerSessionManager) GetSessions(ctx context.Context, wantedProviderNumber int, cuNeededForSession uint64, usedProviders UsedProvidersInf, requestedBlock int64, addon string, extensions []*spectypes.Extension, stateful uint32, virtualEpoch uint64, stickiness string, selectedProvider string, opts ...GetSessionsOptions) (
	consumerSessionMap ConsumerSessionsMap, errRet error,
) {
	// minGroups > 1 (cross-validation group diversity) makes selection fan out across distinct provider
	// groups. Variadic so the many existing non-cross-validation callers are unchanged.
	minGroups := groupBlindMinGroups
	perGroupTarget := groupBlindPerGroupTarget
	var internalPath *string
	if len(opts) > 0 {
		if opts[0].MinGroups > 1 {
			minGroups = opts[0].MinGroups
		}
		if opts[0].PerGroupTarget > 1 {
			perGroupTarget = opts[0].PerGroupTarget
		}
		internalPath = opts[0].InternalPath
	}
	// set usedProviders if they were chosen for this relay
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cantSelectError := usedProviders.TryLockSelection(timeoutCtx)
	if cantSelectError != nil {
		if errors.Is(cantSelectError, ContextDoneNoNeedToLockSelectionError) {
			return nil, utils.LavaFormatDebug("Context deadline exceeded when trying to lock selection", utils.LogAttr("GUID", ctx))
		}
		return nil, utils.LavaFormatError("failed getting sessions from used Providers", nil, utils.LogAttr("usedProviders", usedProviders), utils.LogAttr("endpoint", csm.rpcEndpoint), utils.LogAttr("GUID", ctx))
	}
	defer func() { usedProviders.AddUsed(consumerSessionMap, errRet) }()
	routerKey := NewRouterKeyFromExtensions(extensions)
	initUnwantedProviders := usedProviders.GetUnwantedProvidersToSend(routerKey)

	extensionNames := common.GetExtensionNames(extensions)
	utils.LavaFormatTrace("[Archive Debug] GetSessions extension conversion",
		utils.LogAttr("originalExtensions", extensions),
		utils.LogAttr("extensionNames", extensionNames),
		utils.LogAttr("GUID", ctx))
	// Warm the addon/extension address cache for this router key. The empty-pool reset that used
	// to run here has moved into getSessionWithProviderOrError, so it fires only once the backup
	// tier has also been ruled out rather than before the tier is consulted at all.
	validAddresses := csm.cacheAddonAddresses(addon, extensionNames, ctx)
	// currentlyBlockedProviderAddresses is intentionally not read here: no csm.lock is held, so
	// reading the shared slice for a log attr races a concurrent writer (blockProvider / restore).
	utils.LavaFormatInfo("VALIDATING PROVIDERS", utils.LogAttr("addon", addon), utils.LogAttr("extensions", extensionNames), utils.LogAttr("validAddressesCount", len(validAddresses)), utils.LogAttr("validAddresses", validAddresses), utils.LogAttr("GUID", ctx))

	// providers that we don't try to connect this iteration.
	tempIgnoredProviders := &ignoredProviders{
		providers:    initUnwantedProviders,
		currentEpoch: csm.atomicReadCurrentEpoch(),
	}
	utils.LavaFormatTrace("GetSessions tempIgnoredProviders", utils.LogAttr("tempIgnoredProviders", tempIgnoredProviders), utils.LogAttr("GUID", ctx))

	// Get a valid consumerSessionsWithProvider
	sessionWithProviderMap, err := csm.getSessionWithProviderOrError(ctx, wantedProviderNumber, usedProviders, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, stickiness, selectedProvider, minGroups, perGroupTarget)
	if err != nil {
		utils.LavaFormatTrace("GetSessions error", utils.LogAttr("error", err.Error()), utils.LogAttr("GUID", ctx))
		return nil, err
	}

	// Scales the per-provider blocklisted-session allowance, so it is read after the cascade
	// rather than before it: a last-resort release inside it has to be reflected here.
	numberOfResets := csm.atomicReadNumberOfResets()

	// Save how many sessions we are aiming to have
	wantedSession := len(sessionWithProviderMap)
	// Save sessions to return
	sessions := make(ConsumerSessionsMap, wantedSession)
	for {
		for providerAddress, sessionWithProvider := range sessionWithProviderMap {
			// Extract values from session with provider
			consumerSessionsWithProvider := sessionWithProvider.SessionsWithProvider
			sessionEpoch := sessionWithProvider.CurrentEpoch

			// Get a valid Endpoint from the provider chosen
			connected, endpoints, _, err := consumerSessionsWithProvider.fetchEndpointConnectionFromConsumerSessionWithProvider(ctx, false, false, addon, extensionNames, internalPath)
			if err != nil {
				// verify err is AllProviderEndpointsDisabled and report.
				if errors.Is(err, AllProviderEndpointsDisabledError) {
					tempIgnoredProviders.providers[providerAddress] = struct{}{}
					err = csm.blockProvider(ctx, providerAddress, BlockReasonAllEndpointsDisabled, true, sessionEpoch, MaxConsecutiveConnectionAttempts, 0, false, csm.GenerateReconnectCallback(consumerSessionsWithProvider)) // reporting and blocking provider this epoch
					if err != nil {
						if !errors.Is(err, EpochMismatchError) {
							// only acceptable error is EpochMismatchError so if different, throw fatal
							utils.LavaFormatFatal("Unsupported Error", err, utils.LogAttr("GUID", ctx))
						}
					}
					continue
				} else {
					utils.LavaFormatFatal("Unsupported Error", err, utils.LogAttr("GUID", ctx))
				}
			} else if !connected {
				// If failed to connect we ignore this provider for this get session request only
				// and try again getting a random provider to pick from
				tempIgnoredProviders.providers[providerAddress] = struct{}{}
				continue
			}

			// get the endpoint we got, as its the only one returned when asking fetchEndpointConnectionFromConsumerSessionWithProvider with false value
			endpoint := endpoints[0]

			// we get the reported providers here after we try to connect, so if any provider didn't respond he will already be added to the list.
			reportedProviders := csm.GetReportedProviders(sessionEpoch)

			// Get session from endpoint or create new or continue. if more than 10 connections are open.
			consumerSession, pairingEpoch, err := consumerSessionsWithProvider.GetConsumerSessionInstanceFromEndpoint(endpoint.chosenEndpointConnection, numberOfResets, csm.qosManager, endpoint.endpoint.NetworkAddress)
			if err != nil {
				utils.LavaFormatError("Error on consumerSessionWithProvider.getConsumerSessionInstanceFromEndpoint", err,
					utils.LogAttr("providerAddress", providerAddress),
					utils.LogAttr("validAddresses", csm.validAddresses),
					utils.LogAttr("Error", err.Error()),
					utils.LogAttr("GUID", ctx),
				)
				if errors.Is(err, MaximumNumberOfSessionsExceededError) {
					// we can get a different provider, adding this provider to the list of providers to skip on.
					tempIgnoredProviders.providers[providerAddress] = struct{}{}
				} else if errors.Is(err, MaximumNumberOfBlockListedSessionsError) {
					// provider has too many block listed sessions. we block it until the next epoch and ignore it so it won't pop up again when resetting the provider list.
					tempIgnoredProviders.providers[providerAddress] = struct{}{}
					err = csm.blockProvider(ctx, providerAddress, BlockReasonTooManyDeadSessions, false, sessionEpoch, 0, 0, false, nil)
					if err != nil {
						utils.LavaFormatError("Failed to block provider: ", err, utils.LogAttr("GUID", ctx))
					}
				} else {
					utils.LavaFormatFatal("Unsupported Error", err, utils.LogAttr("GUID", ctx))
				}

				continue
			}

			if pairingEpoch != sessionEpoch {
				// pairingEpoch and SessionEpoch must be the same, we validate them here if they are different we raise an error and continue with pairingEpoch
				utils.LavaFormatError("sessionEpoch and pairingEpoch mismatch", nil, utils.Attribute{Key: "sessionEpoch", Value: sessionEpoch}, utils.Attribute{Key: "pairingEpoch", Value: pairingEpoch})
				sessionEpoch = pairingEpoch
			}

			// If we successfully got a consumerSession we can apply the current CU to the consumerSessionWithProvider.UsedComputeUnits
			err = consumerSessionsWithProvider.addUsedComputeUnits(cuNeededForSession, virtualEpoch)
			if err != nil {
				utils.LavaFormatDebug("consumerSessionWithProvider.addUsedComputeUnit", utils.Attribute{Key: "Error", Value: err.Error()}, utils.LogAttr("GUID", ctx))
				if errors.Is(err, MaxComputeUnitsExceededError) {
					tempIgnoredProviders.providers[providerAddress] = struct{}{}
					// We must unlock the consumer session before continuing.
					consumerSession.Free(nil)
					continue
				} else {
					utils.LavaFormatFatal("Unsupported Error", err, utils.LogAttr("GUID", ctx))
				}
			} else {
				// consumer session is locked and valid, we need to set the relayNumber and the relay cu. before returning.
				// Successfully created/got a consumerSession.

				utils.LavaFormatTrace("Consumer get session",
					utils.LogAttr("provider", providerAddress),
					utils.LogAttr("sessionEpoch", sessionEpoch),
					utils.LogAttr("consumerSession.CUSum", consumerSession.CuSum),
					utils.LogAttr("consumerSession.RelayNum", consumerSession.RelayNum),
					utils.LogAttr("consumerSession.SessionId", consumerSession.SessionId),
					utils.LogAttr("GUID", ctx),
				)

				// If no error, add provider session map
				sessionInfo := &SessionInfo{
					StakeSize:         consumerSessionsWithProvider.getProviderStakeSize(),
					Session:           consumerSession,
					Epoch:             sessionEpoch, // Must use pairing epoch (epoch start block) for provider validation
					ReportedProviders: reportedProviders,
				}

				sessions[providerAddress] = sessionInfo

				qosReport, _ := csm.providerOptimizer.GetReputationReportForProvider(providerAddress)
				consumerSession.SetUsageForSession(cuNeededForSession, qosReport, usedProviders, routerKey)
				// We successfully added provider, we should ignore it if we need to fetch new
				tempIgnoredProviders.providers[providerAddress] = struct{}{}
				if len(sessions) == wantedSession {
					return sessions, nil
				}
				continue
			}
		}

		// If we do not have enough fetch more
		sessionWithProviderMap, err = csm.getSessionWithProviderOrError(ctx, 1, usedProviders, tempIgnoredProviders, cuNeededForSession, requestedBlock, addon, extensionNames, stateful, virtualEpoch, stickiness, selectedProvider, minGroups, perGroupTarget)
		// If error exists but we have sessions, return them
		if err != nil && len(sessions) != 0 {
			return sessions, nil
		}
		// If error happens, and we do not have any sessions return an error
		if err != nil {
			return nil, err
		}
		// if we got here we managed to get more sessions so we will try to connect and return a session to the user.
	}
}

// csm must be rlocked here
func (csm *ConsumerSessionManager) getTopTenProvidersForStatefulCalls(validAddresses []string, ignoredProvidersList map[string]struct{}) []string {
	// sort by cu used, easiest to sort by that factor as it probably means highest QOS and easily read by atomic
	customSort := func(i, j int) bool {
		return csm.pairing[validAddresses[i]].atomicReadUsedComputeUnits() > csm.pairing[validAddresses[j]].atomicReadUsedComputeUnits()
	}
	// Sort the slice using the custom sorting rule
	sort.Slice(validAddresses, customSort)
	addresses := []string{}
	wantedLength := 10
	for _, sortedAddress := range validAddresses {
		// skip ignored providers
		if _, foundInIgnoredProviderList := ignoredProvidersList[sortedAddress]; foundInIgnoredProviderList {
			continue
		}
		// fill the slice until we have 10 providers who are not ignored
		addresses = append(addresses, sortedAddress)
		if len(addresses) >= wantedLength {
			break
		}
	}
	return addresses
}

// convertSelectionStatsToMetrics converts SelectionStats to metrics format with all provider scores
func convertSelectionStatsToMetrics(stats *provideroptimizer.SelectionStats) (allScores []metrics.ProviderSelectionScores, rngValue float64) {
	if stats == nil {
		return nil, 0
	}
	rngValue = stats.RNGValue
	allScores = make([]metrics.ProviderSelectionScores, 0, len(stats.UpstreamScores))
	for _, score := range stats.UpstreamScores {
		allScores = append(allScores, metrics.ProviderSelectionScores{
			ProviderAddress: score.Address,
			Availability:    score.Availability,
			Latency:         score.Latency,
			Sync:            score.Sync,
			Stake:           score.Stake,
			Composite:       score.Composite,
		})
	}
	return allScores, rngValue
}

// resolveSelectedProviderAddress maps a header-supplied provider name onto the address
// the router registered. Provider names are free-form config strings and the pipeline
// does not preserve their case — the helm chart folds node names into the router's
// config, the values file spells them for humans — so the match folds case.
//
// It returns the canonical address rather than the header's spelling because everything
// past this point keys on the router's own name: csm.pairing, the session store, the
// in-request ignored set, the metrics labels. csm.pairing in particular is a
// LavaFormatFatal on a miss, so handing the caller's spelling downstream would turn a
// mis-cased header from a rejected relay into a dead router.
//
// An exact hit wins outright, so a config registering two names that differ only in case
// still resolves the way the caller spelled it. Absent one, a fold matching more than one
// address resolves to nothing and returns those candidates instead: validAddresses is
// built by ranging a map and is reordered as providers are blocked and recovered, so
// picking one would serve the same pinned name from a different upstream run to run —
// the opposite of what pinning is for. The caller reports the collision so the operator
// can rename a provider.
func resolveSelectedProviderAddress(selectedProvider string, addresses []string) (address string, ambiguous []string) {
	if slices.Contains(addresses, selectedProvider) {
		return selectedProvider, nil
	}
	var folded []string
	for _, candidate := range addresses {
		if strings.EqualFold(candidate, selectedProvider) {
			folded = append(folded, candidate)
		}
	}
	if len(folded) == 1 {
		return folded[0], nil
	}
	// Sorted so the same collision reports the same way on every run.
	sort.Strings(folded)
	return "", folded
}

// Get a valid provider address.
// filterRateLimitedProviders drops providers currently held off after a rate limit —
// unless that would leave nothing choosable: the request must still be served (the
// Retry-After stays internal, a customer is never answered with a synthesized 429), so
// when every candidate is held off the soonest-to-expire one stays in.
func (csm *ConsumerSessionManager) filterRateLimitedProviders(ctx context.Context, validAddresses []string, ignoredProvidersList map[string]struct{}) []string {
	reg := csm.rateLimitHoldoff
	if reg == nil {
		return validAddresses
	}
	ready := make([]string, 0, len(validAddresses))
	soonest := ""
	var soonestAt time.Time
	heldCount := 0
	readyChoosable := 0
	for _, addr := range validAddresses {
		readyAt, held := reg.ProviderReadyAt(addr)
		if !held {
			ready = append(ready, addr)
			if _, ignored := ignoredProvidersList[addr]; !ignored {
				readyChoosable++
			}
			continue
		}
		heldCount++
		if _, ignored := ignoredProvidersList[addr]; ignored {
			continue // already out of this request — cannot be the fallback either
		}
		if soonest == "" || readyAt.Before(soonestAt) {
			soonest, soonestAt = addr, readyAt
		}
	}
	if heldCount == 0 {
		return validAddresses
	}
	if readyChoosable > 0 {
		utils.LavaFormatInfo("rate-limit hold-off: skipping held-off providers",
			utils.LogAttr("held", heldCount),
			utils.LogAttr("ready", readyChoosable),
			utils.LogAttr("GUID", ctx),
		)
		return ready
	}
	if soonest == "" {
		return ready // every held candidate is also ignored this request — nothing to add
	}
	utils.LavaFormatWarning("rate-limit hold-off: every candidate held off, keeping the soonest to expire", nil,
		utils.LogAttr("provider", soonest),
		utils.LogAttr("readyAt", soonestAt),
		utils.LogAttr("GUID", ctx),
	)
	return append(ready, soonest)
}

func (csm *ConsumerSessionManager) getValidProviderAddresses(ctx context.Context, wantedProviders int, ignoredProvidersList map[string]struct{}, cu uint64, requestedBlock int64, addon string, extensions []string, stateful uint32, stickiness string, selectedProvider string) (addresses []string, err error) {
	// cs.Lock must be Rlocked here.
	ignoredProvidersListLength := len(ignoredProvidersList)
	validAddresses := csm.getValidAddresses(addon, extensions, ctx)
	validAddressesLength := len(validAddresses)
	totalValidLength := validAddressesLength - ignoredProvidersListLength

	// Handle provider selection via header (smartrouter only)
	if selectedProvider != "" {
		// Resolve to the router's own spelling first, so every check below — and everything
		// downstream — keys on the canonical address rather than on what the header said.
		providerAddress, ambiguous := resolveSelectedProviderAddress(selectedProvider, validAddresses)

		if len(ambiguous) > 0 {
			// Providers whose names differ only in case, and a header matching none of them
			// exactly: which one the caller meant is unknowable. Pinning exists so a request
			// is never served by a provider the caller did not ask for, so report the
			// collision rather than picking one.
			return nil, utils.LavaFormatError(
				"Selected provider name matches more than one provider",
				SelectedProviderUnavailableError,
				utils.LogAttr("selectedProvider", selectedProvider),
				utils.LogAttr("matchingProviders", ambiguous),
				utils.LogAttr("addon", addon),
				utils.LogAttr("extensions", extensions),
				utils.LogAttr("GUID", ctx),
			)
		}

		if providerAddress == "" {
			// Return error instead of falling back to random selection.
			//
			// Deliberately broad. This branch fires both for a name that is not configured at
			// all and for one that is configured but not usable for this request — blocked
			// (removeAddressFromValidAddresses pops it into currentlyBlockedProviderAddresses)
			// or not serving this addon/extension, since getValidAddresses returns the filtered
			// list. Calling that "unknown" would tell an operator their provider does not exist
			// in the middle of an outage, which is the same class of misdirection the split of
			// this error from SelectedProviderAlreadyFailedError exists to remove. The sentinel
			// carries the precision; this description has to stay true for every way the branch
			// is reached.
			return nil, utils.LavaFormatError(
				"Selected provider not available",
				SelectedProviderUnavailableError,
				utils.LogAttr("selectedProvider", selectedProvider),
				utils.LogAttr("validProviders", validAddresses),
				utils.LogAttr("addon", addon),
				utils.LogAttr("extensions", extensions),
				utils.LogAttr("GUID", ctx),
			)
		}

		// If the pinned provider has already been added to ignoredProvidersList during this
		// GetSessions call (e.g. a prior fetchEndpointConnectionFromConsumerSessionWithProvider
		// returned connected=false), the outer loop would otherwise re-call us with the same
		// selectedProvider and we'd return the same address again — an unbounded spin. Bound
		// it here by returning an error the caller propagates as a single attempt.
		//
		// Looked up by the resolved address, exactly: the set is only ever written with
		// addresses that came out of the pairing, so folding case here as well would let a
		// case-twin's failure reject a provider that never failed.
		if _, ignored := ignoredProvidersList[providerAddress]; ignored {
			return nil, utils.LavaFormatWarning(
				"Selected provider cannot be retried",
				SelectedProviderAlreadyFailedError,
				utils.LogAttr("provider", providerAddress),
				utils.LogAttr("selectedProvider", selectedProvider),
				utils.LogAttr("addon", addon),
				utils.LogAttr("extensions", extensions),
				utils.LogAttr("GUID", ctx),
			)
		}

		addresses = []string{providerAddress}
		utils.LavaFormatInfo("Provider selected via header",
			utils.LogAttr("provider", providerAddress),
			utils.LogAttr("requestedProvider", selectedProvider),
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("GUID", ctx))
		return addresses, nil
	}

	if stickysession, ok := csm.stickySessions.Get(stickiness); ok {
		// Check if sticky session provider is still valid
		providerValid := slices.Contains(validAddresses, stickysession.Provider)
		if providerValid {
			addresses = []string{stickysession.Provider}
			utils.LavaFormatTrace("returning sticky session", utils.LogAttr("provider", stickysession.Provider), utils.LogAttr("id", stickiness), utils.LogAttr("GUID", ctx))
			return addresses, nil
		} else {
			utils.LavaFormatTrace("sticky session provider is no longer valid, deleting", utils.LogAttr("provider", stickysession.Provider), utils.LogAttr("id", stickiness), utils.LogAttr("GUID", ctx))
			csm.stickySessions.Delete(stickiness)
		}
	}

	if totalValidLength <= 0 {
		// check all ignored are actually valid addresses
		ignoredProvidersListLength = 0
		for _, address := range validAddresses {
			if _, ok := ignoredProvidersList[address]; ok {
				ignoredProvidersListLength++
			}
		}
		if validAddressesLength-ignoredProvidersListLength <= 0 {
			utils.LavaFormatDebug("Pairing list empty", utils.Attribute{Key: "Provider list", Value: validAddresses}, utils.Attribute{Key: "IgnoredProviderList", Value: ignoredProvidersList}, utils.Attribute{Key: "addon", Value: addon}, utils.Attribute{Key: "extensions", Value: extensions}, utils.LogAttr("GUID", ctx))
			err = PairingListEmptyError
			return addresses, err
		}
	}
	// Rate-limit hold-off (docs/RATE-LIMIT-HOLDOFF.md): prefer providers that are not
	// currently held off after a 429. A header-pinned provider and an existing sticky
	// session bypass this above on purpose — an explicit ask outranks the hold-off.
	validAddresses = csm.filterRateLimitedProviders(ctx, validAddresses, ignoredProvidersList)

	var providers []string
	if stateful == common.CONSISTENCY_SELECT_ALL_PROVIDERS && csm.providerOptimizer.Strategy() != provideroptimizer.StrategyCost {
		providers = csm.getTopTenProvidersForStatefulCalls(validAddresses, ignoredProvidersList)
	} else if stickiness != "" {
		var selectionStats *provideroptimizer.SelectionStats
		providers, selectionStats = csm.providerOptimizer.ChooseUpstreamWithStats(ctx, validAddresses, ignoredProvidersList, cu, requestedBlock)
		if selectionStats != nil {
			csm.setSelectionStats(selectionStats)
		}
		// Track provider selection for metrics with all provider scores
		if len(providers) > 0 {
			if selectionStats == nil {
				utils.LavaFormatWarning("Selection stats missing for sticky session provider", nil,
					utils.LogAttr("provider", providers[0]),
					utils.LogAttr("chainId", csm.rpcEndpoint.ChainID),
					utils.LogAttr("addon", addon),
					utils.LogAttr("extensions", extensions),
					utils.LogAttr("GUID", ctx),
				)
			} else if len(selectionStats.UpstreamScores) == 0 {
				utils.LavaFormatWarning("Selection stats missing provider scores for sticky session", nil,
					utils.LogAttr("provider", providers[0]),
					utils.LogAttr("selectedProvider", selectionStats.SelectedProvider),
					utils.LogAttr("chainId", csm.rpcEndpoint.ChainID),
					utils.LogAttr("addon", addon),
					utils.LogAttr("extensions", extensions),
					utils.LogAttr("GUID", ctx),
				)
			}
			allScores, rngValue := convertSelectionStatsToMetrics(selectionStats)
			csm.consumerMetricsManager.SetProviderSelected(csm.rpcEndpoint.ChainID, csm.rpcEndpoint.ApiInterface, providers[0], allScores, rngValue)
		}
	} else {
		// Make a copy of ignoredProvidersList to avoid modifying the original
		ignoredProvidersListCopy := make(map[string]struct{}, len(ignoredProvidersList))
		for k, v := range ignoredProvidersList {
			ignoredProvidersListCopy[k] = v
		}
		for i := 0; i < wantedProviders; i++ {
			provider, selectionStats := csm.providerOptimizer.ChooseUpstreamWithStats(ctx, validAddresses, ignoredProvidersListCopy, cu, requestedBlock)
			if len(provider) == 0 {
				break
			}
			// Store the latest selection stats for the first provider selection
			if i == 0 && selectionStats != nil {
				csm.setSelectionStats(selectionStats)
			}
			// Track provider selection for metrics with all provider scores
			if selectionStats == nil {
				utils.LavaFormatWarning("Selection stats missing for provider selection", nil,
					utils.LogAttr("provider", provider[0]),
					utils.LogAttr("chainId", csm.rpcEndpoint.ChainID),
					utils.LogAttr("addon", addon),
					utils.LogAttr("extensions", extensions),
					utils.LogAttr("GUID", ctx),
				)
			} else if len(selectionStats.UpstreamScores) == 0 {
				utils.LavaFormatWarning("Selection stats missing provider scores", nil,
					utils.LogAttr("provider", provider[0]),
					utils.LogAttr("selectedProvider", selectionStats.SelectedProvider),
					utils.LogAttr("chainId", csm.rpcEndpoint.ChainID),
					utils.LogAttr("addon", addon),
					utils.LogAttr("extensions", extensions),
					utils.LogAttr("GUID", ctx),
				)
			}
			allScores, rngValue := convertSelectionStatsToMetrics(selectionStats)
			for _, providerAddr := range provider {
				ignoredProvidersListCopy[providerAddr] = struct{}{}
				csm.consumerMetricsManager.SetProviderSelected(csm.rpcEndpoint.ChainID, csm.rpcEndpoint.ApiInterface, providerAddr, allScores, rngValue)
			}
			providers = append(providers, provider...)
		}
	}

	utils.LavaFormatInfo("Choosing providers",
		utils.LogAttr("validAddresses", validAddresses),
		utils.LogAttr("ignoredProvidersList", ignoredProvidersList),
		utils.LogAttr("chosenProviders", providers),
		utils.LogAttr("addon", addon),
		utils.LogAttr("extensions", extensions),
		utils.LogAttr("stateful", stateful),
		utils.LogAttr("GUID", ctx),
	)

	// Archive-specific debug logging
	if len(extensions) > 0 {
		utils.LavaFormatTrace("[Archive Debug] Final provider selection",
			utils.LogAttr("validAddresses", validAddresses),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("chosenProviders", providers),
			utils.LogAttr("GUID", ctx))
	}

	// make sure we have at least 1 valid provider
	if len(providers) == 0 || providers[0] == "" {
		utils.LavaFormatInfo("No providers returned by the optimizer", utils.Attribute{Key: "Provider list", Value: validAddresses}, utils.Attribute{Key: "IgnoredProviderList", Value: ignoredProvidersList}, utils.LogAttr("GUID", ctx))
		err = PairingListEmptyError
		return addresses, err
	}

	// If stickiness is requested, store the first provider for future use
	if stickiness != "" {
		utils.LavaFormatTrace("setting sticky session", utils.LogAttr("provider", providers[0]), utils.LogAttr("id", stickiness), utils.LogAttr("GUID", ctx))
		csm.stickySessions.Set(stickiness, &StickySession{
			Provider: providers[0],
			Epoch:    csm.atomicReadCurrentEpoch(),
		})
		return []string{providers[0]}, nil
	}
	return providers, nil
}

// On cases where the valid provider list is empty, by being already used in this attempt, and we got to a point
// where we need another session (for retry or a timeout happened) we want to try fetching a blocked provider for the list.
// the list will be sorted by most cu served giving the best provider that was blocked a second chance to get back to valid addresses.
func (csm *ConsumerSessionManager) tryGetConsumerSessionWithProviderFromBlockedProviderList(ctx context.Context, wantedProviderNumber int, ignoredProviders *ignoredProviders, cuNeededForSession uint64, requestedBlock int64, addon string, extensions []string, stateful uint32, virtualEpoch uint64, usedProviders UsedProvidersInf) (sessionWithProviderMap SessionWithProviderMap, err error) {
	csm.lock.RLock()
	// we do not defer yet as we might need to unlock due to an epoch change

	// reading the epoch here while locked, to get the epoch of the pairing.
	currentEpoch := csm.atomicReadCurrentEpoch()

	// if len(csm.currentlyBlockedProviderAddresses) == 0 we probably reset the state so we can fetch it normally OR ||
	// on a very rare case epoch change can happen. in this case we should just fetch a provider from the new pairing list.
	// we also enter this case if all validAddresses are inside ignoredProviders
	if len(csm.currentlyBlockedProviderAddresses) == 0 || ignoredProviders.currentEpoch < currentEpoch {
		// epoch changed just now (between the getValidConsumerSessionsWithProvider to tryGetConsumerSessionWithProviderFromBlockedProviderList)
		if ignoredProviders.currentEpoch < currentEpoch {
			utils.LavaFormatDebug("Epoch changed between getValidConsumerSessionsWithProvider to tryGetConsumerSessionWithProviderFromBlockedProviderList getting pairing from new epoch list", utils.LogAttr("GUID", ctx))
		}
		csm.lock.RUnlock() // unlock because getValidConsumerSessionsWithProvider is locking.
		// Emergency blocked-provider fallback: group-blind selection. The diversity / per-group gate still
		// enforces the requirement, so a non-diverse fallback fails rather than under-validates.
		return csm.getValidConsumerSessionsWithProvider(ctx, wantedProviderNumber, ignoredProviders, cuNeededForSession, requestedBlock, addon, extensions, stateful, virtualEpoch, "", "", groupBlindMinGroups, groupBlindPerGroupTarget)
	}

	// if we got here we validated the epoch is still the same epoch as we expected and we need to fetch a session from the blocked provider list.
	defer csm.lock.RUnlock()

	routerKey := NewRouterKey(extensions)
	// csm.currentlyBlockedProviderAddresses is sorted by the provider with the highest cu used this epoch to the lowest
	// meaning if we fetch the first successful index this is probably the highest success ratio to get a response.
	for _, providerAddress := range csm.currentlyBlockedProviderAddresses {
		// check if we have this provider already.
		if _, providerExistInIgnoredProviders := ignoredProviders.providers[providerAddress]; providerExistInIgnoredProviders {
			utils.LavaFormatTrace("[continue] provider already in ignored providers", utils.LogAttr("providerAddress", providerAddress), utils.LogAttr("GUID", ctx))
			continue
		}
		consumerSessionsWithProvider := csm.pairing[providerAddress]
		// Add to ignored (no matter what)
		ignoredProviders.providers[providerAddress] = struct{}{}
		usedProviders.AddUnwantedAddresses(providerAddress, routerKey) // add the address to our unwanted providers to avoid infinite recursion

		// validate this provider has enough cu to be used
		if err := consumerSessionsWithProvider.validateComputeUnits(cuNeededForSession, virtualEpoch); err != nil {
			// we already added to ignored we can just continue to the next provider
			utils.LavaFormatTrace("[continue] no compute units", utils.LogAttr("providerAddress", providerAddress), utils.LogAttr("GUID", ctx))
			continue
		}

		// validate this provider supports the required extension or addon
		if !consumerSessionsWithProvider.IsSupportingAddon(addon) || !consumerSessionsWithProvider.IsSupportingExtensions(extensions, ctx) {
			utils.LavaFormatTrace("[continue] no addon or extensions", utils.LogAttr("providerAddress", providerAddress), utils.LogAttr("GUID", ctx))
			continue
		}

		consumerSessionsWithProvider.atomicWriteBlockedStatus(BlockedProviderSessionUsedStatus) // will add to valid addresses if successful
		// If no error, return session map
		return SessionWithProviderMap{
			providerAddress: &SessionWithProvider{
				SessionsWithProvider: consumerSessionsWithProvider,
				CurrentEpoch:         currentEpoch,
				retryConnecting:      true,
			},
		}, nil
	}

	// if we got here we failed to fetch a valid provider meaning no pairing available.
	return nil, utils.LavaFormatError(csm.rpcEndpoint.ChainID+" could not get a provider address from blocked provider list", PairingListEmptyError, utils.LogAttr("csm.currentlyBlockedProviderAddresses", csm.currentlyBlockedProviderAddresses), utils.LogAttr("addons", addon), utils.LogAttr("extensions", extensions), utils.LogAttr("ignoredProviders", ignoredProviders.providers), utils.LogAttr("GUID", ctx))
}

// getValidConsumerSessionsWithProviderFromBackupProviderList retrieves valid backup provider sessions for emergency fallback when no regular providers are available.
func (csm *ConsumerSessionManager) getValidConsumerSessionsWithProviderFromBackupProviderList(ctx context.Context, ignoredProviders *ignoredProviders, cuNeededForSession uint64, requestedBlock int64, addon string, extensions []string, stateful uint32, virtualEpoch uint64, usedProviders UsedProvidersInf) (sessionWithProviderMap SessionWithProviderMap, err error) {
	csm.lock.RLock()
	defer csm.lock.RUnlock()

	utils.LavaFormatInfo("[BackupProviders] Static providers exhausted — entering backup fallback",
		utils.LogAttr("ignored_providers", ignoredProviders.providers),
		utils.LogAttr("backup_pool_size", len(csm.backupProviders)),
		utils.LogAttr("GUID", ctx),
	)

	currentEpoch := csm.atomicReadCurrentEpoch() // reading the epoch here while locked, to get the epoch of the pairing.
	if ignoredProviders.currentEpoch < currentEpoch {
		utils.LavaFormatDebug("ignoredProviders epoch is not the current epoch, resetting ignoredProviders", utils.Attribute{Key: "ignoredProvidersEpoch", Value: ignoredProviders.currentEpoch}, utils.Attribute{Key: "currentEpoch", Value: currentEpoch}, utils.LogAttr("GUID", ctx))
		ignoredProviders.providers = make(map[string]struct{}) // reset the old providers as epochs changed so we have a new pairing list.
		ignoredProviders.currentEpoch = currentEpoch
	}

	// Check if backup providers exist
	if len(csm.backupProviders) == 0 {
		utils.LavaFormatDebug("No backup providers configured", utils.LogAttr("GUID", ctx))
		return nil, utils.LavaFormatError("no backup providers configured", nil, utils.LogAttr("GUID", ctx))
	}

	// Get valid backup provider addresses that support the required addon and extensions
	backupProviderAddresses := []string{}
	for providerAddress, consumerSessionsWithProvider := range csm.backupProviders {
		// Skip if provider is in ignored list (already tried or failed this request)
		if _, exists := ignoredProviders.providers[providerAddress]; exists {
			continue
		}

		// Skip if provider is blocked this epoch due to repeated failures
		if _, blocked := csm.blockedBackupProviders[providerAddress]; blocked {
			continue
		}

		// Validate backup provider supports required addons and extensions (simplified validation for emergency scenarios)
		if !consumerSessionsWithProvider.IsSupportingAddon(addon) || !consumerSessionsWithProvider.IsSupportingExtensions(extensions, ctx) {
			continue
		}

		backupProviderAddresses = append(backupProviderAddresses, providerAddress)
	}

	if len(backupProviderAddresses) == 0 {
		utils.LavaFormatInfo("[BackupProviders] No eligible backup providers after filtering (all ignored or addon mismatch)",
			utils.LogAttr("ignored_providers", ignoredProviders.providers),
			utils.LogAttr("GUID", ctx),
		)
		return nil, utils.LavaFormatError("no valid backup providers available", nil, utils.LogAttr("GUID", ctx))
	}

	utils.LavaFormatInfo("[BackupProviders] Asking optimizer to select from backup candidates",
		utils.LogAttr("candidates", backupProviderAddresses),
		utils.LogAttr("candidate_count", len(backupProviderAddresses)),
		utils.LogAttr("GUID", ctx),
	)

	// Use the optimizer to select a single backup provider by QoS score.
	// On cold start (no relay history) selection is uniform; as backups serve relays the optimizer
	// accumulates latency/availability data and improves ranking.
	// Returning one provider per call allows each retry to pick the next-best backup.
	selectedAddresses := csm.providerOptimizer.ChooseUpstream(ctx, backupProviderAddresses, ignoredProviders.providers, cuNeededForSession, requestedBlock)
	if len(selectedAddresses) == 0 {
		utils.LavaFormatInfo("[BackupProviders] Optimizer returned no selection from backup candidates",
			utils.LogAttr("candidates", backupProviderAddresses),
			utils.LogAttr("GUID", ctx),
		)
		return nil, utils.LavaFormatError("optimizer returned no backup provider", nil, utils.LogAttr("GUID", ctx))
	}
	selectedAddress := selectedAddresses[0]

	consumerSessionsWithProvider := csm.backupProviders[selectedAddress]
	if consumerSessionsWithProvider == nil {
		utils.LavaFormatFatal("optimizer selected backup provider missing from map", nil,
			utils.Attribute{Key: "selectedAddress", Value: selectedAddress},
			utils.Attribute{Key: "backupProviderAddresses", Value: backupProviderAddresses},
			utils.Attribute{Key: "epochAtStart", Value: currentEpoch},
			utils.Attribute{Key: "currentEpoch", Value: csm.atomicReadCurrentEpoch()},
			utils.LogAttr("GUID", ctx),
		)
	}

	// Add selected provider to ignored so the next retry picks a different backup
	ignoredProviders.providers[selectedAddress] = struct{}{}

	utils.LavaFormatInfo("[BackupProviders] Optimizer selected backup provider",
		utils.LogAttr("selected", selectedAddress),
		utils.LogAttr("candidates_considered", backupProviderAddresses),
		utils.LogAttr("remaining_backups", len(backupProviderAddresses)-1),
		utils.LogAttr("GUID", ctx),
	)

	sessionWithProviderMap = SessionWithProviderMap{
		selectedAddress: &SessionWithProvider{
			SessionsWithProvider: consumerSessionsWithProvider,
			CurrentEpoch:         currentEpoch,
		},
	}

	return sessionWithProviderMap, nil
}

func (csm *ConsumerSessionManager) getValidConsumerSessionsWithProvider(ctx context.Context, wantedProviderNumber int, ignoredProviders *ignoredProviders, cuNeededForSession uint64, requestedBlock int64, addon string, extensions []string, stateful uint32, virtualEpoch uint64, stickiness string, selectedProvider string, minGroups, perGroupTarget int) (sessionWithProviderMap SessionWithProviderMap, err error) {
	csm.lock.RLock()
	defer csm.lock.RUnlock()

	utils.LavaFormatTrace("Called getValidConsumerSessionsWithProvider", utils.LogAttr("wantedProviderNumber", wantedProviderNumber), utils.LogAttr("ignoredProviders", ignoredProviders), utils.LogAttr("GUID", ctx))

	currentEpoch := csm.atomicReadCurrentEpoch() // reading the epoch here while locked, to get the epoch of the pairing.
	if ignoredProviders.currentEpoch < currentEpoch {
		utils.LavaFormatDebug("ignoredProviders epoch is not the current epoch, resetting ignoredProviders", utils.Attribute{Key: "ignoredProvidersEpoch", Value: ignoredProviders.currentEpoch}, utils.Attribute{Key: "currentEpoch", Value: currentEpoch}, utils.LogAttr("GUID", ctx))
		ignoredProviders.providers = make(map[string]struct{}) // reset the old providers as epochs changed so we have a new pairing list.
		ignoredProviders.currentEpoch = currentEpoch
	}

	// Fetch provider addresses. When a cross-validation policy requires group diversity (minGroups > 1)
	// we over-fetch the full QoS-ranked pool and reorder it so the front spans at least minGroups distinct
	// provider groups (highest-QoS per group first), then fill the rest by QoS — otherwise group-blind
	// selection could pick the top providers all from one group and fail the diversity gate even when a
	// diverse set exists. minGroups <= 1 keeps the original group-blind selection byte-identical.
	var providerAddresses []string
	if minGroups > 1 {
		// A group-diversity policy (an operator mandate) needs >= minGroups distinct provider groups, which
		// is fundamentally incompatible with a single-provider directive: lava-select-provider and a sticky
		// session each pin selection to exactly ONE provider (getValidProviderAddresses returns just that
		// address). The operator policy wins (UC-1: stricter validation regardless of what the caller asked),
		// so we intentionally pass empty stickiness/selectedProvider into the diverse fetch below — but make
		// the override OBSERVABLE instead of silently discarding the caller's directive.
		if selectedProvider != "" || stickiness != "" {
			utils.LavaFormatWarning("cross-validation group-diversity policy overrides caller provider selection / stickiness", nil,
				utils.LogAttr("selectedProvider", selectedProvider),
				utils.LogAttr("stickiness", stickiness),
				utils.LogAttr("minGroups", minGroups),
				utils.LogAttr("chainID", csm.rpcEndpoint.ChainID),
				utils.LogAttr("GUID", ctx))
		}
		ranked, rankErr := csm.getValidProviderAddresses(ctx, len(csm.validAddresses), ignoredProviders.providers, cuNeededForSession, requestedBlock, addon, extensions, stateful, "", "")
		if rankErr != nil {
			utils.LavaFormatDebug(csm.rpcEndpoint.ChainID+" could not get group-diverse provider addresses", utils.LogAttr("error", rankErr), utils.LogAttr("GUID", ctx))
			return nil, rankErr
		}
		providerAddresses = csm.orderForGroupDiversity(ranked, wantedProviderNumber, minGroups, perGroupTarget)
	} else {
		providerAddresses, err = csm.getValidProviderAddresses(ctx, wantedProviderNumber, ignoredProviders.providers, cuNeededForSession, requestedBlock, addon, extensions, stateful, stickiness, selectedProvider)
		if err != nil {
			utils.LavaFormatDebug(csm.rpcEndpoint.ChainID+" could not get a provider addresses", utils.LogAttr("error", err), utils.LogAttr("GUID", ctx))
			return nil, err
		}
	}

	// save how many providers we are aiming to return
	wantedProviderNumber = len(providerAddresses)

	// Create map to save sessions with providers
	sessionWithProviderMap = make(SessionWithProviderMap, wantedProviderNumber)

	// Iterate till we fill map or do not have more
	for {
		// Iterate over providers
		for _, providerAddress := range providerAddresses {
			consumerSessionsWithProvider := csm.pairing[providerAddress]
			if consumerSessionsWithProvider == nil {
				utils.LavaFormatFatal("invalid provider address returned from csm.getValidProviderAddresses", nil,
					utils.Attribute{Key: "providerAddress", Value: providerAddress},
					utils.Attribute{Key: "all_providerAddresses", Value: providerAddresses},
					utils.Attribute{Key: "pairing", Value: csm.pairing},
					utils.Attribute{Key: "epochAtStart", Value: currentEpoch},
					utils.Attribute{Key: "currentEpoch", Value: csm.atomicReadCurrentEpoch()},
					utils.Attribute{Key: "validAddresses", Value: csm.getValidAddresses(addon, extensions, ctx)},
					utils.Attribute{Key: "wantedProviderNumber", Value: wantedProviderNumber},
					utils.LogAttr("GUID", ctx),
				)
			}
			if err := consumerSessionsWithProvider.validateComputeUnits(cuNeededForSession, virtualEpoch); err != nil {
				// Add to ignored
				ignoredProviders.providers[providerAddress] = struct{}{}
				continue
			}

			// If no error, add provider session map
			sessionWithProviderMap[providerAddress] = &SessionWithProvider{
				SessionsWithProvider: consumerSessionsWithProvider,
				CurrentEpoch:         currentEpoch,
			}
			// Add to ignored
			ignoredProviders.providers[providerAddress] = struct{}{}

			// If we have enough providers return
			if len(sessionWithProviderMap) == wantedProviderNumber {
				return sessionWithProviderMap, nil
			}
		}

		// If we do not have enough fetch more
		providerAddresses, err = csm.getValidProviderAddresses(ctx, 1, ignoredProviders.providers, cuNeededForSession, requestedBlock, addon, extensions, stateful, stickiness, "")

		// If error exists but we have providers, return them
		if err != nil && len(sessionWithProviderMap) != 0 {
			return sessionWithProviderMap, nil
		}

		// If error happens, and we do not have any provider return error
		if err != nil {
			utils.LavaFormatError("could not get a provider addresses", err, utils.LogAttr("GUID", ctx))
			return nil, err
		}
	}
}

// orderForGroupDiversity reorders a QoS-ranked address list so the front covers up to minGroups distinct
// cross-validation groups (highest-QoS provider per new group first), then fills the remaining slots by
// QoS order, returning at most `wanted` addresses. An empty GroupLabel folds into common.DefaultProviderGroup.
//
// perGroupTarget controls how many providers Phase 1 front-loads from each covered group: 1 (the default)
// reproduces the original "one highest-QoS provider per group" behavior; a larger value (the per-group
// quorum's agreement threshold) front-loads up to that many highest-QoS providers from each of minGroups
// groups, so each group can independently reach its internal quorum before QoS fill claims the slots.
// Must be called with csm.lock held (reads csm.pairing).
func (csm *ConsumerSessionManager) orderForGroupDiversity(ranked []string, wanted, minGroups, perGroupTarget int) []string {
	if perGroupTarget < 1 {
		perGroupTarget = 1
	}
	groupOf := func(addr string) string {
		if cswp, ok := csm.pairing[addr]; ok && cswp.GroupLabel != "" {
			return cswp.GroupLabel
		}
		return common.DefaultProviderGroup
	}

	// Count how many providers each group has available in the ranked pool, so Phase 1 can prefer groups
	// that can actually reach perGroupTarget. Without this, a higher-QoS but under-provisioned group (e.g.
	// one with a single provider) could be opened ahead of a viable one, be unable to reach its internal
	// quorum, and spuriously fail per-group quorum even though a satisfiable set existed.
	groupAvail := make(map[string]int)
	for _, addr := range ranked {
		groupAvail[groupOf(addr)]++
	}

	picked := make([]string, 0, wanted)
	pickedSet := make(map[string]struct{}, wanted)
	groupPickCount := make(map[string]int, minGroups)

	// Phase 1: front-load up to perGroupTarget highest-QoS providers from each of minGroups groups. Run two
	// passes over the QoS-ranked list: the first opens only groups that can reach the full perGroupTarget;
	// the second (a best-effort fallback) opens any remaining group when too few viable groups exist — the
	// per-group gate then fails the request rather than under-validating. In both passes a group is filled
	// only up to perGroupTarget, and no more than minGroups groups are opened.
	phase1 := func(canOpen func(group string) bool) {
		for _, addr := range ranked {
			if len(picked) >= wanted {
				return
			}
			g := groupOf(addr)
			if _, opened := groupPickCount[g]; !opened {
				if len(groupPickCount) >= minGroups || !canOpen(g) {
					continue
				}
			}
			if groupPickCount[g] >= perGroupTarget {
				continue
			}
			picked = append(picked, addr)
			pickedSet[addr] = struct{}{}
			groupPickCount[g]++
		}
	}
	phase1(func(g string) bool { return groupAvail[g] >= perGroupTarget }) // prefer groups that can reach the target
	phase1(func(string) bool { return true })                              // fallback: open any remaining group best-effort

	// Phase 2: fill remaining slots by QoS order with providers not already picked.
	for _, addr := range ranked {
		if len(picked) >= wanted {
			break
		}
		if _, already := pickedSet[addr]; already {
			continue
		}
		picked = append(picked, addr)
		pickedSet[addr] = struct{}{}
	}
	return picked
}

// must be locked before use
func (csm *ConsumerSessionManager) sortBlockedProviderListByCuServed() {
	// Defining the custom sorting rule (used cu per provider)
	// descending order of cu used (highest to lowest)
	customSort := func(i, j int) bool {
		return csm.pairing[csm.currentlyBlockedProviderAddresses[i]].atomicReadUsedComputeUnits() > csm.pairing[csm.currentlyBlockedProviderAddresses[j]].atomicReadUsedComputeUnits()
	}
	// Sort the slice using the custom sorting rule
	sort.Slice(csm.currentlyBlockedProviderAddresses, customSort)
}

// removes a given address from the valid addresses list, recording WHY it was blocked (MAG-2599).
//
// Deliberately not logged here. Two callers reach this with very different volume — blockProvider,
// once per real block, and UpdateAllProviders' epoch re-block pass, once per carried-over provider
// every epoch — so the operator-facing INFO lives in blockProvider, where the full outcome is known
// and the epoch pass cannot reach it.
func (csm *ConsumerSessionManager) removeAddressFromValidAddresses(address string, record BlockRecord) error {
	// cs Must be Locked here.
	for idx, addr := range csm.validAddresses {
		if addr == address {
			// remove the index from the valid list.
			csm.validAddresses = append(csm.validAddresses[:idx], csm.validAddresses[idx+1:]...)
			csm.RemoveAddonAddresses("", nil)
			// add the address to our block provider list.
			csm.currentlyBlockedProviderAddresses = append(csm.currentlyBlockedProviderAddresses, address)
			csm.blockedProviderRecords[address] = record
			// sort the blocked provider list by cu served
			csm.sortBlockedProviderListByCuServed()
			provider, ok := csm.pairing[addr]
			if ok {
				info := csm.RPCEndpoint()
				go func(networkAddress string, chainId string, apiInterface string, providerAddress string) {
					csm.consumerMetricsManager.SetBlockedProvider(chainId, apiInterface, providerAddress, networkAddress, true)
				}(provider.Endpoints[0].NetworkAddress, info.ChainID, info.ApiInterface, addr)
			}
			return nil
		}
	}
	return AddressIndexWasNotFoundError
}

// blockDetail renders the discriminating number a block call site carried, for the operator-facing
// record. Both counters are zero on paths that carry neither (the dead-session cap), which is
// itself the honest answer: nothing was counted, the allowance was simply reached.
func blockDetail(disconnections, consecutiveErrors uint64) string {
	switch {
	case disconnections > 0 && consecutiveErrors > 0:
		return fmt.Sprintf("disconnections=%d consecutiveErrors=%d", disconnections, consecutiveErrors)
	case disconnections > 0:
		return fmt.Sprintf("disconnections=%d", disconnections)
	case consecutiveErrors > 0:
		return fmt.Sprintf("consecutiveErrors=%d", consecutiveErrors)
	default:
		return ""
	}
}

// Blocks a provider making him unavailable for pick this epoch, will also report him as unavailable if reportProvider is set to true.
// Validates that the sessionEpoch is equal to cs.currentEpoch otherwise doesn't take effect.
//
// reason names WHY, and is recorded against the address for as long as the block stands so
// /debug/provider-routing, the per-reason gauge and the log line below can all answer the question
// (MAG-2599). Every call site names one; there is no default.
func (csm *ConsumerSessionManager) blockProvider(ctx context.Context, address string, reason BlockReason, reportProvider bool, sessionEpoch uint64, disconnections uint64, consecutiveErrors uint64, allowSecondChance bool, reconnectCallback func() error) error {
	utils.LavaFormatDebug("🔒 BLOCKING PROVIDER", utils.LogAttr("address", address), utils.LogAttr("reason", reason), utils.LogAttr("GUID", ctx))

	// find Index of the address
	if sessionEpoch != csm.atomicReadCurrentEpoch() { // we read here atomically so cs.currentEpoch cant change in the middle, so we can save time if epochs mismatch
		return EpochMismatchError
	}

	var runSecondChance bool
	// Set only once the provider is genuinely out of rotation, so the INFO below cannot fire for a
	// block that did not happen — an epoch mismatch, or an address in neither pool.
	var blocked *BlockRecord
	var blockedCount, validRemaining int

	csm.lock.Lock() // we lock RW here because we need to make sure nothing changes while we verify validAddresses/addedToPurgeAndReport
	// on unlock we also want to trigger a routine that will remove blocked providers from block list if they exist and we allow them a second chance
	defer func() {
		csm.lock.Unlock()

		// Logged after the unlock, not under it: this is the one operator-facing line for the whole
		// block decision, and formatting it is not worth holding the write lock the relay path
		// needs. Every field was captured above while the lock was held.
		if blocked != nil {
			utils.LavaFormatInfo("provider blocked",
				utils.LogAttr("address", address),
				utils.LogAttr("block_reason", blocked.Reason),
				utils.LogAttr("detail", blocked.Detail),
				utils.LogAttr("reported", blocked.Reported),
				utils.LogAttr("second_chance", blocked.SecondChanceGranted),
				utils.LogAttr("scope", blockScope(blocked.Backup)),
				utils.LogAttr("blocked_count", blockedCount),
				utils.LogAttr("valid_remaining", validRemaining),
				utils.LogAttr("GUID", ctx),
			)
		}

		if runSecondChance {
			// Read on the CALLING goroutine, not inside the timer. retrySecondChanceAfter is a
			// package var that tests rewrite, and reading it from the spawned goroutine has no
			// happens-before edge to that write — a real (if benign in production) cross-goroutine
			// read, and enough to make `go test -race` on this package fail. Hoisting it creates the
			// edge the tests already assume, and no test has to know about it.
			retryAfter := retrySecondChanceAfter
			// if we decide to allow a second chance, this provider will return to our list of valid providers (if it exists)
			go func() {
				<-time.After(retryAfter)
				// check epoch is still relevant, if not just return
				if sessionEpoch != csm.atomicReadCurrentEpoch() {
					return
				}
				utils.LavaFormatDebug("Running second chance for provider", utils.LogAttr("address", address), utils.LogAttr("GUID", ctx))
				csm.validateAndReturnBlockedProviderToValidAddressesList(address, ReleaseSecondChanceTimer)
			}()
		}
	}()

	if sessionEpoch != csm.atomicReadCurrentEpoch() { // After we lock we need to verify again that the epoch didn't change while we waited for the lock.
		return EpochMismatchError
	}

	// Reported is filled in after the report flow below, not from reportProvider: asking to report
	// and actually reporting are different outcomes. A first offence with allowSecondChance set
	// takes the second chance INSTEAD of being reported, so reportProvider=true would claim a
	// register entry that does not exist — and the epoch's release pass reads that field.
	record := BlockRecord{
		Reason: reason,
		Since:  time.Now(),
		Detail: blockDetail(disconnections, consecutiveErrors),
	}

	err := csm.removeAddressFromValidAddresses(address, record)
	if err != nil {
		if errors.Is(err, AddressIndexWasNotFoundError) {
			// Address not in validAddresses — check if it's a backup provider and block it there.
			if _, isBackup := csm.backupProviders[address]; isBackup {
				// Already out — leave the first reason and Since standing, and emit nothing. The
				// regular path gets this for free (removeAddressFromValidAddresses returns
				// AddressIndexWasNotFoundError once the address has left validAddresses); backups
				// had no equivalent, so a failing backup re-fired the INFO on every block-triggering
				// failure, reset its Since so blocked_for never grew, and overwrote the reason that
				// actually took it out with whatever failed last.
				if _, alreadyBlocked := csm.blockedBackupProviders[address]; !alreadyBlocked {
					csm.blockedBackupProviders[address] = struct{}{}
					record.Backup = true
					csm.blockedProviderRecords[address] = record
					blocked = &record
				}
			} else {
				utils.LavaFormatDebug("address was not found in valid addresses list", utils.Attribute{Key: "address", Value: address}, utils.Attribute{Key: "error", Value: err}, utils.Attribute{Key: "validAddresses", Value: csm.validAddresses}, utils.LogAttr("GUID", ctx))
			}
		} else {
			return err
		}
	} else {
		blocked = &record
	}

	reportedNow := false
	if reportProvider { // Report provider flow
		if allowSecondChance { // on epoch change, we don't report providers immediately we allow them a recovery phase.
			if _, ok := csm.secondChanceGivenToAddresses[address]; ok {
				// already exists in second chance, need to block.
				csm.reportedProviders.ReportProvider(address, consecutiveErrors, disconnections, reconnectCallback)
				reportedNow = true
			} else {
				// first time reported, allowing a second chance.
				csm.secondChanceGivenToAddresses[address] = struct{}{}
				// Mark the provider as on probation: it has now consumed its single
				// second chance. A later successful relay (OnSessionDone) clears both
				// this flag and the secondChanceGivenToAddresses entry, so genuine
				// recovery renews eligibility. Without this, in direct-rpc mode (no
				// epoch transitions to clear the map) any provider that trips a block
				// twice — even with full health in between — is hard-blocked for the
				// rest of the process lifetime.
				if provider, ok := csm.pairing[address]; ok {
					provider.atomicMarkSecondChanceProbation()
				}
				// address was removed from valid addresses, we can still return it after a duration for second chance.
				runSecondChance = true
			}
		} else {
			csm.reportedProviders.ReportProvider(address, consecutiveErrors, disconnections, reconnectCallback)
			reportedNow = true
		}
	}

	// The second-chance decision is only known now, and it is the difference between a provider that
	// returns on its own in three minutes and one that does not — so the stored record and the log
	// both have to wait for it.
	if blocked != nil {
		blocked.Reported = reportedNow
		// A backup never gets a second chance it can use: the timer calls
		// validateAndReturnBlockedProviderToValidAddressesList, which walks only
		// currentlyBlockedProviderAddresses, so a provider held in blockedBackupProviders is never
		// found and the timer returns having done nothing. Recording the grant anyway would have the
		// record assert a recovery that cannot happen — and combined with Reported=false it would
		// claim BOTH that the reconnect loop will never look at it and that it comes back on its own.
		//
		// That backups are not released by the timer is pre-existing and is NOT changed here; this
		// only stops the record advertising it. Whether the timer should release backups, or whether
		// backups should skip the chance and be reported immediately, is a behaviour question filed
		// alongside the epoch clean-slate defect.
		blocked.SecondChanceGranted = runSecondChance && !blocked.Backup
		csm.blockedProviderRecords[address] = *blocked
		blockedCount = csm.blockedTotalLocked()
		validRemaining = len(csm.validAddresses)
	} else {
		csm.refreshBlockOutcomeLocked(address, reportedNow, runSecondChance)
	}

	return nil
}

// refreshBlockOutcomeLocked updates the record of an ALREADY-blocked provider with the outcome of a
// repeat block.
//
// A repeat call takes the provider out of nothing — it is already out — so the identity of the block
// (Reason, Since, Detail) stays with the call that actually did it. But the report flow runs
// regardless of that, and the call that genuinely REPORTS a provider is usually the second one: the
// first offence takes the second chance instead, and the repeat finds secondChanceGivenToAddresses
// already set and reports. On that call blocked is nil, so without this the record keeps Reported =
// false forever — the map is edge-triggered and the next edge is the release.
//
// That inverts the contract BlockRecord documents: Reported == false is supposed to mean the
// 30-second reconnect loop will never look at this provider, so an operator reads
// /debug/provider-routing and concludes manual intervention is needed while the reconnect loop is in
// fact already working on it.
//
// Neither field is ever cleared here: a provider that has been reported stays reported for as long
// as the block stands, and a timer that was started cannot be unstarted. csm.lock must be held.
func (csm *ConsumerSessionManager) refreshBlockOutcomeLocked(address string, reportedNow, secondChanceGranted bool) {
	if !reportedNow && !secondChanceGranted {
		return
	}
	record, ok := csm.blockedProviderRecords[address]
	if !ok {
		return // not blocked in either store — nothing this call did, and nothing to refresh
	}
	changed := false
	if reportedNow && !record.Reported {
		record.Reported = true
		changed = true
	}
	// Same backup carve-out as the fresh-block path above: the timer cannot release a backup, so the
	// record must not claim it will.
	if secondChanceGranted && !record.SecondChanceGranted && !record.Backup {
		record.SecondChanceGranted = true
		changed = true
	}
	if changed {
		csm.blockedProviderRecords[address] = record
	}
}

// blockScope renders which pool a block landed in, for logs and debug output.
func blockScope(backup bool) string {
	if backup {
		return "backup"
	}
	return "primary"
}

// releaseWithoutPenalty is the shared body of the two "this told us nothing about the
// provider" release paths. It returns the reserved compute units and unlocks the session,
// deliberately recording NO QoS failure, NO consecutive provider error, and NO optimizer
// availability sample. caller names the entry point so a lock-order violation still
// reports the site that caused it.
func (csm *ConsumerSessionManager) releaseWithoutPenalty(consumerSession *SingleConsumerSession, reason error, caller string) error {
	if err := consumerSession.VerifyLock(); err != nil {
		return fmt.Errorf("%s, consumerSession.lock must be locked before accessing this method: %w", caller, err)
	}

	cuToDecrease := consumerSession.LatestRelayCu
	parentConsumerSessionsWithProvider := consumerSession.Parent
	consumerSession.LatestRelayCu = 0
	consumerSession.Free(reason)

	return parentConsumerSessionsWithProvider.decreaseUsedComputeUnits(cuToDecrease)
}

// OnSessionDiscarded releases a session that was selected but intentionally
// dropped before any relay was dispatched. It returns the reserved compute
// units and unlocks the session without recording a QoS failure or adding a
// consecutive provider error: no upstream request was made, so availability
// was not actually tested.
func (csm *ConsumerSessionManager) OnSessionDiscarded(consumerSession *SingleConsumerSession, reason error) error {
	return csm.releaseWithoutPenalty(consumerSession, reason, "OnSessionDiscarded")
}

// OnSessionCancelled releases a session whose relay WAS dispatched but was then cancelled
// by us — a relay-race loser on a stateful broadcast, or a client that hung up — rather
// than answered or failed by the endpoint (MAG-2648).
//
// It shares OnSessionDiscarded's accounting, and for the same reason: the endpoint's
// availability was never actually tested. It was mid-flight and we told it to stop. Routing
// these through OnSessionFailure is the bug — that path calls AddFailedRelay (which lands
// in the availability ratio), appends a consecutive error, and feeds the optimizer an
// availability sample of 0, penalising a node that did nothing wrong. On a broadcast the
// fastest node wins and EVERY other healthy node takes that hit, so the damage is
// structural rather than correlated with node quality.
//
// It is a separate name from OnSessionDiscarded on purpose: "discarded" means never sent,
// and a reader at the call site needs to be able to tell those apart even though the
// bookkeeping is identical today.
func (csm *ConsumerSessionManager) OnSessionCancelled(consumerSession *SingleConsumerSession, reason error) error {
	return csm.releaseWithoutPenalty(consumerSession, reason, "OnSessionCancelled")
}

// OnSessionRateLimited releases a session whose relay the upstream refused for rate. A
// rate-limit is neither failure nor success — the endpoint is healthy but busy, and it
// was not exercised — so no QoS sample lands in either direction. The hold-off registry,
// not session scoring, is what keeps traffic away from it (docs/RATE-LIMIT-HOLDOFF.md).
func (csm *ConsumerSessionManager) OnSessionRateLimited(consumerSession *SingleConsumerSession, reason error) error {
	return csm.releaseWithoutPenalty(consumerSession, reason, "OnSessionRateLimited")
}

// Report session failure, mark it as blocked from future usages, report if timeout happened.
func (csm *ConsumerSessionManager) OnSessionFailure(consumerSession *SingleConsumerSession, errorReceived error) error {
	// consumerSession must be locked when getting here.
	if err := consumerSession.VerifyLock(); err != nil {
		return fmt.Errorf("OnSessionFailure, consumerSession.lock must be locked before accessing this method, additional info: %w", err)
	}
	// redemptionSession = true, if we got this provider from the blocked provider list.
	// if so, it means we already reported this provider and blocked it we do not need to do it again.
	// due to session failure we also don't need to remove it from the blocked provider list.
	// we will just update the QOS info, and return
	redemptionSession := consumerSession.Parent.atomicReadBlockedStatus() == BlockedProviderSessionUsedStatus

	// consumer Session should be locked here. so we can just apply the session failure here.
	if consumerSession.BlockListed {
		// if consumer session is already blocklisted, free it and return an error.
		// CRITICAL: Must call Free() to release the lock even when returning early
		consumerSession.Free(errorReceived)
		return fmt.Errorf("trying to report a session failure of a blocklisted consumer session: %w", SessionIsAlreadyBlockListedError)
	}

	// check if need to block & report
	var blockProvider, reportProvider bool
	// Why we would block, for the operator-facing record. Overwritten below if the
	// never-served-a-relay rule fires instead of (or as well as) an explicit sentinel.
	blockReason := BlockReasonExplicitSignal
	if errors.Is(errorReceived, ReportAndBlockProviderError) {
		blockProvider = true
		reportProvider = true
	} else if errors.Is(errorReceived, BlockProviderError) {
		blockProvider = true
	}

	if errors.Is(errorReceived, BlockEndpointError) {
		utils.LavaFormatTrace("Got BlockEndpointError, blocking endpoint and session",
			utils.LogAttr("error", errorReceived),
			utils.LogAttr("sessionID", consumerSession.SessionId),
		)

		// Block the endpoint and the consumer session from future usages
		// Only block EndpointConnection for provider-relay sessions
		if consumerSession.EndpointConnection != nil {
			consumerSession.EndpointConnection.blockListed.Store(true)
		}
		consumerSession.BlockListed = true
	}

	csm.qosManager.AddFailedRelay(consumerSession.epoch, consumerSession.SessionId)
	consumerSession.ConsecutiveErrors = append(consumerSession.ConsecutiveErrors, errorReceived)
	consumerSession.errorsCount += 1
	// set allow second change if we want to allow the provider to return the pool without being reported if the downtime was temporary.
	allowSecondChance := false
	// if this session failed more than MaximumNumberOfFailuresAllowedPerConsumerSession times or session went out of sync we block it.
	if len(consumerSession.ConsecutiveErrors) > MaximumNumberOfFailuresAllowedPerConsumerSession || IsSessionSyncLoss(errorReceived) {
		utils.LavaFormatInfo("Blocking consumer session",
			utils.LogAttr("ConsecutiveErrors", consumerSession.ConsecutiveErrors),
			utils.LogAttr("errorsCount", consumerSession.errorsCount),
			utils.LogAttr("id", consumerSession.SessionId),
		)
		consumerSession.BlockListed = true // block this session from future usages

		// check if this session is a redemption session meaning we already blocked and reported the provider if it was necessary.
		if !redemptionSession {
			// we will check the total number of cu for this provider and decide if we need to report it.
			if consumerSession.Parent.atomicReadUsedComputeUnits() <= consumerSession.LatestRelayCu { // if we had 0 successful relays and we reached block session we need to report this provider
				blockProvider = true
				reportProvider = true
				allowSecondChance = true
				blockReason = BlockReasonNeverServed
			}
		}
	}
	cuToDecrease := consumerSession.LatestRelayCu
	// latency, isHangingApi, syncScore aren't updated when there is a failure
	go csm.providerOptimizer.AppendRelayFailure(consumerSession.Parent.PublicLavaAddress)
	consumerSession.LatestRelayCu = 0 // making sure no one uses it in a wrong way
	consecutiveErrors := uint64(len(consumerSession.ConsecutiveErrors))
	parentConsumerSessionsWithProvider := consumerSession.Parent // must read this pointer before unlocking
	csm.updateMetricsManager(consumerSession, time.Duration(0), false)
	// finished with consumerSession here can unlock.
	consumerSession.Free(errorReceived) // we unlock before we change anything in the parent ConsumerSessionsWithProvider

	err := parentConsumerSessionsWithProvider.decreaseUsedComputeUnits(cuToDecrease) // change the cu in parent
	if err != nil {
		return err
	}

	if !redemptionSession && blockProvider {
		publicProviderAddress, pairingEpoch := parentConsumerSessionsWithProvider.getPublicLavaAddressAndPairingEpoch()
		err = csm.blockProvider(context.Background(), publicProviderAddress, blockReason, reportProvider, pairingEpoch, 0, consecutiveErrors, allowSecondChance, nil)
		if err != nil {
			if errors.Is(err, EpochMismatchError) {
				return nil // no effects this epoch has been changed
			}
			return err
		}
	}
	return nil
}

// validating if the provider is currently not in valid addresses list. if the session was successful we can return the provider
// to our valid addresses list and resume its usage
//
// route names WHO is releasing it. There are more ways back into the pool than out of it and they
// can undo one another — the health probe in particular can release a provider on cheap-poll
// evidence moments after real relay traffic blocked it — so the route is logged alongside the
// reason the provider was blocked in the first place (MAG-2599). Grepping one address then yields
// the block and its matching release.
func (csm *ConsumerSessionManager) validateAndReturnBlockedProviderToValidAddressesList(providerAddress string, route ReleaseRoute) {
	csm.lock.Lock()
	defer csm.lock.Unlock()
	csm.validateAndReturnBlockedProviderToValidAddressesListLocked(providerAddress, route)
}

// internal version that assumes csm.lock is already held
func (csm *ConsumerSessionManager) validateAndReturnBlockedProviderToValidAddressesListLocked(providerAddress string, route ReleaseRoute) {
	for idx, addr := range csm.currentlyBlockedProviderAddresses {
		if addr == providerAddress {
			// Remove it from the csm.currentlyBlockedProviderAddresses
			csm.currentlyBlockedProviderAddresses = append(csm.currentlyBlockedProviderAddresses[:idx], csm.currentlyBlockedProviderAddresses[idx+1:]...)
			record, released := csm.releaseBlockRecordLocked(providerAddress, false)
			// Reapply it to the valid addresses — but never duplicate (an address blocked once is
			// absent from validAddresses, yet a concurrent restore path makes the guard cheap insurance).
			if !slices.Contains(csm.validAddresses, addr) {
				csm.validAddresses = append(csm.validAddresses, addr)
			}
			// Purge the current addon addresses so it will be created again next time get session is called.
			csm.RemoveAddonAddresses("", nil)
			// Reset redemption status
			if provider, ok := csm.pairing[providerAddress]; ok {
				info := csm.RPCEndpoint()
				provider.atomicWriteBlockedStatus(BlockedProviderSessionUnusedStatus)
				go func(networkAddress string, chainId string, apiInterface string, providerAddress string) {
					csm.consumerMetricsManager.SetBlockedProvider(chainId, apiInterface, providerAddress, networkAddress, false)
				}(provider.Endpoints[0].NetworkAddress, info.ChainID, info.ApiInterface, providerAddress)
			}
			if released {
				csm.logProviderReleased(providerAddress, record, route, csm.blockedTotalLocked())
			}
			return
		}
	}
	// if we didn't find it, we might had two sessions in parallel and thats ok. the first one dealt with it we can just return
}

// blockRecordOrUnknownLocked returns a blocked provider's record, or a placeholder when none is
// stored. csm.lock must be held.
//
// The placeholder fails CLOSED: Reported is true, so an unknown block faces the epoch's probe rather
// than being waved through. That field decides whether a re-blocked provider has to prove itself,
// and its zero value would read as "never reported, no evidence against it, let it back in" — which
// is precisely the free clean slate the re-block exists to prevent. When we do not know why a
// provider is out, the safe answer is to make it prove itself, not to assume the best.
//
// Reaching this means a block path did not record a reason, which is a bug in that path rather than
// a state to tolerate quietly — hence the warning.
func (csm *ConsumerSessionManager) blockRecordOrUnknownLocked(providerAddress string) BlockRecord {
	if record, ok := csm.blockedProviderRecords[providerAddress]; ok {
		return record
	}
	utils.LavaFormatWarning("blocked provider carried no block record at the epoch boundary", nil,
		utils.LogAttr("address", providerAddress),
	)
	return BlockRecord{Reason: BlockReasonPreviousEpoch, Since: time.Now(), Reported: true}
}

// blockedTotalLocked is how many providers are out across both pools — the number an operator wants
// beside a block or release line. csm.lock must be held.
func (csm *ConsumerSessionManager) blockedTotalLocked() int {
	return len(csm.currentlyBlockedProviderAddresses) + len(csm.blockedBackupProviders)
}

// releaseBlockRecordLocked is called after an address has been removed from ONE of the two blocked
// stores. releasedBackup names which one. It hands back the record describing the block that just
// ended, and reports whether there was one to end.
//
// The two stores hold one record between them, so the release has to ask about both — a provider
// configured as regular AND backup can be blocked in each (the epoch re-block pass does exactly
// that). Dropping the record on the first release would strip the reason from a block that is still
// standing, leaving a genuinely blocked provider with no reason on /debug/provider-routing, missing
// from the per-reason gauge, and eventually released with no log line at all. So the record survives
// while the other store holds the address, re-pointed at the block that remains.
//
// A surviving record does NOT mean the caller should stay quiet. The two stores gate different
// traffic: currentlyBlockedProviderAddresses keeps a provider out of ordinary selection, while
// blockedBackupProviders only gates the backup fallback. Releasing the regular block of a dual-pool
// provider genuinely returns it to service even though its backup block stands — reading that as
// "still idle, say nothing" would hide a real return to service, which is the gap this reporting
// exists to close. So every block that ends is announced, carrying the scope of the block that
// ended rather than of one that remains.
//
// Call it ONLY after actually removing the address from a store — a missing record then means a
// block path failed to write one, which is why that case warns. csm.lock must be held.
func (csm *ConsumerSessionManager) releaseBlockRecordLocked(providerAddress string, releasedBackup bool) (BlockRecord, bool) {
	record, hadRecord := csm.blockedProviderRecords[providerAddress]
	if !hadRecord {
		// The invariant is that a record exists for every blocked address. Reaching here means some
		// block path did not write one — worth surfacing rather than quietly logging "unspecified".
		utils.LavaFormatWarning("released a blocked provider that carried no block record", nil,
			utils.LogAttr("address", providerAddress),
		)
		return BlockRecord{Reason: BlockReasonUnspecified}, false
	}

	ended := record
	ended.Backup = releasedBackup

	if backupRemains, stillBlocked := csm.remainingBlockScopeLocked(providerAddress); stillBlocked {
		record.Backup = backupRemains
		csm.blockedProviderRecords[providerAddress] = record
	} else {
		delete(csm.blockedProviderRecords, providerAddress)
	}
	return ended, true
}

// remainingBlockScopeLocked reports whether the address is still held by either blocked store, and
// if so whether the remaining hold is the backup one. csm.lock must be held.
func (csm *ConsumerSessionManager) remainingBlockScopeLocked(providerAddress string) (backup, stillBlocked bool) {
	if _, ok := csm.blockedBackupProviders[providerAddress]; ok {
		return true, true
	}
	if slices.Contains(csm.currentlyBlockedProviderAddresses, providerAddress) {
		return false, true
	}
	return false, false
}

// logProviderReleased emits the one operator-facing line for a provider returning to routing.
//
// Logged under csm.lock, unlike its counterpart in blockProvider: this helper is reached from six
// call sites, two of which are already inside a caller-held lock, and threading the fields back out
// of all of them would cost more clarity than the lock hold is worth. Releases are rare and the
// line is a handful of preformatted fields.
func (csm *ConsumerSessionManager) logProviderReleased(providerAddress string, record BlockRecord, route ReleaseRoute, blockedCount int) {
	utils.LavaFormatInfo("provider released",
		utils.LogAttr("address", providerAddress),
		utils.LogAttr("released_by", route),
		utils.LogAttr("block_reason", record.Reason),
		utils.LogAttr("blocked_for", record.blockedFor(time.Now()).String()),
		utils.LogAttr("scope", blockScope(record.Backup)),
		utils.LogAttr("blocked_count", blockedCount),
	)
}

// forgetSecondChanceGiven removes a provider from the second-chance memory after
// it has proven recovery with a successful relay (see OnSessionDone). This lets a
// future isolated failure be treated as a first offense again instead of an
// immediate hard block. Must NOT be called while holding csm.lock.
func (csm *ConsumerSessionManager) forgetSecondChanceGiven(providerAddress string) {
	csm.lock.Lock()
	defer csm.lock.Unlock()
	delete(csm.secondChanceGivenToAddresses, providerAddress)
}

// RestoreRecoveredProvider returns a probe-recovered provider to normal routing (F2). The prober
// (Topic D) re-enables an endpoint's transport bit (Endpoint.Enabled) when it proves recovery, but
// the endpoint can still be unroutable because its PROVIDER is blocked at the session-manager level:
// a regular provider in currentlyBlockedProviderAddresses (absent from validAddresses), or a backup
// in blockedBackupProviders. This restores both so selection can reach it again without waiting for
// the next epoch — the whole point of proactive recovery.
//
// It is idempotent (safe to call once per recovered endpoint even when several endpoints of one
// provider recover in the same cycle) and handles a provider that sits in BOTH pools. The prober
// calls this AFTER Endpoint.RecordProbeVerdict has returned (the endpoint mutex is already released),
// so taking csm.lock here introduces no endpoint.mu→csm.lock nesting.
func (csm *ConsumerSessionManager) RestoreRecoveredProvider(providerAddress string) {
	csm.lock.Lock()
	// Regular provider: move it back from the blocked list to validAddresses (also clears redemption
	// status + blocked metric). A no-op if it isn't in the blocked list.
	csm.validateAndReturnBlockedProviderToValidAddressesListLocked(providerAddress, ReleaseHealthProbe)
	// Backup provider: drop it from the blocked-backup set (a no-op if absent). Independent of the
	// regular pool — a provider overlapping both is restored in both.
	if _, blocked := csm.blockedBackupProviders[providerAddress]; blocked {
		delete(csm.blockedBackupProviders, providerAddress)
		if record, released := csm.releaseBlockRecordLocked(providerAddress, true); released {
			csm.logProviderReleased(providerAddress, record, ReleaseHealthProbe, csm.blockedTotalLocked())
		}
	}
	csm.lock.Unlock()

	// Clear any standing report so a re-blocked provider isn't immediately re-blocked at epoch flip.
	// RemoveReport takes its own lock; call it outside csm.lock to avoid any lock-order coupling.
	csm.reportedProviders.RemoveReport(providerAddress)
}

// On a successful session this function will update all necessary fields in the consumerSession. and unlock it when it finishes
func (csm *ConsumerSessionManager) OnSessionDone(
	consumerSession *SingleConsumerSession,
	latestServicedBlock int64,
	specComputeUnits uint64,
	currentLatency time.Duration,
	expectedLatency time.Duration,
	syncGap int64,
	numOfProviders int,
	providersCount uint64,
	isHangingApi bool,
	extensions []*spectypes.Extension,
) error {
	// release locks, update CU, relaynum etc..
	if err := consumerSession.VerifyLock(); err != nil {
		return fmt.Errorf("OnSessionDone, consumerSession.lock must be locked before accessing this method: %w", err)
	}

	if consumerSession.Parent.atomicReadBlockedStatus() == BlockedProviderSessionUsedStatus {
		// we will deal with the removal of this provider from the blocked list so we can for now set it as default
		consumerSession.Parent.atomicWriteBlockedStatus(BlockedProviderSessionUnusedStatus)
		// this provider is probably in the ignored provider list. we need to validate and return it to valid addresses
		providerAddress := consumerSession.Parent.PublicLavaAddress
		// we want this method to run last after we unlock the consumer session
		// golang defer operates in a Last-In-First-Out (LIFO) order, meaning this defer will run last.
		defer func() {
			go csm.validateAndReturnBlockedProviderToValidAddressesList(providerAddress, ReleaseSuccessfulRelay)
		}()
	}

	// A successful relay proves the provider has recovered. If it was on
	// second-chance probation, forget that it ever used its second chance so a
	// future isolated failure is treated as a first offense again (gets a fresh
	// 3-minute second chance) instead of an immediate, lifetime-long hard block.
	// The atomic CAS ensures exactly one cleanup is scheduled across concurrent relays.
	if consumerSession.Parent.atomicTryClearSecondChanceProbation() {
		recoveredProviderAddress := consumerSession.Parent.PublicLavaAddress
		defer func() { go csm.forgetSecondChanceGiven(recoveredProviderAddress) }()
	}

	defer consumerSession.Free(nil)                        // we need to be locked here, if we didn't get it locked we try lock anyway
	consumerSession.CuSum += consumerSession.LatestRelayCu // add CuSum to current cu usage.
	consumerSession.LatestRelayCu = 0                      // reset cu just in case
	consumerSession.ConsecutiveErrors = []error{}
	consumerSession.LatestBlock = latestServicedBlock // update latest serviced block
	// calculate QoS - syncGap is the difference between expected and actual block height (0 if not tracked)
	csm.qosManager.CalculateQoS(csm.atomicReadCurrentEpoch(), consumerSession.SessionId, consumerSession.Parent.PublicLavaAddress, currentLatency, expectedLatency, syncGap, numOfProviders, int64(providersCount))
	if !isHangingApi {
		// Append relay data only for non-hanging apis. Two composed fixes:
		//
		// MAG-1748: latestServicedBlock is the GLOBAL chain-tracker head for methods whose response
		// body carries no block height (eth_getBalance/eth_call/eth_estimateGas) — identical for every
		// provider, so feeding it would leave the per-provider sync-score blind to a stale provider.
		// Prefer the per-endpoint ChainTracker block (Endpoint.LatestBlock, kept current regardless of
		// method) so a lagging provider is actually demoted.
		syncBlock := uint64(latestServicedBlock)
		if drsc, ok := consumerSession.Connection.(*DirectRPCSessionConnection); ok && drsc.Endpoint != nil {
			// Read the per-endpoint tip from the shared single-source-of-truth store (keyed
			// by chain+apiInterface+url). This used to read drsc.Endpoint.LatestBlock, a
			// second copy written ungated; the store is fed through the gated poll/relay
			// observers, so a lagging provider is demoted against a consistent tip.
			//
			// It can also carry a PEER pod's observation (the fleet tracker gate), i.e. a height
			// this pod did not itself serve. Accepted deliberately — block height is a property
			// of the endpoint, not of the path to it — but it means a provider lagging only on
			// THIS pod's path may not be demoted.
			info := csm.RPCEndpoint()
			tipKey := endpointtip.Key(info.ChainID, info.ApiInterface, drsc.Endpoint.NetworkAddress)
			if endpointBlock := endpointtip.Default().Block(tipKey); endpointBlock > 0 {
				syncBlock = uint64(endpointBlock)
			}
		}
		// F4/F5: resolve THIS interface's consensus baseline so the optimizer measures sync lag
		// against the agreed tip — or omits sync when there is no fresh majority — instead of the
		// shared-getter/max-across-providers behavior. The per-endpoint block above is the provider's
		// observed position; syncRef is the reference it is compared against.
		syncRef := csm.resolveSyncReference()
		go csm.providerOptimizer.AppendRelayDataConsensus(consumerSession.Parent.PublicLavaAddress, currentLatency, specComputeUnits, syncBlock, syncRef)
	}

	csm.updateMetricsManager(consumerSession, currentLatency, !isHangingApi) // apply latency only for non hanging apis
	return nil
}

// updates QoS metrics for a provider
// consumerSession should still be locked when accessing this method as it fetches information from the session it self
func (csm *ConsumerSessionManager) updateMetricsManager(consumerSession *SingleConsumerSession, relayLatency time.Duration, sessionSuccessful bool) {
	if csm.consumerMetricsManager == nil {
		return
	}
	info := csm.RPCEndpoint()
	apiInterface := info.ApiInterface
	chainId := info.ChainID

	var lastQos *pairingtypes.QualityOfServiceReport
	lastQoSReport := csm.qosManager.GetLastQoSReport(csm.atomicReadCurrentEpoch(), consumerSession.SessionId)
	if lastQoSReport != nil {
		qos := *lastQoSReport
		lastQos = &qos
	}

	var lastReputation *pairingtypes.QualityOfServiceReport
	lastReputationReport := csm.qosManager.GetLastReputationQoSReport(csm.atomicReadCurrentEpoch(), consumerSession.SessionId)
	if lastReputationReport != nil {
		qosRep := *lastReputationReport
		lastReputation = &qosRep
	}
	publicProviderAddress := consumerSession.Parent.PublicLavaAddress
	publicProviderEndpoint := consumerSession.Parent.Endpoints[0].NetworkAddress
	// Capture the session-owned fields while the caller still holds the session lock (this method is
	// documented as called under it). The goroutine below outlives OnSessionDone's `defer Free`, so a
	// concurrent relay can re-acquire the session and mutate LatestBlock/RelayNum (SetUsageForSession)
	// — reading them inside the goroutine was a data race. Snapshot, then read only locals.
	latestBlock := consumerSession.LatestBlock
	relayNum := consumerSession.RelayNum

	go func() {
		csm.consumerMetricsManager.SetQOSMetrics(chainId, apiInterface, publicProviderAddress, publicProviderEndpoint, lastQos, lastReputation, latestBlock, relayNum, relayLatency, sessionSuccessful)
	}()
}

// setSelectionStats stores the latest selection stats (thread-safe)
func (csm *ConsumerSessionManager) setSelectionStats(stats *provideroptimizer.SelectionStats) {
	csm.selectionStatsLock.Lock()
	defer csm.selectionStatsLock.Unlock()
	csm.latestSelectionStats = stats
}

// GetSelectionStats retrieves the latest selection stats (thread-safe)
func (csm *ConsumerSessionManager) GetSelectionStats() *provideroptimizer.SelectionStats {
	csm.selectionStatsLock.RLock()
	defer csm.selectionStatsLock.RUnlock()
	return csm.latestSelectionStats
}

// Get the reported providers currently stored in the session manager.
func (csm *ConsumerSessionManager) GetReportedProviders(epoch uint64) []*pairingtypes.ReportedProvider {
	// Hold the read lock for the entire operation so that UpdateAllProviders cannot
	// reset reportedProviders and rebuild pairing between the epoch check and the
	// pairing lookup, which would cause a spurious "not found" error.
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	if epoch != csm.atomicReadCurrentEpoch() {
		return nil // if epochs are not equal, we will return an empty list.
	}
	reportedProviders := csm.reportedProviders.GetReportedProviders()
	filteredReportedProviders := []*pairingtypes.ReportedProvider{}
	for _, reportedProvider := range reportedProviders {
		_, ok := csm.pairing[reportedProvider.Address]
		if !ok {
			// Provider may be a backup provider — they are stored separately
			// from the main pairing but can still be reported on failure.
			_, ok = csm.backupProviders[reportedProvider.Address]
		}
		if !ok {
			utils.LavaFormatError("Failed to find a reported provider in pairing list", nil, utils.LogAttr("provider_address", reportedProvider.Address), utils.LogAttr("epoch", csm.currentEpoch))
			continue
		}
		filteredReportedProviders = append(filteredReportedProviders, reportedProvider)
	}
	return filteredReportedProviders
}

// Atomically read csm.pairingAddressesLength for data reliability.
func (csm *ConsumerSessionManager) GetAtomicPairingAddressesLength() uint64 {
	return atomic.LoadUint64(&csm.pairingAddressesLength)
}

// On a successful Subscribe relay
func (csm *ConsumerSessionManager) OnSessionDoneIncreaseCUOnly(consumerSession *SingleConsumerSession, latestServicedBlock int64) error {
	if err := consumerSession.VerifyLock(); err != nil {
		return fmt.Errorf("OnSessionDoneIncreaseRelayAndCu consumerSession.lock must be locked before accessing this method: %w", err)
	}

	defer consumerSession.Free(nil) // we need to be locked here, if we didn't get it locked we try lock anyway
	consumerSession.LatestBlock = latestServicedBlock
	consumerSession.CuSum += consumerSession.LatestRelayCu // add CuSum to current cu usage.
	consumerSession.LatestRelayCu = 0                      // reset cu just in case
	consumerSession.ConsecutiveErrors = []error{}
	return nil
}

func (csm *ConsumerSessionManager) GenerateReconnectCallback(consumerSessionsWithProvider *ConsumerSessionsWithProvider) func() error {
	return func() error {
		ctx := utils.WithUniqueIdentifier(context.Background(), utils.GenerateUniqueIdentifier()) // unique identifier for retries
		_, providerAddress, err := csm.probeProvider(ctx, consumerSessionsWithProvider, csm.atomicReadCurrentEpoch(), true)
		if err == nil {
			utils.LavaFormatInfo("Reconnecting provider succeeded returning provider to valid addresses list", utils.LogAttr("provider", providerAddress))
			// csm.pairing and csm.backupProviders are built from separate inputs by
			// UpdateAllProviders with no dedup, so a provider address can legitimately
			// appear in both. The old "if backup else primary" branching silently dropped
			// the primary-side unblock for overlap cases — a provider blocked as primary
			// via currentlyBlockedProviderAddresses would never return to validAddresses
			// just because it happened to also exist in backupProviders.
			//
			// Handle both sides independently:
			//   - backup side: delete from blockedBackupProviders when present
			//   - primary side: validateAndReturnBlockedProviderToValidAddressesList —
			//     idempotent for addresses not in currentlyBlockedProviderAddresses
			//     (see line ~1739 comment), so calling unconditionally is safe.
			csm.lock.Lock()
			if _, inBackup := csm.backupProviders[providerAddress]; inBackup {
				if _, blocked := csm.blockedBackupProviders[providerAddress]; blocked {
					delete(csm.blockedBackupProviders, providerAddress)
					if record, released := csm.releaseBlockRecordLocked(providerAddress, true); released {
						csm.logProviderReleased(providerAddress, record, ReleaseReconnectLoop, csm.blockedTotalLocked())
					}
				}
			}
			csm.lock.Unlock()
			csm.validateAndReturnBlockedProviderToValidAddressesList(providerAddress, ReleaseReconnectLoop)
		}
		return err
	}
}

// checkAndUnblockHealthyReBlockedProviders checks providers that were re-blocked from previous epoch
// and immediately unblocks them if their probe was successful. This only happens at epoch transitions.
// Other providers and normal blocking behavior during an epoch remain unchanged.
func (csm *ConsumerSessionManager) checkAndUnblockHealthyReBlockedProviders(ctx context.Context, epoch uint64) {
	// Check if epoch is still current - if epoch changed, our previousEpochBlockedProviders data is stale
	currentEpoch := csm.atomicReadCurrentEpoch()
	if epoch != currentEpoch {
		utils.LavaFormatDebug("Skipping re-blocked provider check due to epoch change",
			utils.Attribute{Key: "requestedEpoch", Value: epoch},
			utils.Attribute{Key: "currentEpoch", Value: currentEpoch},
		)
		return
	}

	// Ensure context has unique identifier for probing
	if _, found := utils.GetUniqueIdentifier(ctx); !found {
		ctx = utils.AppendUniqueIdentifier(ctx, utils.GenerateUniqueIdentifier())
	}

	// Clean up previousEpochBlockedProviders after processing
	defer func() {
		csm.lock.Lock()
		csm.previousEpochBlockedProviders = make(map[string]BlockRecord)
		csm.lock.Unlock()
	}()

	type reBlockedProviderInfo struct {
		cswp     *ConsumerSessionsWithProvider
		isBackup bool
	}

	csm.lock.Lock()

	// First pass: Identify which re-blocked providers had successful probes
	providersNeedingComprehensiveProbe := make(map[string]reBlockedProviderInfo)

	for blockedAddr, carried := range csm.previousEpochBlockedProviders {
		cswp, exists := csm.pairing[blockedAddr]
		isBackup := false
		if !exists {
			cswp, exists = csm.backupProviders[blockedAddr]
			isBackup = true
		}
		if !exists {
			continue // Provider not in current pairing or backup list
		}

		// A backup ALWAYS takes the comprehensive-probe branch: !isBackup short-circuits, so a
		// backup's report state is never consulted at all. That is deliberate — report state is
		// only meaningful as a health signal for a provider we actually have evidence about, and
		// a backup that was never selected has none. Falling through to "not reported" for it would
		// read that as "healthy" and unblock it with no real check.
		//
		// The report state comes from the CARRIED BLOCK RECORD, not from csm.reportedProviders.
		// The live register cannot answer this question at an epoch transition: UpdateAllProviders
		// calls reportedProviders.Reset() in its body, while this pass runs from a defer that
		// sleeps first — so the register is always already empty by the time we get here. Reading
		// it made every non-backup look unreported, sent all of them down the immediate-unblock
		// branch, and left this else-branch unreachable for regular providers. The whole point of
		// re-blocking them ("prevents known-bad providers from getting a clean slate") was undone
		// in the same tick.
		//
		// The record is the honest source: it holds whether the provider was reported at the moment
		// it was blocked, which is exactly what the register held before the reset.
		if !isBackup && !carried.Reported {
			// Non-backup provider that was never reported — no evidence it is bad. Unblock
			// immediately.
			utils.LavaFormatInfo("Re-blocked provider's probe succeeded, immediately unblocking",
				utils.Attribute{Key: "provider", Value: blockedAddr},
				utils.Attribute{Key: "isBackup", Value: isBackup},
				utils.Attribute{Key: "epoch", Value: epoch},
				utils.LogAttr("GUID", ctx),
			)
			csm.validateAndReturnBlockedProviderToValidAddressesListLocked(blockedAddr, ReleaseEpochNotReported)
			// No RemoveReport here. UpdateAllProviders resets the register in its body before this
			// pass runs, so there has never been anything to remove — and this branch no longer
			// consults it at all. Leaving the call in would suggest the register is meaningful on
			// this path, which is the assumption that produced the bug above.
		} else {
			// Either a backup provider (always routed here by the short-circuit above),
			// or a non-backup that was reported for relay failures.
			// In both cases, run a comprehensive probe with tryReconnect=true.
			providersNeedingComprehensiveProbe[blockedAddr] = reBlockedProviderInfo{cswp: cswp, isBackup: isBackup}
			utils.LavaFormatDebug("Re-blocked provider needs explicit probe",
				utils.Attribute{Key: "provider", Value: blockedAddr},
				utils.Attribute{Key: "isBackup", Value: isBackup},
				utils.Attribute{Key: "epoch", Value: epoch},
				utils.LogAttr("GUID", ctx),
			)
		}
	}
	csm.lock.Unlock()

	// Second pass: For providers that failed initial probe, try comprehensive probe with reconnection
	for blockedAddr, info := range providersNeedingComprehensiveProbe {
		utils.LavaFormatDebug("Attempting comprehensive probe with endpoint reconnection",
			utils.Attribute{Key: "provider", Value: blockedAddr},
			utils.Attribute{Key: "isBackup", Value: info.isBackup},
			utils.Attribute{Key: "epoch", Value: epoch},
			utils.LogAttr("GUID", ctx),
		)

		_, providerAddress, err := csm.probeProvider(ctx, info.cswp, epoch, true)

		if err == nil {
			utils.LavaFormatInfo("Re-blocked provider's comprehensive probe succeeded, immediately unblocking",
				utils.Attribute{Key: "provider", Value: providerAddress},
				utils.Attribute{Key: "isBackup", Value: info.isBackup},
				utils.Attribute{Key: "epoch", Value: epoch},
				utils.LogAttr("GUID", ctx),
			)
			if info.isBackup {
				csm.lock.Lock()
				delete(csm.blockedBackupProviders, providerAddress)
				record, released := csm.releaseBlockRecordLocked(providerAddress, true)
				blockedCount := csm.blockedTotalLocked()
				csm.lock.Unlock()
				if released {
					csm.logProviderReleased(providerAddress, record, ReleaseEpochProbe, blockedCount)
				}
			} else {
				csm.validateAndReturnBlockedProviderToValidAddressesList(providerAddress, ReleaseEpochProbe)
			}
			csm.reportedProviders.RemoveReport(providerAddress)
		} else {
			utils.LavaFormatDebug("Re-blocked provider still unhealthy after comprehensive probe, keeping blocked",
				utils.Attribute{Key: "provider", Value: providerAddress},
				utils.Attribute{Key: "isBackup", Value: info.isBackup},
				utils.Attribute{Key: "error", Value: err.Error()},
				utils.Attribute{Key: "epoch", Value: epoch},
				utils.LogAttr("GUID", ctx),
			)
		}
	}
}

func NewConsumerSessionManager(
	rpcEndpoint *RPCEndpoint,
	providerOptimizer ProviderOptimizer,
	consumerMetricsManager metrics.ConsumerMetricsManagerInf,
	consumerPublicAddress string,
	activeSubscriptionProvidersStorage *ActiveSubscriptionProvidersStorage,
) *ConsumerSessionManager {
	csm := &ConsumerSessionManager{
		reportedProviders:      NewReportedProviders(),
		consumerMetricsManager: metrics.SafeMetrics(consumerMetricsManager),
		consumerPublicAddress:  consumerPublicAddress,
		qosManager:             qos.NewQoSManager(),
		getLavaBlockHeight:     func() int64 { return 0 }, // default to 0, should be set by caller
		blockedBackupProviders: make(map[string]struct{}),
		blockedProviderRecords: make(map[string]BlockRecord),
		rateLimitHoldoff:       holdoff.Shared,
	}
	csm.rpcEndpoint = rpcEndpoint
	csm.providerOptimizer = providerOptimizer
	csm.activeSubscriptionProvidersStorage = activeSubscriptionProvidersStorage
	csm.stickySessions = NewStickySessionStore()
	csm.startStateSizesPublisher()
	return csm
}

// SetLavaBlockHeightCallback sets the callback function to get current Lava blockchain block height
// This must be called after creating the ConsumerSessionManager
func (csm *ConsumerSessionManager) SetLavaBlockHeightCallback(getLavaBlockHeight func() int64) {
	csm.getLavaBlockHeight = getLavaBlockHeight
}

// ResetTransientFailureState clears every cross-epoch failure-tracking store
// without forcing an epoch transition. The "live pairing" (pairing,
// pairingAddresses, validAddresses, currentlyBlockedProviderAddresses,
// backupProviders) is left intact so the very next relay can route normally.
//
// Used by the /debug/reset-all debug endpoint to return the router to a clean
// state between black-box test runs.
//
// Cleared:
//   - previousEpochBlockedProviders (cross-epoch known-bad memory)
//   - secondChanceGivenToAddresses (per-epoch second-chance memory)
//   - blockedBackupProviders (backup-provider failure memory for this epoch)
//   - stickySessions (session affinities)
//   - reportedProviders (accumulated unresponsiveness reports)
//
// We deliberately do NOT touch currentlyBlockedProviderAddresses: blocking is
// a destructive move (removeAddressFromValidAddresses pops out of
// validAddresses and pushes onto currentlyBlockedProviderAddresses), so
// clearing the latter without restoring those addresses to validAddresses
// would put the provider in routing limbo until the next epoch transition.
// Unblocking is an epoch-boundary operation by design.
func (csm *ConsumerSessionManager) ResetTransientFailureState() {
	csm.lock.Lock()
	csm.previousEpochBlockedProviders = make(map[string]BlockRecord)
	csm.secondChanceGivenToAddresses = make(map[string]struct{})
	// Backup blocks are cleared here, so their reason records go with them. Regular blocks are
	// deliberately preserved (see the doc comment above), and so are their records — including for a
	// provider blocked in BOTH pools, whose regular block survives this reset.
	clearedBackups := csm.blockedBackupProviders
	csm.blockedBackupProviders = make(map[string]struct{})
	for address := range clearedBackups {
		csm.releaseBlockRecordLocked(address, true)
	}
	csm.lock.Unlock()

	// stickySessions and reportedProviders carry their own locks; do not hold
	// csm.lock across them to avoid lock-ordering hazards with the rest of the
	// session-manager code.
	if csm.stickySessions != nil {
		csm.stickySessions.Clear()
	}
	if csm.reportedProviders != nil {
		csm.reportedProviders.Reset()
	}

	// Emit the state-size gauges immediately so /debug/reset-all callers see
	// the post-condition (everything at 0) on the very next /metrics scrape,
	// without waiting for the periodic publisher tick.
	csm.publishStateSizes()
}

// ResetEndpointHealth re-enables every endpoint across this manager's active pairing and
// backup providers via Endpoint.ResetHealth (clears ConnectionRefusals, sets Enabled = true).
// Returns the number of endpoints that were actually unhealthy and got re-enabled.
//
// Why this is needed and not covered by the other resets: an endpoint that hits
// MaxConsecutiveConnectionAttempts is disabled (Endpoint.Enabled = false). The ONLY paths
// back are a successful relay through that endpoint or the per-epoch updateEpoch tick, which
// is the only other ResetHealth caller. After an all-providers-down burst every endpoint is
// disabled, so no relay can succeed to re-enable them — and ResetBlockedProviders only refills
// validAddresses, so the next selection immediately re-blocks the providers on their still-
// disabled endpoints. Without re-enabling endpoint health the router stays stuck until the
// next epoch or a pod restart. The debug reset endpoint calls this to recover synchronously.
//
// Snapshot the provider records under csm.lock, then call ResetHealth without holding it:
// ResetHealth and the per-cswp Endpoints read carry their own locks, and not holding csm.lock
// across them avoids the lock-ordering hazards the sibling reset helpers warn about.
func (csm *ConsumerSessionManager) ResetEndpointHealth() int {
	csm.lock.RLock()
	cswps := make([]*ConsumerSessionsWithProvider, 0, len(csm.pairing)+len(csm.backupProviders))
	for _, cswp := range csm.pairing {
		cswps = append(cswps, cswp)
	}
	for _, cswp := range csm.backupProviders {
		cswps = append(cswps, cswp)
	}
	csm.lock.RUnlock()

	count := 0
	for _, cswp := range cswps {
		if cswp == nil {
			continue
		}
		cswp.Lock.RLock()
		endpoints := cswp.Endpoints
		cswp.Lock.RUnlock()
		for _, endpoint := range endpoints {
			if endpoint.ResetHealth() {
				count++
			}
		}
	}
	return count
}

// ResetBlockedProviders is the direct-rpc-mode escape hatch for
// /debug/reset-all. It bypasses the epoch-boundary invariant documented on
// ResetTransientFailureState by atomically restoring every
// currentlyBlockedProviderAddresses entry back to validAddresses, then
// resetting the per-provider redemption flag.
//
// Why ResetTransientFailureState alone is not enough:
// The lava-pairing-network mode relies on UpdateAllProviders firing on every
// epoch transition to repopulate validAddresses from pairingAddresses — that
// is the only path that drains currentlyBlockedProviderAddresses without
// stranding addresses in routing limbo. In direct-rpc mode the pairing list
// is loaded once from YAML and never refreshed, so currentlyBlockedProvider
// Addresses grows monotonically as tests trigger blockProvider, and the
// pool of routable providers shrinks until tests cascade.
//
// We pair the three state changes (clear blocked list, refill valid list,
// purge addon cache) inside a single lock-held block so observers never see
// the "blocked-but-not-valid" state that the original doc warned against.
// The addon-cache purge mirrors what removeAddressFromValidAddresses and
// validateAndReturnBlockedProviderToValidAddressesListLocked already do on
// every single-provider mutation.
func (csm *ConsumerSessionManager) ResetBlockedProviders() {
	csm.lock.Lock()
	hadBlocked := len(csm.currentlyBlockedProviderAddresses) > 0
	if hadBlocked {
		// setValidAddressesToDefaultValue("", nil, ctx) refills validAddresses
		// from pairingAddresses but does NOT touch addonAddresses in the
		// no-addon branch. RemoveAddonAddresses("", nil) wipes the whole map
		// so the next request rebuilds against the restored pool.
		csm.setValidAddressesToDefaultValue("", nil, context.Background(), ReleaseOperatorReset)
		csm.RemoveAddonAddresses("", nil)
	}
	pairing := csm.pairing
	endpoint := csm.RPCEndpoint()
	csm.lock.Unlock()

	if !hadBlocked {
		return
	}

	// Clear the per-provider redemption flag for every entry returned to
	// validAddresses. blockProvider sets this to Used in the second-chance
	// path; without resetting, future failures see a "Used" status and skip
	// retry logic. Done outside csm.lock — atomicWriteBlockedStatus does not
	// take csm.lock, and the metric goroutine must not hold it either.
	for addr, provider := range pairing {
		if provider == nil {
			continue
		}
		provider.atomicWriteBlockedStatus(BlockedProviderSessionUnusedStatus)
		if csm.consumerMetricsManager != nil && len(provider.Endpoints) > 0 {
			go func(networkAddress, chainID, apiInterface, providerAddress string) {
				csm.consumerMetricsManager.SetBlockedProvider(chainID, apiInterface, providerAddress, networkAddress, false)
			}(provider.Endpoints[0].NetworkAddress, endpoint.ChainID, endpoint.ApiInterface, addr)
		}
	}

	csm.publishStateSizes()
}

// stateSizesPublishInterval drives the periodic CSM state-size gauge tick.
// Variable (not const) so unit tests can shorten it to deterministic timing
// without exposing the goroutine internals.
var stateSizesPublishInterval = time.Second

// publishStateSizes reads the current size of every black-box state store
// that /debug/reset-all promises to clear and emits them as Prometheus gauges
// (MAG-1762). Safe to call from any goroutine: takes its locks lazily.
//
// Callers MUST NOT hold csm.lock — this function takes csm.lock.RLock
// internally for the two map reads and then defers to the external stores
// (which carry their own locks).
func (csm *ConsumerSessionManager) publishStateSizes() {
	if csm == nil || csm.consumerMetricsManager == nil || csm.rpcEndpoint == nil {
		return
	}

	csm.lock.RLock()
	// currentlyBlockedProviderAddresses is the standing block: these providers receive no traffic
	// until they recover. previousEpochBlockedProviders is only the cross-epoch carry-over set,
	// populated at an epoch boundary and cleared moments later by the re-block pass — publishing
	// that as "blocked providers" reported 0 through entire outages.
	blockedCount := len(csm.currentlyBlockedProviderAddresses)
	prevEpochBlockedCount := len(csm.previousEpochBlockedProviders)
	blockedBackupCount := len(csm.blockedBackupProviders)

	// Snapshot the per-provider blocked state for the whole pairing, not just the addresses that
	// changed. The per-provider gauge MUST be level-triggered: the blocked list is also drained
	// wholesale by setValidAddressesToDefaultValue — on every epoch transition, and on the
	// pool-empty release in releaseBlockedProvidersIfPoolEmpty — and neither drain publishes
	// anything per provider. Edge-triggered publishing alone therefore left every series it
	// emptied stuck at 1 for a provider that was back in rotation and serving fine (MAG-3106).
	// Republishing the full truth each tick makes the gauge self-correcting instead.
	blockedSet := make(map[string]struct{}, blockedCount)
	for _, address := range csm.currentlyBlockedProviderAddresses {
		blockedSet[address] = struct{}{}
	}
	providerBlocked := make(map[string]bool, len(csm.pairing))
	for address := range csm.pairing {
		_, blocked := blockedSet[address]
		providerBlocked[address] = blocked
	}
	blockedByReason := csm.blockedCountsByReasonLocked()
	csm.lock.RUnlock()

	var stickyCount, reportedCount int
	if csm.stickySessions != nil {
		stickyCount = csm.stickySessions.Len()
	}
	if csm.reportedProviders != nil {
		reportedCount = csm.reportedProviders.Len()
	}

	chainID := csm.rpcEndpoint.ChainID
	apiInterface := csm.rpcEndpoint.ApiInterface
	csm.consumerMetricsManager.SetCSMBlockedProvidersCount(chainID, apiInterface, blockedCount)
	csm.consumerMetricsManager.SetCSMBlockedProvidersByReason(chainID, apiInterface, blockedByReason)
	csm.consumerMetricsManager.SetCSMPreviousEpochBlockedProvidersCount(chainID, apiInterface, prevEpochBlockedCount)
	csm.consumerMetricsManager.SetCSMBlockedBackupProvidersCount(chainID, apiInterface, blockedBackupCount)
	csm.consumerMetricsManager.SetCSMStickySessionsCount(chainID, apiInterface, stickyCount)
	csm.consumerMetricsManager.SetCSMReportedProvidersCount(chainID, apiInterface, reportedCount)
	// The endpoint argument is unused by the smart router (a node URL can carry an API key and
	// must never become a label) but is fixed by the shared interface.
	for address, blocked := range providerBlocked {
		csm.consumerMetricsManager.SetBlockedProvider(chainID, apiInterface, address, "", blocked)
	}
}

// blockedCountsByReasonLocked counts the current blocks per reason, across both the regular and
// backup pools.
//
// Every known reason is present, zeros included. Publishing the zeros is the whole point: a reason
// that stops applying must be actively set to 0, or its series stays at its last value forever and
// a sum() over the gauge double-counts. csm.lock must be held (read lock is enough).
func (csm *ConsumerSessionManager) blockedCountsByReasonLocked() map[string]int {
	reasons := AllBlockReasons()
	counts := make(map[string]int, len(reasons))
	for _, reason := range reasons {
		counts[string(reason)] = 0
	}
	for _, record := range csm.blockedProviderRecords {
		reason := record.Reason
		if reason == "" {
			reason = BlockReasonUnspecified
		}
		counts[string(reason)]++
	}
	return counts
}

// startStateSizesPublisher kicks off the periodic gauge tick. Per-CSM
// goroutine, follows the same shape as the ReconnectProviders ticker in
// reported_providers.go (no stop channel — bound to process lifetime).
//
// Skipped when the metrics manager is the NoOp variant — SafeMetrics(nil)
// wraps a typed NoOp into the interface, so a plain nil check is never true;
// we detect the NoOp explicitly to avoid spinning a useless goroutine in
// every test that passes nil metrics.
func (csm *ConsumerSessionManager) startStateSizesPublisher() {
	if csm == nil || csm.consumerMetricsManager == nil {
		return
	}
	if _, isNoOp := csm.consumerMetricsManager.(metrics.NoOpConsumerMetrics); isNoOp {
		return
	}
	go func() {
		ticker := time.NewTicker(stateSizesPublishInterval)
		defer ticker.Stop()
		for range ticker.C {
			csm.publishStateSizes()
		}
	}()
}
