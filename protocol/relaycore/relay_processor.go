package relaycore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

type RelayProcessor struct {
	usedProviders                *lavasession.UsedProviders
	responses                    chan *RelayResponse
	crossValidationParams        *common.CrossValidationParams // nil for Stateless/Stateful, non-nil for CrossValidation
	lock                         sync.RWMutex
	guid                         uint64
	selection                    Selection
	consistency                  Consistency
	debugRelay                   bool
	allowSessionDegradation      uint32 // used in the scenario where extension was previously used.
	metricsInf                   MetricsInterface
	chainIdAndApiInterfaceGetter ChainIdAndApiInterfaceGetter
	relayRetriesManager          *lavaprotocol.RelayRetriesManager
	ResultsManager
	RelayStateMachine
	// quorumMap tracks, per identical response hash, how many providers returned it and which distinct
	// cross-validation groups they span. Both must be updated together (in handleResponse) so the
	// diversity-aware early-exit cannot drift; see quorumStat.
	quorumMap                       map[[32]byte]*quorumStat
	currentQuorumEqualResults       int      // max count across hashes — kept for logging only, not for decisions
	statefulRelayTargets            []string // stores all providers that received a stateful relay
	crossValidationQueriedProviders []string // stores all providers that were queried for cross-validation (even if response not received)
	// crossValidationRelayDeadline is the latest batch's relay upper bound, stamped at launch:
	// launch time + relayTimeout + the largest per-endpoint url.Timeout override (mirroring
	// LowerContextTimeoutWithDuration, the actual bound on each detached relay). The straggler
	// watcher uses it so its deadline neither undershoots a slow-RPC endpoint's legitimate late
	// response nor outlives the true bound (MAG-2187). Guarded by rp.lock.
	crossValidationRelayDeadline time.Time
	// crossValidationFailFastReason carries the structured reason for a request-time cross-validation
	// fail-fast (capacity/diversity checks that abort before any relay completes, so no RelayResult is
	// produced). It rides on the shared processor back to the caller, which synthesizes the
	// lava-cross-validation-failure-reason header from it — the error returns are left byte-for-byte
	// unchanged so the state machine's PairingListEmptyError stop logic is unaffected. Guarded by rp.lock.
	crossValidationFailFastReason string
}

// quorumStat is the per-hash agreement tally for cross-validation: how many providers returned this exact
// response, and the per-group breakdown of those providers (group label -> count, empty label folded into
// common.DefaultProviderGroup). len(groupCounts) is the number of distinct groups (the diversity count used by MinGroups);
// the per-group values are what PerGroupQuorum needs (each group's own internal tally for this hash).
type quorumStat struct {
	count       int
	groupCounts map[string]int
}

func NewRelayProcessor(
	ctx context.Context,
	crossValidationParams *common.CrossValidationParams, // nil for Stateless/Stateful
	consistency Consistency,
	metricsInf MetricsInterface,
	chainIdAndApiInterfaceGetter ChainIdAndApiInterfaceGetter,
	relayRetriesManager *lavaprotocol.RelayRetriesManager,
	relayStateMachine RelayStateMachine,
) *RelayProcessor {
	guid, _ := utils.GetUniqueIdentifier(ctx)
	selection := relayStateMachine.GetSelection()

	// Defensive validation - these should never fail in production as params
	// are validated at parse time, but guards against programming errors
	if selection == CrossValidation && crossValidationParams == nil {
		utils.LavaFormatFatal("CrossValidation selection requires non-nil crossValidationParams", nil)
	}
	if crossValidationParams != nil {
		if crossValidationParams.AgreementThreshold < 1 {
			utils.LavaFormatFatal("invalid cross-validation AgreementThreshold", nil,
				utils.LogAttr("AgreementThreshold", crossValidationParams.AgreementThreshold))
		}
		if crossValidationParams.MaxParticipants < 1 {
			utils.LavaFormatFatal("invalid cross-validation MaxParticipants", nil,
				utils.LogAttr("MaxParticipants", crossValidationParams.MaxParticipants))
		}
		if crossValidationParams.MaxParticipants > MaxCallsPerRelay {
			utils.LavaFormatFatal("cross-validation MaxParticipants exceeds maximum allowed",
				nil,
				utils.LogAttr("MaxParticipants", crossValidationParams.MaxParticipants),
				utils.LogAttr("MaxCallsPerRelay", MaxCallsPerRelay))
		}
	}

	chainID, _ := chainIdAndApiInterfaceGetter.GetChainIdAndApiInterface()
	relayProcessor := &RelayProcessor{
		crossValidationParams:        crossValidationParams,
		responses:                    make(chan *RelayResponse, MaxCallsPerRelay), // buffered to prevent blocking
		ResultsManager:               NewResultsManager(guid, chainID),
		guid:                         guid,
		consistency:                  consistency,
		debugRelay:                   relayStateMachine.GetDebugState(),
		metricsInf:                   metricsInf,
		chainIdAndApiInterfaceGetter: chainIdAndApiInterfaceGetter,
		relayRetriesManager:          relayRetriesManager,
		RelayStateMachine:            relayStateMachine,
		selection:                    selection,
		usedProviders:                relayStateMachine.GetUsedProviders(),
		quorumMap:                    make(map[[32]byte]*quorumStat),
		currentQuorumEqualResults:    0,
	}
	relayProcessor.RelayStateMachine.SetResultsChecker(relayProcessor)
	relayProcessor.RelayStateMachine.SetRelayRetriesManager(relayRetriesManager)
	return relayProcessor
}

func (rp *RelayProcessor) GetCrossValidationParams() *common.CrossValidationParams {
	return rp.crossValidationParams
}

// getAgreementThreshold returns the agreement threshold, or noCrossValidationRequirement when not in
// CrossValidation mode.
func (rp *RelayProcessor) getAgreementThreshold() int {
	if rp.crossValidationParams != nil {
		return rp.crossValidationParams.AgreementThreshold
	}
	return noCrossValidationRequirement
}

// getMaxParticipants returns the max participants, or noCrossValidationRequirement when not in
// CrossValidation mode.
func (rp *RelayProcessor) getMaxParticipants() int {
	if rp.crossValidationParams != nil {
		return rp.crossValidationParams.MaxParticipants
	}
	return noCrossValidationRequirement
}

// getMinGroups returns the required number of distinct provider groups
// (noCrossValidationRequirement = no diversity requirement).
func (rp *RelayProcessor) getMinGroups() int {
	if rp.crossValidationParams != nil && rp.crossValidationParams.MinGroups > noCrossValidationRequirement {
		return rp.crossValidationParams.MinGroups
	}
	return noCrossValidationRequirement
}

// perGroupQuorum reports whether the stronger per-group-quorum variant is active (each of MinGroups groups
// must independently reach AgreementThreshold matching responses, then per-group winners must agree).
func (rp *RelayProcessor) perGroupQuorum() bool {
	return rp.crossValidationParams != nil && rp.crossValidationParams.PerGroupQuorum
}

// qualifyingGroupCount returns how many groups in the per-group breakdown independently reached the
// agreement threshold for a single hash — i.e. the number of groups that "corroborate" that hash. This is
// the per-group-quorum analogue of "distinct groups" (len(groupCounts)).
func qualifyingGroupCount(groupCounts map[string]int, threshold int) int {
	n := 0
	for _, c := range groupCounts {
		if c >= threshold {
			n++
		}
	}
	return n
}

// hashQuorumReached reports whether a single hash's tally satisfies the active quorum rule. In the default
// (MinGroups) mode: total count >= threshold AND distinct groups >= minGroups. In per-group-quorum mode:
// at least minGroups groups each independently reached the threshold for this hash.
//
// Core semantic worth stating outright: once minGroups > 1, a hash whose matching providers all fall in a
// single group can NEVER win, no matter how many providers returned it — len(groupCounts) is 1 and fails the
// diversity gate. Diversity is weighed over raw count: a smaller multi-group agreement beats a larger
// single-group one (e.g. with threshold 2 / minGroups 2, two single-node groups B+C outvote a 3-node group
// A, and A is then the cross_validation_mismatch outlier). This is the anti-Sybil intent — one group is
// treated as one operator/failure domain, so cross-group agreement is trusted over node count — but it
// assumes groups are independent; the startup SPOF warning in validateCrossValidationStartup flags fleets
// where that diversity rests on groups too small to corroborate a value on their own.
func (rp *RelayProcessor) hashQuorumReached(count int, groupCounts map[string]int, threshold, minGroups int) bool {
	if rp.perGroupQuorum() {
		return qualifyingGroupCount(groupCounts, threshold) >= minGroups
	}
	return count >= threshold && len(groupCounts) >= minGroups
}

// crossValidationQuorumReached reports whether some response hash satisfies the active quorum rule. This is
// the single diversity-aware early-exit predicate used by both checkEndProcessing and HasRequiredNodeResults
// so the two cannot disagree. With MinGroups <= 1 and per-group disabled it reduces exactly to "some hash
// reached the threshold" (the pre-1.2 behavior). In per-group mode it must NOT early-exit on the weaker
// cross-group condition, or selection could stop before each group reaches its own internal quorum and then
// fail the stricter check. Assumes rp.lock is held.
func (rp *RelayProcessor) crossValidationQuorumReached() bool {
	threshold := rp.getAgreementThreshold()
	minGroups := rp.getMinGroups()
	for hash, stat := range rp.quorumMap {
		// Empty/nil replies keep the zero hash (handleResponse only hashes non-empty data). A nil/empty
		// consensus is a FALLBACK: responsesCrossValidation accepts it only when no real hash formed a
		// quorum (the real-over-nil preference). Early-exiting on the zero bucket would commit to that
		// fallback before real responses still in flight could form a (preferred) real quorum — and in
		// per-group mode the final check excludes nils entirely. So the early-exit ignores the zero bucket
		// in ALL modes; the nil fallback is resolved at final eval once every response is in (or the batch
		// is exhausted). The cost is at most waiting out an all-nil batch instead of exiting at threshold.
		//
		// Backwards-compat note (UC-7): this skip applies even to the legacy header-only path (MinGroups<=1,
		// not per-group), so an all-nil batch no longer early-exits there either. It STILL forms a quorum at
		// final eval via the nil fallback, so no empty-response quorum case is broken — only the latency
		// changes. This is a deliberate decision (commit c0c77a8) and supersedes development-plan item
		// 2.3-R1's "default mode still counts the zero hash". Pinned by
		// TestCrossValidationQuorumReached_LegacyHeaderOnlyNilBackwardsCompat + TestResponsesCrossValidation_LegacyNilFallback.
		if hash == ([32]byte{}) {
			continue
		}
		if rp.hashQuorumReached(stat.count, stat.groupCounts, threshold, minGroups) {
			return true
		}
	}
	return false
}

// quorumGroupOf returns the group label used for diversity counting for a result, folding an empty label
// into the implicit common.DefaultProviderGroup.
func quorumGroupOf(result common.RelayResult) string {
	if result.ProviderInfo.ProviderGroup == "" {
		return common.DefaultProviderGroup
	}
	return result.ProviderInfo.ProviderGroup
}

// true if we never got an extension. (default value)
func (rp *RelayProcessor) GetAllowSessionDegradation() bool {
	return atomic.LoadUint32(&rp.allowSessionDegradation) == 0
}

// in case we had an extension and managed to get a session successfully, we prevent session degradation.
func (rp *RelayProcessor) SetDisallowDegradation() {
	atomic.StoreUint32(&rp.allowSessionDegradation, 1)
}

// SetStatefulRelayTargets stores the list of providers that received a stateful relay
func (rp *RelayProcessor) SetStatefulRelayTargets(providers []string) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.statefulRelayTargets = providers
}

// GetStatefulRelayTargets returns the list of providers that received a stateful relay
func (rp *RelayProcessor) GetStatefulRelayTargets() []string {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.statefulRelayTargets
}

// SetCrossValidationQueriedProviders stores the list of all providers that were queried for cross-validation
// This includes providers whose responses may not have been received (due to early exit when threshold met)
func (rp *RelayProcessor) SetCrossValidationQueriedProviders(providers []string) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.crossValidationQueriedProviders = providers
}

// GetCrossValidationQueriedProviders returns the list of all providers that were queried for cross-validation
func (rp *RelayProcessor) GetCrossValidationQueriedProviders() []string {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.crossValidationQueriedProviders
}

// SetCrossValidationRelayDeadline stamps the latest batch's relay upper bound at launch time
// (launch + relayTimeout + max per-endpoint url.Timeout). See the field comment.
func (rp *RelayProcessor) SetCrossValidationRelayDeadline(deadline time.Time) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.crossValidationRelayDeadline = deadline
}

// GetCrossValidationRelayDeadline returns the stamped relay upper bound, or the zero time if no
// batch was launched (callers fall back to a relayTimeout-based bound).
func (rp *RelayProcessor) GetCrossValidationRelayDeadline() time.Time {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.crossValidationRelayDeadline
}

// SetCrossValidationFailFastReason records why a cross-validation request was aborted before any relay
// completed (a capacity/diversity check that fails fast). The caller reads it back off the shared
// processor to synthesize the failure-reason header, since no RelayResult exists on this path.
func (rp *RelayProcessor) SetCrossValidationFailFastReason(reason string) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	rp.crossValidationFailFastReason = reason
}

// GetCrossValidationFailFastReason returns the request-time fail-fast reason, or "" if the request did
// not abort on a capacity/diversity check.
func (rp *RelayProcessor) GetCrossValidationFailFastReason() string {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.crossValidationFailFastReason
}

func (rp *RelayProcessor) String() string {
	if rp == nil {
		return ""
	}

	usedProviders := rp.GetUsedProviders()

	currentlyUsedAddresses := usedProviders.CurrentlyUsedAddresses()
	unwantedAddresses := usedProviders.AllUnwantedAddresses()
	return fmt.Sprintf("relayProcessor {%s, unwantedAddresses: %s,currentlyUsedAddresses:%s}",
		rp.ResultsManager.String(), strings.Join(unwantedAddresses, ";"), strings.Join(currentlyUsedAddresses, ";"))
}

func (rp *RelayProcessor) GetUsedProviders() *lavasession.UsedProviders {
	if rp == nil {
		utils.LavaFormatError("RelayProcessor.GetUsedProviders is nil, misuse detected", nil)
		return nil
	}
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.usedProviders
}

// this function returns all results that came from a node, meaning success, and node errors
func (rp *RelayProcessor) NodeResults() []common.RelayResult {
	if rp == nil {
		return nil
	}
	rp.readExistingResponses()
	return rp.ResultsManager.NodeResults()
}

func (rp *RelayProcessor) SetResponse(response *RelayResponse) {
	if rp == nil {
		return
	}
	if response == nil {
		return
	}
	rp.responses <- response
}

func (rp *RelayProcessor) checkEndProcessing(responsesCount int) bool {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	// Common exit condition: all responses received from all providers in the batch
	if responsesCount >= rp.usedProviders.SessionsLatestBatch() {
		utils.LavaFormatDebug("[RelayProcessor] checkEndProcessing - all responses received",
			utils.LogAttr("GUID", rp.guid),
			utils.LogAttr("selection", rp.selection),
			utils.LogAttr("responsesCount", responsesCount),
			utils.LogAttr("SessionsLatestBatch", rp.usedProviders.SessionsLatestBatch()))
		return true
	}

	// Mode-specific early exit conditions
	switch rp.selection {
	case CrossValidation:
		// Early exit only once the agreement threshold is met AND spans the required number of distinct
		// groups — exiting on count alone could stop before a later same-hash response from a new group
		// arrives, then fail the diversity gate (the seam bug 1.2 must avoid).
		if rp.crossValidationQuorumReached() {
			utils.LavaFormatDebug("[RelayProcessor] checkEndProcessing - CrossValidation quorum (count+diversity) met",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("agreementThreshold", rp.getAgreementThreshold()),
				utils.LogAttr("minGroups", rp.getMinGroups()),
				utils.LogAttr("currentEqualResults", rp.currentQuorumEqualResults))
			return true
		}
	case Stateless, Stateful:
		// Early exit if we have a successful result
		if rp.ResultsManager.RequiredResults(1, rp.selection) {
			utils.LavaFormatDebug("[RelayProcessor] checkEndProcessing - RequiredResults met",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("selection", rp.selection))
			return true
		}
	}

	return false
}

func (rp *RelayProcessor) getInputMsgInfoHashString() (string, error) {
	hash, err := rp.RelayStateMachine.GetProtocolMessage().GetRawRequestHash()
	hashString := ""
	if err == nil {
		hashString = string(hash)
	}
	return hashString, err
}

// GetResultsSummary returns a pure data summary for the policy engine. No decisions.
func (rp *RelayProcessor) GetResultsSummary() ResultsSummary {
	if rp == nil {
		return ResultsSummary{}
	}
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	resultsCount, nodeErrors, specialNodeErrors, protocolErrors := rp.GetResults()
	_, nodeErrorResults, protocolErrorResults := rp.GetResultsData()
	_, hashErr := rp.getInputMsgInfoHashString()

	// Check node errors: IsNonRetryable is the umbrella retry gate.
	// IsUnsupportedMethod is a subset kept for zero-CU and caching only.
	hasNonRetryableNodeError := false
	hasUnsupportedMethod := false
	for _, result := range nodeErrorResults {
		if result.IsNonRetryable {
			hasNonRetryableNodeError = true
		}
		if result.IsUnsupportedMethod {
			hasUnsupportedMethod = true
		}
	}

	// Check protocol errors for permanent failures and epoch mismatch
	hasPermanentProtocolError := false
	hasEpochMismatch := false
	for _, protocolError := range protocolErrorResults {
		if errors.Is(protocolError.GetError(), lavasession.EpochMismatchError) {
			hasEpochMismatch = true
			continue
		}
		if chainlib.IsUnsupportedMethodError(protocolError.GetError()) {
			hasPermanentProtocolError = true
			continue
		}
		if !chainlib.ShouldRetryError(protocolError.GetError()) {
			hasPermanentProtocolError = true
		}
	}

	return ResultsSummary{
		SuccessCount:              resultsCount,
		NodeErrors:                nodeErrors,
		SpecialNodeErrors:         specialNodeErrors,
		ProtocolErrors:            protocolErrors,
		HasNonRetryableNodeError:  hasNonRetryableNodeError,
		HasUnsupportedMethod:      hasUnsupportedMethod,
		HasPermanentProtocolError: hasPermanentProtocolError,
		HasEpochMismatch:          hasEpochMismatch,
		HashErr:                   hashErr,
	}
}

func (rp *RelayProcessor) HasRequiredNodeResults(tries int) (bool, int) {
	if rp == nil {
		return false, 0
	}
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	resultsCount, nodeErrors, specialNodeErrors, protocolErrors := rp.GetResults()

	hash, hashErr := rp.getInputMsgInfoHashString()

	// CrossValidation mode: check if agreementThreshold is met across the required number of groups
	if rp.selection == CrossValidation {
		if rp.crossValidationQuorumReached() {
			if hashErr == nil {
				go rp.relayRetriesManager.RemoveHashFromCache(hash)
			}
			if rp.debugRelay {
				utils.LavaFormatDebug("HasRequiredNodeResults CrossValidation quorum (count+diversity) met",
					utils.LogAttr("GUID", rp.guid),
					utils.LogAttr("tries", tries),
					utils.LogAttr("agreementThreshold", rp.getAgreementThreshold()),
					utils.LogAttr("minGroups", rp.getMinGroups()),
					utils.LogAttr("currentQuorumEqualResults", rp.currentQuorumEqualResults),
					utils.LogAttr("resultsCount", resultsCount),
				)
			}
			return true, nodeErrors
		}
		// CrossValidation doesn't retry - return true only when all expected responses received
		// (The state machine handles no-retry logic)
		if rp.debugRelay {
			utils.LavaFormatDebug("HasRequiredNodeResults CrossValidation threshold not met",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("tries", tries),
				utils.LogAttr("agreementThreshold", rp.getAgreementThreshold()),
				utils.LogAttr("currentQuorumEqualResults", rp.currentQuorumEqualResults),
				utils.LogAttr("resultsCount", resultsCount),
			)
		}
		return false, nodeErrors
	}

	// Original logic for Stateless and Stateful modes
	// For Stateless/Stateful, we need at least 1 successful response
	if resultsCount >= 1 {
		if hashErr == nil { // Incase we had a successful relay we can remove the hash from our relay retries map
			// Use a routine to run it in parallel
			go rp.relayRetriesManager.RemoveHashFromCache(hash)
		}
		if rp.debugRelay {
			utils.LavaFormatDebug("HasRequiredNodeResults requirements met",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("tries", tries),
				utils.LogAttr("resultsCount", resultsCount),
				utils.LogAttr("nodeErrors", nodeErrors),
				utils.LogAttr("specialNodeErrors", specialNodeErrors),
				utils.LogAttr("currentQuorumEqualResults", rp.currentQuorumEqualResults),
			)
		}
		return true, nodeErrors
	}

	// No successful results — signal false unconditionally.
	// The state machine calls policy.Decide() to determine whether to retry.
	if rp.debugRelay {
		utils.LavaFormatDebug("HasRequiredNodeResults no success, signaling false",
			utils.LogAttr("GUID", rp.guid),
			utils.LogAttr("tries", tries),
			utils.LogAttr("resultsCount", resultsCount),
			utils.LogAttr("nodeErrors", nodeErrors),
			utils.LogAttr("specialNodeErrors", specialNodeErrors),
			utils.LogAttr("protocolErrors", protocolErrors),
		)
	}
	return false, nodeErrors
}

// canonicalResponseHash returns a hash of a provider response that is invariant
// to JSON object key ordering and insignificant whitespace. Cross-validation
// must place two providers that return the same semantic answer in the same
// quorum bucket even when their JSON envelope keys are serialized in a different
// order — e.g. {"jsonrpc","id","result"} from one provider vs
// {"id","jsonrpc","result"} from another. Hashing the raw bytes would split
// these into separate buckets and falsely fail agreement (see MAG-2062).
//
// It decodes into an interface{} and re-marshals; Go's encoding/json sorts map
// keys alphabetically on marshal, yielding a deterministic canonical byte form.
// json.Number (via UseNumber) preserves the literal numeric token rather than
// coercing through float64 — without this, two distinct large integers could
// round to the same float64 and produce a *false agreement*, which is worse
// than the false negative this fixes.
//
// Canonicalization normalizes structure only (key order, whitespace), never
// values: json.Number keeps numeric literals distinct, so the same value sent
// as 1.0 vs 1, or 100 vs 1e2, still hashes differently. That is intentional —
// the alternative (collapsing numerics) would risk a false agreement.
//
// If the data is not valid JSON (e.g. binary gRPC payloads), is not exactly one
// JSON value (trailing bytes or multiple concatenated documents), or fails to
// re-marshal, it falls back to hashing the raw bytes. The single-value check
// matters for safety: json.Decoder.Decode reads only the first value and
// silently ignores any trailing data, so without it two byte-different payloads
// (e.g. "{...}A" vs "{...}B") would collapse into one bucket — a false agreement
// that the raw-byte hash would otherwise have caught.
func canonicalResponseHash(data []byte) [32]byte {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return sha256.Sum256(data)
	}
	// Reject trailing data: a canonical response must be exactly one JSON value.
	if _, err := dec.Token(); err != io.EOF {
		return sha256.Sum256(data)
	}
	canonical, err := json.Marshal(v)
	if err != nil {
		return sha256.Sum256(data)
	}
	return sha256.Sum256(canonical)
}

// responseContentHash is the single content-hash rule for every cross-validation comparison:
// the canonical hash for non-empty data, the zero sentinel for empty/nil (so an empty reply only
// ever matches the nil-fallback consensus). handleResponse, the responsesCrossValidation
// recompute fallback, and the straggler classifier all go through here — one rule, no manual
// mirroring to drift (a canonicalization change like MAG-2062 lands everywhere at once).
func responseContentHash(data []byte) [32]byte {
	if len(data) == 0 {
		return [32]byte{}
	}
	return canonicalResponseHash(data)
}

func (rp *RelayProcessor) handleResponse(response *RelayResponse) {
	// Cache the SHA256 of the reply data BEFORE storing it. ResultsManager keeps RelayResult BY VALUE, so
	// if we hashed after SetResponse the stored copy in successResults would keep the zero hash and the
	// header path would compare zero hashes and treat real dissent as agreement. Computed for any reply
	// with data; only successful replies are counted toward quorum below (empty/nil replies stay zero).
	//
	// Gated on CrossValidation mode: the hash is only consumed by the CV quorum/header path, so canonicalizing
	// (decode + re-marshal of a potentially large body) for Stateless/Stateful traffic would be wasted work.
	// The winner path in responsesCrossValidation recomputes on a zero hash, so a missed cache stays correct.
	if rp.selection == CrossValidation && response != nil && response.RelayResult.GetReply() != nil && len(response.RelayResult.GetReply().GetData()) > 0 {
		// Canonicalize before hashing so semantically-identical responses that
		// differ only in JSON key order share a quorum bucket (MAG-2062).
		// responseContentHash is the shared rule with the responsesCrossValidation
		// fallback and the straggler classifier.
		response.RelayResult.ResponseHash = responseContentHash(response.RelayResult.GetReply().GetData())
	}

	nodeError := rp.ResultsManager.SetResponse(response, rp.RelayStateMachine.GetProtocolMessage())

	// send relay error metrics only on non stateful queries, as stateful queries always return X-1/X errors.
	if nodeError != nil && rp.selection != Stateful {
		chainId, apiInterface := rp.chainIdAndApiInterfaceGetter.GetChainIdAndApiInterface()
		go rp.metricsInf.SetRelayNodeErrorMetric(chainId, apiInterface, response.RelayResult.ProviderInfo.ProviderAddress, rp.RelayStateMachine.GetProtocolMessage().GetApi().Name)
		utils.LavaFormatInfo("Relay received a node error", utils.LogAttr("GUID", rp.guid), utils.LogAttr("Error", nodeError), utils.LogAttr("provider", response.RelayResult.ProviderInfo), utils.LogAttr("Request", rp.RelayStateMachine.GetProtocolMessage().GetApi().Name))
	}

	// Only successful responses (not errors) count toward cross-validation quorum.
	// Gate on CrossValidation mode: the quorumMap and per-group tally are only
	// consumed in CV mode (checkEndProcessing / HasRequiredNodeResults), so
	// Stateless/Stateful traffic skips the bookkeeping entirely — the hash was
	// already canonicalized above (before SetResponse) only when in CV mode.
	if rp.selection == CrossValidation && response != nil && nodeError == nil && response.Err == nil {
		hash := response.RelayResult.ResponseHash // already cached above (before SetResponse), canonicalized
		stat := rp.quorumMap[hash]
		if stat == nil {
			stat = &quorumStat{groupCounts: make(map[string]int)}
			rp.quorumMap[hash] = stat
		}
		stat.count++
		stat.groupCounts[quorumGroupOf(response.RelayResult)]++ // total count and per-group count recorded together
		if stat.count > rp.currentQuorumEqualResults {
			rp.currentQuorumEqualResults = stat.count
		}
	}

	// Update consistency cache only for successful responses (not stale/error responses)
	if response != nil && response.Err == nil && response.RelayResult.Reply != nil {
		if rp.consistency != nil && response.RelayResult.Reply.LatestBlock > 0 {
			// set consistency when possible
			blockSeen := response.RelayResult.Reply.LatestBlock
			userData := rp.RelayStateMachine.GetProtocolMessage().GetUserData()
			utils.LavaFormatDebug("updating consistency seenBlock",
				utils.LogAttr("blockSeen", blockSeen),
				utils.LogAttr("dappID", userData.DappId),
				utils.LogAttr("consumerIP", userData.ConsumerIp),
			)
			rp.consistency.SetSeenBlock(blockSeen, userData)
		} else {
			utils.LavaFormatTrace("consistency update skipped",
				utils.LogAttr("consistency_nil", rp.consistency == nil),
				utils.LogAttr("latestBlock", response.RelayResult.Reply.LatestBlock),
			)
		}
	}
}

func (rp *RelayProcessor) readExistingResponses() {
	for {
		select {
		case response := <-rp.responses:
			rp.handleResponse(response)
		default:
			// No more responses immediately available, exit the loop
			return
		}
	}
}

// CrossValidationStragglerResult describes how a cross-validation straggler resolved (MAG-2187): a
// provider that was queried but whose response had not arrived when the quorum early-exit built the
// reply (surfaced to the client in the pending-providers header).
type CrossValidationStragglerResult struct {
	ProviderAddress string
	ProviderGroup   string        // "" folds to common.DefaultProviderGroup at the metric layer
	Outcome         string        // one of common.CrossValidationStragglerOutcome*
	Delay           time.Duration // how long after the reply the straggler resolved (watch duration for not-received)
}

// WatchCrossValidationStragglers consumes responses that arrive on the processor's channel AFTER the
// early-exit reply, classifies each against the reached consensus, and reports it via record. It returns
// once every pending provider is accounted for, maxWait elapses, or ctx is done; providers still missing
// at that point are reported once with the not-received outcome. Responses from providers outside
// pendingProviders (e.g. an earlier retry batch's straggler, which the pending header does not cover)
// are consumed and dropped.
//
// Concurrency contract: start this only after the reply headers are built. WaitForResults has returned
// by then and nothing else reads rp.responses post-reply (readExistingResponses only runs inside
// NodeResults, which has no post-reply caller), so the watcher is the channel's sole reader. It never
// touches ResultsManager/quorumMap — classification is pure — so it cannot race the header path's reads.
func (rp *RelayProcessor) WatchCrossValidationStragglers(ctx context.Context, pendingProviders []string, consensusHash [32]byte, maxWait time.Duration, record func(CrossValidationStragglerResult)) {
	if rp == nil || record == nil {
		return
	}
	pending := make(map[string]struct{}, len(pendingProviders))
	for _, addr := range pendingProviders {
		if addr != "" {
			pending[addr] = struct{}{}
		}
	}
	if len(pending) == 0 {
		return
	}
	protocolMessage := rp.RelayStateMachine.GetProtocolMessage()
	start := time.Now()
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	resolve := func(response *RelayResponse) {
		if response == nil {
			return
		}
		addr := response.RelayResult.ProviderInfo.ProviderAddress
		if _, watched := pending[addr]; !watched {
			return
		}
		delete(pending, addr)
		record(CrossValidationStragglerResult{
			ProviderAddress: addr,
			ProviderGroup:   response.RelayResult.ProviderInfo.ProviderGroup,
			Outcome:         classifyStragglerResponse(response, consensusHash, protocolMessage),
			Delay:           time.Since(start),
		})
	}
	// resolveFromRecorded resolves pending providers whose response never reaches this watcher's
	// channel reads because another reader consumed it: a state-machine reader still blocked on
	// rp.responses when processing timed out drains responses while dying (select prefers ready
	// channels over its cancelled ctx pseudo-randomly). Whatever it consumed went through
	// handleResponse and is recorded in the ResultsManager, so classify from the stored result
	// instead of misreporting the provider as not-received. Checked at watcher start (theft happens
	// at reply time) and again at give-up as the final word before declaring not-received.
	resolveFromRecorded := func() {
		if len(pending) == 0 {
			return
		}
		// resolveOne records a watched provider from its stored result and drops it from pending;
		// a no-op for providers not in the pending set (already resolved, or never watched).
		resolveOne := func(addr, group, outcome string) {
			if _, watched := pending[addr]; !watched {
				return
			}
			delete(pending, addr)
			record(CrossValidationStragglerResult{
				ProviderAddress: addr,
				ProviderGroup:   group,
				Outcome:         outcome,
				Delay:           time.Since(start),
			})
		}
		successResults, nodeErrorResults, protocolErrorResults := rp.GetResultsData()
		for _, result := range successResults {
			// handleResponse cached ResponseHash via responseContentHash (zero for empty replies),
			// so this is the same comparison classifyStragglerResponse would have made.
			outcome := common.CrossValidationStragglerOutcomeDisagreed
			if result.ResponseHash == consensusHash {
				outcome = common.CrossValidationStragglerOutcomeAgreed
			}
			resolveOne(result.ProviderInfo.ProviderAddress, result.ProviderInfo.ProviderGroup, outcome)
		}
		for _, result := range nodeErrorResults {
			resolveOne(result.ProviderInfo.ProviderAddress, result.ProviderInfo.ProviderGroup, common.CrossValidationStragglerOutcomeNodeError)
		}
		for _, relayError := range protocolErrorResults {
			resolveOne(relayError.ProviderInfo.ProviderAddress, relayError.ProviderInfo.ProviderGroup, common.CrossValidationStragglerOutcomeProtocolError)
		}
	}
	// giveUp runs on deadline/cancel. It first drains responses already sitting in the buffered
	// channel: select picks among ready cases pseudo-randomly, so the timer can win a round even
	// though a response arrived in time (and record()'s log+metric I/O can push the loop past the
	// deadline while later responses sit buffered) — those must be classified by content, not
	// misreported as not-received. Then it consults the recorded results (responses another reader
	// consumed). Only providers with genuinely no response are reported missing.
	giveUp := func() {
		for len(pending) > 0 {
			select {
			case response := <-rp.responses:
				resolve(response)
			default:
				resolveFromRecorded()
				for addr := range pending {
					record(CrossValidationStragglerResult{
						ProviderAddress: addr,
						Outcome:         common.CrossValidationStragglerOutcomeNotReceived,
						Delay:           time.Since(start),
					})
				}
				return
			}
		}
	}
	resolveFromRecorded()
	for len(pending) > 0 {
		select {
		case response := <-rp.responses:
			resolve(response)
		case <-timer.C:
			giveUp()
			return
		case <-ctx.Done():
			giveUp()
			return
		}
	}
}

// classifyStragglerResponse maps a late cross-validation response to its straggler outcome. Hashing
// goes through responseContentHash — the same rule as handleResponse — so a late empty reply only
// "agrees" with a nil-fallback consensus, exactly as it would have at quorum time.
func classifyStragglerResponse(response *RelayResponse, consensusHash [32]byte, protocolMessage chainlib.ProtocolMessage) string {
	if response.Err != nil {
		return common.CrossValidationStragglerOutcomeProtocolError
	}
	reply := response.RelayResult.GetReply()
	if reply == nil {
		return common.CrossValidationStragglerOutcomeProtocolError
	}
	if protocolMessage != nil {
		if foundError, _ := protocolMessage.CheckResponseError(reply.Data, response.RelayResult.StatusCode); foundError {
			return common.CrossValidationStragglerOutcomeNodeError
		}
	}
	if responseContentHash(reply.Data) == consensusHash {
		return common.CrossValidationStragglerOutcomeAgreed
	}
	return common.CrossValidationStragglerOutcomeDisagreed
}

// this function waits for the processing results, they are written by multiple go routines and read by this go routine
// it then updates the responses in their respective place, node errors, protocol errors or success results
func (rp *RelayProcessor) WaitForResults(ctx context.Context) error {
	if rp == nil {
		return utils.LavaFormatError("RelayProcessor.WaitForResults is nil, misuse detected", nil)
	}
	responsesCount := 0
	for {
		select {
		case response := <-rp.responses:
			responsesCount++
			rp.handleResponse(response)
			if rp.checkEndProcessing(responsesCount) {
				// we can finish processing
				return nil
			}
		case <-ctx.Done():
			return utils.LavaFormatDebug("cancelled relay processor", utils.LogAttr("total responses", responsesCount))
		}
	}
}

// resultCount is the per-hash agreement tally built while scanning cross-validation responses: how many
// providers returned this exact response hash, one representative result for it, and the per-group breakdown
// of those providers (len(groupCounts) is the distinct-group count).
type resultCount struct {
	count       int
	result      common.RelayResult
	groupCounts map[string]int // per-group tally among the providers with this hash (len = distinct groups)
}

// quorumWinner is the outcome of selecting a cross-validation consensus winner over the per-hash tallies.
// `found` reports whether any candidate (a real response hash, or the nil-reply fallback) satisfied the
// quorum; the remaining fields let the caller both build the success result and, when found is false,
// classify the failure (no-agreement vs diversity-unmet vs group-quorum-unmet).
type quorumWinner struct {
	found            bool
	result           common.RelayResult
	hash             [32]byte // zero for a nil-reply winner (the empty-reply sentinel), as before
	count            int      // total agreement count of the chosen winner
	distinctGroups   int      // distinct groups of the chosen winner (for logging)
	qualifyingGroups int      // per-group: groups that EACH independently corroborated the chosen winner

	// Auxiliary counts for the failure-reason split, valid even when found is false:
	maxCount              int  // highest agreement count across ALL candidates, including nil replies
	maxRealCount          int  // highest REAL response-hash count, before nil replies inflate maxCount
	anyGroupReachedQuorum bool // per-group: did any single group reach its internal threshold at all
}

// selectQuorumWinner picks the cross-validation consensus winner from the per-hash agreement tallies. It is
// a pure function of its inputs (no RelayProcessor state) so the selection matrix can be unit-tested directly.
//
// Default (MinGroups) mode: the highest-count response hash that meets BOTH the agreement threshold AND the
// group-diversity requirement — a higher-count hash that fails diversity must NOT shadow a lower-count hash
// that satisfies both (P1). Per-group mode: the hash corroborated by the most groups that EACH independently
// reached the threshold, requiring at least minGroups such groups (tie-broken by total count). maxCount
// tracks the best total count regardless, only to distinguish the failure reasons.
//
// Nil replies are a fallback consensus, preferred only when no real response hash formed a diverse quorum.
// Per-group mode excludes them entirely: an empty reply is not an independent corroboration of a value.
// Ties on the deciding metric resolve to whichever hash map iteration visits first (unspecified order);
// callers and tests must not depend on the identity of a winner among equal candidates.
func selectQuorumWinner(guid uint64, countMap map[[32]byte]*resultCount, results []common.RelayResult, nilReplies, nilReplyIdx int, nilReplyGroups map[string]struct{}, crossValidationSize, minGroups int, perGroup bool) quorumWinner {
	var w quorumWinner
	for hash, count := range countMap {
		utils.LavaFormatDebug("🔍 [Quorum] Response group details",
			utils.LogAttr("GUID", guid),
			utils.LogAttr("responseHashHex", fmt.Sprintf("%x", hash[:8])),
			utils.LogAttr("matchingProviders", count.count),
			utils.LogAttr("distinctGroups", len(count.groupCounts)),
			utils.LogAttr("provider", count.result.ProviderInfo.ProviderAddress),
		)
		if count.count > w.maxCount {
			w.maxCount = count.count
		}
		if perGroup {
			qualGroups := qualifyingGroupCount(count.groupCounts, crossValidationSize)
			if qualGroups > 0 {
				w.anyGroupReachedQuorum = true
			}
			if qualGroups >= minGroups {
				if !w.found || qualGroups > w.qualifyingGroups || (qualGroups == w.qualifyingGroups && count.count > w.count) {
					w.found = true
					w.qualifyingGroups = qualGroups
					w.count = count.count
					w.result = count.result
					w.hash = hash
					w.distinctGroups = len(count.groupCounts)
				}
			}
		} else if count.count >= crossValidationSize && len(count.groupCounts) >= minGroups {
			// Default mode: count alone is never enough — len(groupCounts) >= minGroups means a single-group
			// hash can't win regardless of count (see hashQuorumReached for the full rule and rationale).
			if !w.found || count.count > w.count {
				w.found = true
				w.count = count.count
				w.result = count.result
				w.hash = hash
				w.distinctGroups = len(count.groupCounts)
			}
		}
	}

	// Capture the best REAL response-hash agreement count before nil replies inflate maxCount. The
	// no-agreement vs diversity-unmet split must key off real agreement: a large nil count that did NOT form
	// a diverse quorum means nothing agreed (no-agreement), not "a quorum agreed but failed to span the
	// required groups" (diversity-unmet).
	w.maxRealCount = w.maxCount

	if nilReplies > w.maxCount {
		w.maxCount = nilReplies
	}
	if !perGroup && !w.found && nilReplies >= crossValidationSize && len(nilReplyGroups) >= minGroups {
		w.found = true
		w.count = nilReplies
		w.result = results[nilReplyIdx]
		w.distinctGroups = len(nilReplyGroups)
		// Note: w.hash is intentionally left zero — the nil/empty-reply consensus has no real response hash.
		utils.LavaFormatInfo("🔍 [Quorum] Nil replies reached quorum",
			utils.LogAttr("GUID", guid),
			utils.LogAttr("nilRepliesCount", nilReplies),
			utils.LogAttr("requiredQuorumSize", crossValidationSize),
		)
	}
	return w
}

func (rp *RelayProcessor) responsesCrossValidation(results []common.RelayResult, crossValidationSize int) (returnedResult *common.RelayResult, failureReason string, processingError error) {
	if crossValidationSize <= 0 {
		return nil, "", errors.New("crossValidationSize must be greater than zero")
	}

	// Log quorum validation start
	utils.LavaFormatInfo("🔍 [Quorum Validation] Starting consensus check",
		utils.LogAttr("GUID", rp.guid),
		utils.LogAttr("totalResults", len(results)),
		utils.LogAttr("requiredQuorumSize", crossValidationSize),
		utils.LogAttr("agreementThreshold", rp.getAgreementThreshold()),
		utils.LogAttr("maxParticipants", rp.getMaxParticipants()),
	)

	countMap := make(map[[32]byte]*resultCount)
	nilReplies := 0
	nilReplyIdx := -1
	nilReplyGroups := make(map[string]struct{}) // distinct groups among nil/empty repliers (for the diversity gate)

	// Helper function to check if response data is valid
	isValidResponse := func(data []byte) bool {
		return len(data) > 0
	}

	for idx, result := range results {
		if result.Reply != nil && result.Reply.Data != nil && isValidResponse(result.Reply.Data) {
			// Use cached hash if available (set in handleResponse), otherwise compute it
			// This eliminates redundant SHA256 computation for responses already hashed
			hash := result.ResponseHash
			if hash == [32]byte{} {
				// Fallback: hash not cached (e.g., for old code paths or error cases).
				// responseContentHash is the shared rule with handleResponse, so the
				// cached and recomputed hashes agree (MAG-2062).
				hash = responseContentHash(result.Reply.Data)
			}

			if count, exists := countMap[hash]; exists {
				count.count++
				count.groupCounts[quorumGroupOf(result)]++
				utils.LavaFormatDebug("🔍 [Quorum] Response hash matches existing group",
					utils.LogAttr("GUID", rp.guid),
					utils.LogAttr("providerIdx", idx),
					utils.LogAttr("provider", result.ProviderInfo.ProviderAddress),
					utils.LogAttr("responseHashHex", fmt.Sprintf("%x", hash[:8])),
					utils.LogAttr("groupCount", count.count),
				)
			} else {
				countMap[hash] = &resultCount{
					count:       1,
					result:      result,
					groupCounts: map[string]int{quorumGroupOf(result): 1},
				}
				utils.LavaFormatDebug("🔍 [Quorum] New unique response hash detected",
					utils.LogAttr("GUID", rp.guid),
					utils.LogAttr("providerIdx", idx),
					utils.LogAttr("provider", result.ProviderInfo.ProviderAddress),
					utils.LogAttr("responseHashHex", fmt.Sprintf("%x", hash[:8])),
					utils.LogAttr("uniqueHashesCount", len(countMap)),
				)
			}
		} else {
			nilReplies++
			nilReplyIdx = idx
			nilReplyGroups[quorumGroupOf(result)] = struct{}{}
			utils.LavaFormatDebug("🔍 [Quorum] Nil or invalid response detected",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("providerIdx", idx),
				utils.LogAttr("nilRepliesCount", nilReplies),
			)
		}
	}

	minGroups := rp.getMinGroups()
	perGroup := rp.perGroupQuorum()

	utils.LavaFormatInfo("🔍 [Quorum] Response groups summary",
		utils.LogAttr("GUID", rp.guid),
		utils.LogAttr("uniqueResponseGroups", len(countMap)),
		utils.LogAttr("nilReplies", nilReplies),
		utils.LogAttr("minGroups", minGroups),
		utils.LogAttr("perGroupQuorum", perGroup),
	)

	winner := selectQuorumWinner(rp.guid, countMap, results, nilReplies, nilReplyIdx, nilReplyGroups, crossValidationSize, minGroups, perGroup)

	if !winner.found {
		if perGroup {
			// Per-group mode: either too few groups reached their own internal quorum, or the per-group
			// winners disagreed across groups (no single hash was corroborated by MinGroups groups). Both
			// surface as group-quorum-unmet, distinct from the MinGroups-mode diversity-unmet/no-agreement.
			return nil, common.CrossValidationReasonGroupQuorumUnmet, utils.LavaFormatInfo("cross-validation failed: per-group quorum not reached",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("anyGroupReachedInternalQuorum", winner.anyGroupReachedQuorum),
				utils.LogAttr("minGroups", minGroups),
				utils.LogAttr("agreementThreshold", crossValidationSize),
				utils.LogAttr("maxParticipants", rp.getMaxParticipants()))
		}
		if winner.maxRealCount < crossValidationSize {
			// No REAL response hash reached the agreement threshold (a plentiful nil count that failed to
			// form a diverse quorum is still "nothing agreed", not a diversity failure).
			if rp.selection == CrossValidation {
				return nil, common.CrossValidationReasonNoAgreement, utils.LavaFormatInfo("cross-validation failed: agreement threshold not reached",
					utils.LogAttr("nilReplies", nilReplies),
					utils.LogAttr("results", len(results)),
					utils.LogAttr("maxMatchingResults", winner.maxCount),
					utils.LogAttr("agreementThreshold", crossValidationSize),
					utils.LogAttr("maxParticipants", rp.getMaxParticipants()))
			}
			// Stateless/Stateful modes - return original error message
			return nil, common.CrossValidationReasonNoAgreement, utils.LavaFormatInfo("❌ [Quorum] FAILED - Majority count is less than required quorum size",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("nilReplies", nilReplies),
				utils.LogAttr("totalResults", len(results)),
				utils.LogAttr("maxCount", winner.maxCount),
				utils.LogAttr("crossValidationSize", crossValidationSize))
		}
		// Some hash reached the agreement threshold, but none spanned MinGroups distinct groups (1.2c).
		// A quorum within too few groups is a failure, not a success (a single-group outage/compromise
		// must not satisfy quorum on its own).
		return nil, common.CrossValidationReasonDiversityUnmet, utils.LavaFormatInfo("cross-validation failed: group-diversity requirement not met",
			utils.LogAttr("GUID", rp.guid),
			utils.LogAttr("bestAgreementCount", winner.maxCount),
			utils.LogAttr("minGroups", minGroups),
			utils.LogAttr("agreementThreshold", crossValidationSize))
	}

	mostCommonResult := winner.result
	mostCommonResult.CrossValidation = winner.count
	mostCommonResult.ResponseHash = winner.hash // ensure the returned consensus carries the winning hash

	// Log successful quorum consensus
	utils.LavaFormatInfo("✅ [Quorum] CONSENSUS REACHED",
		utils.LogAttr("GUID", rp.guid),
		utils.LogAttr("consensusProvider", mostCommonResult.ProviderInfo.ProviderAddress),
		utils.LogAttr("consensusHashHex", fmt.Sprintf("%x", winner.hash[:8])),
		utils.LogAttr("agreementCount", winner.count),
		utils.LogAttr("requiredQuorumSize", crossValidationSize),
		utils.LogAttr("totalResults", len(results)),
		utils.LogAttr("uniqueResponseGroups", len(countMap)),
		utils.LogAttr("consensusDistinctGroups", winner.distinctGroups),
		utils.LogAttr("perGroupQuorum", perGroup),
		utils.LogAttr("corroboratingGroups", winner.qualifyingGroups),
		utils.LogAttr("latestBlock", mostCommonResult.Reply.LatestBlock),
	)

	return &mostCommonResult, "", nil
}

// this function returns the results according to the defined strategy
// results were stored in WaitForResults and now there's logic to select which results are returned to the user
// will return an error if we did not meet quota of replies, if we did we follow the strategies:
// if return strategy == get_first: return the first success, if none: get best node error
// if strategy == stateless get majority of node responses
// on error: we will return a placeholder relayResult, with a provider address and a status code
func (rp *RelayProcessor) ProcessingResult() (returnedResult *common.RelayResult, processingError error) {
	if rp == nil {
		return nil, utils.LavaFormatError("RelayProcessor.ProcessingResult is nil, misuse detected", nil)
	}

	rp.lock.RLock()
	defer rp.lock.RUnlock()

	successResults, nodeErrors, protocolErrors := rp.GetResultsData()
	successResultsCount, nodeErrorCount, protocolErrorCount := len(successResults), len(nodeErrors), len(protocolErrors)

	if rp.debugRelay {
		// adding as much debug info as possible. all successful relays, all node errors and all protocol errors
		utils.LavaFormatDebug("[Processing Result] Debug Relay",
			utils.LogAttr("GUID", rp.guid),
			utils.LogAttr("selection", rp.selection),
			utils.LogAttr("agreementThreshold", rp.getAgreementThreshold()),
			utils.LogAttr("maxParticipants", rp.getMaxParticipants()))
		utils.LavaFormatDebug("[Processing Debug] number of node results", utils.LogAttr("GUID", rp.guid), utils.LogAttr("successResultsCount", successResultsCount), utils.LogAttr("nodeErrorCount", nodeErrorCount), utils.LogAttr("protocolErrorCount", protocolErrorCount))
		for idx, result := range successResults {
			utils.LavaFormatDebug("[Processing Debug] success result", utils.LogAttr("GUID", rp.guid), utils.LogAttr("idx", idx), utils.LogAttr("result", result))
		}
		for idx, result := range nodeErrors {
			utils.LavaFormatDebug("[Processing Debug] node result", utils.LogAttr("GUID", rp.guid), utils.LogAttr("idx", idx), utils.LogAttr("result", result))
		}
		for idx, result := range protocolErrors {
			utils.LavaFormatDebug("[Processing Debug] protocol error", utils.LogAttr("GUID", rp.guid), utils.LogAttr("idx", idx), utils.LogAttr("result", result))
		}
		utils.LavaFormatDebug("[ProcessingResult]:", utils.LogAttr("GUID", rp.guid), utils.LogAttr("successResultsCount", successResultsCount))
	}

	// Process results based on selection mode
	switch rp.selection {
	case CrossValidation:
		return rp.processCrossValidationResult(successResults, successResultsCount, nodeErrorCount, rp.getAgreementThreshold())

	case Stateful, Stateless:
		// Stateful (fan-out, no retries) and Stateless (sequential retries) differ only in the state
		// machine; result selection here is identical for both.
		return rp.processNonCrossValidationResult(successResults, nodeErrors, successResultsCount, nodeErrorCount, protocolErrorCount)

	default:
		return nil, utils.LavaFormatError("unknown selection mode", nil, utils.LogAttr("selection", rp.selection))
	}
}

// processCrossValidationResult handles result processing for CrossValidation mode.
// Only successful responses count towards consensus - node errors are ignored.
func (rp *RelayProcessor) processCrossValidationResult(
	successResults []common.RelayResult,
	successResultsCount, nodeErrorCount, requiredCrossValidationSize int,
) (*common.RelayResult, error) {
	// Check if we have enough successful responses
	if successResultsCount >= requiredCrossValidationSize {
		result, failureReason, err := rp.responsesCrossValidation(successResults, requiredCrossValidationSize)
		if err == nil {
			// Successes formed a quorum
			utils.LavaFormatInfo("✅ [ProcessingResult] Quorum formed with success responses",
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("successCount", successResultsCount),
				utils.LogAttr("quorumCount", result.CrossValidation),
				utils.LogAttr("selectedProvider", result.ProviderInfo.ProviderAddress),
				utils.LogAttr("nodeErrorCount", nodeErrorCount),
			)
			return result, nil
		}
		// Successful responses exist but did not form a quorum (either no agreement or insufficient group
		// diversity). Carry the specific reason on the minimal result so the client header can expose it.
		return &common.RelayResult{StatusCode: http.StatusInternalServerError, CrossValidationFailureReason: failureReason},
			utils.LavaFormatError("cross-validation failed: successful responses did not reach a diverse quorum",
				err,
				utils.LogAttr("GUID", rp.guid),
				utils.LogAttr("failureReason", failureReason),
				utils.LogAttr("successCount", successResultsCount),
				utils.LogAttr("agreementThreshold", requiredCrossValidationSize),
				utils.LogAttr("nodeErrorCount", nodeErrorCount),
			)
	}

	// Not enough successful responses
	// Return a minimal result so headers can be attached
	return &common.RelayResult{StatusCode: http.StatusInternalServerError, CrossValidationFailureReason: common.CrossValidationReasonInsufficientResponses},
		utils.LavaFormatError("cross-validation failed: insufficient successful responses",
			nil,
			utils.LogAttr("GUID", rp.guid),
			utils.LogAttr("successCount", successResultsCount),
			utils.LogAttr("agreementThreshold", requiredCrossValidationSize),
			utils.LogAttr("nodeErrorCount", nodeErrorCount),
			utils.LogAttr("maxParticipants", rp.getMaxParticipants()),
		)
}

// processNonCrossValidationResult handles Stateful and Stateless modes, which share the same result
// selection: first success, else first node error, else a failure result built from the best
// node/protocol error. (Stateful fans out without retries and Stateless retries sequentially, but that
// difference lives in the state machine, not here.)
func (rp *RelayProcessor) processNonCrossValidationResult(
	successResults, nodeErrors []common.RelayResult,
	successResultsCount, nodeErrorCount, protocolErrorCount int,
) (*common.RelayResult, error) {
	// Return first success if available
	if successResultsCount > 0 {
		result := successResults[0]
		return &result, nil
	}

	// No successes, return first node error if available
	if nodeErrorCount > 0 {
		result := nodeErrors[0]
		return &result, nil
	}

	// No node responses at all - build a failure result from the best node/protocol error.
	return rp.buildFailureResult(nodeErrorCount, protocolErrorCount)
}

// buildFailureResult constructs an error result when no consensus can be reached. It returns the best
// node/protocol error's own RelayResult, whose ProviderInfo.ProviderAddress is a SINGLE provider — the
// source of the error body being returned. It must never be a comma-joined list: packing the whole
// attempted set here made appendHeadersToRelayResult treat the joined blob as the resolver name and
// append it after the per-attempt names, so on the all-transport-errors path Lava-Provider-Address
// listed ~2x the providers and disagreed with Lava-Retries (MAG-2351).
func (rp *RelayProcessor) buildFailureResult(
	nodeErrorCount, protocolErrorCount int,
) (*common.RelayResult, error) {
	returnedResult := &common.RelayResult{StatusCode: http.StatusInternalServerError}
	var processingError error

	var bestLavaError *common.LavaError
	if nodeErrorCount > 0 {
		// Prefer node errors over protocol errors
		nodeErr := rp.GetBestNodeErrorMessageForUser()
		processingError = nodeErr.Err
		bestLavaError = nodeErr.LavaError
		if nodeErr.Response != nil {
			returnedResult = &nodeErr.Response.RelayResult
		}
	} else if protocolErrorCount > 0 {
		protocolErr := rp.GetBestProtocolErrorMessageForUser()
		processingError = protocolErr.Err
		bestLavaError = protocolErr.LavaError
		if protocolErr.Response != nil {
			returnedResult = &protocolErr.Response.RelayResult
		}
	}

	// Log with classified error code for metrics/observability
	if bestLavaError != nil {
		chainID, _ := rp.chainIdAndApiInterfaceGetter.GetChainIdAndApiInterface()
		common.LogCodedError("failed relay, insufficient results", processingError, bestLavaError,
			chainID, 0, "", utils.LogAttr("GUID", rp.guid))
	}

	return returnedResult, utils.LavaFormatError("failed relay, insufficient results", processingError, utils.LogAttr("GUID", rp.guid))
}
