package rpcsmartrouter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/internal/chainqueries"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/probing"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaypolicy"
	"github.com/magma-Devs/smart-router/protocol/tracing"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/protocopy"
	"github.com/magma-Devs/smart-router/version"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	grpcmetadata "google.golang.org/grpc/metadata"
)

const (
	// maximum number of retries to send due to the ticker, if we didn't get a response after 10 different attempts then just wait.
	MaximumNumberOfTickerRelayRetries = 10
	MaxRelayRetries                   = 6
	SendRelayAttempts                 = 3
	initRelaysDappId                  = "-init-"
	initRelaysSmartRouterIp           = ""

	PairingInitializationTimeout = 30 * time.Second
	PairingCheckInterval         = 1 * time.Second
	RelayRetryBackoffDuration    = 2 * time.Millisecond
)

// implements Relay Sender interfaced and uses an ChainListener to get it called
type RPCSmartRouterServer struct {
	chainParser            chainlib.ChainParser
	chainState             *chainstate.ChainState // MAG-2160 (Topic C): per-chain consensus tip — replaces the global ChainTracker, the estimator, and the atomic
	sessionManager         *lavasession.ConsumerSessionManager
	listenEndpoint         *lavasession.RPCEndpoint
	rpcSmartRouterLogs     *metrics.RPCConsumerLogs
	cache                  *performance.Cache
	smartRouterConsistency relaycore.Consistency
	consistencyConfig      *relaycore.ConsistencyValidationConfig // Configuration for consistency validation
	sharedState            bool                                   // using the cache backend to sync the latest seen block
	relaysMonitor          *metrics.RelaysMonitor
	debugRelays            bool
	chainListener          chainlib.ChainListener
	relayRetriesManager    *lavaprotocol.RelayRetriesManager
	initialized            atomic.Bool
	latestBlockHeight      atomic.Uint64 // MAG-2160: retained only as the cold-start fallback for getLatestBlock (the estimator is retired)
	enableSelectionStats   bool          // feature flag to enable selection stats header

	// Per-endpoint ChainTracker manager for continuous block polling
	endpointChainTrackerManager *endpointstate.EndpointMonitor

	// Direct WS subscription manager (nil if not configured); retained so
	// graceful shutdown can call Close() to drain upstream WS pools.
	wsSubscriptionManager chainlib.WSSubscriptionManager

	// gRPC streaming subscription manager (nil if not configured)
	grpcSubscriptionManager *DirectGRPCSubscriptionManager

	// Endpoint-scoped metrics manager (new spec)
	smartRouterEndpointMetrics *metrics.SmartRouterMetricsManager

	// Per-method cross-validation policy resolver (nil/empty => header-driven CV only).
	crossValidationResolver *CrossValidationPolicyResolver

	// probeStats holds the most-recent runProbeLoop cycle telemetry for /debug/probe-loop
	// (MAG-2202 endpoint 4). Written off the data plane by runProbeCycle; read by the debug handler.
	probeStats probeLoopStats
}

func (rpcss *RPCSmartRouterServer) ServeRPCRequests(
	ctx context.Context,
	listenEndpoint *lavasession.RPCEndpoint,
	chainParser chainlib.ChainParser,
	sessionManager *lavasession.ConsumerSessionManager,
	cache *performance.Cache,
	rpcSmartRouterLogs *metrics.RPCConsumerLogs,
	smartRouterConsistency relaycore.Consistency,
	relaysMonitor *metrics.RelaysMonitor,
	cmdFlags common.ConsumerCmdFlags,
	sharedState bool,
	wsSubscriptionManager chainlib.WSSubscriptionManager,
	smartRouterEndpointMetrics *metrics.SmartRouterMetricsManager,
) (err error) {
	rpcss.sessionManager = sessionManager
	rpcss.listenEndpoint = listenEndpoint
	rpcss.cache = cache
	rpcss.rpcSmartRouterLogs = rpcSmartRouterLogs
	rpcss.chainParser = chainParser
	rpcss.smartRouterConsistency = smartRouterConsistency
	rpcss.sharedState = sharedState
	rpcss.wsSubscriptionManager = wsSubscriptionManager
	rpcss.debugRelays = cmdFlags.DebugRelays
	rpcss.enableSelectionStats = cmdFlags.EnableSelectionStats
	rpcss.relayRetriesManager = lavaprotocol.NewRelayRetriesManager()

	// Load optional per-method cross-validation policies (empty => header-driven CV only, fully
	// backwards compatible). Fail fast on invalid config.
	cvConfig, cvErr := ParseCrossValidationConfig(viper.GetViper())
	if cvErr != nil {
		return cvErr
	}
	cvResolver, cvErr := NewCrossValidationPolicyResolver(cvConfig)
	if cvErr != nil {
		return cvErr
	}
	rpcss.crossValidationResolver = cvResolver
	if cvResolver.HasPolicies() {
		// Providers are already registered (UpdateAllProviders runs before ServeRPCRequests), so the
		// configured group layout is the upper bound for the startup capacity checks.
		groupAssignments := sessionManager.ProviderGroupAssignments()
		groupSizes := make(map[string]int, len(groupAssignments))
		for label, addrs := range groupAssignments {
			groupSizes[label] = len(addrs)
		}
		if cvStartupErr := validateCrossValidationStartup(cvResolver, chainParser, listenEndpoint.ChainID, listenEndpoint.ApiInterface, sessionManager.NumberOfValidProviderGroups(), groupSizes); cvStartupErr != nil {
			return cvStartupErr
		}
		// Log the resolved provider->group layout once at startup so operators can confirm the diversity
		// their config yields (a min-groups policy is only as good as the group spread of the fleet).
		utils.LavaFormatInfo("cross-validation per-method policies loaded",
			utils.LogAttr("policies", cvResolver.NumPolicies()),
			utils.LogAttr("chainID", listenEndpoint.ChainID),
			utils.LogAttr("apiInterface", listenEndpoint.ApiInterface),
			utils.LogAttr("distinctGroups", len(groupAssignments)),
			utils.LogAttr("groupSizes", groupSizes),
			utils.LogAttr("groupAssignments", groupAssignments))
	}

	// Initialize consistency validation config from chain spec values
	blockLagForQosSync, averageBlockTime, blockDistanceForFinalizedData, _ := chainParser.ChainBlockStats()
	rpcss.consistencyConfig = relaycore.NewConsistencyValidationConfig(
		blockLagForQosSync,
		blockDistanceForFinalizedData,
		averageBlockTime,
		relaycore.ConsistencyBlockGapFactorOverride, // polling-relief: 0 = default x2
	)

	// Assign before creating the manager so that goroutines spawned by
	// initializeChainTrackers always observe the field on their first callback.
	rpcss.smartRouterEndpointMetrics = smartRouterEndpointMetrics

	// Per-chain ChainState tip (MAG-2160 / Topic C). Constructed BEFORE the monitor so the
	// monitor's OnTipObservation hook can feed it. Phase 2 wires the writes (relay+poll
	// observations → SetLatestBlock) and the internal consensus recompute tick; the read sites
	// still use the legacy sources until phase 3.
	rpcss.chainState = chainstate.New(listenEndpoint.ChainID, chainstate.DefaultConfig(averageBlockTime))

	// Topic E (MAG-2160 Finding 2 / F4): point the relay sync dimension at THIS chain+interface's
	// consensus baseline, so relay sync lag is measured against the agreed tip rather than
	// max-block-across-providers (which one fast/lying reporter could inflate, dinging the whole pod).
	// The getter is installed on the per-interface CSM (NOT the shared per-chain optimizer, whose
	// single getter slot the last interface to start would overwrite — the F4 bug). It is read-only
	// toward the data plane (reads ChainState.GetConsensusBaselineWithTime). When there is no fresh
	// majority the relay omits the sync update rather than falling back to the legacy reference (F5).
	if rpcss.sessionManager != nil {
		rpcss.sessionManager.SetConsensusBaselineGetter(func() (uint64, time.Time, bool) {
			block, at, fresh := rpcss.chainState.GetConsensusBaselineWithTime()
			if !fresh || block <= 0 {
				return 0, time.Time{}, false
			}
			return uint64(block), at, true
		})
	}

	// Initialize per-endpoint ChainTracker manager for continuous block polling
	rpcss.endpointChainTrackerManager = endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      chainParser,
		ChainID:          listenEndpoint.ChainID,
		ApiInterface:     listenEndpoint.ApiInterface,
		AverageBlockTime: averageBlockTime,
		BlocksToSave:     endpointstate.DefaultBlocksToSave,
		// Feed every positive poll/relay block into the per-chain tip (cheap monotonic write).
		OnTipObservation: func(block int64) {
			rpcss.chainState.SetLatestBlock(block)
		},
		OnNewBlock: func(endpointURL string, fromBlock, toBlock int64) {
			utils.LavaFormatTrace("endpoint ChainTracker detected new block",
				utils.LogAttr("endpoint", endpointURL),
				utils.LogAttr("fromBlock", fromBlock),
				utils.LogAttr("toBlock", toBlock),
			)
			rpcss.smartRouterEndpointMetrics.RecordBlockFetch(listenEndpoint.ChainID, listenEndpoint.ApiInterface, endpointURL, true, true)
			rpcss.smartRouterEndpointMetrics.SetEndpointLatestBlock(listenEndpoint.ChainID, listenEndpoint.ApiInterface, endpointURL, toBlock)
		},
		OnFork: func(endpointURL string, blockNum int64) {
			utils.LavaFormatWarning("endpoint ChainTracker detected fork", nil,
				utils.LogAttr("endpoint", endpointURL),
				utils.LogAttr("blockNum", blockNum),
			)
		},
		OnFetchError: func(endpointURL string) {
			rpcss.smartRouterEndpointMetrics.RecordBlockFetch(listenEndpoint.ChainID, listenEndpoint.ApiInterface, endpointURL, true, false)
		},
	})

	// Consensus recompute tick (MAG-2160 / Topic C): periodically pull all per-endpoint
	// observation snapshots and let ChainState recompute its strict-majority baseline +
	// realign down. Off the hot path (relays/polls only do the cheap SetLatestBlock write via
	// OnTipObservation). The snapshot is taken under the monitor lock and released before
	// Recompute touches the ChainState lock — the two locks never nest.
	go rpcss.runChainStateConsensusLoop(ctx, averageBlockTime)

	// Proactive health prober (MAG-2160 / Topic D): on its own cadence, score EVERY direct-RPC
	// endpoint from stored telemetry + the consensus baseline (zero upstream calls), proactively
	// re-enable recovered endpoints, and feed one QoS sample per provider. Replaces the synthetic
	// direct-RPC probe (the legacy AppendProbeRelayData feed is gated off for static providers).
	go rpcss.runProbeLoop(ctx, validatedProbeCadence(lavasession.ProbeLoopInterval), probing.DefaultVerdictConfig(averageBlockTime))

	// NewChainListener now accepts WSSubscriptionManager interface, which is implemented
	// by both ConsumerWSSubscriptionManager (provider-relay mode) and
	// DirectWSSubscriptionManager (direct RPC mode for smart router).
	rpcss.chainListener, err = chainlib.NewChainListener(ctx, listenEndpoint, rpcss, rpcss, rpcSmartRouterLogs, chainParser, nil, wsSubscriptionManager)
	if err != nil {
		return err
	}

	go rpcss.chainListener.Serve(ctx, cmdFlags)

	initialRelays := true
	rpcss.relaysMonitor = relaysMonitor

	// we trigger a latest block call to get some more information on our RPC endpoints, using the relays monitor
	if cmdFlags.RelaysHealthEnableFlag {
		rpcss.relaysMonitor.SetRelaySender(func() (bool, error) {
			success, err := rpcss.sendCraftedRelaysWrapper(ctx, initialRelays)
			if success {
				initialRelays = false
			}
			return success, err
		})
		rpcss.relaysMonitor.Start(ctx)
	} else {
		rpcss.sendCraftedRelaysWrapper(ctx, true)
	}

	// Initialize ChainTrackers for all direct RPC endpoints in the background
	// This ensures fresh block data is available from startup, avoiding stale data issues
	go rpcss.initializeChainTrackers(ctx)

	return nil
}

func (rpcss *RPCSmartRouterServer) SetConsistencySeenBlock(blockSeen int64, key string) {
	rpcss.smartRouterConsistency.SetSeenBlockFromKey(blockSeen, key)
}

func (rpcss *RPCSmartRouterServer) GetListeningAddress() string {
	return rpcss.chainListener.GetListeningAddress()
}

// GetGRPCReflectionConnection implements chainlib.GRPCReflectionProvider.
// This enables gRPC reflection for tools like grpcurl when using Direct RPC mode.
// Returns a connection to the upstream gRPC server for reflection requests.
func (rpcss *RPCSmartRouterServer) GetGRPCReflectionConnection(ctx context.Context) (*grpc.ClientConn, func(), error) {
	if rpcss.grpcSubscriptionManager == nil {
		return nil, nil, fmt.Errorf("gRPC reflection not available: no gRPC subscription manager configured")
	}

	return rpcss.grpcSubscriptionManager.GetReflectionConnection(ctx)
}

func (rpcss *RPCSmartRouterServer) sendCraftedRelaysWrapper(ctx context.Context, initialRelays bool) (bool, error) {
	if initialRelays {
		// Only start after everything is initialized - check consumer session manager
		rpcss.waitForPairing(ctx)
	}
	success, err := rpcss.sendCraftedRelays(MaxRelayRetries, initialRelays)
	if success {
		rpcss.initialized.Store(true)
	}
	return success, err
}

func (rpcss *RPCSmartRouterServer) waitForPairing(ctx context.Context) {
	reinitializedChan := make(chan bool, 1) // Buffered channel to prevent deadlock

	go func() {
		ticker := time.NewTicker(PairingCheckInterval)
		defer ticker.Stop() // Ensure ticker is cleaned up

		for {
			select {
			case <-ctx.Done():
				// Context cancelled, exit goroutine
				return
			case <-ticker.C:
				if rpcss.sessionManager.Initialized() {
					// Non-blocking send to prevent deadlock
					select {
					case reinitializedChan <- true:
					default:
						// Channel already has value or receiver gone, but we can exit
					}
					return
				}
			}
		}
	}()

	numberOfTimesChecked := 0
	for {
		select {
		case <-reinitializedChan:
			return
		case <-ctx.Done():
			// Context cancelled, exit function
			return
		case <-time.After(PairingInitializationTimeout):
			numberOfTimesChecked += 1
			utils.LavaFormatWarning("failed initial relays, csm was not initialized after timeout, or pairing list is empty for that chain", nil,
				utils.LogAttr("times_checked", numberOfTimesChecked),
				utils.LogAttr("chainID", rpcss.listenEndpoint.ChainID),
				utils.LogAttr("APIInterface", rpcss.listenEndpoint.ApiInterface),
			)
		}
	}
}

func (rpcss *RPCSmartRouterServer) craftRelay(ctx context.Context) (ok bool, relay *pairingtypes.RelayPrivateData, chainMessage chainlib.ChainMessage, err error) {
	parsing, apiCollection, ok := rpcss.chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	if !ok {
		return false, nil, nil, utils.LavaFormatWarning("did not send initial relays because the spec does not contain required tag", nil,
			utils.LogAttr("tag", spectypes.FUNCTION_TAG_GET_BLOCKNUM),
			utils.LogAttr("chainID", rpcss.listenEndpoint.ChainID),
			utils.LogAttr("APIInterface", rpcss.listenEndpoint.ApiInterface),
		)
	}
	collectionData := apiCollection.CollectionData

	path := parsing.ApiName
	data := []byte(parsing.FunctionTemplate)
	chainMessage, err = rpcss.chainParser.ParseMsg(path, data, collectionData.Type, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	if err != nil {
		return false, nil, nil, utils.LavaFormatError("failed creating chain message in rpc consumer init relays", err,
			utils.LogAttr("chainID", rpcss.listenEndpoint.ChainID),
			utils.LogAttr("APIInterface", rpcss.listenEndpoint.ApiInterface))
	}

	reqBlock, _ := chainMessage.RequestedBlock()
	seenBlock := int64(0)
	relay = lavaprotocol.NewRelayData(ctx, collectionData.Type, path, data, seenBlock, reqBlock, rpcss.listenEndpoint.ApiInterface, chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), nil)
	return true, relay, chainMessage, nil
}

// validateCrossValidationStartup enforces, at startup, the cross-validation policy guards that need
// spec/provider context:
//   - The stateful-write guard: an enabled CV policy on a CONSISTENCY_SELECT_ALL_PROVIDERS method is a
//     no-op and must be rejected. It FAILS CLOSED — if the parser cannot classify stateful methods we
//     refuse to start rather than silently allow a write-method policy through.
//   - The min-groups capacity bound: an enabled min-groups policy that requires more distinct groups than
//     the endpoint has configured can never be satisfied.
//
// configuredGroups is the total distinct provider-group count for the endpoint (the per-request check
// tightens it to the addon/extension candidate set). groupSizes maps each group label to its provider
// count, used by the per-group-quorum capacity check (a per-group policy needs MinGroups groups that EACH
// have >= Threshold providers). Both are passed in to keep this function testable.
func validateCrossValidationStartup(resolver *CrossValidationPolicyResolver, chainParser chainlib.ChainParser, chainID, apiInterface string, configuredGroups int, groupSizes map[string]int) error {
	if !resolver.HasPolicies() {
		return nil
	}
	statefulChecker, ok := chainParser.(interface{ ApiHasStatefulCategory(string) bool })
	if !ok {
		return utils.LavaFormatError("cross-validation policies are configured but the chain parser cannot classify stateful methods; cannot enforce the write-method guard", nil,
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface))
	}
	isStateful := func(c, a, method string) bool {
		if !strings.EqualFold(c, chainID) || !strings.EqualFold(a, apiInterface) {
			return false // only this endpoint's parser can classify its own chain/api
		}
		return statefulChecker.ApiHasStatefulCategory(method)
	}
	if guardErr := resolver.ValidateNoStatefulPolicies(isStateful); guardErr != nil {
		return guardErr
	}
	if requiredGroups := resolver.MaxResolvedMinGroups(chainID, apiInterface); requiredGroups > 1 && configuredGroups > 0 && configuredGroups < requiredGroups {
		return utils.LavaFormatError("cross-validation min-groups policy cannot be satisfied: configured provider groups are fewer than required", nil,
			utils.LogAttr("requiredGroups", requiredGroups),
			utils.LogAttr("configuredGroups", configuredGroups),
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface))
	}
	// Per-group-quorum capacity: each per-group policy needs MinGroups groups that EACH have >= Threshold
	// providers. Without enough adequately-staffed groups the policy can never succeed (every request would
	// fail group-quorum-unmet), so reject it at startup rather than at runtime. Skipped when groupSizes is
	// empty (provider data unavailable) to avoid a false negative.
	if len(groupSizes) > 0 {
		for _, req := range resolver.PerGroupRequirements(chainID, apiInterface) {
			adequateGroups := 0
			for _, size := range groupSizes {
				if size >= req.Threshold {
					adequateGroups++
				}
			}
			if adequateGroups < req.MinGroups {
				return utils.LavaFormatError("cross-validation per-group-quorum policy cannot be satisfied: too few provider groups have enough providers to each reach the agreement threshold", nil,
					utils.LogAttr("requiredGroups", req.MinGroups),
					utils.LogAttr("agreementThreshold", req.Threshold),
					utils.LogAttr("groupsWithEnoughProviders", adequateGroups),
					utils.LogAttr("groupSizes", groupSizes),
					utils.LogAttr("chainID", chainID),
					utils.LogAttr("apiInterface", apiInterface))
			}
		}
	}
	// SPOF advisory for DEFAULT min-groups mode (per-group mode is hard-failed on the stronger shape above).
	// When a diversity policy (MinGroups > 1) is active but some groups are smaller than the agreement
	// threshold, a diverse quorum can be carried by groups too small to corroborate a value on their own — so
	// the diversity guarantee rests on single points of failure (two such groups colluding, or both wrong at a
	// block boundary, outvote a larger honest group). This is still a SATISFIABLE config (default mode counts
	// agreement across groups, not within them), so it is a WARNING, not a startup error. Skipped when
	// groupSizes is empty (provider data unavailable) to avoid a false warning.
	if len(groupSizes) > 0 {
		for _, req := range resolver.MinGroupsRequirements(chainID, apiInterface) {
			if req.MinGroups <= 1 {
				continue // no diversity requirement, so nothing rests on under-staffed groups
			}
			if below := groupsBelowThreshold(groupSizes, req.Threshold); len(below) > 0 {
				utils.LavaFormatWarning("cross-validation group-diversity may rest on single points of failure: some provider groups are smaller than the agreement threshold and cannot corroborate a response on their own, yet can still carry the required group diversity", nil,
					utils.LogAttr("groupsBelowThreshold", below),
					utils.LogAttr("agreementThreshold", req.Threshold),
					utils.LogAttr("minGroups", req.MinGroups),
					utils.LogAttr("groupSizes", groupSizes),
					utils.LogAttr("chainID", chainID),
					utils.LogAttr("apiInterface", apiInterface))
			}
		}
	}
	return nil
}

// groupsBelowThreshold returns, sorted for stable logging, the labels of provider groups whose size is below
// the agreement threshold. It is a pure function so the startup SPOF advisory can be unit-tested directly.
//
// Such a group can still contribute to the distinct-group count that default-mode min-groups quorum requires
// (default mode counts agreement ACROSS groups, so a single-node group is a valid corroborator), yet it
// cannot reach the agreement threshold on its own — so a diverse quorum can end up carried by groups too
// small to corroborate a value internally. This is the SPOF condition the startup warning reports; it is
// advisory only (the config is still satisfiable), never a startup error.
func groupsBelowThreshold(groupSizes map[string]int, threshold int) []string {
	var below []string
	for label, size := range groupSizes {
		if size < threshold {
			below = append(below, label)
		}
	}
	sort.Strings(below)
	return below
}

// crossValidationSuccessOutliers returns the successful responses whose content diverged from the reached
// consensus — the ONLY inputs to the mismatch metric (1.3). It returns nil unless quorum was reached
// (cvSuccess) and the method is deterministic; node/protocol errors and quorum failures are not content
// outliers, and non-deterministic methods legitimately differ. A response with no provider address is
// skipped.
func crossValidationSuccessOutliers(successResults []common.RelayResult, consensusHash [32]byte, cvSuccess, deterministic bool) []common.RelayResult {
	if !cvSuccess || !deterministic {
		return nil
	}
	// When the reached consensus is the nil/empty-reply fallback, consensusHash is left as the zero
	// sentinel: there is no real content consensus to diverge FROM. Comparing real responses against the
	// zero hash would flag every substantive responder as a content outlier and inflate mismatch alerts —
	// yet in that case the empty-reply majority is the anomaly, not the lone real responder. Emit nothing.
	if consensusHash == ([32]byte{}) {
		return nil
	}
	var outliers []common.RelayResult
	for _, result := range successResults {
		if result.ProviderInfo.ProviderAddress != "" && result.ResponseHash != consensusHash {
			outliers = append(outliers, result)
		}
	}
	return outliers
}

// preferStructuralFailureReason overwrites a cross-validation FAILURE result's reason with a structural
// request-time fail-fast reason (insufficient-capacity / insufficient-groups) when one was set. The
// structural reason means the fleet cannot satisfy the policy at all — strictly more actionable for the
// client than the final-eval reason (no-agreement / diversity-unmet, i.e. "the providers disagreed"). A
// no-op when there is no result or no fail-fast reason. Callers gate this on failure (err != nil) so a
// successful relay never inherits a stale reason.
func preferStructuralFailureReason(result *common.RelayResult, failFastReason string) {
	if result != nil && failFastReason != "" {
		result.CrossValidationFailureReason = failFastReason
	}
}

// crossValidationFinalityLabel returns the tri-state finality label (finalized / not_finalized / unknown)
// for the request, used by the mismatch metric so alerts can prioritise post-finality divergence over
// natural pre-finality propagation lag. "unknown" covers sentinel requested blocks (latest/pending/
// not-applicable) and the case where the latest block is not yet known.
func (rpcss *RPCSmartRouterServer) crossValidationFinalityLabel(protocolMessage chainlib.ProtocolMessage) string {
	requestedBlock, _ := protocolMessage.RequestedBlock()
	// Use the router's chain-tracker/estimator-aware latest block, not just the cached counter, so the
	// finality label is correct whenever any latest-block source is available.
	latestBlock := int64(rpcss.getLatestBlock())
	_, _, blockDistanceForFinalizedData, _ := rpcss.chainParser.ChainBlockStats()
	return crossValidationFinality(requestedBlock, latestBlock, int64(blockDistanceForFinalizedData))
}

// crossValidationFinality is the pure tri-state finality classifier (finalized / not_finalized / unknown).
// "unknown" covers a sentinel requested block (latest/pending/not-applicable, all < 0) and an unknown
// latest block (<= 0).
func crossValidationFinality(requestedBlock, latestBlock, finalizationDistance int64) string {
	if requestedBlock < 0 || latestBlock <= 0 {
		return "unknown"
	}
	if spectypes.IsFinalizedBlock(requestedBlock, latestBlock, finalizationDistance) {
		return "finalized"
	}
	return "not_finalized"
}

// validateCrossValidationCapacity returns an error when CrossValidation mode is active but the request's
// concrete candidate set (providers that support the request's addon + extensions) cannot satisfy the
// requested shape: either MaxParticipants exceeds the number of candidate endpoints, or MinGroups exceeds
// the number of distinct candidate provider groups (empty GroupLabel counts as common.DefaultProviderGroup).
// Counting against the addon/extension-filtered candidate set — not all valid providers — avoids passing a
// request that is actually satisfiable only by a narrower, under-grouped set. The MinGroups check is the
// Phase 1.1 capacity guard; full group-aware quorum selection lands in Phase 1.2.
// The returned string is the structured cross-validation failure reason (one of the
// CrossValidationReasonInsufficient* request-time values), so the caller can surface it to the client via
// the failure-reason header; it is "" when there is no error.
func (rpcss *RPCSmartRouterServer) validateCrossValidationCapacity(ctx context.Context, selection relaycore.Selection, params *common.CrossValidationParams, addon string, extensions []string) (string, error) {
	if selection != relaycore.CrossValidation || params == nil {
		return "", nil
	}
	candidateProviders, candidateGroups := rpcss.sessionManager.ProviderAndGroupCountsForRequest(addon, extensions, ctx)
	if params.MaxParticipants > candidateProviders {
		return common.CrossValidationReasonInsufficientCapacity, utils.LavaFormatError("requested cross-validation maxParticipants exceeds available candidate endpoints",
			lavasession.PairingListEmptyError,
			utils.LogAttr("maxParticipants", params.MaxParticipants),
			utils.LogAttr("candidateEndpoints", candidateProviders),
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("GUID", ctx))
	}
	if params.MinGroups > candidateGroups {
		return common.CrossValidationReasonInsufficientGroups, utils.LavaFormatError("cross-validation minGroups exceeds available distinct candidate provider groups",
			lavasession.PairingListEmptyError,
			utils.LogAttr("minGroups", params.MinGroups),
			utils.LogAttr("candidateProviderGroups", candidateGroups),
			utils.LogAttr("addon", addon),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("GUID", ctx))
	}
	// Per-group quorum (2.3) is stricter than MinGroups diversity: each of MinGroups groups must
	// independently reach AgreementThreshold matching responses. Two extra request-time guards:
	//   1. The shape must be self-consistent — MaxParticipants >= MinGroups * AgreementThreshold — so a
	//      caller-loosened max (or any effective params) can physically fit a quorum per group. This catches
	//      caller-induced impossibility that config-time Validate cannot (the caller raises the threshold).
	//   2. At least MinGroups candidate groups must EACH have >= AgreementThreshold providers; distinct-group
	//      count alone (checked above) is insufficient when a group has too few providers to ever corroborate.
	if params.PerGroupQuorum {
		if needed := params.MinGroups * params.AgreementThreshold; params.MaxParticipants < needed {
			return common.CrossValidationReasonInsufficientCapacity, utils.LavaFormatError("per-group cross-validation requires maxParticipants >= minGroups * agreementThreshold",
				lavasession.PairingListEmptyError,
				utils.LogAttr("maxParticipants", params.MaxParticipants),
				utils.LogAttr("minGroups", params.MinGroups),
				utils.LogAttr("agreementThreshold", params.AgreementThreshold),
				utils.LogAttr("needed", needed),
				utils.LogAttr("GUID", ctx))
		}
		groupCounts := rpcss.sessionManager.GroupCountsForRequest(addon, extensions, ctx)
		adequateGroups := countAdequateGroups(groupCounts, params.AgreementThreshold)
		if adequateGroups < params.MinGroups {
			return common.CrossValidationReasonInsufficientGroups, utils.LavaFormatError("per-group cross-validation: too few candidate groups have enough providers to each reach the agreement threshold",
				lavasession.PairingListEmptyError,
				utils.LogAttr("minGroups", params.MinGroups),
				utils.LogAttr("agreementThreshold", params.AgreementThreshold),
				utils.LogAttr("groupsWithEnoughProviders", adequateGroups),
				utils.LogAttr("candidateGroupCounts", groupCounts),
				utils.LogAttr("addon", addon),
				utils.LogAttr("extensions", extensions),
				utils.LogAttr("GUID", ctx))
		}
	}
	return "", nil
}

// countAdequateGroups returns how many groups in the per-group breakdown have at least `threshold`
// providers/sessions — i.e. how many groups can independently reach their internal quorum. Mirrors
// relaycore's per-group qualifying-group count for the capacity/post-filter guards.
func countAdequateGroups(groupCounts map[string]int, threshold int) int {
	n := 0
	for _, c := range groupCounts {
		if c >= threshold {
			n++
		}
	}
	return n
}

// crossValidationGroupShortfall evaluates whether a set of per-group counts can still satisfy the group
// requirement of a cross-validation policy, returning the qualifying-group count and a non-empty structured
// fail reason when it cannot. For PerGroupQuorum a group qualifies only with >= AgreementThreshold members
// (so MinGroups groups can each reach their internal quorum); otherwise any non-empty group qualifies
// (MinGroups diversity). Used by the post-consistency-filter guard so per-group failures are caught before
// relays launch. Returns ("", 0-ok) shape: failReason == "" means the requirement is still satisfiable.
func crossValidationGroupShortfall(groupCounts map[string]int, params *common.CrossValidationParams) (qualifying int, failReason string) {
	if params == nil || params.MinGroups <= 1 {
		return len(groupCounts), ""
	}
	if params.PerGroupQuorum {
		qualifying = countAdequateGroups(groupCounts, params.AgreementThreshold)
		if qualifying < params.MinGroups {
			return qualifying, common.CrossValidationReasonInsufficientGroups
		}
		return qualifying, ""
	}
	qualifying = len(groupCounts)
	if qualifying < params.MinGroups {
		return qualifying, common.CrossValidationReasonInsufficientGroups
	}
	return qualifying, ""
}

// crossValidationFailFastResult builds the minimal RelayResult returned on a request-time cross-validation
// fail-fast: no relay completed, so there is no upstream reply, but the cross-validation status and the
// structured failure reason are carried as response-header metadata. The interface listeners write this
// metadata onto the HTTP error response (alongside the JSON-RPC error body), so the client can distinguish
// a capacity/diversity failure from a generic upstream error without a parallel signalling mechanism.
func crossValidationFailFastResult(reason string) *common.RelayResult {
	return &common.RelayResult{
		StatusCode:                   http.StatusInternalServerError,
		CrossValidationFailureReason: reason,
		Reply: &pairingtypes.RelayReply{
			Metadata: []pairingtypes.Metadata{
				{Name: common.CROSS_VALIDATION_STATUS_HEADER_NAME, Value: "failed"},
				{Name: common.CROSS_VALIDATION_FAILURE_REASON_HEADER, Value: reason},
			},
		},
	}
}

// crossValidationFailFast records the request/failed cross-validation metrics for a request-time
// structural fail-fast and returns the minimal failure result. This path returns from SendParsedRelay
// BEFORE appendHeadersToRelayResult — where requests_total / failed_total are normally emitted — so without
// this a structural failure (insufficient-capacity / insufficient-groups) would never be counted. The
// fail-fast and normal return paths are mutually exclusive, so this never double-counts a quorum-time
// failure. No agreeing/disagreeing providers are reported because no relay completed. Emitted synchronously
// (a cheap counter increment on an already-failing path) so the count is deterministic for callers/tests.
func (rpcss *RPCSmartRouterServer) crossValidationFailFast(reason string, protocolMessage chainlib.ProtocolMessage) *common.RelayResult {
	if rpcss.rpcSmartRouterLogs != nil && rpcss.listenEndpoint != nil {
		chainID, apiInterface, method := rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface, protocolMessage.GetApi().GetName()
		rpcss.rpcSmartRouterLogs.SetCrossValidationMetric(chainID, apiInterface, method, false, nil, nil)
		// Bounded by-reason breakdown for the structural fail-fast (insufficient-capacity / insufficient-groups).
		rpcss.rpcSmartRouterLogs.SetCrossValidationFailureMetric(chainID, apiInterface, method, reason)
	}
	return crossValidationFailFastResult(reason)
}

func (rpcss *RPCSmartRouterServer) sendRelayWithRetries(ctx context.Context, retries int, initialRelays bool, protocolMessage chainlib.ProtocolMessage) (bool, error) {
	success := false
	var err error
	usedProviders := lavasession.NewUsedProviders(nil)
	usedProviders.SetChainID(rpcss.listenEndpoint.ChainID)
	usedProviders.SetEligibilityFunc(relaypolicy.DecideEligibility)

	// Create state machine first - it determines Selection type based on per-method policy + headers
	stateMachine, err := NewSmartRouterRelayStateMachineWithPolicy(ctx, usedProviders, rpcss, protocolMessage, nil, rpcss.debugRelays, rpcss.crossValidationResolver, rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface)
	if err != nil {
		return false, err
	}

	// Get cross-validation parameters from the state machine (nil for Stateless/Stateful)
	crossValidationParams := stateMachine.GetCrossValidationParams()

	// Initial/health relays are not client-facing, so the structured reason is unused here.
	if _, err := rpcss.validateCrossValidationCapacity(ctx, stateMachine.GetSelection(), crossValidationParams, chainlib.GetAddon(protocolMessage), common.GetExtensionNames(protocolMessage.GetExtensions())); err != nil {
		return false, err
	}

	// Direct RPC flow: pass nil for availabilityDegrader since there are no Lava protocol sessions.
	// QoS punishment for node errors is handled by the optimizer via AppendRelayFailure in OnSessionFailure.
	relayProcessor := relaycore.NewRelayProcessor(
		ctx,
		crossValidationParams,
		rpcss.smartRouterConsistency,
		rpcss.rpcSmartRouterLogs,
		rpcss,
		rpcss.relayRetriesManager,
		stateMachine,
	)
	usedEndpointsResets := 1
	for i := 0; i < retries; i++ {
		// Check if we even have enough endpoints to communicate with them all.
		// If we have 1 endpoint we will reset the used endpoints always.
		// Instead of spamming no pairing available on bootstrap
		if ((i + 1) * usedEndpointsResets) > rpcss.sessionManager.GetNumberOfValidProviders() {
			usedEndpointsResets++
			relayProcessor.GetUsedProviders().ClearUnwanted()
		}
		err = rpcss.sendRelayToEndpoint(ctx, 1, relaycore.GetEmptyRelayState(ctx, protocolMessage), relayProcessor, nil)
		if errors.Is(err, lavasession.PairingListEmptyError) {
			// we don't have pairings anymore, could be related to unwanted endpoints
			relayProcessor.GetUsedProviders().ClearUnwanted()
			err = rpcss.sendRelayToEndpoint(ctx, 1, relaycore.GetEmptyRelayState(ctx, protocolMessage), relayProcessor, nil)
		}
		if err != nil {
			utils.LavaFormatError("[-] failed sending init relay", err, []utils.Attribute{{Key: "GUID", Value: ctx}, {Key: "chainID", Value: rpcss.listenEndpoint.ChainID}, {Key: "APIInterface", Value: rpcss.listenEndpoint.ApiInterface}, {Key: "relayProcessor", Value: relayProcessor}}...)
		} else {
			err := relayProcessor.WaitForResults(ctx)
			if err != nil {
				utils.LavaFormatError("[-] failed sending init relay", err, []utils.Attribute{{Key: "GUID", Value: ctx}, {Key: "chainID", Value: rpcss.listenEndpoint.ChainID}, {Key: "APIInterface", Value: rpcss.listenEndpoint.ApiInterface}, {Key: "relayProcessor", Value: relayProcessor}}...)
			} else {
				relayResult, err := relayProcessor.ProcessingResult()
				if err == nil && relayResult != nil && relayResult.Reply != nil {
					utils.LavaFormatInfo("[+] init relay succeeded",
						utils.LogAttr("GUID", ctx),
						utils.LogAttr("chainID", rpcss.listenEndpoint.ChainID),
						utils.LogAttr("APIInterface", rpcss.listenEndpoint.ApiInterface),
						utils.LogAttr("latestBlock", relayResult.Reply.LatestBlock),
						utils.LogAttr("endpoint", relayResult.ProviderInfo.ProviderAddress),
					)
					if relayResult.Reply.LatestBlock > 0 {
						rpcss.updateLatestBlockHeight(uint64(relayResult.Reply.LatestBlock), relayResult.ProviderInfo.ProviderAddress)
					}
					rpcss.relaysMonitor.LogRelay()
					success = true
					// If this is the first time we send relays, we want to send all of them, instead of break on first successful relay
					// That way, we populate the endpoints with the latest blocks with successful relays
					if !initialRelays {
						break
					}
				} else if err != nil {
					utils.LavaFormatError("[-] failed sending init relay", err, []utils.Attribute{{Key: "GUID", Value: ctx}, {Key: "chainID", Value: rpcss.listenEndpoint.ChainID}, {Key: "APIInterface", Value: rpcss.listenEndpoint.ApiInterface}, {Key: "relayProcessor", Value: relayProcessor}}...)
				} else {
					utils.LavaFormatError("[-] failed sending init relay - nil result", nil, []utils.Attribute{{Key: "GUID", Value: ctx}, {Key: "chainID", Value: rpcss.listenEndpoint.ChainID}, {Key: "APIInterface", Value: rpcss.listenEndpoint.ApiInterface}, {Key: "relayProcessor", Value: relayProcessor}}...)
				}
			}
		}
		time.Sleep(RelayRetryBackoffDuration)
	}

	return success, err
}

// sending a few latest blocks relays to RPC endpoints in order to have data for endpoint selection when relays start arriving
func (rpcss *RPCSmartRouterServer) sendCraftedRelays(retries int, initialRelays bool) (bool, error) {
	utils.LavaFormatDebug("Sending crafted relays",
		utils.LogAttr("chainId", rpcss.listenEndpoint.ChainID),
		utils.LogAttr("apiInterface", rpcss.listenEndpoint.ApiInterface),
	)

	ctx := utils.WithUniqueIdentifier(context.Background(), utils.GenerateUniqueIdentifier())
	ok, relay, chainMessage, _ := rpcss.craftRelay(ctx)
	if !ok {
		return true, nil
	}
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relay, initRelaysDappId, initRelaysSmartRouterIp)
	return rpcss.sendRelayWithRetries(ctx, retries, initialRelays, protocolMessage)
}

func (rpcss *RPCSmartRouterServer) getLatestBlock() uint64 {
	// Site A (MAG-2160): the router-wide tip (archive routing + cache-finalization) is the
	// per-chain ChainState consensus tip — TTL-fresh, monotonic, outlier-guarded — replacing the
	// global-tracker → estimator → atomic ladder.
	if rpcss.chainState != nil {
		if block, ok := rpcss.chainState.GetLatestBlock(); ok && block > 0 {
			return uint64(block)
		}
		// ChainState is the authority once it has EVER observed a tip. If it is Initialized but
		// not fresh, the tip has aged past TTL — we must NOT revive a frozen atomic value
		// (Finding 1). Report unknown (0); a stale-but-real tip is worse than an honest "unknown",
		// and the atomic's last write could itself be the stale value we just rejected.
		if rpcss.chainState.Initialized() {
			return 0
		}
	}

	// Genuine cold-start ONLY (ChainState never initialized): in the bootstrap window before the
	// first observation, fall back to the monotonic atomic — seeded by the tip-eligible init
	// relay — so getLatestBlock isn't 0 during startup. Once ChainState initializes, this branch
	// is never taken again, so a post-TTL gap cannot resurrect a frozen atomic.
	if latest := rpcss.latestBlockHeight.Load(); latest > 0 {
		return latest
	}

	return 0
}

func (rpcss *RPCSmartRouterServer) updateLatestBlockHeight(blockHeight uint64, providerAddress string) {
	for {
		current := rpcss.latestBlockHeight.Load()
		if blockHeight <= current {
			break
		}
		if rpcss.latestBlockHeight.CompareAndSwap(current, blockHeight) {
			// Update router-level latest block metric
			if rpcss.smartRouterEndpointMetrics != nil {
				rpcss.smartRouterEndpointMetrics.SetRouterLatestBlock(
					rpcss.listenEndpoint.ChainID,
					rpcss.listenEndpoint.ApiInterface,
					int64(blockHeight),
				)
			}
			break
		}
	}
}

func (rpcss *RPCSmartRouterServer) SendRelay(
	ctx context.Context,
	url string,
	req string,
	connectionType string,
	dappID string,
	consumerIp string,
	analytics *metrics.RelayMetrics,
	metadata []pairingtypes.Metadata,
) (relayResult *common.RelayResult, errRet error) {
	// Inject client IP into context so IP forwarding (X-Forwarded-For) works when using HTTP listener.
	// GetIpFromGrpcContext reads from gRPC peer or from incoming metadata.
	if consumerIp != "" {
		md := grpcmetadata.Pairs(common.IP_FORWARDING_HEADER_NAME, consumerIp)
		ctx = grpcmetadata.NewIncomingContext(ctx, md)
	}

	// Extract W3C TraceContext (traceparent/tracestate) from incoming request headers
	// so that the relay span becomes a child of the caller's trace when present.
	ctx = tracing.ExtractHTTP(ctx, metadata)

	// Start the inbound SERVER span. It covers the full relay lifecycle
	// including parsing — all downstream helpers create child spans from
	// the resulting context. We also pin the span on the context via
	// WithRelaySpan so deeply nested helpers (e.g. cache lookup) can decorate
	// it directly without plumbing the span through every signature.
	ctx, span := tracing.StartServerSpan(ctx, tracing.SpanSendRelay)
	defer span.End()
	ctx = tracing.WithRelaySpan(ctx, span)

	chainId, apiInterface := rpcss.GetChainIdAndApiInterface()
	guid, _ := utils.GetUniqueIdentifier(ctx)
	tracing.RecordRelayAttributes(span, guid, chainId, apiInterface)
	if tracing.IsTraceBodyEnabled() {
		tracing.RecordBody(span, tracing.AttrRelayRequestBody, []byte(req))
	}

	protocolMessage, err := rpcss.ParseRelay(ctx, url, req, connectionType, dappID, consumerIp, metadata)
	if err != nil {
		tracing.RecordError(span, err)
		return nil, err
	}

	if api := protocolMessage.GetApi(); api != nil {
		tracing.RecordRelayMethod(span, api.Name)
	}

	return rpcss.SendParsedRelay(ctx, analytics, protocolMessage)
}

// ParseRelay gets the relay request data from the ChainListener,
// parses the request into an APIMessage, validates it against the spec,
// and constructs the relay request data for sending to RPC endpoints.
func (rpcss *RPCSmartRouterServer) ParseRelay(
	ctx context.Context,
	url string,
	req string,
	connectionType string,
	dappID string,
	consumerIp string,
	metadata []pairingtypes.Metadata,
) (protocolMessage chainlib.ProtocolMessage, err error) {
	ctx, span := tracing.StartInternalSpan(ctx, tracing.SpanParseRelay)
	defer span.End()

	// remove lava directive headers
	metadata, directiveHeaders := rpcss.LavaDirectiveHeaders(metadata)
	extensions := rpcss.getExtensionsFromDirectiveHeaders(directiveHeaders)
	utils.LavaFormatTrace("[Archive Debug] ParseRelay extensions",
		utils.LogAttr("extensions", extensions),
		utils.LogAttr("GUID", ctx))
	utils.LavaFormatTrace("[Archive Debug] Calling chainParser.ParseMsg", utils.LogAttr("url", url), utils.LogAttr("req", req), utils.LogAttr("extensions", extensions), utils.LogAttr("chainParserType", rpcss.chainParser), utils.LogAttr("GUID", ctx))
	chainMessage, err := rpcss.chainParser.ParseMsg(url, []byte(req), connectionType, metadata, extensions)
	if err != nil {
		tracing.RecordError(span, err)
		return nil, err
	}

	rpcss.HandleDirectiveHeadersForMessage(chainMessage, directiveHeaders)

	// do this in a loop with retry attempts, configurable via a flag, limited by the number of endpoints
	reqBlock, _ := chainMessage.RequestedBlock()
	seenBlock, _ := rpcss.smartRouterConsistency.GetSeenBlock(common.UserData{DappId: dappID, ConsumerIp: consumerIp})
	if seenBlock < 0 {
		seenBlock = 0
	}

	relayRequestData := lavaprotocol.NewRelayData(ctx, connectionType, url, []byte(req), seenBlock, reqBlock, rpcss.listenEndpoint.ApiInterface, chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage = chainlib.NewProtocolMessage(chainMessage, directiveHeaders, relayRequestData, dappID, consumerIp)
	return protocolMessage, nil
}

func (rpcss *RPCSmartRouterServer) SendParsedRelay(
	ctx context.Context,
	analytics *metrics.RelayMetrics,
	protocolMessage chainlib.ProtocolMessage,
) (relayResult *common.RelayResult, errRet error) {
	// Sends a relay request directly to RPC endpoints
	// Uses quorum comparison if enabled to verify response consistency across multiple endpoints
	ctx, span := tracing.StartInternalSpan(ctx, tracing.SpanParsedRelay)
	defer span.End()

	relaySentTime := time.Now()
	relayProcessor, err := rpcss.ProcessRelaySend(ctx, protocolMessage, analytics)
	if err != nil && (relayProcessor == nil || !relayProcessor.HasResults()) {
		userData := protocolMessage.GetUserData()
		// we can't send anymore, and we don't have any responses
		utils.LavaFormatError("failed getting responses from RPC endpoints", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx}, utils.LogAttr("endpoint", rpcss.listenEndpoint.Key()), utils.LogAttr("userIp", userData.ConsumerIp), utils.LogAttr("relayProcessor", relayProcessor))

		// A request-time cross-validation fail-fast (capacity/diversity check that aborts before any relay
		// completes) produces no RelayResult, so the structured failure-reason header would otherwise be
		// dropped here. Synthesize a minimal result carrying it so the client can still distinguish a
		// capacity/diversity failure from a generic upstream error (the "reuse header channel" contract).
		if relayProcessor != nil {
			if reason := relayProcessor.GetCrossValidationFailFastReason(); reason != "" {
				return rpcss.crossValidationFailFast(reason, protocolMessage), err
			}
		}

		return nil, err
	}

	returnedResult, err := func() (*common.RelayResult, error) {
		_, processSpan := tracing.StartInternalSpan(ctx, tracing.SpanProcessingResult)
		defer processSpan.End()
		r, e := relayProcessor.ProcessingResult()
		if r != nil && r.Reply != nil {
			if tracing.IsTraceBodyEnabled() {
				tracing.RecordBody(processSpan, tracing.AttrRelayResponseBody, r.Reply.Data)
			}
		}
		return r, e
	}()

	// A request-time cross-validation fail-fast (a capacity/diversity guard that aborted a LATER batch)
	// may have set a structural reason on the shared processor even though an EARLIER batch produced a
	// non-quorum result — so HasResults() was true and the early-return above was skipped, and the result
	// here carries only the final-eval reason (no-agreement / diversity-unmet). A structural reason
	// (insufficient-capacity / insufficient-groups: the fleet cannot satisfy the policy at all) is strictly
	// more actionable for the client than "the providers disagreed", so on a FAILURE prefer it rather than
	// letting the two reason channels disagree. Only on failure (err != nil): a later successful batch
	// leaves err == nil and must not inherit a stale fail-fast reason.
	if err != nil && relayProcessor != nil {
		preferStructuralFailureReason(returnedResult, relayProcessor.GetCrossValidationFailFastReason())
	}

	utils.LavaFormatInfo("ProcessingResult RETURNED",
		utils.LogAttr("has_result", returnedResult != nil),
		utils.LogAttr("has_reply", returnedResult != nil && returnedResult.Reply != nil),
		utils.LogAttr("reply_size", func() int {
			if returnedResult != nil && returnedResult.Reply != nil {
				return len(returnedResult.Reply.Data)
			}
			return 0
		}()),
		utils.LogAttr("error", err),
		utils.LogAttr("GUID", ctx),
	)

	rpcss.appendHeadersToRelayResult(ctx, returnedResult, relayProcessor.ProtocolErrors(), relayProcessor, protocolMessage, protocolMessage.GetApi().GetName(), analytics, err == nil)
	if err != nil {
		return returnedResult, utils.LavaFormatError("failed processing responses from RPC endpoints", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx}, utils.LogAttr("endpoint", rpcss.listenEndpoint.Key()))
	}

	if analytics != nil {
		currentLatency := time.Since(relaySentTime)
		analytics.Latency = currentLatency.Milliseconds()
		api := protocolMessage.GetApi()
		analytics.ComputeUnits = api.ComputeUnits
		analytics.ApiMethod = api.Name
		if rpcss.smartRouterEndpointMetrics != nil {
			rpcss.smartRouterEndpointMetrics.RecordRouterEndToEndLatency(
				rpcss.listenEndpoint.ChainID,
				rpcss.listenEndpoint.ApiInterface,
				api.Name,
				float64(currentLatency.Milliseconds()),
			)
		}
	}
	rpcss.relaysMonitor.LogRelay()
	return returnedResult, nil
}

func (rpcss *RPCSmartRouterServer) GetChainIdAndApiInterface() (string, string) {
	return rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface
}

func (rpcss *RPCSmartRouterServer) ProcessRelaySend(ctx context.Context, protocolMessage chainlib.ProtocolMessage, analytics *metrics.RelayMetrics) (*relaycore.RelayProcessor, error) {
	ctx, span := tracing.StartInternalSpan(ctx, tracing.SpanProcessRelaySend)
	defer span.End()

	// make sure all of the child contexts are cancelled when we exit
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	usedProviders := lavasession.NewUsedProviders(protocolMessage)
	usedProviders.SetChainID(rpcss.listenEndpoint.ChainID)
	usedProviders.SetEligibilityFunc(relaypolicy.DecideEligibility)

	// Record retry count when this span ends. BatchNumber is incremented once per
	// GetSessions success (one batch may contain N parallel sessions for cross-validation,
	// so parallel calls don't inflate retry_count). retry_count = max(0, batches - 1).
	defer func() {
		tracing.RecordRetryCount(span, usedProviders.BatchNumber()-1)
	}()

	// Create state machine first - it determines Selection type based on per-method policy + headers
	stateMachine, err := NewSmartRouterRelayStateMachineWithPolicy(ctx, usedProviders, rpcss, protocolMessage, analytics, rpcss.debugRelays, rpcss.crossValidationResolver, rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface)
	if err != nil {
		tracing.RecordError(span, err)
		return nil, err
	}

	// Get cross-validation parameters from the state machine (nil for Stateless/Stateful)
	crossValidationParams := stateMachine.GetCrossValidationParams()

	// Direct RPC flow: pass nil for availabilityDegrader since there are no Lava protocol sessions.
	// QoS punishment for node errors is handled by the optimizer via AppendRelayFailure in OnSessionFailure.
	// Created before the capacity check so a request-time fail-fast can carry its structured reason back on
	// the shared processor (the check aborts before any RelayResult exists).
	relayProcessor := relaycore.NewRelayProcessor(
		ctx,
		crossValidationParams,
		rpcss.smartRouterConsistency,
		rpcss.rpcSmartRouterLogs,
		rpcss,
		rpcss.relayRetriesManager,
		stateMachine,
	)

	if reason, err := rpcss.validateCrossValidationCapacity(ctx, stateMachine.GetSelection(), crossValidationParams, chainlib.GetAddon(protocolMessage), common.GetExtensionNames(protocolMessage.GetExtensions())); err != nil {
		if reason != "" {
			relayProcessor.SetCrossValidationFailFastReason(reason)
		}
		tracing.RecordError(span, err)
		// Return the processor (not nil) so SendParsedRelay can read the fail-fast reason and surface the
		// failure-reason header to the client.
		return relayProcessor, err
	}

	relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
	if err != nil {
		tracing.RecordError(span, err)
		return relayProcessor, err
	}

	utils.LavaFormatInfo("🎬 STARTING TASK CHANNEL LOOP",
		utils.LogAttr("GUID", ctx),
	)

	for task := range relayTaskChannel {
		utils.LavaFormatInfo("📨 RECEIVED TASK FROM CHANNEL",
			utils.LogAttr("is_done", task.IsDone()),
			utils.LogAttr("has_error", task.Err != nil),
			utils.LogAttr("num_providers", task.NumOfProviders),
			utils.LogAttr("GUID", ctx),
		)

		if task.IsDone() {
			utils.LavaFormatInfo("🏁 TASK IS DONE - RETURNING FROM ProcessRelaySend",
				utils.LogAttr("error", task.Err),
				utils.LogAttr("GUID", ctx),
			)
			if task.Err != nil {
				tracing.RecordError(span, task.Err)
			}
			return relayProcessor, task.Err
		}
		utils.LavaFormatTrace("[RPCSmartRouterServer] ProcessRelaySend - task", utils.LogAttr("GUID", ctx), utils.LogAttr("numOfEndpoints", task.NumOfProviders))
		err := rpcss.sendRelayToEndpoint(ctx, task.NumOfProviders, task.RelayState, relayProcessor, task.Analytics)

		utils.LavaFormatInfo("UPDATING BATCH",
			utils.LogAttr("error", err),
			utils.LogAttr("GUID", ctx),
		)

		relayProcessor.UpdateBatch(err)

		utils.LavaFormatInfo("LOOPING BACK TO RECEIVE NEXT TASK",
			utils.LogAttr("GUID", ctx),
		)
	}

	// shouldn't happen.
	utils.LavaFormatError("CHANNEL CLOSED UNEXPECTEDLY", nil,
		utils.LogAttr("GUID", ctx),
	)
	return relayProcessor, utils.LavaFormatError("ProcessRelaySend channel closed unexpectedly", nil)
}

func (rpcss *RPCSmartRouterServer) CreateDappKey(userData common.UserData) string {
	return rpcss.smartRouterConsistency.Key(userData)
}

func (rpcss *RPCSmartRouterServer) CancelSubscriptionContext(subscriptionKey string) {
	// Direct RPC subscription managers handle their own lifecycle.
}

func (rpcss *RPCSmartRouterServer) getEarliestBlockHashRequestedFromCacheReply(cacheReply *pairingtypes.CacheRelayReply) (int64, int64) {
	blocksHashesToHeights := cacheReply.GetBlocksHashesToHeights()
	earliestRequestedBlock := spectypes.NOT_APPLICABLE
	latestRequestedBlock := spectypes.NOT_APPLICABLE

	for _, blockHashToHeight := range blocksHashesToHeights {
		if blockHashToHeight.Height >= 0 && (earliestRequestedBlock == spectypes.NOT_APPLICABLE || blockHashToHeight.Height < earliestRequestedBlock) {
			earliestRequestedBlock = blockHashToHeight.Height
		}
		if blockHashToHeight.Height >= 0 && (latestRequestedBlock == spectypes.NOT_APPLICABLE || blockHashToHeight.Height > latestRequestedBlock) {
			latestRequestedBlock = blockHashToHeight.Height
		}
	}
	return latestRequestedBlock, earliestRequestedBlock
}

func (rpcss *RPCSmartRouterServer) resolveRequestedBlock(reqBlock int64, seenBlock int64, latestBlockHashRequested int64, protocolMessage chainlib.ProtocolMessage) int64 {
	if reqBlock == spectypes.LATEST_BLOCK && seenBlock != 0 {
		// make optimizer select an endpoint that is likely to have the latest seen block
		reqBlock = seenBlock
	}

	// Following logic to set the requested block as a new value fetched from the cache reply.
	// 1. We managed to get a value from the cache reply. (latestBlockHashRequested >= 0)
	// 2. We didn't manage to parse the block and used the default value meaning we didnt have knowledge of the requested block (reqBlock == spectypes.LATEST_BLOCK && protocolMessage.GetUsedDefaultValue())
	// 3. The requested block is smaller than the latest block hash requested from the cache reply (reqBlock >= 0 && reqBlock < latestBlockHashRequested)
	// 4. The requested block is not applicable meaning block parsing failed completely (reqBlock == spectypes.NOT_APPLICABLE)
	if latestBlockHashRequested >= 0 &&
		((reqBlock == spectypes.LATEST_BLOCK && protocolMessage.GetUsedDefaultValue()) ||
			reqBlock >= 0 && reqBlock < latestBlockHashRequested) {
		reqBlock = latestBlockHashRequested
	}
	return reqBlock
}

func (rpcss *RPCSmartRouterServer) newBlocksHashesToHeightsSliceFromRequestedBlockHashes(requestedBlockHashes []string) []*pairingtypes.BlockHashToHeight {
	var blocksHashesToHeights []*pairingtypes.BlockHashToHeight
	for _, blockHash := range requestedBlockHashes {
		blocksHashesToHeights = append(blocksHashesToHeights, &pairingtypes.BlockHashToHeight{Hash: blockHash, Height: spectypes.NOT_APPLICABLE})
	}
	return blocksHashesToHeights
}

func deepCopyRelayPrivateData(original *pairingtypes.RelayPrivateData) *pairingtypes.RelayPrivateData {
	if original == nil {
		return nil
	}

	// Deep copy all byte slices and string slices
	dataCopy := make([]byte, len(original.Data))
	copy(dataCopy, original.Data)

	saltCopy := make([]byte, len(original.Salt))
	copy(saltCopy, original.Salt)

	metadataCopy := make([]pairingtypes.Metadata, len(original.Metadata))
	copy(metadataCopy, original.Metadata)

	extensionsCopy := make([]string, len(original.Extensions))
	copy(extensionsCopy, original.Extensions)

	return &pairingtypes.RelayPrivateData{
		ConnectionType: original.ConnectionType,
		ApiUrl:         original.ApiUrl,
		Data:           dataCopy,
		RequestBlock:   original.RequestBlock,
		ApiInterface:   original.ApiInterface,
		Salt:           saltCopy,
		Metadata:       metadataCopy,
		Addon:          original.Addon,
		Extensions:     extensionsCopy,
		SeenBlock:      original.SeenBlock,
	}
}

// sendRelayToDirectEndpoints handles relay for direct RPC sessions (smart router direct mode)
func (rpcss *RPCSmartRouterServer) sendRelayToDirectEndpoints(
	ctx context.Context,
	sessions lavasession.ConsumerSessionsMap,
	protocolMessage chainlib.ProtocolMessage,
	relayProcessor *relaycore.RelayProcessor,
	analytics *metrics.RelayMetrics,
) error {
	chainMessage := protocolMessage

	// Extract original request bytes (for batch support - we need to forward the original JSON)
	originalRequestData := protocolMessage.RelayPrivateData().Data

	// Get relay timeout
	_, averageBlockTime, _, _ := rpcss.chainParser.ChainBlockStats()
	relayTimeout := chainlib.GetRelayTimeout(protocolMessage, averageBlockTime)

	utils.LavaFormatDebug("sending direct RPC relay",
		utils.LogAttr("num_endpoints", len(sessions)),
		utils.LogAttr("timeout", relayTimeout),
		utils.LogAttr("method", chainMessage.GetApi().Name),
		utils.LogAttr("GUID", ctx),
	)

	// Pre-request consistency validation: filter out endpoints that are too far behind
	validSessions, failedSessions, filterErr := rpcss.filterEndpointsByConsistency(ctx, sessions, protocolMessage)

	// Release failed sessions:
	// - ReleaseFromLatestBatch decrements UsedProviders.sessionsLatestBatch so
	//   RelayProcessor.checkEndProcessing matches the goroutines we will
	//   actually launch. Without this the CV path can wait the full
	//   processingTimeout (~30s) for responses that never arrive.
	// - OnSessionFailure provides QoS punishment, unlocks the session, and
	//   marks the provider unwanted for retry exclusion.
	usedProviders := relayProcessor.GetUsedProviders()
	releaseRouterKey := lavasession.NewRouterKeyFromExtensions(protocolMessage.GetExtensions())
	for endpointAddress, sessionInfo := range failedSessions {
		if sessionInfo != nil && sessionInfo.Session != nil {
			utils.LavaFormatDebug("releasing stale session via OnSessionFailure",
				utils.LogAttr("endpoint", endpointAddress),
				utils.LogAttr("error", lavasession.ConsistencyPreValidationError),
				utils.LogAttr("GUID", ctx),
			)
			usedProviders.ReleaseFromLatestBatch(endpointAddress, releaseRouterKey, lavasession.ConsistencyPreValidationError)
			rpcss.sessionManager.OnSessionFailure(sessionInfo.Session, lavasession.ConsistencyPreValidationError)
		}
	}

	// If ALL sessions failed consistency validation, return error to trigger retry with different providers
	if filterErr != nil {
		// This is a request-time candidate-set failure (the whole batch was filtered out before any relay
		// ran), so carry the structured reason on the shared processor for the failure-reason header — same
		// as the other fail-fast sites. The error itself is left unchanged. If a later retry batch succeeds,
		// HasResults() becomes true and SendParsedRelay takes the normal path, ignoring this field.
		if relayProcessor.GetSelection() == relaycore.CrossValidation {
			relayProcessor.SetCrossValidationFailFastReason(common.CrossValidationReasonInsufficientCapacity)
		}
		return filterErr
	}

	// Post-filter CV guard: fail fast with a precise error message when the
	// filter dropped the surviving session count below the agreement threshold.
	// Returning PairingListEmptyError surfaces the precise cause; the CV
	// short-circuit in Policy.OnSendRelayResult ensures the state machine
	// stops immediately rather than retrying with NumOfProviders=1 (which
	// would silently violate quorum and mask this error).
	selection := relayProcessor.GetSelection()
	crossValidationParams := relayProcessor.GetCrossValidationParams()
	if selection == relaycore.CrossValidation && crossValidationParams != nil &&
		len(validSessions) < crossValidationParams.AgreementThreshold {
		// Release the surviving valid sessions before returning. No goroutines
		// will be launched for them, so without this they stay in
		// UsedProviders.providers (CurrentlyUsed > 0). The state machine's
		// validateReturnCondition only delivers the err to returnCondition when
		// CurrentlyUsed == 0, so the request would otherwise stall for the
		// full processingTimeout (~30s) instead of failing fast.
		for endpointAddress, sessionInfo := range validSessions {
			if sessionInfo != nil && sessionInfo.Session != nil {
				usedProviders.ReleaseFromLatestBatch(endpointAddress, releaseRouterKey, nil)
				sessionInfo.Session.Free(nil)
			}
		}
		// Carry the structured reason on the shared processor so SendParsedRelay can surface the
		// failure-reason header; the error itself is left unchanged so the state machine's
		// PairingListEmptyError stop logic is unaffected.
		relayProcessor.SetCrossValidationFailFastReason(common.CrossValidationReasonInsufficientCapacity)
		return utils.LavaFormatError("insufficient sessions for cross-validation consensus after consistency filter",
			lavasession.PairingListEmptyError,
			utils.LogAttr("agreementThreshold", crossValidationParams.AgreementThreshold),
			utils.LogAttr("sessionsAcquired", len(sessions)),
			utils.LogAttr("sessionsAfterFilter", len(validSessions)),
			utils.LogAttr("sessionsFiltered", len(failedSessions)),
			utils.LogAttr("GUID", ctx),
		)
	}

	// Post-filter group guard (1.2d + 2.3): the consistency filter may drop an entire group, or drop a
	// group below its per-group quorum, while leaving enough total sessions to pass the count check above.
	// Fail fast when the survivors can no longer span the required groups, rather than launching relays the
	// group/per-group gate will reject. For MinGroups diversity: require MinGroups distinct surviving
	// groups. For PerGroupQuorum: require MinGroups surviving groups that EACH still have >= threshold
	// sessions (a distinct-group count alone passes even when a group dropped below its internal quorum).
	if selection == relaycore.CrossValidation && crossValidationParams != nil && crossValidationParams.MinGroups > 1 {
		survivingGroupCounts := make(map[string]int)
		for _, sessionInfo := range validSessions {
			if sessionInfo == nil || sessionInfo.Session == nil || sessionInfo.Session.Parent == nil {
				continue
			}
			label := common.DefaultProviderGroup
			if g := sessionInfo.Session.Parent.GroupLabel; g != "" {
				label = g
			}
			survivingGroupCounts[label]++
		}
		if qualifyingGroups, failReason := crossValidationGroupShortfall(survivingGroupCounts, crossValidationParams); failReason != "" {
			for endpointAddress, sessionInfo := range validSessions {
				if sessionInfo != nil && sessionInfo.Session != nil {
					usedProviders.ReleaseFromLatestBatch(endpointAddress, releaseRouterKey, nil)
					sessionInfo.Session.Free(nil)
				}
			}
			relayProcessor.SetCrossValidationFailFastReason(failReason)
			return utils.LavaFormatError("insufficient provider groups for cross-validation after consistency filter ("+failReason+")",
				lavasession.PairingListEmptyError,
				utils.LogAttr("minGroups", crossValidationParams.MinGroups),
				utils.LogAttr("perGroupQuorum", crossValidationParams.PerGroupQuorum),
				utils.LogAttr("agreementThreshold", crossValidationParams.AgreementThreshold),
				utils.LogAttr("qualifyingGroupsAfterFilter", qualifyingGroups),
				utils.LogAttr("survivingGroupCounts", survivingGroupCounts),
				utils.LogAttr("sessionsAfterFilter", len(validSessions)),
				utils.LogAttr("GUID", ctx),
			)
		}
	}

	sessions = validSessions

	// Capture the batch number synchronously before launching goroutines so each
	// relayInnerDirect span can label itself as `relay.attempt = N`. This must happen
	// here (not inside the goroutine) because by the time a goroutine runs, a hedge or
	// retry batch from the state machine may have already incremented BatchNumber.
	relayAttempt := relayProcessor.GetUsedProviders().BatchNumber()

	// Launch goroutines for each direct RPC endpoint (parallel relay pattern)
	for endpointAddress, sessionInfo := range sessions {
		go func(endpointAddress string, sessionInfo *lavasession.SessionInfo) {
			// Derive from ctx so IP forwarding metadata (and other values) are preserved.
			goroutineCtx, goroutineCtxCancel := context.WithCancel(ctx)

			guid, found := utils.GetUniqueIdentifier(ctx)
			if found {
				goroutineCtx = utils.WithUniqueIdentifier(goroutineCtx, guid)
			}

			// StartInternalSpan declines to create a span when there is no
			// recording parent in context, which protects against orphan root
			// spans during init/crafted relays from sendRelayWithRetries.
			spanCtx, provSpan := tracing.StartInternalSpan(goroutineCtx, tracing.SpanRelayInnerDirect)
			tracing.RecordProviderAttributes(provSpan, guid, endpointAddress)
			tracing.RecordRelayAttempt(provSpan, relayAttempt)

			singleConsumerSession := sessionInfo.Session

			localRelayResult := &common.RelayResult{
				ProviderInfo: common.ProviderInfo{ProviderAddress: endpointAddress},
				Finalized:    true, // Direct responses don't need consensus
			}

			var errResponse error

			// CRITICAL: Use defer to set response (same as provider-relay pattern)
			// This ensures all work completes before response is sent
			defer func() {
				// Set response for relay processor (MUST be in defer!)
				relayProcessor.SetResponse(&relaycore.RelayResponse{
					RelayResult: *localRelayResult,
					Err:         errResponse,
				})

				// Close context
				goroutineCtxCancel()
			}()

			apiMethod := chainMessage.GetApi().Name

			if rpcss.smartRouterEndpointMetrics != nil {
				rpcss.smartRouterEndpointMetrics.RecordDirectRelayStart(
					rpcss.listenEndpoint.ChainID,
					rpcss.listenEndpoint.ApiInterface,
					endpointAddress,
					apiMethod,
				)
			}

			// Resolve the endpoint + its direct connection, ensure the per-endpoint ChainTracker,
			// and capture its observation generation BEFORE dispatching the relay (MAG-2159
			// finding 5). Capturing pre-dispatch is the safety property: a relay that completes
			// after this URL's tracker is removed and recreated (a new incarnation, new
			// generation) carries the OLD generation, so RecordRelayObservation's gate rejects
			// it — instead of misattributing it to the new tracker, which is exactly what
			// re-reading the live generation post-relay would do. Resolved here once and reused
			// in the success path below.
			var targetEndpoint *lavasession.Endpoint
			var directConn lavasession.DirectRPCConnection
			if drsc, ok := singleConsumerSession.Connection.(*lavasession.DirectRPCSessionConnection); ok {
				targetEndpoint = drsc.Endpoint
				directConn = drsc.DirectConnection
			}
			var harvestGen uint64
			if targetEndpoint != nil && directConn != nil {
				rpcss.ensureEndpointChainTracker(goroutineCtx, targetEndpoint, directConn)
				harvestGen = rpcss.endpointObservationGeneration(targetEndpoint.NetworkAddress)
			}

			relayLatency, err, _ := rpcss.relayInnerDirect(
				spanCtx,
				singleConsumerSession,
				localRelayResult,
				relayTimeout,
				chainMessage,
				originalRequestData,
				analytics,
			)

			if rpcss.smartRouterEndpointMetrics != nil {
				if analytics != nil {
					analytics.IsWrite = chainlib.GetStateful(chainMessage) != common.NO_STATE
					analytics.IsArchive = chainqueries.IsArchiveRequest(chainMessage)
					analytics.IsDebugTrace = chainqueries.IsDebugOrTraceRequest(chainMessage)
					analytics.IsBatch = chainqueries.IsBatchRequest(chainMessage)
				}
				rpcss.smartRouterEndpointMetrics.RecordDirectRelayEnd(
					rpcss.listenEndpoint.ChainID,
					rpcss.listenEndpoint.ApiInterface,
					endpointAddress,
					apiMethod,
					float64(relayLatency.Milliseconds()),
					err == nil,
					analytics,
				)
			}

			// Handle response
			if err != nil {
				tracing.RecordError(provSpan, err)
				utils.LavaFormatDebug("direct RPC relay failed in goroutine",
					utils.LogAttr("endpoint", endpointAddress),
					utils.LogAttr("error", err.Error()),
					utils.LogAttr("latency", relayLatency),
					utils.LogAttr("GUID", goroutineCtx),
				)
				errResponse = err
			} else {
				utils.LavaFormatDebug("direct RPC relay succeeded in goroutine",
					utils.LogAttr("endpoint", endpointAddress),
					utils.LogAttr("latency", relayLatency),
					utils.LogAttr("GUID", goroutineCtx),
				)

				// Cache write for successful responses (non-blocking)
				rpcss.tryCacheWrite(goroutineCtx, protocolMessage, localRelayResult)
			}
			provSpan.End()

			// Update session manager with result
			// Check status code to determine if session should fail
			// For REST 5xx/429, err == nil but we still want to fail the session for QoS/retry
			statusCode := localRelayResult.StatusCode
			shouldFailSession := err != nil || statusCode >= 500 || statusCode == 429

			if !shouldFailSession {
				// Success or client error (4xx except 429) - update session as success.
				// targetEndpoint / directConn were resolved and the tracker ensured before
				// dispatch (above), so harvestGen is the generation captured pre-relay.

				// MAG-2159 (Topic B) tip harvest + tip-state update — gated on tip-eligibility so
				// historical responses cannot poison the endpoint tip / estimator / metric
				// (finding 4). harvestGen was captured before dispatch (finding 5).
				rpcss.harvestAndUpdateTipFromRelay(targetEndpoint, chainMessage, localRelayResult.Reply, harvestGen, endpointAddress)

				// latestServicedBlock: the block this response actually serviced (may be
				// historical), used for OnSessionDone and propagated to downstream consumers
				// (consistency, caching). This is intentionally NOT gated on tip-eligibility — it
				// is "the block returned by this response", a separate concept from "the current
				// tip" updated above. Site B (MAG-2160): when the response carried no block, fall
				// back to THIS endpoint's own observed latest — never another node's tip. The old
				// global-tracker fallback stamped provider[0]'s value onto this endpoint's
				// QoS/seenBlock (cross-attribution S6); the per-endpoint value is the honest one.
				latestBlock := int64(0)
				if localRelayResult.Reply != nil && localRelayResult.Reply.LatestBlock > 0 {
					latestBlock = localRelayResult.Reply.LatestBlock
				} else if targetEndpoint != nil && rpcss.endpointChainTrackerManager != nil {
					// MAG-2160 Finding 6: read this endpoint's freshest OBSERVATION record, not the
					// dedicated tracker's atomic (GetLatestBlockNum). Under the MAG-2159 traffic gate
					// the tracker's poll cycle is skipped while served relays keep the tip current, so
					// the tracker atomic can lag the latest relay-harvested block (or still be 0 on a
					// purely relay-fed endpoint). The observation store is updated on every harvest, so
					// it is the honest, freshest per-endpoint value.
					if obsv, ok := rpcss.endpointChainTrackerManager.GetObservation(targetEndpoint.NetworkAddress); ok {
						latestBlock = obsv.LatestBlock
					}
					if latestBlock > 0 && localRelayResult.Reply != nil {
						localRelayResult.Reply.LatestBlock = latestBlock
					}
					utils.LavaFormatTrace("using this endpoint's own latest block (site B)",
						utils.LogAttr("endpoint", endpointAddress),
						utils.LogAttr("latest_block", latestBlock),
						utils.LogAttr("GUID", goroutineCtx),
					)
				}

				// Calculate syncGap (detect lagging endpoints). Site C (MAG-2160 Finding 2): an
				// endpoint is judged against the strict-majority CONSENSUS baseline, NOT the
				// optimistic observed tip. Measuring against the observed tip would penalize the
				// whole pod whenever a single endpoint races ahead (baseline 1000, one node reports
				// 1099 → everyone at 1000 gets a bogus syncGap of 99). When there is no fresh
				// consensus baseline (single-endpoint pod, or a baseline aged past TTL) we DO NOT
				// substitute the observed tip — syncGap stays 0, so no endpoint is penalized against
				// a reference the pod has not actually agreed on.
				syncGap := int64(0)
				if targetEndpoint != nil && rpcss.chainState != nil {
					if baseline, ok := rpcss.chainState.GetConsensusBaseline(); ok && baseline > 0 {
						endpointLatest := targetEndpoint.LatestBlock.Load()
						if endpointLatest > 0 {
							syncGap = baseline - endpointLatest
							if syncGap < 0 {
								syncGap = 0 // Endpoint ahead of the baseline is fine
							}
							utils.LavaFormatDebug("calculated sync gap",
								utils.LogAttr("endpoint", endpointAddress),
								utils.LogAttr("consensus_baseline", baseline),
								utils.LogAttr("endpoint_latest", endpointLatest),
								utils.LogAttr("sync_gap", syncGap),
								utils.LogAttr("GUID", goroutineCtx),
							)
						}
					}
				}

				// Call OnSessionDone with correct signature
				numSessions := len(sessions)
				errSession := rpcss.sessionManager.OnSessionDone(
					singleConsumerSession,
					latestBlock, // latestServicedBlock
					chainlib.GetComputeUnits(protocolMessage), // specComputeUnits
					relayLatency, // currentLatency
					singleConsumerSession.CalculateExpectedLatency(relayTimeout), // expectedLatency
					syncGap,             // Real syncGap (detect lagging endpoints)
					numSessions,         // numOfEndpoints (int)
					uint64(numSessions), // providersCount (uint64)
					protocolMessage.GetApi().Category.HangingApi, // hangingApi
					protocolMessage.GetExtensions(),              // extensions
				)
				if errSession != nil {
					utils.LavaFormatWarning("OnSessionDone failed for direct RPC", errSession,
						utils.LogAttr("GUID", goroutineCtx),
					)
				}
			} else {
				// Failure case: err != nil OR status >= 500 OR status == 429
				failureErr := err
				if failureErr == nil {
					// REST 5xx/429 with err == nil - create descriptive error
					failureErr = fmt.Errorf("upstream returned HTTP %d", statusCode)
				}
				rpcss.sessionManager.OnSessionFailure(singleConsumerSession, failureErr)
			}

			// NOTE: Don't call Free() here - OnSessionDone/OnSessionFailure already do it!
		}(endpointAddress, sessionInfo)
	}

	// NOTE: Don't call WaitForResults here!
	// The state machine already calls it via readResultsFromProcessor
	// Calling it twice causes a deadlock

	utils.LavaFormatInfo("GOROUTINES LAUNCHED - RETURNING TO LET STATE MACHINE WAIT",
		utils.LogAttr("num_endpoints", len(sessions)),
		utils.LogAttr("GUID", ctx),
	)

	return nil
}

// filterEndpointsByConsistency filters out endpoints that are too far behind the user's seen block.
// This is pre-request validation to avoid wasting requests on endpoints that are likely stale.
// Returns: validSessions (pass validation), failedSessions (too far behind), and error (if ALL failed).
// Uses per-endpoint ChainTracker for accurate, continuously-polled block data.
func (rpcss *RPCSmartRouterServer) filterEndpointsByConsistency(
	ctx context.Context,
	sessions lavasession.ConsumerSessionsMap,
	protocolMessage chainlib.ProtocolMessage,
) (validSessions lavasession.ConsumerSessionsMap, failedSessions lavasession.ConsumerSessionsMap, filterErr error) {
	_, span := tracing.StartInternalSpan(ctx, tracing.SpanFilterEndpointsByConsistency)
	defer func() {
		tracing.RecordConsistencyStats(span, len(sessions), len(validSessions), len(failedSessions))
		if filterErr != nil {
			tracing.RecordError(span, filterErr)
		}
		span.End()
	}()

	// Skip if consistency config is not set
	if rpcss.consistencyConfig == nil || rpcss.smartRouterConsistency == nil {
		return sessions, nil, nil
	}

	// Get requested block and check if we should validate
	reqBlock, _ := protocolMessage.RequestedBlock()
	if relaycore.ShouldSkipConsistencyValidation(reqBlock) {
		return sessions, nil, nil
	}

	// Get user's seen block
	userData := protocolMessage.GetUserData()
	seenBlock, found := rpcss.smartRouterConsistency.GetSeenBlock(userData)
	if !found || seenBlock <= 0 {
		// No prior seen block - skip validation
		return sessions, nil, nil
	}

	// Validate each endpoint
	validSessions = make(lavasession.ConsumerSessionsMap, len(sessions))
	failedSessions = make(lavasession.ConsumerSessionsMap)

	for endpointAddress, sessionInfo := range sessions {
		if sessionInfo == nil || sessionInfo.Session == nil {
			continue
		}

		// Get endpoint's latest block from ChainTracker (if available), fallback to reactive value
		endpointLatest := int64(0)
		endpointURL := ""

		// Extract the actual endpoint URL (ChainTrackers are stored by URL, not provider name)
		if drsc, ok := sessionInfo.Session.Connection.(*lavasession.DirectRPCSessionConnection); ok && drsc.Endpoint != nil {
			endpointURL = drsc.Endpoint.NetworkAddress
		}

		// First, try to get from ChainTracker manager (continuously polled, fresh data)
		// ChainTrackers are keyed by URL, so multiple providers pointing to same URL share one tracker
		if rpcss.endpointChainTrackerManager != nil && endpointURL != "" {
			endpointLatest = rpcss.endpointChainTrackerManager.GetLatestBlockNum(endpointURL)
		}

		// Fallback: if ChainTracker has no data yet, use the endpoint's reactive LatestBlock
		if endpointLatest == 0 {
			if drsc, ok := sessionInfo.Session.Connection.(*lavasession.DirectRPCSessionConnection); ok && drsc.Endpoint != nil {
				endpointLatest = drsc.Endpoint.LatestBlock.Load()
			}
		}

		// If we still have no block data, skip validation for this endpoint (allow first relay)
		if endpointLatest == 0 {
			trackerState := endpointstate.EndpointChainTrackerMissing
			trackerLastError := ""
			if rpcss.endpointChainTrackerManager != nil && endpointURL != "" {
				trackerState, trackerLastError, _ = rpcss.endpointChainTrackerManager.GetTrackerState(endpointURL)
			}
			utils.LavaFormatDebug("skipping consistency validation because endpoint latest block is unknown",
				utils.LogAttr("endpoint", endpointAddress),
				utils.LogAttr("endpointURL", endpointURL),
				utils.LogAttr("seenBlock", seenBlock),
				utils.LogAttr("requestedBlock", reqBlock),
				utils.LogAttr("trackerState", trackerState),
				utils.LogAttr("trackerLastError", trackerLastError),
				utils.LogAttr("GUID", ctx),
			)
			validSessions[endpointAddress] = sessionInfo
			continue
		}

		// Validate endpoint capability
		err := relaycore.ValidateEndpointCapability(
			endpointLatest,
			seenBlock,
			reqBlock,
			rpcss.consistencyConfig,
		)
		if err != nil {
			// Endpoint is too far behind - add to failed sessions
			utils.LavaFormatDebug("skipping endpoint due to consistency check",
				utils.LogAttr("endpoint", endpointAddress),
				utils.LogAttr("endpointLatest", endpointLatest),
				utils.LogAttr("seenBlock", seenBlock),
				utils.LogAttr("source", func() string {
					if rpcss.endpointChainTrackerManager != nil && endpointURL != "" && rpcss.endpointChainTrackerManager.GetLatestBlockNum(endpointURL) > 0 {
						return "ChainTracker"
					}
					return "reactive"
				}()),
				utils.LogAttr("GUID", ctx),
			)
			failedSessions[endpointAddress] = sessionInfo
			continue
		}

		validSessions[endpointAddress] = sessionInfo
	}

	skippedCount := len(failedSessions)

	// If ALL endpoints failed validation, return error to trigger retry
	if len(validSessions) == 0 && skippedCount > 0 {
		utils.LavaFormatDebug("all endpoints failed consistency pre-validation, triggering retry",
			utils.LogAttr("totalEndpoints", len(sessions)),
			utils.LogAttr("skippedCount", skippedCount),
			utils.LogAttr("seenBlock", seenBlock),
			utils.LogAttr("GUID", ctx),
		)
		return nil, failedSessions, utils.LavaFormatError("all endpoints failed consistency pre-validation",
			lavasession.ConsistencyPreValidationError,
			utils.LogAttr("totalEndpoints", len(sessions)),
			utils.LogAttr("seenBlock", seenBlock),
		)
	}

	if skippedCount > 0 {
		utils.LavaFormatDebug("filtered endpoints by consistency",
			utils.LogAttr("totalEndpoints", len(sessions)),
			utils.LogAttr("validEndpoints", len(validSessions)),
			utils.LogAttr("skippedCount", skippedCount),
			utils.LogAttr("seenBlock", seenBlock),
			utils.LogAttr("GUID", ctx),
		)
	}

	return validSessions, failedSessions, nil
}

// ensureEndpointChainTracker ensures that a ChainTracker exists for the given endpoint.
// If not, it creates one lazily. This enables continuous block polling for accurate
// consistency pre-validation.
// This is called the first time we successfully communicate with an endpoint.
// recordRelayBlockObservation harvests a reliable current-tip block (see tipBlockFromRelay)
// into the per-endpoint observation store (MAG-2159 / Topic B), tagged Source=Relay. It is
// keyed on endpoint.NetworkAddress — the same key the dedicated poll path uses
// (EndpointMonitor.GetOrCreateTracker) — so relay and poll observations land on one
// record. gen is the observation generation captured for this endpoint (after ensuring its
// tracker); RecordRelayObservation rejects the write if gen no longer matches the live
// generation, so a relay from a removed/replaced tracker cannot corrupt the record
// (finding 5). Side-effect-free: it never touches QoS or endpoint.Enabled. No-op when no
// monitor is wired, the endpoint is nil, or block is non-positive.
func (rpcss *RPCSmartRouterServer) recordRelayBlockObservation(endpoint *lavasession.Endpoint, gen uint64, block int64) {
	if rpcss.endpointChainTrackerManager == nil || endpoint == nil || block <= 0 {
		return
	}
	rpcss.endpointChainTrackerManager.RecordRelayObservation(endpoint.NetworkAddress, gen, block, time.Now())
}

// minChainStateRecomputeInterval floors the consensus recompute cadence so very fast chains
// don't recompute the strict-majority baseline excessively (it's a windowed, off-hot-path step).
const minChainStateRecomputeInterval = time.Second

// runChainStateConsensusLoop periodically recomputes the per-chain ChainState consensus
// baseline (MAG-2160 / Topic C) until ctx is cancelled. The cadence is the chain's average
// block time, floored at minChainStateRecomputeInterval.
func (rpcss *RPCSmartRouterServer) runChainStateConsensusLoop(ctx context.Context, averageBlockTime time.Duration) {
	interval := averageBlockTime
	if interval < minChainStateRecomputeInterval {
		interval = minChainStateRecomputeInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rpcss.recomputeChainStateConsensus()
		}
	}
}

// recomputeChainStateConsensus pulls one snapshot of all per-endpoint observations (single
// monitor lock, released before touching ChainState) and hands them to ChainState.Recompute,
// which computes the strict-majority baseline and realigns the tip downward if needed.
func (rpcss *RPCSmartRouterServer) recomputeChainStateConsensus() {
	if rpcss.chainState == nil || rpcss.endpointChainTrackerManager == nil {
		return
	}
	// MAG-2160 Finding 5: do NOT early-return on an empty snapshot. An empty (or sub-majority)
	// snapshot must still reach ChainState.Recompute so it CLEARS any previously-set baseline —
	// otherwise a pod that has lost all its endpoints keeps anti-lie-guarding against a baseline
	// no live endpoint still supports. Recompute(empty) is the explicit "no consensus" signal.
	snap := rpcss.endpointChainTrackerManager.SnapshotObservations()
	obs := make([]chainstate.BlockObservation, 0, len(snap))
	for url, o := range snap {
		if o.LatestBlock <= 0 {
			continue
		}
		obs = append(obs, chainstate.BlockObservation{URL: url, Block: o.LatestBlock, ObservedAt: o.ObservedAt})
	}
	rpcss.chainState.Recompute(obs)
}

// probeQoSAppender is the narrow optimizer capability the prober uses to feed QoS (Topic E's
// AppendProbeData). The concrete *provideroptimizer.ProviderOptimizer satisfies it; an inline
// assertion keeps the lavasession optimizer interface unchanged.
type probeQoSAppender interface {
	AppendProbeData(provider string, availability float64, latency time.Duration, hasLatency bool, syncBlock uint64, hasSync bool, syncRef provideroptimizer.SyncReference)
}

// Compile-time guard: the concrete optimizer must satisfy probeQoSAppender. Without this, the
// inline type assertion in runProbeLoop would silently degrade to a nil appender (re-enable-only,
// no QoS feed) if AppendProbeData's signature ever drifts — a regression no test would catch.
var _ probeQoSAppender = (*provideroptimizer.ProviderOptimizer)(nil)

// defaultProbeCadence is the prober's own polling period — distinct from the legacy synthetic
// probe's PeriodicProbeProvidersInterval. It floors runProbeLoop's cadence so time.NewTicker can
// never be handed a non-positive duration (which panics).
const defaultProbeCadence = 5 * time.Second

// validatedProbeCadence validates the operator-configured probe cadence (MAG-2161 D5): a non-positive
// value (which would panic time.NewTicker and is never a sane config) is rejected back to the default
// with a warning, so a misconfiguration degrades loudly to the default rather than crashing the loop.
func validatedProbeCadence(configured time.Duration) time.Duration {
	if configured <= 0 {
		utils.LavaFormatWarning("invalid probe-loop-interval, falling back to default", nil,
			utils.LogAttr("configured", configured),
			utils.LogAttr("default", defaultProbeCadence),
		)
		return defaultProbeCadence
	}
	return configured
}

// probeLoopStats holds the most-recent runProbeLoop cycle telemetry for /debug/probe-loop (MAG-2202
// endpoint 4): the configured cadence plus a snapshot of the LAST completed cycle. Written once per
// cycle by runProbeCycle (off the data plane) and read by the debug handler, under its own mutex so
// a debug read never contends with relay/probe state. One per RPCSmartRouterServer, like the chain
// it probes. The zero value is a valid "no cycle run yet" state.
type probeLoopStats struct {
	mu               sync.Mutex
	cycleIntervalMs  int64     // configured --probe-loop-interval; set once when the loop starts
	cyclesCompleted  uint64    // monotonic count of completed cycles (F6 liveness)
	lastCycleStarted time.Time // wall-clock the last cycle began (zero before the first cycle)
	lastCycleDurMs   int64     // wall-clock duration of the last cycle
	endpointsScored  int       // endpoints scored in the last cycle
	reEnabledCount   int       // endpoints the probe re-enabled in the last cycle (F1)
	syncOmittedCount int       // providers whose last-cycle QoS sample fed no sync evidence (F5)
}

// setInterval publishes the effective probe cadence. Called once at loop start.
func (s *probeLoopStats) setInterval(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cycleIntervalMs = d.Milliseconds()
}

// recordCycle overwrites the last-cycle snapshot and bumps the completed-cycle counter.
func (s *probeLoopStats) recordCycle(startedAt time.Time, dur time.Duration, scored, reEnabled, syncOmitted int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cyclesCompleted++
	s.lastCycleStarted = startedAt
	s.lastCycleDurMs = dur.Milliseconds()
	s.endpointsScored = scored
	s.reEnabledCount = reEnabled
	s.syncOmittedCount = syncOmitted
}

// probeLoopSnapshot is the read-only view the /debug/probe-loop handler emits per chain.
type probeLoopSnapshot struct {
	CycleIntervalMs     int64
	CyclesCompleted     uint64
	LastCycleStartedAt  time.Time
	LastCycleDurationMs int64
	EndpointsScored     int
	ReEnabledCount      int
	SyncOmittedCount    int
}

// snapshot returns a consistent copy of the stats under the lock (no mutex in the returned value).
func (s *probeLoopStats) snapshot() probeLoopSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return probeLoopSnapshot{
		CycleIntervalMs:     s.cycleIntervalMs,
		CyclesCompleted:     s.cyclesCompleted,
		LastCycleStartedAt:  s.lastCycleStarted,
		LastCycleDurationMs: s.lastCycleDurMs,
		EndpointsScored:     s.endpointsScored,
		ReEnabledCount:      s.reEnabledCount,
		SyncOmittedCount:    s.syncOmittedCount,
	}
}

// runProbeLoop is the Topic D proactive health prober (MAG-2161). On its own cadence it reads stored
// per-endpoint telemetry (Topic A) + the consensus baseline (Topic C) — making NO upstream call —
// renders a health verdict for EVERY direct-RPC endpoint (regular AND backup), proactively
// re-enables recovered endpoints (the O1 win), and feeds ONE aggregated QoS sample per provider
// (Topic E contract). It is read-only toward the data plane: it never writes block/consensus state.
func (rpcss *RPCSmartRouterServer) runProbeLoop(ctx context.Context, cadence time.Duration, cfg probing.VerdictConfig) {
	if rpcss.sessionManager == nil || rpcss.endpointChainTrackerManager == nil {
		return
	}
	if cadence <= 0 {
		cadence = defaultProbeCadence
	}
	// Publish the effective cadence for /debug/probe-loop (CycleIntervalMs) before the first tick,
	// so the endpoint reports the interval even before a cycle has completed.
	rpcss.probeStats.setInterval(cadence)
	// The optimizer's AppendProbeData is optional (inline assertion) so the loop degrades to
	// re-enable-only if a future optimizer lacks it, rather than failing to start.
	appender, _ := rpcss.sessionManager.GetProviderOptimizer().(probeQoSAppender)

	ticker := time.NewTicker(cadence)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rpcss.runProbeCycle(appender, cfg)
		}
	}
}

// runProbeCycle pulls this server's live dependencies and runs one probe cycle. It resolves THIS
// interface's consensus baseline once — feeding both the verdict (keeping-up check) and the QoS
// sync reference (F4: scoped to this interface; F5: sync fed only when a fresh majority exists).
func (rpcss *RPCSmartRouterServer) runProbeCycle(appender probeQoSAppender, cfg probing.VerdictConfig) {
	var baseline int64
	var hasBaseline bool
	syncRef := provideroptimizer.SyncReference{ConsensusConfigured: true}
	if rpcss.chainState != nil {
		if block, at, ok := rpcss.chainState.GetConsensusBaselineWithTime(); ok && block > 0 {
			baseline, hasBaseline = block, true
			syncRef.Block, syncRef.Time, syncRef.Fresh = uint64(block), at, true
		}
	}
	startedAt := time.Now()
	scored, reEnabled, syncOmitted := runProbeCycleCore(
		rpcss.sessionManager.GetAllDirectRPCEndpoints(),
		rpcss.endpointChainTrackerManager.GetObservation,
		baseline, hasBaseline, syncRef, startedAt, cfg, appender,
		rpcss.sessionManager.RestoreRecoveredProvider,
	)
	rpcss.probeStats.recordCycle(startedAt, time.Since(startedAt), scored, reEnabled, syncOmitted)
}

// runProbeCycleCore is the pure probe-cycle body (no rpcss fields), so it is testable with
// constructed endpoints + a fake observation getter + a fake appender. For every endpoint it renders
// a verdict, applies the proactive re-enable, then feeds ONE aggregated QoS sample per provider
// (rule E2). A nil appender still performs the re-enable (QoS feed simply skipped).
//
// Returns the per-cycle telemetry for /debug/probe-loop (MAG-2202 endpoint 4): scored = endpoints
// that received a verdict; reEnabled = endpoints the probe re-enabled this cycle (F1); syncOmitted =
// providers whose QoS sample fed NO sync evidence (F5: no fresh consensus baseline, or no block in
// the sample). syncOmitted is 0 when appender is nil (no QoS feed happens, so nothing is omitted).
func runProbeCycleCore(
	endpoints []*lavasession.EndpointWithDirectConnection,
	getObservation func(url string) (endpointstate.EndpointObservation, bool),
	baseline int64,
	hasBaseline bool,
	syncRef provideroptimizer.SyncReference,
	now time.Time,
	cfg probing.VerdictConfig,
	appender probeQoSAppender,
	onRecover func(provider string),
) (scored, reEnabled, syncOmitted int) {
	// One verdict per endpoint (regular + backup), grouped by provider for the single-sample rule.
	verdictsByProvider := make(map[string][]probing.EndpointVerdict)
	for _, ep := range endpoints {
		if ep == nil || ep.Endpoint == nil {
			continue
		}
		scored++
		obs, _ := getObservation(ep.Endpoint.NetworkAddress)
		verdict := probing.RenderEndpointVerdict(obs, baseline, hasBaseline, now, cfg)
		// Proactive re-enable from POST-DISABLE successful-poll evidence only (F1). RecordProbeVerdict
		// releases the endpoint mutex before returning, so onRecover (which takes csm.lock) cannot
		// nest under endpoint.mu — no lock-order inversion (F2).
		if ep.Endpoint.RecordProbeVerdict(verdict.Recovery.LastSuccessfulPoll, verdict.Recovery.PollHealthy, cfg.ReEnableHysteresis) {
			reEnabled++
			if onRecover != nil {
				onRecover(ep.ProviderAddress)
			}
		}
		verdictsByProvider[ep.ProviderAddress] = append(verdictsByProvider[ep.ProviderAddress], verdict)
	}

	if appender == nil {
		return scored, reEnabled, syncOmitted // re-enable still happened above; QoS feed unavailable
	}
	for provider, verdicts := range verdictsByProvider {
		sample, ok := probing.AggregateProviderSample(verdicts)
		if !ok {
			continue
		}
		// hasSync only when an accepted consensus baseline exists (F5): no baseline → no sync evidence,
		// never the legacy max-across-providers reference. syncRef carries the same baseline.
		hasSync := sample.HasBlock && hasBaseline
		if !hasSync {
			syncOmitted++ // F5: this provider's QoS sample carries no sync evidence this cycle
		}
		appender.AppendProbeData(provider, sample.Availability, sample.Latency, sample.HasLatency, sample.Block, hasSync, syncRef)
	}
	return scored, reEnabled, syncOmitted
}

// harvestAndUpdateTipFromRelay applies a served relay's block to TIP state — but only when the
// response is tip-eligible (MAG-2159 finding 4). tipBlockFromRelay distinguishes a "block
// associated with a response" (historical: eth_getBlockByNumber(N), getBlockByHash,
// getTransactionReceipt, getLogs — all carry a positive Reply.LatestBlock) from a "current-tip
// observation" (eth_blockNumber / eth_getBlockByNumber("latest") / Solana result.context.slot).
// ONLY the latter may move tip state: the per-endpoint observation store, the endpoint's
// reactive LatestBlock (read by consistency pre-validation), the bootstrap atomic (latestBlockHeight,
// the cold-start fallback for getLatestBlock), and the latest-block metric. A historical response
// updates NONE of these, so it can no longer poison
// the endpoint tip. "The block this response serviced" (latestServicedBlock) is a separate
// concept the caller handles ungated. No-op when targetEndpoint is nil or the response is not
// tip-eligible. harvestGen is the generation captured before dispatch (finding 5).
func (rpcss *RPCSmartRouterServer) harvestAndUpdateTipFromRelay(
	targetEndpoint *lavasession.Endpoint,
	chainMessage chainlib.ChainMessage,
	reply *pairingtypes.RelayReply,
	harvestGen uint64,
	endpointAddress string,
) {
	if targetEndpoint == nil {
		return
	}
	tip, ok := rpcss.tipBlockFromRelay(chainMessage, reply)
	if !ok {
		return
	}
	rpcss.recordRelayBlockObservation(targetEndpoint, harvestGen, tip)
	targetEndpoint.LatestBlock.Store(tip)
	targetEndpoint.LastBlockUpdate = time.Now()
	rpcss.updateLatestBlockHeight(uint64(tip), endpointAddress)
	if rpcss.smartRouterEndpointMetrics != nil {
		// endpointAddress is the provider name (session map key), so resolveProviderName
		// returns it unchanged — endpoint_id = provider name in the metric.
		rpcss.smartRouterEndpointMetrics.SetEndpointLatestBlock(
			rpcss.listenEndpoint.ChainID,
			rpcss.listenEndpoint.ApiInterface,
			endpointAddress,
			tip,
		)
	}
}

// endpointObservationGeneration returns the live observation generation for an endpoint
// URL (0 if no monitor or no active tracker). The relay-harvest path captures it after
// ensuring the tracker and passes it to recordRelayBlockObservation.
func (rpcss *RPCSmartRouterServer) endpointObservationGeneration(endpointURL string) uint64 {
	if rpcss.endpointChainTrackerManager == nil {
		return 0
	}
	gen, _ := rpcss.endpointChainTrackerManager.ObservationGeneration(endpointURL)
	return gen
}

// tipBlockFromRelay returns a reliable current-tip block observed from a served relay
// response, and whether one is available — distinguishing a "block associated with a
// response" (historical) from a "current-tip observation" (MAG-2159 findings 1 & 2).
//
// Tip sources, by transport/chain:
//   - Solana family (JSON-RPC): result.context.slot — the slot the query was processed at,
//     i.e. the node's current tip — present on most successful Solana responses
//     (getBalance, getAccountInfo, getLatestBlockhash, ...). Chain-aware; never applied to
//     other chains. See extractSolanaContextSlot.
//   - Otherwise (EVM/gRPC): Reply.LatestBlock is the tip ONLY for a method whose semantics make the
//     reply's block the node's current tip, identified by SPEC TAG (not by RequestedBlock alone):
//     1. GET_BLOCKNUM (eth_blockNumber-equivalent): the result IS the tip.
//     2. GET_BLOCK_BY_NUM (eth_getBlockByNumber-equivalent) AND RequestedBlock == LATEST_BLOCK.
//     Methods with no parseable block param (eth_getTransactionReceipt, eth_getBlockByHash, ...)
//     fall back to a DEFAULT parser that ALSO reports RequestedBlock == LATEST_BLOCK, while
//     Reply.LatestBlock is the HISTORICAL block of the tx/block — so RequestedBlock == LATEST_BLOCK
//     cannot discriminate, and neither can GetUsedDefaultValue() (non-deterministic for
//     eth_blockNumber across runs). The spec tag is the only reliable discriminator. A concrete
//     eth_getBlockByNumber(N) requests N (not LATEST) and is historical.
//     Reply.LatestBlock is populated for JSON-RPC and gRPC; REST harvests nothing.
//
// Note: RequestedBlock() is not concretized during the relay flow (the
// UpdateLatestBlockInMessage call in relaycore/results_manager.go is disabled), so it still
// reads LATEST_BLOCK here for latest-requesting methods.
func (rpcss *RPCSmartRouterServer) tipBlockFromRelay(chainMessage chainlib.ChainMessage, reply *pairingtypes.RelayReply) (int64, bool) {
	if reply == nil {
		return 0, false
	}
	if common.IsSolanaFamily(rpcss.listenEndpoint.ChainID) {
		return extractSolanaContextSlot(reply.Data)
	}
	if reply.LatestBlock <= 0 {
		return 0, false
	}
	// A response is a CURRENT-TIP observation in exactly two method-defined cases (NOT by RequestedBlock
	// alone — receipt/by-hash also default to LATEST_BLOCK but carry a HISTORICAL Reply.LatestBlock):
	//   1. The GET_BLOCKNUM method (eth_blockNumber-equivalent): its result IS the node's tip. (It has
	//      no block param, so it parses to LATEST via the default — indistinguishable from a receipt by
	//      RequestedBlock/UsedDefaultValue — and must be matched by its spec tag.)
	//   2. The GET_BLOCK_BY_NUM method (eth_getBlockByNumber-equivalent) when it requested the LATEST
	//      block. A concrete eth_getBlockByNumber(N) requests N (not LATEST) and is historical.
	// Everything else — receipt, by-hash, logs — is neither tagged method and is dropped.
	// Deliberately does NOT consult GetUsedDefaultValue (which is unreliable for eth_blockNumber).
	if rpcss.isGetBlockNumMethod(chainMessage) {
		return reply.LatestBlock, true
	}
	requestedLatest, _ := chainMessage.RequestedBlock()
	if requestedLatest == spectypes.LATEST_BLOCK && rpcss.isGetBlockByNumMethod(chainMessage) {
		return reply.LatestBlock, true
	}
	return 0, false
}

// isMethodTagged reports whether the message's API is the method the spec marks with the given tag.
func (rpcss *RPCSmartRouterServer) isMethodTagged(chainMessage chainlib.ChainMessage, tag spectypes.FUNCTION_TAG) bool {
	if rpcss.chainParser == nil {
		return false
	}
	parsing, _, ok := rpcss.chainParser.GetParsingByTag(tag)
	if !ok || parsing == nil {
		return false
	}
	api := chainMessage.GetApi()
	return api != nil && api.Name == parsing.ApiName
}

// isGetBlockNumMethod: the "current block number" call (e.g. eth_blockNumber) — result IS the tip.
func (rpcss *RPCSmartRouterServer) isGetBlockNumMethod(chainMessage chainlib.ChainMessage) bool {
	return rpcss.isMethodTagged(chainMessage, spectypes.FUNCTION_TAG_GET_BLOCKNUM)
}

// isGetBlockByNumMethod: the "get block by number" call (e.g. eth_getBlockByNumber) — a tip only when
// it requested LATEST.
func (rpcss *RPCSmartRouterServer) isGetBlockByNumMethod(chainMessage chainlib.ChainMessage) bool {
	return rpcss.isMethodTagged(chainMessage, spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM)
}

func (rpcss *RPCSmartRouterServer) ensureEndpointChainTracker(
	ctx context.Context,
	endpoint *lavasession.Endpoint,
	directConnection lavasession.DirectRPCConnection,
) {
	if rpcss.endpointChainTrackerManager == nil || endpoint == nil || directConnection == nil {
		return
	}

	// Check if ChainTracker already exists
	endpointURL := endpoint.NetworkAddress
	if _, exists := rpcss.endpointChainTrackerManager.GetTracker(endpointURL); exists {
		return
	}

	// Create the ChainTracker SYNCHRONOUSLY (Finding 4). GetOrCreateTracker registers the tracker
	// and allocates its observation generation under the manager lock with NO network I/O — the
	// blocking poll loop is started internally via `go startTrackerWithRetry`. Running it inline
	// (not in a goroutine) guarantees the generation EXISTS before this relay's dispatch captures
	// it via endpointObservationGeneration: an async creation let an early relay capture generation
	// 0 (no tracker yet), so its harvested tip was recorded against a generation that the real
	// tracker would never match — silently dropping the first relay's tip. The poll loop stays
	// async, so dispatch is not blocked on the network.
	if _, err := rpcss.endpointChainTrackerManager.GetOrCreateTracker(endpoint, directConnection); err != nil {
		utils.LavaFormatWarning("failed to create ChainTracker for endpoint", err,
			utils.LogAttr("endpoint", endpointURL),
		)
	}
}

// initializeChainTrackers creates ChainTrackers for all direct RPC endpoints on startup.
// This runs in the background and ensures fresh block data is available from the start,
// avoiding issues where endpoints have stale or no block data for consistency validation.
// If initialization fails for an endpoint, lazy initialization (ensureEndpointChainTracker) serves as fallback.
func (rpcss *RPCSmartRouterServer) initializeChainTrackers(ctx context.Context) {
	// Small delay to let startup complete and connections stabilize
	select {
	case <-ctx.Done():
		return
	case <-time.After(100 * time.Millisecond):
	}

	if rpcss.endpointChainTrackerManager == nil || rpcss.sessionManager == nil {
		return
	}

	endpoints := rpcss.sessionManager.GetAllDirectRPCEndpoints()
	if len(endpoints) == 0 {
		utils.LavaFormatDebug("no direct RPC endpoints to initialize ChainTrackers")
		return
	}

	utils.LavaFormatInfo("initializing ChainTrackers for direct RPC endpoints",
		utils.LogAttr("count", len(endpoints)),
		utils.LogAttr("chainID", rpcss.listenEndpoint.ChainID),
	)

	successCount := 0
	failCount := 0

	// Initialize ChainTrackers with staggered delays to avoid thundering herd
	for i, ep := range endpoints {
		select {
		case <-ctx.Done():
			utils.LavaFormatDebug("ChainTracker initialization cancelled",
				utils.LogAttr("completed", i),
				utils.LogAttr("total", len(endpoints)),
			)
			return
		default:
		}

		// Stagger by 50ms between endpoints to avoid rate limiting
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}

		_, err := rpcss.endpointChainTrackerManager.GetOrCreateTracker(ep.Endpoint, ep.DirectConnection)
		if err != nil {
			utils.LavaFormatWarning("failed to initialize ChainTracker", err,
				utils.LogAttr("endpoint", ep.Endpoint.NetworkAddress),
				utils.LogAttr("provider", ep.ProviderAddress),
			)
			failCount++
		} else {
			successCount++
		}
	}

	utils.LavaFormatInfo("ChainTracker initialization complete",
		utils.LogAttr("success", successCount),
		utils.LogAttr("failed", failCount),
		utils.LogAttr("chainID", rpcss.listenEndpoint.ChainID),
	)
}

// isFinalizedForCacheWrite picks the chain tip used to decide whether a
// cache-write goes to the long-TTL finalized store or the short-TTL temp
// store. Reply.LatestBlock is unreliable for methods that echo the requested
// block (eth_getBlockByNumber returns result.number = requested), so the
// router's tracked tip (the per-chain ChainState consensus tip, with the
// bootstrap atomic latestBlockHeight as cold-start fallback, surfaced via
// getLatestBlock()) wins when it is higher.
func isFinalizedForCacheWrite(requestedBlock, replyLatestBlock, trackedLatestBlock, finalizationDistance int64) bool {
	latest := replyLatestBlock
	if trackedLatestBlock > latest {
		latest = trackedLatestBlock
	}
	return spectypes.IsFinalizedBlock(requestedBlock, latest, finalizationDistance)
}

// tryCacheWrite attempts to write a successful relay response to the cache.
// It runs in a separate goroutine to avoid blocking the relay response.
// Cache writes are skipped when:
// - Cache is not active
// - Quorum is enabled (quorum requires fresh endpoint validation)
// - Response is a node error
// - Requested block is NOT_APPLICABLE
func (rpcss *RPCSmartRouterServer) tryCacheWrite(
	ctx context.Context,
	protocolMessage chainlib.ProtocolMessage,
	relayResult *common.RelayResult,
) {
	// Skip if cache is not active
	if !rpcss.cache.CacheActive() {
		return
	}

	// Skip if stateful (stateful requests mutate state - must not cache)
	if chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
		return
	}

	// Skip if this is a node error
	if relayResult.IsNodeError {
		utils.LavaFormatDebug("cache write skipped: node error response",
			utils.LogAttr("GUID", ctx),
		)
		return
	}

	// Skip if no reply data
	if relayResult.Reply == nil {
		utils.LavaFormatDebug("cache write skipped: no reply data",
			utils.LogAttr("GUID", ctx),
		)
		return
	}

	// Get request data for cache key computation
	relayData := protocolMessage.RelayPrivateData()
	if relayData == nil {
		utils.LavaFormatDebug("cache write skipped: no relay data",
			utils.LogAttr("GUID", ctx),
		)
		return
	}

	// Skip if requested block is NOT_APPLICABLE
	requestedBlock, _ := protocolMessage.RequestedBlock()
	if requestedBlock == spectypes.NOT_APPLICABLE {
		utils.LavaFormatDebug("cache write skipped: NOT_APPLICABLE block",
			utils.LogAttr("GUID", ctx),
		)
		return
	}

	// Validate status code (same as consumer: skip caching for 429, 504, and non-2xx in strict mode)
	// This is checked in the success path, but we double-check here for safety
	statusCode := relayResult.StatusCode
	if statusCode == http.StatusTooManyRequests || statusCode == http.StatusGatewayTimeout {
		utils.LavaFormatDebug("cache write skipped: error status code",
			utils.LogAttr("statusCode", statusCode),
			utils.LogAttr("GUID", ctx),
		)
		return
	}
	// Strict mode: only cache 2xx responses
	if statusCode != 0 && (statusCode < 200 || statusCode >= 300) {
		utils.LavaFormatDebug("cache write skipped: non-2xx status code",
			utils.LogAttr("statusCode", statusCode),
			utils.LogAttr("GUID", ctx),
		)
		return
	}

	// Compute cache key via the protocol message so the SET key matches the GET key,
	// including any explicit lava-extension directive folded in by HashCacheRequest.
	chainId := rpcss.listenEndpoint.ChainID
	hashKey, _, hashErr := protocolMessage.HashCacheRequest(chainId)
	if hashErr != nil {
		utils.LavaFormatDebug("cache write skipped: hash computation failed",
			utils.LogAttr("error", hashErr),
			utils.LogAttr("GUID", ctx),
		)
		return
	}

	// Get chain stats for finalization check
	_, averageBlockTime, blockDistanceForFinalizedData, _ := rpcss.chainParser.ChainBlockStats()

	// Determine if response is finalized. Prefer the router's tracked chain
	// tip over Reply.LatestBlock: for eth_getBlockByNumber the per-response
	// value is the requested block itself (extractBlockHeightFromEVMResponse
	// reads result.number), so the naive check never marks any historical
	// block finalized and every entry takes the ~625 ms non-finalized TTL.
	latestBlock := relayResult.Reply.LatestBlock
	finalized := isFinalizedForCacheWrite(requestedBlock, latestBlock, int64(rpcss.getLatestBlock()), int64(blockDistanceForFinalizedData))

	// Convert LATEST_BLOCK to actual block number for cache key
	// This must match the logic in cache lookup (sendRelayToEndpoint) to ensure cache hits
	requestedBlockForCache := requestedBlock
	if requestedBlock == spectypes.LATEST_BLOCK {
		// Use the latest block from the response (most accurate)
		if latestBlock > 0 {
			requestedBlockForCache = latestBlock
		} else if relayData.SeenBlock > 0 {
			// Fallback to seen block
			requestedBlockForCache = relayData.SeenBlock
		} else {
			// Skip caching if we can't determine the actual block
			utils.LavaFormatDebug("cache write skipped: cannot resolve LATEST_BLOCK",
				utils.LogAttr("GUID", ctx),
			)
			return
		}
	}

	// Get seen block
	seenBlock := relayData.SeenBlock

	// Get shared state ID if enabled
	sharedStateId := ""
	if rpcss.sharedState {
		sharedStateId = rpcss.listenEndpoint.Key()
	}

	// Deep copy reply to avoid race conditions (cache write is async)
	copyReply := &pairingtypes.RelayReply{}
	if copyErr := protocopy.DeepCopyProtoObject(relayResult.Reply, copyReply); copyErr != nil {
		utils.LavaFormatDebug("cache write skipped: failed to copy reply",
			utils.LogAttr("error", copyErr),
			utils.LogAttr("GUID", ctx),
		)
		return
	}

	// Write to cache in a non-blocking goroutine
	go func() {
		cacheCtx, cancel := context.WithTimeout(context.Background(), common.CacheWriteTimeout)
		defer cancel()

		err := rpcss.cache.SetEntry(cacheCtx, &pairingtypes.RelayCacheSet{
			RequestHash:           hashKey,
			ChainId:               chainId,
			RequestedBlock:        requestedBlockForCache, // Use resolved block (LATEST_BLOCK converted to actual)
			SeenBlock:             seenBlock,
			BlockHash:             nil, // SmartRouter cache doesn't use block hashes
			Response:              copyReply,
			Finalized:             finalized,
			OptionalMetadata:      nil,
			SharedStateId:         sharedStateId,
			AverageBlockTime:      int64(averageBlockTime),
			IsNodeError:           false, // We only cache successful non-error responses
			BlocksHashesToHeights: nil,   // Not available in direct RPC mode
		})

		if err != nil {
			utils.LavaFormatWarning("cache write failed", err,
				utils.LogAttr("chainId", chainId),
				utils.LogAttr("requestedBlock", requestedBlockForCache),
				utils.LogAttr("GUID", ctx),
			)
		} else {
			utils.LavaFormatDebug("cache write succeeded",
				utils.LogAttr("chainId", chainId),
				utils.LogAttr("requestedBlock", requestedBlockForCache),
				utils.LogAttr("finalized", finalized),
				utils.LogAttr("GUID", ctx),
			)
		}
	}()
}

func (rpcss *RPCSmartRouterServer) sendRelayToEndpoint(
	ctx context.Context,
	numOfEndpoints int,
	relayState *relaycore.RelayState,
	relayProcessor *relaycore.RelayProcessor,
	analytics *metrics.RelayMetrics,
) (errRet error) {
	// Send relay to direct RPC endpoints:
	// - Get sessions from ConsumerSessionManager for the requested endpoints
	// - Send the relay request directly to the RPC node
	// - Handle QoS updates based on response latency and success
	// - Update endpoint health status on connection failures
	// Use the latest protocol message from the relay state machine to ensure we have any archive upgrades
	protocolMessage := relayProcessor.GetProtocolMessage()
	// IMPORTANT: Create an isolated copy of RelayPrivateData at function entry to prevent race conditions.
	// This ensures that modifications in this call don't affect goroutines from previous calls,
	// and goroutines launched in this call aren't affected by future calls.
	localRelayData := deepCopyRelayPrivateData(protocolMessage.RelayPrivateData())
	if localRelayData == nil {
		return utils.LavaFormatError("RelayPrivateData is nil", nil, utils.LogAttr("GUID", ctx))
	}

	userData := protocolMessage.GetUserData()
	var sharedStateId string // defaults to "", if shared state is disabled then no shared state will be used.
	if rpcss.sharedState {
		sharedStateId = rpcss.smartRouterConsistency.Key(userData) // use same key as we use for consistency, (for better consistency :-D)
	}

	chainId, apiInterface := rpcss.GetChainIdAndApiInterface()

	// Get Session. we get session here so we can use the epoch in the callbacks
	reqBlock, _ := protocolMessage.RequestedBlock()

	// try using cache before sending relay
	earliestBlockHashRequested := spectypes.NOT_APPLICABLE
	latestBlockHashRequested := spectypes.NOT_APPLICABLE
	var cacheError error
	selection := relayProcessor.GetSelection()
	crossValidationParams := relayProcessor.GetCrossValidationParams()

	// Cache lookup: only if cache is active, cross-validation is disabled, and request is not stateful
	crossValidationEnabled := selection == relaycore.CrossValidation && crossValidationParams != nil
	if rpcss.cache.CacheActive() {
		if crossValidationEnabled {
			// Cross-validation requires fresh endpoint validation - cache would defeat consensus verification
			utils.LavaFormatDebug("Cache bypassed due to cross-validation requirements",
				utils.LogAttr("GUID", ctx),
				utils.LogAttr("cacheActive", true),
				utils.LogAttr("selection", selection),
				utils.LogAttr("agreementThreshold", crossValidationParams.AgreementThreshold),
				utils.LogAttr("reason", "cross-validation requires fresh endpoint validation, cache would defeat consensus verification"),
			)
		} else if chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
			// Stateful requests (e.g. eth_sendTransaction) mutate state - must not read from cache
			utils.LavaFormatDebug("Cache bypassed due to stateful request",
				utils.LogAttr("GUID", ctx),
				utils.LogAttr("cacheActive", true),
				utils.LogAttr("api", protocolMessage.GetApi().Name),
				utils.LogAttr("reason", "stateful requests mutate state and cannot use cached responses"),
			)
		} else if protocolMessage.GetForceCacheRefresh() {
			// User requested cache bypass via header
			utils.LavaFormatDebug("Cache bypassed due to force-cache-refresh header",
				utils.LogAttr("GUID", ctx),
				utils.LogAttr("cacheActive", true),
			)
		} else {
			// Proceed with cache lookup
			utils.LavaFormatDebug("Cache lookup attempt",
				utils.LogAttr("GUID", ctx),
				utils.LogAttr("cacheActive", true),
				utils.LogAttr("reqBlock", reqBlock),
				utils.LogAttr("forceCacheRefresh", false),
				utils.LogAttr("selection", selection),
			)
			allowCacheLookup := reqBlock != spectypes.NOT_APPLICABLE

			if allowCacheLookup {
				var cacheReply *pairingtypes.CacheRelayReply
				hashKey, outputFormatter, err := protocolMessage.HashCacheRequest(chainId)
				if err != nil {
					utils.LavaFormatError("sendRelayToEndpoint failed getting hash for cache request", err, utils.LogAttr("GUID", ctx))
				} else {
					utils.LavaFormatDebug("Cache lookup hash generated",
						utils.LogAttr("GUID", ctx),
						utils.LogAttr("hashKey", fmt.Sprintf("%x", hashKey)),
						utils.LogAttr("apiUrl", localRelayData.ApiUrl),
					)

					// Resolve the requested block for cache lookup
					// The cache server doesn't accept negative blocks
					requestedBlockForCache := reqBlock
					if reqBlock == spectypes.LATEST_BLOCK {
						// For LATEST_BLOCK queries, use the latest known block from smartRouterConsistency
						// This ensures methods like eth_blockNumber use the actual current block for caching,
						// not the potentially stale seenBlock from when this request started.
						// The consistency cache is updated immediately after each successful response,
						// so it reflects the most recent block across all requests for this user.
						latestKnownBlock, found := rpcss.smartRouterConsistency.GetSeenBlock(userData)
						if found && latestKnownBlock > 0 {
							requestedBlockForCache = latestKnownBlock
						} else if localRelayData.SeenBlock != 0 {
							// Fallback to seen block from the protocol message
							requestedBlockForCache = localRelayData.SeenBlock
						} else {
							requestedBlockForCache = 0 // Final fallback
						}
					}

					// Always use finalized=false for lookups
					// The cache will search both tempCache and finalizedCache, finding data in either
					lookupFinalized := false

					cacheCtx, cancel := context.WithTimeout(ctx, common.CacheTimeout)

					utils.LavaFormatDebug("Cache lookup configuration",
						utils.LogAttr("GUID", ctx),
						utils.LogAttr("reqBlock", reqBlock),
						utils.LogAttr("requestedBlockForCache", requestedBlockForCache),
						utils.LogAttr("seenBlock", localRelayData.SeenBlock),
						utils.LogAttr("lookupFinalized", lookupFinalized),
					)

					cacheLatencyMs := func() float64 {
						_, cacheSpan := tracing.StartInternalSpan(ctx, tracing.SpanCacheLookup)
						defer cacheSpan.End()
						cacheStart := time.Now()
						cacheReply, cacheError = rpcss.cache.GetEntry(cacheCtx, &pairingtypes.RelayCacheGet{
							RequestHash:           hashKey,
							RequestedBlock:        requestedBlockForCache,
							ChainId:               chainId,
							BlockHash:             nil,
							Finalized:             lookupFinalized,
							SharedStateId:         sharedStateId,
							SeenBlock:             localRelayData.SeenBlock,
							BlocksHashesToHeights: rpcss.newBlocksHashesToHeightsSliceFromRequestedBlockHashes(protocolMessage.GetRequestedBlocksHashes()),
						}) // caching in the consumer doesn't care about hashes, and we don't have data on finalization yet
						cancel()
						latencyMs := float64(time.Since(cacheStart).Milliseconds())
						cacheHit := cacheError == nil && cacheReply != nil && cacheReply.GetReply() != nil
						tracing.RecordCacheResult(ctx, cacheSpan, cacheHit, latencyMs)
						return latencyMs
					}()

					// Generate the actual cache key that will be used for lookup
					actualLookupCacheKey := make([]byte, len(hashKey))
					copy(actualLookupCacheKey, hashKey)
					actualLookupCacheKey = binary.LittleEndian.AppendUint64(actualLookupCacheKey, uint64(requestedBlockForCache))

					utils.LavaFormatDebug("Cache lookup result",
						utils.LogAttr("GUID", ctx),
						utils.LogAttr("hashKeyHex", fmt.Sprintf("%x", hashKey)),
						utils.LogAttr("actualLookupCacheKeyHex", fmt.Sprintf("%x", actualLookupCacheKey)),
						utils.LogAttr("reqBlock", reqBlock),
						utils.LogAttr("requestedBlockForCache", requestedBlockForCache),
						utils.LogAttr("seenBlock", localRelayData.SeenBlock),
						utils.LogAttr("cacheError", cacheError),
						utils.LogAttr("replyFound", cacheReply != nil && cacheReply.GetReply() != nil),
					)
					reply := cacheReply.GetReply()

					// read seen block from cache even if we had a miss we still want to get the seen block so we can use it to get the right endpoint.
					cacheSeenBlock := cacheReply.GetSeenBlock()
					// check if the cache seen block is greater than my local seen block, this means the user requested this
					// request spoke with another consumer instance and use that block for inter consumer consistency.
					if rpcss.sharedState && cacheSeenBlock > localRelayData.SeenBlock {
						utils.LavaFormatDebug("shared state seen block is newer", utils.LogAttr("cache_seen_block", cacheSeenBlock), utils.LogAttr("local_seen_block", localRelayData.SeenBlock), utils.LogAttr("GUID", ctx))
						localRelayData.SeenBlock = cacheSeenBlock
						// setting the fetched seen block from the cache server to our local cache as well.
						rpcss.smartRouterConsistency.SetSeenBlock(cacheSeenBlock, userData)
					}

					// handle cache reply
					if cacheError == nil && reply != nil {
						// Cache hit - return cached response
						utils.LavaFormatDebug("cache hit",
							utils.LogAttr("chainId", chainId),
							utils.LogAttr("requestedBlock", requestedBlockForCache),
							utils.LogAttr("GUID", ctx),
						)
						reply.Data = outputFormatter(reply.Data)

						// If this is a cached error response with placeholder GUID, replace it with current request GUID
						replyDataStr := string(reply.Data)
						if strings.Contains(replyDataStr, `"Error_GUID":"CACHED_ERROR"`) {
							guid, guidOk := utils.GetUniqueIdentifier(ctx)
							if guidOk {
								guidStr := strconv.FormatUint(guid, 10)
								// Replace the placeholder GUID with the actual request GUID
								replyDataStr = strings.Replace(replyDataStr, `"Error_GUID":"CACHED_ERROR"`, `"Error_GUID":"`+guidStr+`"`, 1)
								reply.Data = []byte(replyDataStr)
							}
						}

						relayResult := common.RelayResult{
							Reply: reply,
							Request: &pairingtypes.RelayRequest{
								RelayData: localRelayData,
							},
							Finalized:    false, // cache responses are not considered finalized
							StatusCode:   200,
							ProviderInfo: common.ProviderInfo{ProviderAddress: ""},
						}
						// MAG-2160 Finding 1: a cache hit's reply.LatestBlock is the block that was
						// current when the response was CACHED — it is not a fresh observation of the
						// chain head and is not gated on tip-eligibility, so it must NOT feed the
						// bootstrap atomic (it would freeze a stale value during cold start). Tip state
						// is advanced only by tip-eligible live relays (harvestAndUpdateTipFromRelay)
						// and the per-endpoint observation store, never by cache replays.
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
						go rpcss.smartRouterEndpointMetrics.RecordCacheResult(chainId, apiInterface, protocolMessage.GetApi().GetName(), true, cacheLatencyMs)
						return nil
					}
					go rpcss.smartRouterEndpointMetrics.RecordCacheResult(chainId, apiInterface, protocolMessage.GetApi().GetName(), false, cacheLatencyMs)
					// Cache miss - will relay to endpoint
					latestBlockHashRequested, earliestBlockHashRequested = rpcss.getEarliestBlockHashRequestedFromCacheReply(cacheReply)
					utils.LavaFormatTrace("[Archive Debug] Reading block hashes from cache", utils.LogAttr("latestBlockHashRequested", latestBlockHashRequested), utils.LogAttr("earliestBlockHashRequested", earliestBlockHashRequested), utils.LogAttr("GUID", ctx))
				}
			} else {
				utils.LavaFormatDebug("skipping cache due to requested block being NOT_APPLICABLE",
					utils.LogAttr("GUID", ctx),
					utils.LogAttr("apiName", protocolMessage.GetApi().Name),
					utils.LogAttr("reqBlock", reqBlock),
				)
			}
		}
	}

	addon := chainlib.GetAddon(protocolMessage)
	reqBlock = rpcss.resolveRequestedBlock(reqBlock, localRelayData.SeenBlock, latestBlockHashRequested, protocolMessage)
	// check whether we need a new protocol message with the new earliest block hash requested
	protocolMessage = rpcss.updateProtocolMessageIfNeededWithNewEarliestData(ctx, relayState, protocolMessage, earliestBlockHashRequested, addon)

	// Smart router doesn't track epochs, use fixed value
	virtualEpoch := uint64(0)
	extensions := protocolMessage.GetExtensions()
	utils.LavaFormatTrace("[Archive Debug] Extensions to send", utils.LogAttr("extensions", extensions), utils.LogAttr("GUID", ctx))

	// Debug: Check if the protocol message has the archive extension in its internal state
	utils.LavaFormatTrace("[Archive Debug] RelayPrivateData extensions", utils.LogAttr("relayPrivateDataExtensions", localRelayData.Extensions), utils.LogAttr("GUID", ctx))
	usedProviders := relayProcessor.GetUsedProviders()
	directiveHeaders := protocolMessage.GetDirectiveHeaders()

	// stickines id for future use
	stickiness, ok := directiveHeaders[common.STICKINESS_HEADER_NAME]
	if ok {
		utils.LavaFormatTrace("found stickiness header", utils.LogAttr("id", stickiness), utils.LogAttr("GUID", ctx))
	}

	// provider selection via header (smartrouter only)
	selectedProvider := ""
	if providerAddr, exists := directiveHeaders[common.SELECT_PROVIDER_HEADER_NAME]; exists {
		selectedProvider = providerAddr
		utils.LavaFormatTrace("found provider selection header", utils.LogAttr("provider", selectedProvider), utils.LogAttr("GUID", ctx))
	}

	// Group-aware fan-out: when a cross-validation policy requires group diversity, select across at
	// least MinGroups distinct provider groups (1.2a). Default 1 leaves selection group-blind. For
	// per-group quorum (2.3), also front-load AgreementThreshold providers per group so each group can
	// independently reach its internal quorum — otherwise QoS-skewed selection starves the smaller groups.
	sessionOpts := lavasession.GetSessionsOptions{MinGroups: 1, PerGroupTarget: 1}
	if cvp := relayProcessor.GetCrossValidationParams(); cvp != nil && cvp.MinGroups > 1 {
		sessionOpts.MinGroups = cvp.MinGroups
		if cvp.PerGroupQuorum {
			sessionOpts.PerGroupTarget = cvp.AgreementThreshold
		}
	}

	_, sessSpan := tracing.StartInternalSpan(ctx, tracing.SpanGetSessions)
	sessions, err := rpcss.sessionManager.GetSessions(ctx, numOfEndpoints, chainlib.GetComputeUnits(protocolMessage), usedProviders, reqBlock, addon, extensions, chainlib.GetStateful(protocolMessage), virtualEpoch, stickiness, selectedProvider, sessionOpts)
	tracing.RecordSessionStats(sessSpan, numOfEndpoints, len(sessions))
	if err != nil {
		tracing.RecordError(sessSpan, err)
	}
	sessSpan.End()

	if err != nil {
		if errors.Is(err, lavasession.PairingListEmptyError) {
			if addon != "" {
				return utils.LavaFormatError("No Providers For Addon", err, utils.LogAttr("addon", addon), utils.LogAttr("extensions", extensions), utils.LogAttr("userIp", userData.ConsumerIp), utils.LogAttr("GUID", ctx))
			} else if len(extensions) > 0 && relayProcessor.GetAllowSessionDegradation() { // if we have no providers for that extension, use a regular provider, otherwise return the extension results
				sessions, err = rpcss.sessionManager.GetSessions(ctx, numOfEndpoints, chainlib.GetComputeUnits(protocolMessage), usedProviders, reqBlock, addon, []*spectypes.Extension{}, chainlib.GetStateful(protocolMessage), virtualEpoch, stickiness, selectedProvider, sessionOpts)
				if err != nil {
					return err
				}
				localRelayData.Extensions = []string{} // reset request data extensions in our local copy
				extensions = []*spectypes.Extension{}  // reset extensions too so we wont hit SetDisallowDegradation
			} else {
				return err
			}
		} else {
			return err
		}
	}

	// For stateful APIs, capture all endpoints that we're sending the relay to
	// This must be done immediately after GetSessions while all endpoints are still in the sessions map
	if chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
		statefulRelayTargets := make([]string, 0, len(sessions))
		for endpointAddress := range sessions {
			statefulRelayTargets = append(statefulRelayTargets, endpointAddress)
		}
		relayProcessor.SetStatefulRelayTargets(statefulRelayTargets)
	}

	// For cross-validation, capture all endpoints that were queried
	// This must be done immediately after GetSessions, before any responses come back
	// so we track all queried endpoints even if their response doesn't arrive (early exit when threshold met)
	if selection == relaycore.CrossValidation {
		queriedProviders := make([]string, 0, len(sessions))
		for providerPublicAddress := range sessions {
			queriedProviders = append(queriedProviders, providerPublicAddress)
		}
		relayProcessor.SetCrossValidationQueriedProviders(queriedProviders)

		// Verify we have enough sessions to meet the agreement threshold
		// If not, fail early with a clear error rather than proceeding knowing consensus is impossible
		if crossValidationParams != nil && len(sessions) < crossValidationParams.AgreementThreshold {
			relayProcessor.SetCrossValidationFailFastReason(common.CrossValidationReasonInsufficientCapacity)
			return utils.LavaFormatError("insufficient sessions for cross-validation consensus",
				lavasession.PairingListEmptyError,
				utils.LogAttr("agreementThreshold", crossValidationParams.AgreementThreshold),
				utils.LogAttr("sessionsAcquired", len(sessions)),
				utils.LogAttr("GUID", ctx))
		}
	}

	// making sure next get sessions wont use regular endpoints
	if len(extensions) > 0 {
		relayProcessor.SetDisallowDegradation()
	}

	if rpcss.debugRelays {
		routerKey := lavasession.NewRouterKeyFromExtensions(extensions)
		utils.LavaFormatDebug("[Before Send] returned the following sessions",
			utils.LogAttr("sessions", sessions),
			utils.LogAttr("usedProviders.GetUnwantedProvidersToSend", usedProviders.GetUnwantedProvidersToSend(routerKey)),
			utils.LogAttr("usedProviders.GetErroredProviders", usedProviders.GetErroredProviders(routerKey)),
			utils.LogAttr("addons", addon),
			utils.LogAttr("extensions", extensions),
			utils.LogAttr("AllowSessionDegradation", relayProcessor.GetAllowSessionDegradation()),
			utils.LogAttr("GUID", ctx),
		)
	}

	// Smart router supports direct RPC sessions only.
	if len(sessions) == 0 {
		return utils.LavaFormatError("no sessions available for direct RPC", nil, utils.LogAttr("GUID", ctx))
	}
	for endpointAddress, sessionInfo := range sessions {
		if sessionInfo == nil || sessionInfo.Session == nil || !sessionInfo.Session.IsDirectRPC() {
			return utils.LavaFormatError("rpcsmartrouter only supports direct RPC sessions", nil,
				utils.LogAttr("endpoint", endpointAddress),
				utils.LogAttr("GUID", ctx),
			)
		}
	}

	utils.LavaFormatDebug("routing to direct RPC flow (direct-only)",
		utils.LogAttr("num_sessions", len(sessions)),
		utils.LogAttr("GUID", ctx),
	)
	return rpcss.sendRelayToDirectEndpoints(ctx, sessions, protocolMessage, relayProcessor, analytics)
}

// relayInnerDirect handles relay requests using direct RPC connections (smart router mode)
func (rpcss *RPCSmartRouterServer) relayInnerDirect(
	ctx context.Context,
	singleConsumerSession *lavasession.SingleConsumerSession,
	relayResult *common.RelayResult,
	relayTimeout time.Duration,
	chainMessage chainlib.ChainMessage,
	originalRequestData []byte,
	analytics *metrics.RelayMetrics,
) (relayLatency time.Duration, err error, needsBackoff bool) {
	// Get direct connection from session
	directConnection, ok := singleConsumerSession.GetDirectConnection()
	if !ok {
		return 0, fmt.Errorf("session does not have direct RPC connection"), false
	}

	if rpcss.debugRelays {
		utils.LavaFormatDebug("Sending direct RPC relay",
			utils.LogAttr("timeout", relayTimeout),
			utils.LogAttr("method", chainMessage.GetApi().Name),
			utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
			utils.LogAttr("protocol", directConnection.GetProtocol()),
			utils.LogAttr("GUID", ctx),
		)
	}

	// Check for gRPC streaming method - currently not supported in Direct RPC mode
	// TODO: Full streaming support requires ChainListener changes to maintain client connections
	// and route repliesChan messages back to the client. For now, we refuse streaming RPCs
	// to avoid resource leaks (upstream subscriptions left running without consumers).
	if rpcss.grpcSubscriptionManager != nil && directConnection.GetProtocol() == "grpc" {
		methodPath := chainMessage.GetApi().Name
		isStreaming, _, streamErr := rpcss.grpcSubscriptionManager.IsStreamingMethod(ctx, methodPath)
		if streamErr == nil && isStreaming {
			utils.LavaFormatWarning("gRPC streaming methods not yet supported in Direct RPC mode",
				nil,
				utils.LogAttr("method", methodPath),
				utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
			)
			return 0, fmt.Errorf("gRPC streaming method %q not supported in Direct RPC mode; use provider-based relay for streaming", methodPath), false
		}
	}

	// Create direct RPC relay sender
	// Use provider name (configured name) instead of raw URL to avoid leaking API keys
	endpointName := singleConsumerSession.Parent.PublicLavaAddress
	// Resolve chain family for Tier 2 error classification
	senderChainFamily := common.ChainFamily(-1)
	if family, ok := common.GetChainFamily(rpcss.listenEndpoint.ChainID); ok {
		senderChainFamily = family
	}
	directSender := &DirectRPCRelaySender{
		directConnection:    directConnection,
		endpointName:        endpointName,
		originalRequestData: originalRequestData,
		chainFamily:         senderChainFamily,
		groupLabel:          singleConsumerSession.Parent.GroupLabel,
	}

	// Send relay directly to RPC endpoint
	startTime := time.Now()
	result, err := directSender.SendDirectRelay(ctx, chainMessage, relayTimeout)
	relayLatency = time.Since(startTime)

	// Get endpoint for health tracking (use stored reference, not string lookup)
	var targetEndpoint *lavasession.Endpoint
	if drsc, ok := singleConsumerSession.Connection.(*lavasession.DirectRPCSessionConnection); ok {
		targetEndpoint = drsc.Endpoint // Robust: use stored reference
	}

	if err != nil {
		utils.LavaFormatDebug("direct RPC relay failed",
			utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
			utils.LogAttr("error", err.Error()),
			utils.LogAttr("latency", relayLatency),
			utils.LogAttr("GUID", ctx),
		)

		// Classify error using the error registry and decide on health tracking
		// Try to extract LavaError from classifiedError (already classified by classifyAndWrap)
		classified := extractLavaError(err)
		if classified == nil {
			// Fallback: derive transport and chain family, classify from scratch
			transport := common.TransportJsonRPC
			switch directConnection.GetProtocol() {
			case lavasession.DirectRPCProtocolGRPC:
				transport = common.TransportGRPC
			case lavasession.DirectRPCProtocolHTTP, lavasession.DirectRPCProtocolHTTPS:
				// HTTP could be JSON-RPC or REST — use the endpoint's API interface
				if rpcss.listenEndpoint.ApiInterface == "rest" {
					transport = common.TransportREST
				}
			}

			// Resolve chain family for Tier 2 matchers
			chainFamily := common.ChainFamily(-1)
			if family, ok := common.GetChainFamily(rpcss.listenEndpoint.ChainID); ok {
				chainFamily = family
			}

			errorCode := 0
			if httpErr, ok := err.(*lavasession.HTTPStatusError); ok {
				errorCode = httpErr.StatusCode
			}
			classified = common.ClassifyError(common.DetectConnectionError(err), chainFamily, transport, errorCode, err.Error())
		}

		// PROTOCOL_CONTEXT_CANCELED is expected in two cases:
		// 1. Relay race: multiple goroutines race in parallel; when one wins, ProcessRelaySend
		//    returns and its defer cancel() cancels the parent ctx, which cancels all still-in-flight
		//    goroutines — those see context.Canceled as a result.
		// 2. Client disconnect: the upstream caller closed the connection before we responded.
		// Neither case is a provider fault — classifyEndpointHealth handles the carve-out
		// using common.IsClientCancellation so every endpoint-health decision site uses the
		// same rule.
		//
		// Compute the cancellation flag BEFORE logging so race losers don't get
		// tagged as errors and don't pollute smartrouter_errors_total — on a busy router
		// doing parallel races, the race-loser count would otherwise swamp the
		// real error count.
		isClientCancel := common.IsClientCancellation(err, ctx)
		if isClientCancel {
			utils.LavaFormatDebug("direct RPC relay cancelled by client (race loser or client disconnect)",
				utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
				utils.LogAttr("ctx_err", ctx.Err()),
				utils.LogAttr("GUID", ctx),
			)
		} else {
			common.LogCodedError("direct RPC relay error", err, classified,
				rpcss.listenEndpoint.ChainID, 0, err.Error(),
				utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
			)
		}

		// Decide endpoint health based on error classification.
		var shouldMarkUnhealthy bool
		shouldMarkUnhealthy, needsBackoff = classifyEndpointHealth(classified, isClientCancel)

		// Apply health tracking based on error classification
		if shouldMarkUnhealthy && targetEndpoint != nil {
			targetEndpoint.MarkUnhealthy()
			rpcss.smartRouterEndpointMetrics.SetEndpointOverallHealth(rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface, endpointName, false)
		}

		return relayLatency, err, needsBackoff
	}

	// Check status code even when err == nil (for REST 5xx/429).
	//
	// HTTP 501 (Not Implemented) is deliberately excluded: a Cosmos REST node
	// returns it to mean "I don't implement this method" — a node-capability
	// error, not a server/transport failure. Routing it through this branch
	// would convert it into a synthetic transport error (triggering retry +
	// backoff) and discard the node's response. Excluding it lets the result
	// flow through as the NodeError the REST sender already produced
	// (IsNodeError=true), where it classifies as NODE_UNIMPLEMENTED
	// (non-retryable) and is returned to the client.
	statusCode := result.StatusCode
	if (statusCode >= 500 && statusCode != http.StatusNotImplemented) || statusCode == 429 {
		shouldMarkUnhealthy, needsBackoffHTTP := classifyHTTPStatus(statusCode)
		needsBackoff = needsBackoffHTTP

		if shouldMarkUnhealthy && targetEndpoint != nil {
			targetEndpoint.MarkUnhealthy()
			rpcss.smartRouterEndpointMetrics.SetEndpointOverallHealth(rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface, endpointName, false)
			utils.LavaFormatDebug("endpoint returned error status",
				utils.LogAttr("status", statusCode),
				utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
			)
		} else if statusCode == 429 {
			utils.LavaFormatDebug("endpoint rate limited",
				utils.LogAttr("status", statusCode),
				utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
			)
		}

		// Return error to trigger backoff (but preserve result for client)
		return relayLatency, fmt.Errorf("HTTP %d", statusCode), needsBackoff
	}

	// Success - reset endpoint health
	if targetEndpoint != nil {
		if targetEndpoint.ResetHealth() {
			rpcss.smartRouterEndpointMetrics.SetEndpointOverallHealth(rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface, endpointName, true)
		}
	}

	// Update relayResult with the response
	relayResult.Reply = result.Reply
	relayResult.Finalized = result.Finalized
	relayResult.StatusCode = result.StatusCode
	relayResult.IsNodeError = result.IsNodeError
	relayResult.IsNonRetryable = result.IsNonRetryable
	relayResult.ProviderInfo = result.ProviderInfo
	if relayResult.Reply != nil {
		relayResult.Reply.Metadata = append(relayResult.Reply.Metadata, pairingtypes.Metadata{
			Name:  common.LAVA_RELAY_PROTOCOL_HEADER_NAME,
			Value: string(directConnection.GetProtocol()),
		})
	}

	// Update analytics
	if analytics != nil {
		analytics.Success = true
	}

	utils.LavaFormatTrace("direct RPC relay succeeded",
		utils.LogAttr("endpoint", singleConsumerSession.Parent.PublicLavaAddress),
		utils.LogAttr("latency", relayLatency),
		utils.LogAttr("status_code", result.StatusCode),
		utils.LogAttr("response_size", len(result.Reply.Data)),
		utils.LogAttr("GUID", ctx),
	)

	return relayLatency, nil, false
}

func (rpcss *RPCSmartRouterServer) GetProcessingTimeout(chainMessage chainlib.ChainMessage) (processingTimeout time.Duration, relayTimeout time.Duration) {
	_, averageBlockTime, _, _ := rpcss.chainParser.ChainBlockStats()
	relayTimeout = chainlib.GetRelayTimeout(chainMessage, averageBlockTime)
	processingTimeout = common.GetTimeoutForProcessing(relayTimeout, chainlib.GetTimeoutInfo(chainMessage))
	return processingTimeout, relayTimeout
}

func (rpcss *RPCSmartRouterServer) LavaDirectiveHeaders(metadata []pairingtypes.Metadata) ([]pairingtypes.Metadata, map[string]string) {
	metadataRet := []pairingtypes.Metadata{}
	headerDirectives := map[string]string{}
	for _, metaElement := range metadata {
		name := strings.ToLower(metaElement.Name)
		if _, found := common.SPECIAL_LAVA_DIRECTIVE_HEADERS[name]; found {
			headerDirectives[name] = metaElement.Value
		} else {
			metadataRet = append(metadataRet, metaElement)
		}
	}
	return metadataRet, headerDirectives
}

func (rpcss *RPCSmartRouterServer) getExtensionsFromDirectiveHeaders(directiveHeaders map[string]string) extensionslib.ExtensionInfo {
	extensionsStr, ok := directiveHeaders[common.EXTENSION_OVERRIDE_HEADER_NAME]
	if ok {
		utils.LavaFormatTrace("[Archive Debug] Found extension override header", utils.LogAttr("extensionsStr", extensionsStr))
		extensions := strings.Split(extensionsStr, ",")
		_, extensions, _ = rpcss.chainParser.SeparateAddonsExtensions(context.Background(), extensions)
		utils.LavaFormatTrace("[Archive Debug] Processed extensions", utils.LogAttr("extensions", extensions))
		if len(extensions) == 1 && extensions[0] == "none" {
			// none eliminates existing extensions
			return extensionslib.ExtensionInfo{LatestBlock: rpcss.getLatestBlock(), ExtensionOverride: []string{}}
		} else if len(extensions) > 0 {
			// All extensions from headers use AdditionalExtensions (consistent behavior)
			utils.LavaFormatTrace("[Archive Debug] Using AdditionalExtensions for all header extensions", utils.LogAttr("extensions", extensions))
			return extensionslib.ExtensionInfo{LatestBlock: rpcss.getLatestBlock(), AdditionalExtensions: extensions}
		}
	}
	utils.LavaFormatTrace("[Archive Debug] No extension override header found")
	return extensionslib.ExtensionInfo{LatestBlock: rpcss.getLatestBlock()}
}

func (rpcss *RPCSmartRouterServer) HandleDirectiveHeadersForMessage(chainMessage chainlib.ChainMessage, directiveHeaders map[string]string) {
	timeoutStr, ok := directiveHeaders[common.RELAY_TIMEOUT_HEADER_NAME]
	if ok {
		timeout, err := time.ParseDuration(timeoutStr)
		if err == nil {
			// set an override timeout
			utils.LavaFormatDebug("User indicated to set the timeout using flag", utils.LogAttr("timeout", timeoutStr))
			chainMessage.TimeoutOverride(timeout)
		}
	}

	_, ok = directiveHeaders[common.FORCE_CACHE_REFRESH_HEADER_NAME]
	chainMessage.SetForceCacheRefresh(ok)
}

// Iterating over metadataHeaders adding each trailer that fits the header if found to relayResult.Relay.Metadata
func (rpcss *RPCSmartRouterServer) getMetadataFromRelayTrailer(metadataHeaders []string, relayResult *common.RelayResult) {
	for _, metadataHeader := range metadataHeaders {
		trailerValue := relayResult.ProviderTrailer.Get(metadataHeader)
		if len(trailerValue) > 0 {
			extensionMD := pairingtypes.Metadata{
				Name:  metadataHeader,
				Value: trailerValue[0],
			}
			relayResult.Reply.Metadata = append(relayResult.Reply.Metadata, extensionMD)
		}
	}
}

// RelayProcessorForHeaders interface for methods used by appendHeadersToRelayResult
type RelayProcessorForHeaders interface {
	GetCrossValidationParams() *common.CrossValidationParams // nil for Stateless/Stateful
	GetSelection() relaycore.Selection
	GetResultsData() ([]common.RelayResult, []common.RelayResult, []relaycore.RelayError)
	GetStatefulRelayTargets() []string
	GetCrossValidationQueriedProviders() []string // all providers queried (even if response not received)
	GetUsedProviders() *lavasession.UsedProviders
	NodeErrors() (ret []common.RelayResult)
}

func (rpcss *RPCSmartRouterServer) appendHeadersToRelayResult(ctx context.Context, relayResult *common.RelayResult, protocolErrors uint64, relayProcessor RelayProcessorForHeaders, protocolMessage chainlib.ProtocolMessage, apiName string, analytics *metrics.RelayMetrics, success bool) {
	metadataReply := []pairingtypes.Metadata{}

	// Check if cross-validation is enabled via Selection type
	selection := relayProcessor.GetSelection()

	if selection == relaycore.CrossValidation {
		// For cross-validation mode: show all participating providers and status
		successResults, nodeErrorResults, protocolErrorResults := relayProcessor.GetResultsData()
		cvParams := relayProcessor.GetCrossValidationParams()

		// Get all providers that were queried (set before any responses came back)
		// This includes providers whose responses may not have been received due to early exit
		allProvidersList := relayProcessor.GetCrossValidationQueriedProviders()
		sort.Strings(allProvidersList)

		// Determine cross-validation status and agreeing/disagreeing providers
		cvSuccess := relayResult != nil && cvParams != nil && relayResult.CrossValidation >= cvParams.AgreementThreshold
		cvStatus := "failed"
		var agreeingProvidersList, disagreeingProvidersList []string

		// Error providers always disagree regardless of consensus outcome
		for _, result := range nodeErrorResults {
			if result.ProviderInfo.ProviderAddress != "" {
				disagreeingProvidersList = append(disagreeingProvidersList, result.ProviderInfo.ProviderAddress)
			}
		}
		for _, result := range protocolErrorResults {
			if result.ProviderInfo.ProviderAddress != "" {
				disagreeingProvidersList = append(disagreeingProvidersList, result.ProviderInfo.ProviderAddress)
			}
		}

		if cvSuccess {
			cvStatus = "success"
			winningHash := relayResult.ResponseHash
			for _, result := range successResults {
				if result.ProviderInfo.ProviderAddress == "" {
					continue
				}
				if result.ResponseHash == winningHash {
					agreeingProvidersList = append(agreeingProvidersList, result.ProviderInfo.ProviderAddress)
				} else {
					disagreeingProvidersList = append(disagreeingProvidersList, result.ProviderInfo.ProviderAddress)
				}
			}
		} else {
			// No consensus — every successful provider is also in conflict
			for _, result := range successResults {
				if result.ProviderInfo.ProviderAddress != "" {
					disagreeingProvidersList = append(disagreeingProvidersList, result.ProviderInfo.ProviderAddress)
				}
			}
		}

		// Deduplicate and sort both lists for stable metric labels and headers
		agreeingProvidersList = dedupSortedStrings(agreeingProvidersList)
		disagreeingProvidersList = dedupSortedStrings(disagreeingProvidersList)

		// Emit cross-validation metric (even on failure)
		if cvParams != nil && rpcss.listenEndpoint != nil && rpcss.rpcSmartRouterLogs != nil {
			chainId, apiInterface := rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface
			go rpcss.rpcSmartRouterLogs.SetCrossValidationMetric(
				chainId, apiInterface, apiName, cvSuccess,
				agreeingProvidersList, disagreeingProvidersList,
			)
			// Bounded by-reason breakdown for a quorum-time failure (no-agreement / diversity-unmet /
			// group-quorum-unmet / insufficient-responses). The reason rides on the minimal failure result;
			// it is "" on success, where SetCrossValidationFailureMetric is a no-op. The request-time
			// structural fail-fast emits its own reason via crossValidationFailFast (the two paths are
			// mutually exclusive).
			if !cvSuccess && relayResult != nil && relayResult.CrossValidationFailureReason != "" {
				go rpcss.rpcSmartRouterLogs.SetCrossValidationFailureMetric(
					chainId, apiInterface, apiName, relayResult.CrossValidationFailureReason,
				)
			}
		}

		// Mismatch alerting surface (1.3): record one bounded group+finality-labeled metric per distinct
		// outlier group — but ONLY for a SUCCESSFUL content outlier (a successful response whose hash differs
		// from the consensus, with quorum reached) on a DETERMINISTIC method. Node/protocol errors,
		// no-agreement, diversity-unmet and insufficient-responses are NOT content outliers (they belong to
		// the structured failure signal), and non-deterministic methods legitimately differ.
		deterministic := protocolMessage.GetApi() != nil && protocolMessage.GetApi().Category.Deterministic
		var consensusHash [32]byte
		if relayResult != nil {
			consensusHash = relayResult.ResponseHash
		}
		successOutliers := crossValidationSuccessOutliers(successResults, consensusHash, cvSuccess, deterministic)
		if len(successOutliers) > 0 && rpcss.smartRouterEndpointMetrics != nil && rpcss.listenEndpoint != nil {
			chainId, apiInterface := rpcss.listenEndpoint.ChainID, rpcss.listenEndpoint.ApiInterface
			finality := rpcss.crossValidationFinalityLabel(protocolMessage)
			outlierGroups := map[string]struct{}{}
			for _, outlier := range successOutliers {
				group := outlier.ProviderInfo.ProviderGroup
				if group == "" {
					group = common.DefaultProviderGroup
				}
				outlierGroups[group] = struct{}{}
				utils.LavaFormatInfo("cross-validation outlier detected",
					utils.LogAttr("GUID", ctx),
					utils.LogAttr("provider", outlier.ProviderInfo.ProviderAddress),
					utils.LogAttr("group", group),
					utils.LogAttr("method", apiName),
					utils.LogAttr("finality", finality),
					utils.LogAttr("consensusHashHex", fmt.Sprintf("%x", consensusHash[:8])),
					utils.LogAttr("outlierHashHex", fmt.Sprintf("%x", outlier.ResponseHash[:8])),
				)
			}
			for group := range outlierGroups {
				rpcss.smartRouterEndpointMetrics.SetCrossValidationMismatchMetric(chainId, apiInterface, apiName, group, finality)
			}
		}

		// Add cross-validation headers (always, even on failure)
		metadataReply = append(metadataReply, pairingtypes.Metadata{
			Name:  common.CROSS_VALIDATION_STATUS_HEADER_NAME,
			Value: cvStatus,
		})

		// Add all providers header (comma-separated for easy parsing)
		metadataReply = append(metadataReply, pairingtypes.Metadata{
			Name:  common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME,
			Value: strings.Join(allProvidersList, ","),
		})

		// Add agreeing providers header (comma-separated for easy parsing)
		metadataReply = append(metadataReply, pairingtypes.Metadata{
			Name:  common.CROSS_VALIDATION_AGREEING_PROVIDERS_HEADER,
			Value: strings.Join(agreeingProvidersList, ","),
		})

		// Add disagreeing providers header so clients see dissent (providers whose response conflicted
		// with the consensus, plus node/protocol-error providers) without needing debug mode.
		metadataReply = append(metadataReply, pairingtypes.Metadata{
			Name:  common.CROSS_VALIDATION_DISAGREEING_PROVIDERS_HEADER,
			Value: strings.Join(disagreeingProvidersList, ","),
		})

		// On failure, surface WHY (no-agreement / diversity-unmet / insufficient-responses) so clients
		// and metrics can distinguish a diversity failure from an ordinary no-agreement.
		if relayResult != nil && relayResult.CrossValidationFailureReason != "" {
			metadataReply = append(metadataReply, pairingtypes.Metadata{
				Name:  common.CROSS_VALIDATION_FAILURE_REASON_HEADER,
				Value: relayResult.CrossValidationFailureReason,
			})
		}
	} else if relayResult != nil {
		// For non-cross-validation mode: keep existing single provider behavior
		providerAddress := relayResult.GetProvider()
		if providerAddress == "" {
			providerAddress = "Cached"
		}
		metadataReply = append(metadataReply, pairingtypes.Metadata{
			Name:  common.PROVIDER_ADDRESS_HEADER_NAME,
			Value: providerAddress,
		})

		// add the relay retried count: total attempts minus 1 (the initial attempt is not a retry)
		successResults, nodeErrorResults, protocolErrorResults := relayProcessor.GetResultsData()
		totalAttempts := uint64(len(successResults)) + uint64(len(nodeErrorResults)) + protocolErrors

		// Stateful selection fans out to all top providers in a single batch and
		// never retries (relaypolicy.Decide returns Stop for Stateful). Failures
		// inside that fan-out are expected — only one provider needs to win —
		// so they must not inflate Lava-Retries. Without this absorption,
		// healthy stateful traffic shows non-zero retry rates whenever any
		// parallel attempt loses the race or returns an error.
		totalRetries := uint64(0)
		if selection != relaycore.Stateful && totalAttempts > 1 {
			totalRetries = totalAttempts - 1
		}

		if totalRetries > 0 {
			metadataReply = append(metadataReply, pairingtypes.Metadata{
				Name:  common.RETRY_COUNT_HEADER_NAME,
				Value: strconv.FormatUint(totalRetries, 10),
			})
			// Record retry incident metrics
			if rpcss.listenEndpoint != nil && rpcss.rpcSmartRouterLogs != nil {
				chainId := rpcss.listenEndpoint.ChainID
				apiInterface := rpcss.listenEndpoint.ApiInterface
				go rpcss.rpcSmartRouterLogs.RecordIncidentRetry(chainId, apiInterface, apiName, totalRetries, success)
			}

			// When there are retries, show all attempted providers in
			// chronological-ish order (failures before the resolver, the resolver
			// last). "Cached" stays in the list so the entry count matches
			// Lava-Retries — without it, retries=N and a single provider name
			// disagree on how many actors participated (MAG-1653 Bug #2).
			//
			// Ordering rule: walk protocol errors → node errors → successes,
			// skipping any entry whose address matches the resolver, then
			// append the resolver (`providerAddress`, which is "Cached" when
			// relayResult.GetProvider() was empty). The skip-then-append is
			// load-bearing: dedup alone preserves first-seen position, so if
			// the resolver happened to be in successResults[0] (e.g. it
			// completed before the loser was even recorded), the final
			// addProvider(providerAddress) would be a no-op and the chain
			// tail would be a *loser*, violating "last entry == response
			// source" (MAG-1871). Walking slices makes the value
			// deterministic across runs; the explicit final append makes
			// the contract structurally true.
			seen := make(map[string]struct{})
			allProvidersList := make([]string, 0)
			addProvider := func(addr string) {
				if addr == "" {
					return
				}
				if _, ok := seen[addr]; ok {
					return
				}
				seen[addr] = struct{}{}
				allProvidersList = append(allProvidersList, addr)
			}
			for _, r := range protocolErrorResults {
				if r.ProviderInfo.ProviderAddress == providerAddress {
					continue
				}
				addProvider(r.ProviderInfo.ProviderAddress)
			}
			for _, r := range nodeErrorResults {
				if r.ProviderInfo.ProviderAddress == providerAddress {
					continue
				}
				addProvider(r.ProviderInfo.ProviderAddress)
			}
			for _, r := range successResults {
				if r.ProviderInfo.ProviderAddress == providerAddress {
					continue
				}
				addProvider(r.ProviderInfo.ProviderAddress)
			}
			addProvider(providerAddress) // resolver — "Cached" or the winning real provider, always last

			if len(allProvidersList) > 0 {
				allProvidersString := strings.Join(allProvidersList, ",")
				for i, metadata := range metadataReply {
					if metadata.Name == common.PROVIDER_ADDRESS_HEADER_NAME {
						metadataReply[i].Value = allProvidersString
						break
					}
				}
			}
		}
	}

	// Record consistency and hedge incident metrics (only for non-cross-validation mode)
	if selection != relaycore.CrossValidation && rpcss.listenEndpoint != nil && rpcss.rpcSmartRouterLogs != nil {
		chainId := rpcss.listenEndpoint.ChainID
		apiInterface := rpcss.listenEndpoint.ApiInterface
		// Consistency: triggered when the consumer enforces a minimum seen block height
		if protocolMessage.RelayPrivateData().SeenBlock > 0 {
			go rpcss.rpcSmartRouterLogs.RecordIncidentConsistency(chainId, apiInterface, apiName, success)
		}
		// Hedge: triggered when the batch ticker fired during this relay
		if analytics != nil && analytics.HedgeCount > 0 {
			go rpcss.rpcSmartRouterLogs.RecordIncidentHedgeResult(chainId, apiInterface, apiName, analytics.HedgeCount, success)
		}
	}

	// If relayResult is nil but we have headers to add (e.g., cross-validation failure),
	// we still need to return early as there's no way to attach headers to the error response.
	// The cross-validation info is included in the error message and metrics have been emitted.
	if relayResult == nil {
		return
	}

	// Add selection stats header if feature is enabled
	if rpcss.enableSelectionStats {
		if selectionStats := rpcss.sessionManager.GetSelectionStats(); selectionStats != nil {
			statsString := selectionStats.FormatSelectionStats()
			if statsString != "" {
				metadataReply = append(metadataReply, pairingtypes.Metadata{
					Name:  common.SELECTION_STATS_HEADER_NAME,
					Value: statsString,
				})
			}
		}
	}

	if relayResult.Reply == nil {
		relayResult.Reply = &pairingtypes.RelayReply{}
	}
	if relayResult.Reply.LatestBlock > 0 {
		metadataReply = append(metadataReply,
			pairingtypes.Metadata{
				Name:  common.PROVIDER_LATEST_BLOCK_HEADER_NAME,
				Value: strconv.FormatInt(relayResult.Reply.LatestBlock, 10),
			})
	}
	guid, found := utils.GetUniqueIdentifier(ctx)
	if found && guid != 0 {
		guidStr := strconv.FormatUint(guid, 10)
		metadataReply = append(metadataReply,
			pairingtypes.Metadata{
				Name:  common.GUID_HEADER_NAME,
				Value: guidStr,
			})
	}

	// add stateful API (hanging, transactions)
	if protocolMessage.GetApi().Category.Stateful == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
		metadataReply = append(metadataReply,
			pairingtypes.Metadata{
				Name:  common.STATEFUL_API_HEADER,
				Value: "true",
			})

		// add all providers that received the stateful relay
		statefulRelayTargets := relayProcessor.GetStatefulRelayTargets()
		if len(statefulRelayTargets) > 0 {
			allProvidersString := fmt.Sprintf("%v", statefulRelayTargets)
			metadataReply = append(metadataReply,
				pairingtypes.Metadata{
					Name:  common.STATEFUL_ALL_PROVIDERS_HEADER_NAME,
					Value: allProvidersString,
				})
		}
	}

	// add user requested API
	metadataReply = append(metadataReply,
		pairingtypes.Metadata{
			Name:  common.USER_REQUEST_TYPE,
			Value: apiName,
		})

	// add is node error flag
	if relayResult.IsNodeError {
		metadataReply = append(metadataReply,
			pairingtypes.Metadata{
				Name:  common.LAVA_IDENTIFIED_NODE_ERROR_HEADER,
				Value: "true",
			})
	}

	// MAG-1818: signal that the hedge ticker dispatched at least one speculative
	// batch on this request. Independent of Lava-Retries, which inclusively counts
	// canceled hedge primaries — tests need this orthogonal flag to distinguish
	// "hedge fired" from "classical retry." Omit-when-false convention (no "false"
	// emitted), mirroring lava-identified-node-error above.
	if analytics != nil && analytics.HedgeCount > 0 {
		metadataReply = append(metadataReply,
			pairingtypes.Metadata{
				Name:  common.LAVA_HEDGE_TRIGGERED_HEADER,
				Value: "true",
			})
	}

	// fetch trailer information from the provider by using the provider trailer field.
	rpcss.getMetadataFromRelayTrailer(chainlib.TrailersToAddToHeaderResponse, relayResult)

	directiveHeaders := protocolMessage.GetDirectiveHeaders()
	_, debugRelays := directiveHeaders[common.LAVA_DEBUG_RELAY]
	if debugRelays {
		metadataReply = append(metadataReply,
			pairingtypes.Metadata{
				Name:  common.REQUESTED_BLOCK_HEADER_NAME,
				Value: strconv.FormatInt(protocolMessage.RelayPrivateData().GetRequestBlock(), 10),
			})

		routerKey := lavasession.NewRouterKeyFromExtensions(protocolMessage.GetExtensions())
		erroredProviders := relayProcessor.GetUsedProviders().GetErroredProviders(routerKey)
		if len(erroredProviders) > 0 {
			erroredProvidersArray := make([]string, len(erroredProviders))
			idx := 0
			for providerAddress := range erroredProviders {
				erroredProvidersArray[idx] = providerAddress
				idx++
			}
			erroredProvidersString := fmt.Sprintf("%v", erroredProvidersArray)
			erroredProvidersMD := pairingtypes.Metadata{
				Name:  common.ERRORED_PROVIDERS_HEADER_NAME,
				Value: erroredProvidersString,
			}
			relayResult.Reply.Metadata = append(relayResult.Reply.Metadata, erroredProvidersMD)
		}

		nodeErrors := relayProcessor.NodeErrors()
		if len(nodeErrors) > 0 {
			nodeErrorHeaderString := ""
			for _, nodeError := range nodeErrors {
				nodeErrorHeaderString += fmt.Sprintf("%s: %s,", nodeError.GetProvider(), string(nodeError.Reply.Data))
			}
			relayResult.Reply.Metadata = append(relayResult.Reply.Metadata,
				pairingtypes.Metadata{
					Name:  common.NODE_ERRORS_PROVIDERS_HEADER_NAME,
					Value: nodeErrorHeaderString,
				})
		}

		if relayResult.Request != nil && relayResult.Request.RelaySession != nil {
			currentReportedProviders := rpcss.sessionManager.GetReportedProviders(uint64(relayResult.Request.RelaySession.Epoch))
			if len(currentReportedProviders) > 0 {
				reportedProvidersArray := make([]string, len(currentReportedProviders))
				for idx, providerAddress := range currentReportedProviders {
					reportedProvidersArray[idx] = providerAddress.Address
				}
				reportedProvidersString := fmt.Sprintf("%v", reportedProvidersArray)
				reportedProvidersMD := pairingtypes.Metadata{
					Name:  common.REPORTED_PROVIDERS_HEADER_NAME,
					Value: reportedProvidersString,
				}
				relayResult.Reply.Metadata = append(relayResult.Reply.Metadata, reportedProvidersMD)
			}
		}

		relayResult.Reply.Metadata = append(relayResult.Reply.Metadata, pairingtypes.Metadata{
			Name:  common.SMART_ROUTER_VERSION_HEADER_NAME,
			Value: version.Version,
		})
	}

	relayResult.Reply.Metadata = append(relayResult.Reply.Metadata, metadataReply...)
}

func (rpcss *RPCSmartRouterServer) IsHealthy() bool {
	return rpcss.relaysMonitor.IsHealthy()
}

func (rpcss *RPCSmartRouterServer) IsInitialized() bool {
	if rpcss == nil {
		return false
	}

	return rpcss.initialized.Load()
}

func (rpcss *RPCSmartRouterServer) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guid := utils.GenerateUniqueIdentifier()
	ctx = utils.WithUniqueIdentifier(ctx, guid)
	url, data, connectionType, metadata, err := rpcss.chainParser.ExtractDataFromRequest(req)
	if err != nil {
		return nil, err
	}
	// Use client IP for IP forwarding when available (Raw HTTP transport has no consumerIp param)
	consumerIp := req.RemoteAddr
	if h := req.Header.Get(common.IP_FORWARDING_HEADER_NAME); h != "" {
		consumerIp = h
	}
	relayResult, err := rpcss.SendRelay(ctx, url, data, connectionType, "", consumerIp, nil, metadata)
	if err != nil {
		return nil, err
	}
	resp, err := rpcss.chainParser.SetResponseFromRelayResult(relayResult)
	return resp, err
}

// dedupSortedStrings returns a sorted, deduplicated copy of the input slice.
func dedupSortedStrings(s []string) []string {
	if len(s) == 0 {
		return s
	}
	sort.Strings(s)
	out := s[:1]
	for _, v := range s[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func (rpcss *RPCSmartRouterServer) updateProtocolMessageIfNeededWithNewEarliestData(
	ctx context.Context,
	relayState *relaycore.RelayState,
	protocolMessage chainlib.ProtocolMessage,
	earliestBlockHashRequested int64,
	addon string,
) chainlib.ProtocolMessage {
	if !relayState.GetIsEarliestUsed() && earliestBlockHashRequested != spectypes.NOT_APPLICABLE {
		// We got a earliest block data from cache, we need to create a new protocol message with the new earliest block hash parsed
		// and update the extension rules with the new earliest block data as it might be archive.
		// Setting earliest used to attempt this only once.
		relayState.SetIsEarliestUsed()
		relayRequestData := protocolMessage.RelayPrivateData()
		userData := protocolMessage.GetUserData()
		// Preserve the client's directive headers (e.g. lava-extension, force-cache-refresh) across
		// the re-parse. Passing nil drops them, which on the archive/earliest-block path would make
		// the rebuilt message's cache key disagree with the original message's key (and silently
		// discards the directive the client sent).
		directiveMetadata := metadataFromDirectiveHeaders(protocolMessage.GetDirectiveHeaders())
		newProtocolMessage, err := rpcss.ParseRelay(ctx, relayRequestData.ApiUrl, string(relayRequestData.Data), relayRequestData.ConnectionType, userData.DappId, userData.ConsumerIp, directiveMetadata)
		if err != nil {
			utils.LavaFormatError("failed copying protocol message in sendRelayToEndpoint", err)
			return protocolMessage
		}

		extensionAdded := newProtocolMessage.UpdateEarliestAndValidateExtensionRules(rpcss.chainParser.ExtensionsParser(), earliestBlockHashRequested, addon, relayRequestData.SeenBlock)
		if extensionAdded && relayState.CheckIsArchive(newProtocolMessage.RelayPrivateData()) {
			relayState.SetIsArchive(true)
		}
		relayState.SetProtocolMessage(newProtocolMessage)
		return newProtocolMessage
	}
	return protocolMessage
}

// metadataFromDirectiveHeaders rebuilds relay metadata entries from already-parsed directive
// headers, so they can be re-fed through ParseRelay (which re-splits metadata into directives).
// Used to carry directives across a re-parse without losing them.
func metadataFromDirectiveHeaders(directiveHeaders map[string]string) []pairingtypes.Metadata {
	if len(directiveHeaders) == 0 {
		return nil
	}
	metadata := make([]pairingtypes.Metadata, 0, len(directiveHeaders))
	for name, value := range directiveHeaders {
		metadata = append(metadata, pairingtypes.Metadata{Name: name, Value: value})
	}
	return metadata
}

// classifyHTTPStatus classifies an HTTP status code for endpoint health decisions.
func classifyHTTPStatus(code int) (shouldMarkUnhealthy, needsBackoff bool) {
	switch {
	case code >= 500:
		return true, true
	case code == 429:
		return false, true
	default:
		return false, false
	}
}
