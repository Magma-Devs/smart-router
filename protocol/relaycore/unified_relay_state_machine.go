package relaycore

import (
	context "context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	common "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	lavasession "github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils"
)

// UnifiedRelayStateMachine is the single state machine implementation used by both
// Consumer and SmartRouter. Behavior differences are controlled via StateMachineConfig.
// Retry decisions are centralized in the policy engine (RelayPolicyInf).
type UnifiedRelayStateMachine struct {
	ctx                   context.Context
	relaySender           RelaySenderInf
	resultsChecker        ResultsCheckerInf
	analytics             *metrics.RelayMetrics
	selection             Selection
	crossValidationParams *common.CrossValidationParams
	debugRelays           bool
	batchUpdate           chan error
	usedProviders         *lavasession.UsedProviders
	relayRetriesManager   *lavaprotocol.RelayRetriesManager
	relayState            []*RelayState
	protocolMessage       chainlib.ProtocolMessage
	relayStateLock        sync.RWMutex
	config                StateMachineConfig
	policy                RelayPolicyInf

	// stopReason is why the state machine stopped, carried out on the Done instruction so the
	// request's final line can report it. Written and read only on the state machine's own
	// goroutine; stopReasonLock guards it against a future caller off that goroutine.
	stopReason     string
	stopReasonLock sync.RWMutex

	// hedgePending records that the ticker has asked for a hedge whose dispatch has not yet been
	// reported back. HedgeCount is incremented when that dispatch SUCCEEDS rather than when the
	// ticker asks, because the two are not the same event: with an attempt in flight and the pool
	// empty, the ticker asks every window and every ask fails, which inflated the count by one per
	// window for the life of the request.
	//
	// pairingEmptyWarned keeps the "all providers exhausted" WARNING to the first time it is true
	// for a request. It is a fact about the request, not about the window, so repeating it once per
	// window says nothing new — roughly 180 identical lines on a 180s request.
	//
	// pendingStopReason holds a reason that was decided while attempts were STILL IN FLIGHT, so it
	// describes the dispatcher rather than the request's ending. It becomes the stop reason only if
	// the request goes on to end with nothing in flight — see recordStopReason and settleStopReason.
	//
	// All three are touched only from the select loop in GetRelayTaskChannel, which is a single
	// goroutine, so none needs a lock.
	hedgePending       bool
	pairingEmptyWarned bool
	pendingStopReason  string
}

// setStopReason records why the state machine is stopping. Last writer wins: a request that
// exhausts retries and then hits the processing timeout stopped for the timeout.
func (sm *UnifiedRelayStateMachine) setStopReason(reason string) {
	sm.stopReasonLock.Lock()
	defer sm.stopReasonLock.Unlock()
	sm.stopReason = reason
}

func (sm *UnifiedRelayStateMachine) getStopReason() string {
	sm.stopReasonLock.RLock()
	defer sm.stopReasonLock.RUnlock()
	return sm.stopReason
}

// stopReasonOr returns the recorded reason, falling back to a caller-supplied one. The deadline
// paths use it so a policy decision that already stopped the request — "Stateful", which is why
// there was no retry — survives the timeout that ends the wait.
func (sm *UnifiedRelayStateMachine) stopReasonOr(fallback string) string {
	if reason := sm.getStopReason(); reason != "" {
		return reason
	}
	return fallback
}

// relaysInFlight reports whether any attempt from this request is still running.
func (sm *UnifiedRelayStateMachine) relaysInFlight() bool {
	return sm.usedProviders != nil && sm.usedProviders.CurrentlyUsed() > 0
}

// recordStopReason records why there will be no further attempt — which is not the same claim as
// "the request is over", now that an attempt outlives the window that dispatched it.
//
// Every reason on these paths is decided while relays may still be running: the policy's own branch
// says so ("in-flight relays from earlier batches may still succeed"), and the circuit breaker's
// warning says so too ("relays already in flight may still answer"). A reason filed then describes
// the dispatcher, and it would take the slot the timeout needs — availability scoring blames an
// endpoint only on StopReasonProcessingTimeout, so a hung endpoint on a request that really did run
// out of budget would be forgiven because the pool had emptied, or the retries had run out, minutes
// earlier. That was harmless while an attempt died at its window, because "cannot start more" and
// "the request is over" were then the same instant.
//
// So it is held rather than dropped: if the last attempt finishes before the budget, this is still
// the honest reason and settleStopReason promotes it. If the budget expires first, the timeout is.
func (sm *UnifiedRelayStateMachine) recordStopReason(reason string) {
	if sm.relaysInFlight() {
		sm.pendingStopReason = reason
		return
	}
	sm.setStopReason(reason)
}

// settleStopReason names the request's stop reason at the one moment it is certainly over: the
// return condition, which fires only once nothing is in flight. A reason held back by
// recordStopReason is the right answer here and nowhere earlier.
func (sm *UnifiedRelayStateMachine) settleStopReason() string {
	if reason := sm.getStopReason(); reason != "" {
		return reason
	}
	if sm.pendingStopReason != "" {
		sm.setStopReason(sm.pendingStopReason)
	}
	return sm.getStopReason()
}

func NewUnifiedRelayStateMachine(
	ctx context.Context,
	usedProviders *lavasession.UsedProviders,
	relaySender RelaySenderInf,
	protocolMessage chainlib.ProtocolMessage,
	analytics *metrics.RelayMetrics,
	debugRelays bool,
	config StateMachineConfig,
	policy RelayPolicyInf,
	// cvOverride, when non-nil, forces CrossValidation with these already-resolved params. The
	// rpcsmartrouter layer sets it from a per-method policy (it owns the policy resolver; relaycore
	// must not import it). nil => the legacy header-driven decision below, unchanged.
	cvOverride *common.CrossValidationParams,
	// forbidCallerCrossValidation, when true, suppresses the caller-header-driven CrossValidation decision
	// for this method: the request's lava-cross-validation-* headers are ignored entirely (not even
	// validated) and the method routes by its normal stateful/stateless category. The rpcsmartrouter layer
	// sets it from a per-method `forbid-caller-cv` policy. It is moot when cvOverride != nil (an operator
	// that mandates CV cannot also forbid it — Validate rejects that combination upstream).
	forbidCallerCrossValidation bool,
) (RelayStateMachine, error) {
	var selection Selection
	var cvParams *common.CrossValidationParams

	if cvOverride != nil {
		// Per-method policy forced cross-validation. Checked BEFORE the Stateful branch so a
		// policy-enabled method is never silently routed to Stateful (Finding A).
		selection = CrossValidation
		cvParams = cvOverride
		utils.LavaFormatDebug("[StateMachine] CrossValidation mode enabled (policy-resolved)",
			utils.LogAttr("maxParticipants", cvOverride.MaxParticipants),
			utils.LogAttr("agreementThreshold", cvOverride.AgreementThreshold),
			utils.LogAttr("minGroups", cvOverride.MinGroups),
			utils.LogAttr("GUID", ctx))
	} else if forbidCallerCrossValidation {
		// Operator policy forbids caller-driven CV for this method. Ignore any cross-validation headers
		// (deliberately disregarded, so they are not even parsed/validated) and route by the method's normal
		// category — exactly as if the request had sent no CV headers. This is what makes `forbid-caller-cv`
		// truly disable cross-validation for the method; without skipping the header read below, a caller
		// could still turn CV on via headers.
		utils.LavaFormatDebug("[StateMachine] caller cross-validation headers ignored (forbidden by per-method policy)",
			utils.LogAttr("GUID", ctx))
		if chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
			selection = Stateful
		} else {
			selection = Stateless
		}
	} else if crossValidationParams, headersPresent, err := protocolMessage.GetCrossValidationParameters(); headersPresent && err != nil {
		return nil, utils.LavaFormatError("invalid cross-validation headers", err, utils.LogAttr("GUID", ctx))
	} else if headersPresent {
		selection = CrossValidation
		cvParams = &crossValidationParams
		utils.LavaFormatDebug("[StateMachine] CrossValidation mode enabled",
			utils.LogAttr("maxParticipants", crossValidationParams.MaxParticipants),
			utils.LogAttr("agreementThreshold", crossValidationParams.AgreementThreshold),
			utils.LogAttr("GUID", ctx))
	} else if chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
		selection = Stateful
	} else {
		selection = Stateless
	}

	return &UnifiedRelayStateMachine{
		ctx:                   ctx,
		usedProviders:         usedProviders,
		relaySender:           relaySender,
		protocolMessage:       protocolMessage,
		analytics:             analytics,
		selection:             selection,
		crossValidationParams: cvParams,
		debugRelays:           debugRelays,
		batchUpdate:           make(chan error, config.MaxRetries),
		relayState:            make([]*RelayState, 0),
		config:                config,
		policy:                policy,
	}, nil
}

func (sm *UnifiedRelayStateMachine) Initialized() bool {
	return sm.relayRetriesManager != nil && sm.resultsChecker != nil
}

func (sm *UnifiedRelayStateMachine) SetRelayRetriesManager(relayRetriesManager *lavaprotocol.RelayRetriesManager) {
	sm.relayRetriesManager = relayRetriesManager
}

func (sm *UnifiedRelayStateMachine) SetResultsChecker(resultsChecker ResultsCheckerInf) {
	sm.resultsChecker = resultsChecker
}

func (sm *UnifiedRelayStateMachine) GetUsedProviders() *lavasession.UsedProviders {
	return sm.usedProviders
}

func (sm *UnifiedRelayStateMachine) GetSelection() Selection {
	return sm.selection
}

func (sm *UnifiedRelayStateMachine) GetCrossValidationParams() *common.CrossValidationParams {
	return sm.crossValidationParams
}

func (sm *UnifiedRelayStateMachine) appendRelayState(nextState *RelayState) {
	sm.relayStateLock.Lock()
	defer sm.relayStateLock.Unlock()
	sm.relayState = append(sm.relayState, nextState)
}

func (sm *UnifiedRelayStateMachine) getLatestState() *RelayState {
	sm.relayStateLock.RLock()
	defer sm.relayStateLock.RUnlock()
	if len(sm.relayState) == 0 {
		return nil
	}
	return sm.relayState[len(sm.relayState)-1]
}

// stateTransition creates the next relay state. If the policy returned a mutation,
// it is applied here via applyMutation. Otherwise falls back to UpgradeToArchiveIfNeeded.
func (sm *UnifiedRelayStateMachine) stateTransition(relayState *RelayState, numberOfNodeErrors uint64, mutation *MutationOutput) {
	batchNumber := sm.usedProviders.BatchNumber()
	var nextState *RelayState
	if relayState == nil {
		nextState = NewRelayState(sm.ctx, sm.protocolMessage, 0, sm.relayRetriesManager, sm.relaySender, &ArchiveStatus{})
	} else {
		protocolMessage := sm.GetProtocolMessage()
		archiveStatus := relayState.GetArchiveStatus()

		var upgradedProtocolMessage chainlib.ProtocolMessage
		if mutation != nil && (mutation.ArchiveAction != ArchiveNoChange || mutation.CacheHashes) {
			upgradedProtocolMessage = sm.applyMutation(protocolMessage, archiveStatus, *mutation)
		} else {
			// Fallback: policy.Decide() returned no archive mutation (e.g. epoch-mismatch
			// retry at step 4, or initial state). Legacy UpgradeToArchiveIfNeeded applies
			// batch-number-based archive logic that mirrors decideMutation(). Both paths
			// must stay in sync until the fallback is eliminated.
			upgradedProtocolMessage = UpgradeToArchiveIfNeeded(sm.ctx, protocolMessage, archiveStatus, sm.relaySender, sm.relayRetriesManager, batchNumber, numberOfNodeErrors)
		}

		// MAG-2228: a mutation that changes the extensions (archive add/remove) rebuilds the
		// message under a different routerKey. The used/unwanted-provider exclusion is keyed
		// by routerKey, so providers already tried under the old key would not be excluded
		// under the new one and a just-failed provider could be re-selected for the retry.
		// Carry the exclusion across the toggle.
		oldRouterKey := lavasession.NewRouterKeyFromExtensions(protocolMessage.GetExtensions())
		newRouterKey := lavasession.NewRouterKeyFromExtensions(upgradedProtocolMessage.GetExtensions())
		if oldRouterKey.String() != newRouterKey.String() {
			sm.usedProviders.MigrateUnwantedProviders(oldRouterKey, newRouterKey)
		}

		nextState = NewRelayState(sm.ctx, upgradedProtocolMessage, relayState.GetStateNumber()+1, sm.relayRetriesManager, sm.relaySender, archiveStatus)
	}
	sm.appendRelayState(nextState)
}

// applyMutation applies the policy's archive/cache mutation to the protocol message.
func (sm *UnifiedRelayStateMachine) applyMutation(protocolMessage chainlib.ProtocolMessage, archiveStatus *ArchiveStatus, mutation MutationOutput) chainlib.ProtocolMessage {
	if mutation.CacheHashes {
		cacheBlockHashes(protocolMessage, archiveStatus, sm.relayRetriesManager)
	}

	switch mutation.ArchiveAction {
	case ArchiveAdd:
		return addArchiveExtension(sm.ctx, protocolMessage, archiveStatus, sm.relaySender)
	case ArchiveRemove:
		return removeArchiveExtension(sm.ctx, protocolMessage, archiveStatus, sm.relaySender)
	default:
		return protocolMessage
	}
}

// getResultsSummary retrieves the ResultsSummary from the results checker.
func (sm *UnifiedRelayStateMachine) getResultsSummary() ResultsSummary {
	if sm.resultsChecker == nil {
		return ResultsSummary{}
	}
	return sm.resultsChecker.GetResultsSummary()
}

// signalReturnCondition hands the main loop a return reason without ever blocking.
//
// The loop reads returnCondition at most once and may already have left on another
// branch, so only the first signal can matter. A plain send from the third trigger in
// one burst parked its goroutine forever, pinning the whole state machine and the
// request it carries (MAG-3722).
func signalReturnCondition(returnCondition chan<- error, err error) {
	select {
	case returnCondition <- err:
	default:
	}
}

// buildDecisionInput assembles the DecisionInput for the policy engine.
func (sm *UnifiedRelayStateMachine) buildDecisionInput(numberOfNodeErrors uint64, isTickerHedge bool) DecisionInput {
	latestState := sm.getLatestState()
	var archiveStatus *ArchiveStatus
	if latestState != nil {
		archiveStatus = latestState.GetArchiveStatus()
	}

	return DecisionInput{
		Selection:     sm.selection,
		AttemptNumber: sm.usedProviders.BatchNumber(),
		IsBatch:       sm.protocolMessage.IsBatch(),
		Summary:       sm.getResultsSummary(),
		ArchiveStatus: archiveStatus,
		NodeErrors:    numberOfNodeErrors,
		IsTickerHedge: isTickerHedge,
	}
}

func (sm *UnifiedRelayStateMachine) GetDebugState() bool {
	return sm.debugRelays
}

func (sm *UnifiedRelayStateMachine) GetProtocolMessage() chainlib.ProtocolMessage {
	latestState := sm.getLatestState()
	if latestState == nil {
		return sm.protocolMessage
	}
	return latestState.GetProtocolMessage()
}

// endOfRoadReason names why processingCtx ended, distinguishing our own budget expiring from the
// caller cancelling from outside.
//
// Both arrive here as a non-nil ctx.Err(), and both used to be labelled ProcessingTimeout. That
// label is what availability scoring reads to decide an endpoint "ran out of road" and may be
// blamed, so a websocket or gRPC client hanging up mid-request could record a failure against an
// endpoint that was healthy and still working. Only a genuine deadline earns the timeout label.
//
// A reason already recorded by the policy still wins over both — see stopReasonOr.
func (sm *UnifiedRelayStateMachine) endOfRoadReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return sm.stopReasonOr(StopReasonProcessingTimeout)
	}
	return sm.stopReasonOr(StopReasonCallerGone)
}

// checkAndHandleTimeout checks if processingCtx has expired and handles cleanup if so.
func (sm *UnifiedRelayStateMachine) checkAndHandleTimeout(
	processingCtx context.Context,
	relayTaskChannel chan RelayStateSendInstructions,
	processingTimeout time.Duration,
	location string,
) bool {
	if processingCtx.Err() == nil {
		return false
	}

	userData := sm.GetProtocolMessage().GetUserData()
	utils.LavaFormatWarning("Relay processing timeout expired",
		nil,
		utils.LogAttr("location", location),
		utils.LogAttr("processingTimeout", processingTimeout),
		utils.LogAttr("dappId", userData.DappId),
		utils.LogAttr("consumerIp", userData.ConsumerIp),
		utils.LogAttr("api", sm.GetProtocolMessage().GetApi().Name),
		utils.LogAttr("GUID", sm.ctx),
		utils.LogAttr("batchNumber", sm.usedProviders.BatchNumber()),
		utils.LogAttr("consecutiveBatchErrors", sm.policy.GetConsecutiveBatchErrors()),
	)

	relayTaskChannel <- RelayStateSendInstructions{Err: processingCtx.Err(), Done: true, StopReason: sm.endOfRoadReason(processingCtx.Err())}
	return true
}

func (sm *UnifiedRelayStateMachine) GetRelayTaskChannel() (chan RelayStateSendInstructions, error) {
	if !sm.Initialized() {
		return nil, utils.LavaFormatError("UnifiedRelayStateMachine was not initialized properly", nil)
	}

	relayTaskChannel := make(chan RelayStateSendInstructions, 1)
	go func() {
		gotResults := make(chan bool, 1)
		processingTimeout, relayTimeout := sm.relaySender.GetProcessingTimeout(sm.GetProtocolMessage())
		if sm.debugRelays {
			utils.LavaFormatDebug("Relay initiated with the following timeout schedule", utils.LogAttr("processingTimeout", processingTimeout), utils.LogAttr("attemptWindow", relayTimeout), utils.LogAttr("GUID", sm.ctx))
		}
		processingCtx, processingCtxCancel := context.WithTimeout(sm.ctx, processingTimeout)
		defer processingCtxCancel()

		numberOfNodeErrorsAtomic := atomic.Uint64{}
		readResultsFromProcessor := func() {
			utils.LavaFormatTrace("[StateMachine] Waiting for results", utils.LogAttr("batch", sm.usedProviders.BatchNumber()), utils.LogAttr("GUID", sm.ctx))
			sm.resultsChecker.WaitForResults(processingCtx)
			metRequiredNodeResults, numberOfNodeErrors := sm.resultsChecker.HasRequiredNodeResults(sm.usedProviders.BatchNumber())
			numberOfNodeErrorsAtomic.Store(uint64(numberOfNodeErrors))
			gotResults <- metRequiredNodeResults
		}
		go readResultsFromProcessor()
		returnCondition := make(chan error, 1)
		validateReturnCondition := func(err error) {
			batchOnStart := sm.usedProviders.BatchNumber()
			time.Sleep(15 * time.Millisecond)
			utils.LavaFormatTrace("[StateMachine] validating return condition", utils.LogAttr("batch", sm.usedProviders.BatchNumber()), utils.LogAttr("GUID", sm.ctx))
			if batchOnStart == sm.usedProviders.BatchNumber() && sm.usedProviders.CurrentlyUsed() == 0 {
				utils.LavaFormatTrace("[StateMachine] return condition triggered", utils.LogAttr("batch", sm.usedProviders.BatchNumber()), utils.LogAttr("err", err), utils.LogAttr("GUID", sm.ctx))
				signalReturnCondition(returnCondition, err)
			}
		}

		// initialize relay state
		sm.stateTransition(nil, 0, nil)

		// Determine number of providers for initial batch
		var numProviders int
		if sm.selection == CrossValidation && sm.crossValidationParams != nil {
			numProviders = sm.crossValidationParams.MaxParticipants
		} else {
			numProviders = 1
		}

		// Send First Message
		relayTaskChannel <- RelayStateSendInstructions{
			Analytics:      sm.analytics,
			RelayState:     sm.getLatestState(),
			NumOfProviders: numProviders,
		}

		// relayTimeout is the attempt WINDOW: when to dispatch another endpoint. It no longer also
		// kills the attempt in flight, so this genuinely hedges rather than replaces.
		//
		// time.NewTicker panics on a non-positive interval, and nothing recovers a panic in this
		// goroutine: it ends the process, for every request on it (MAG-3600). GetRelayTimeout never
		// returns one, but the window arrives through an interface, so the line is held here too.
		// A window that means nothing means no hedging, not a crash and not a hedge storm.
		if relayTimeout <= 0 {
			utils.LavaFormatWarning("[StateMachine] non-positive attempt window, this relay will not hedge", nil,
				utils.LogAttr("attemptWindow", relayTimeout),
				utils.LogAttr("GUID", sm.ctx),
			)
			relayTimeout = time.Duration(math.MaxInt64) // never fires within the request
		}
		startNewBatchTicker := time.NewTicker(relayTimeout)
		defer startNewBatchTicker.Stop()

		// Start the relay state machine
		for {
			// SmartRouter: Priority check for processing timeout before select
			if sm.config.EnableTimeoutPriority {
				if sm.checkAndHandleTimeout(processingCtx, relayTaskChannel, processingTimeout, "priority_check") {
					return
				}
			}

			select {
			case err := <-sm.batchUpdate:
				isPairingListEmpty := err != nil && errors.Is(err, lavasession.PairingListEmptyError)
				result := sm.policy.OnSendRelayResult(err, isPairingListEmpty, sm.selection)

				switch result {
				case SendSuccess:
					// An attempt actually went out. If the ticker asked for this one, that is a
					// hedge that fired, and the only point at which counting it is truthful.
					if sm.hedgePending {
						sm.hedgePending = false
						if sm.analytics != nil {
							sm.analytics.HedgeCount++
						}
					}
				case SendStop:
					// This arm stops without consulting the policy, so it names its own reason:
					// Decide never runs here, and the field would otherwise be blank on exactly
					// the exhaustion cases an operator reads the line to understand.
					if isPairingListEmpty && sm.config.EnableCircuitBreaker {
						// "Exhausted" describes the dispatcher, not the endpoints: the pool is empty
						// because every provider is already busy on this request. recordStopReason
						// holds it back while that is true, so it cannot take the slot the timeout
						// needs for availability scoring.
						// Read once: the log is the evidence for this decision, so it has to report
						// the value the decision actually used.
						stillInFlight := sm.usedProviders.CurrentlyUsed()
						sm.recordStopReason("AllProvidersExhausted")
						// Once per request, not once per window. The ticker keeps asking for a hedge
						// while an attempt is in flight, and every ask lands here, so an unconditional
						// WARNING produced one identical line per window for the whole request.
						// Repeats carry no new information: the same pool is still empty for the same
						// reason. The trace keeps them available when debugging a single relay.
						if sm.pairingEmptyWarned {
							utils.LavaFormatTrace("[StateMachine] circuit breaker: pool still empty",
								utils.LogAttr("GUID", sm.ctx),
								utils.LogAttr("stillInFlight", stillInFlight),
							)
						} else {
							sm.pairingEmptyWarned = true
							utils.LavaFormatWarning("Circuit breaker: all providers exhausted, stopping new attempts — relays already in flight may still answer",
								nil,
								utils.LogAttr("GUID", sm.ctx),
								utils.LogAttr("batchNumber", sm.usedProviders.BatchNumber()),
								utils.LogAttr("stillInFlight", stillInFlight),
							)
						}
					} else if sm.usedProviders.BatchNumber() == 0 && sm.policy.GetConsecutiveBatchErrors() == sm.config.SendRelayAttempts+1 {
						sm.recordStopReason("FirstMessageFailed")
						utils.LavaFormatWarning("Failed Sending First Message", err, utils.LogAttr("consecutive errors", sm.policy.GetConsecutiveBatchErrors()), utils.LogAttr("GUID", sm.ctx))
					} else {
						sm.recordStopReason("BatchSendFailed")
					}
					// The request is ending; a hedge the ticker asked for will never go out.
					sm.hedgePending = false
					go validateReturnCondition(err)
				case SendRetry:
					if sm.config.EnableTimeoutPriority {
						if sm.checkAndHandleTimeout(processingCtx, relayTaskChannel, processingTimeout, "batchUpdate_error") {
							return
						}
					}
					utils.LavaFormatTrace("[StateMachine] batchUpdate - send retry", utils.LogAttr("batch", sm.usedProviders.BatchNumber()), utils.LogAttr("GUID", sm.ctx))
					relayTaskChannel <- RelayStateSendInstructions{RelayState: sm.getLatestState(), NumOfProviders: 1}
				}

			case success := <-gotResults:
				utils.LavaFormatTrace("[StateMachine] success := <-gotResults", utils.LogAttr("batch", sm.usedProviders.BatchNumber()), utils.LogAttr("GUID", sm.ctx))
				if success {
					utils.LavaFormatTrace("[StateMachine] successfully sent message", utils.LogAttr("GUID", sm.ctx))
					relayTaskChannel <- RelayStateSendInstructions{Done: true, StopReason: "Success"}
					return
				}

				if sm.config.EnableTimeoutPriority {
					if sm.checkAndHandleTimeout(processingCtx, relayTaskChannel, processingTimeout, "gotResults_retry") {
						return
					}
				}

				nodeErrors := numberOfNodeErrorsAtomic.Load()
				output := sm.policy.Decide(sm.buildDecisionInput(nodeErrors, false))
				// A retry is the exceptional path — the common case is a single attempt that
				// stops — and it is the one an operator needs in order to explain a slow or
				// multi-provider relay from production INFO logs. Log those at INFO; leave the
				// ordinary Stop at DEBUG so steady-state traffic stays quiet.
				logDecision := utils.LavaFormatDebug
				if output.Action != ActionStop {
					logDecision = utils.LavaFormatInfo
				}
				logDecision("[StateMachine] policy.Decide",
					utils.LogAttr("GUID", sm.ctx),
					utils.LogAttr("action", output.Action),
					utils.LogAttr("reason", output.Reason),
					utils.LogAttr("batchNumber", sm.usedProviders.BatchNumber()),
				)

				if output.Action == ActionRetry {
					// A retry after a completed attempt, not a hedge. Clear any hedge the ticker asked
					// for and never got out, so this dispatch is not counted as that hedge firing.
					sm.hedgePending = false
					sm.stateTransition(sm.getLatestState(), nodeErrors, &output.Mutation)
					relayTaskChannel <- RelayStateSendInstructions{RelayState: sm.getLatestState(), NumOfProviders: 1}
				} else {
					// Held back, not filed, while relays are still in flight — the same reason the
					// line below does not return immediately. One of them may still answer, and if
					// none does because the budget ran out, that timeout is what the request stopped
					// for and what availability scoring has to read.
					sm.recordStopReason(output.Reason)
					// Don't return immediately — in-flight relays from earlier batches
					// may still succeed. validateReturnCondition waits 15ms and checks
					// whether any relays are still CurrentlyUsed before concluding.
					go validateReturnCondition(nil)
				}
				go readResultsFromProcessor()

			case <-startNewBatchTicker.C:
				if sm.config.EnableTimeoutPriority {
					if sm.checkAndHandleTimeout(processingCtx, relayTaskChannel, processingTimeout, "ticker_retry") {
						return
					}
				}

				nodeErrors := numberOfNodeErrorsAtomic.Load()
				output := sm.policy.Decide(sm.buildDecisionInput(nodeErrors, true))
				if output.Action == ActionRetry {
					utils.LavaFormatTrace("[StateMachine] ticker triggered", utils.LogAttr("batch", sm.usedProviders.BatchNumber()), utils.LogAttr("GUID", sm.ctx))
					sm.stateTransition(sm.getLatestState(), nodeErrors, &output.Mutation)
					relayTaskChannel <- RelayStateSendInstructions{RelayState: sm.getLatestState(), NumOfProviders: 1}
					// Counted when the dispatch is confirmed, in the batchUpdate arm — asking for a
					// hedge is not the same as sending one.
					sm.hedgePending = true
				}

			case returnErr := <-returnCondition:
				utils.LavaFormatTrace("[StateMachine] returnErr := <-returnCondition", utils.LogAttr("batch", sm.usedProviders.BatchNumber()), utils.LogAttr("GUID", sm.ctx))
				// validateReturnCondition only fires with nothing in flight, so this is the moment a
				// reason held back by recordStopReason becomes the truth about the whole request.
				relayTaskChannel <- RelayStateSendInstructions{Err: returnErr, Done: true, StopReason: sm.settleStopReason()}
				return

			case <-processingCtx.Done():
				if sm.config.EnableTimeoutPriority {
					sm.checkAndHandleTimeout(processingCtx, relayTaskChannel, processingTimeout, "processingCtx_done_backup")
				} else {
					userData := sm.GetProtocolMessage().GetUserData()
					utils.LavaFormatWarning("Relay Got processingCtx timeout", nil,
						utils.LogAttr("processingTimeout", processingTimeout),
						utils.LogAttr("dappId", userData.DappId),
						utils.LogAttr("consumerIp", userData.ConsumerIp),
						utils.LogAttr("protocolMessage.GetApi().Name", sm.GetProtocolMessage().GetApi().Name),
						utils.LogAttr("GUID", sm.ctx),
						utils.LogAttr("batchNumber", sm.usedProviders.BatchNumber()),
						utils.LogAttr("consecutiveBatchErrors", sm.policy.GetConsecutiveBatchErrors()),
					)
					relayTaskChannel <- RelayStateSendInstructions{Err: processingCtx.Err(), Done: true, StopReason: sm.endOfRoadReason(processingCtx.Err())}
				}
				return
			}
		}
	}()
	return relayTaskChannel, nil
}

func (sm *UnifiedRelayStateMachine) UpdateBatch(err error) {
	sm.batchUpdate <- err
}
