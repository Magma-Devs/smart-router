package common

// legacyKey identifies a legacy sdkerrors.Error by the (codespace, code) tuple
// that cosmossdk.io/errors uses for uniqueness. Keying on the tuple — rather
// than the raw code — prevents numeric collisions with foreign Cosmos modules
// that may flow through ClassifyLegacyError.
type legacyKey struct {
	codespace string
	code      uint32
}

// legacyCodeToRouterError maps existing sdkerrors errors (from routersession,
// protocolerrors, chaintracker, common, performance) to their RouterError
// equivalents. The key is the (codespace, code) tuple taken directly from
// each sdkerrors.New(...) definition.
//
// The upstream chain SDK uses sdkerrors.New(<name>, <code>, <desc>) where the first argument is
// the codespace — so each error's codespace is the descriptive name string
// from its definition. Keep this map in sync with the source files when
// adding or renaming legacy errors.
//
// Usage:
//
//	routerErr := ClassifyLegacyError(legacyErr, transport)
var legacyCodeToRouterError = map[legacyKey]*RouterError{
	// --- protocol/common/errors.go ---
	{"ContextDeadlineExceeded Error", 300}:                 RouterErrorContextDeadline,
	{"Disallowed StatusCode Error", 504}:                   RouterErrorNodeGatewayTimeout,
	{"Disallowed StatusCode Error", 429}:                   RouterErrorNodeRateLimited,
	{"Disallowed StatusCode Error", 800}:                   RouterErrorNodeServerError,
	{"APINotSupported Error", 900}:                         RouterErrorNodeMethodNotFound,
	{"SubscriptionNotFoundError Error", 901}:               RouterErrorSubscriptionNotFound,
	{"ProviderFinalizationDataAccountability Error", 3365}: RouterErrorFinalizationError,

	// --- protocol/routersession/errors.go (consumer) ---
	{"pairingListEmpty Error", 665}:                   RouterErrorNoProviders,
	{"AllProviderEndpointsDisabled Error", 667}:       RouterErrorAllEndpointsDisabled,
	{"MaximumNumberOfSessionsExceeded Error", 668}:    RouterErrorSessionNotFound,
	{"MaxComputeUnitsExceeded Error", 669}:            RouterErrorMaxCUExceeded,
	{"ReportingAnOldEpoch Error", 670}:                RouterErrorEpochMismatch,
	{"AddressIndexWasNotFound Error", 671}:            RouterErrorConsumerNotRegistered,
	{"SessionIsAlreadyBlockListed Error", 673}:        RouterErrorConsumerBlocked,
	{"SessionOutOfSync Error", 677}:                   RouterErrorSessionOutOfSync,
	{"MaximumNumberOfBlockListedSessions Error", 678}: RouterErrorSessionNotFound,
	{"ContextDoneNoNeedToLockSelection Error", 687}:   RouterErrorContextDeadline,
	{"ConsistencyPreValidation Error", 699}:           RouterErrorConsistencyError,

	// --- protocol/routersession/errors.go (provider) ---
	{"InvalidEpoch Error", 881}:                    RouterErrorEpochMismatch,
	{"NewSessionWithRelayNum Error", 882}:          RouterErrorRelayNumberMismatch,
	{"ConsumerIsBlockListed Error", 883}:           RouterErrorConsumerBlocked,
	{"ConsumerNotActive Error", 884}:               RouterErrorConsumerNotRegistered,
	{"SessionDoesNotExist Error", 885}:             RouterErrorSessionNotFound,
	{"MaximumCULimitReachedByConsumer Error", 886}: RouterErrorMaxCUExceeded,
	{"ProviderConsumerCuMisMatch Error", 887}:      RouterErrorSessionOutOfSync,
	{"RelayNumberMismatch Error", 888}:             RouterErrorRelayNumberMismatch,
	{"SubscriptionInitiationError Error", 889}:     RouterErrorSubscriptionInitFailed,
	{"EpochIsNotRegisteredError Error", 890}:       RouterErrorEpochMismatch,
	{"ConsumerIsNotRegisteredError Error", 891}:    RouterErrorConsumerNotRegistered,
	{"SubscriptionAlreadyExists Error", 892}:       RouterErrorSubscriptionAlreadyExists,
	// SubscriptionPointerIsNil is a provider-side invariant violation (a bug),
	// not a user-facing "subscription missing" signal. Classify as an internal
	// relay-processing failure so it surfaces in internal-error dashboards.
	{"SubscriptionPointerIsNil Error", 896}:                    RouterErrorRelayProcessingFailed,
	{"CouldNotFindIndexAsConsumerNotYetRegistered Error", 897}: RouterErrorConsumerNotRegistered,
	{"ProviderIndexMisMatch Error", 898}:                       RouterErrorSessionOutOfSync,
	{"SessionIdNotFound Error", 899}:                           RouterErrorSessionNotFound,

	// --- protocol/relayprotocol/protocolerrors/errors.go ---
	{"ProviderFinalizationData Error", 3365}:               RouterErrorFinalizationError,
	{"ProviderFinalizationDataAccountability Error", 3366}: RouterErrorFinalizationError,
	{"HashesConsensus Error", 3367}:                        RouterErrorHashConsensusError,
	{"Consistency Error", 3368}:                            RouterErrorConsistencyError,
	{"UnhandledRelayReceiver Error", 3369}:                 RouterErrorNodeMethodNotFound,
	{"DisabledRelayReceiverError Error", 3370}:             RouterErrorNodeMethodNotSupported,
	{"NoResponseTimeout Error", 685}:                       RouterErrorNoResponseTimeout,

	// --- protocol/chaintracker/errors.go ---
	{"Invalid value for latestBlockNum", 10703}: RouterErrorChainBlockNotFound,
	{"Error FailedToFetchLatestBlock", 10705}:   RouterErrorChainBlockNotFound,
	// 10706/10707/10709 are client-side input-validation failures (malformed
	// range, invalid specific block) — not chain-state-availability issues.
	// Classifying them as USER_INVALID_PARAMS keeps the chain-data-unavailable
	// metric clean for real node/chain issues and prevents retries that
	// burn CU on requests that can never succeed.
	{"Error InvalidRequestedBlocks", 10706}:          RouterErrorUserInvalidParams,
	{"RequestedBlocksOutOfRange", 10707}:             RouterErrorUserInvalidParams,
	{"Error ErrorFailedToFetchTooEarlyBlock", 10708}: RouterErrorChainBlockTooOld,
	{"Error InvalidRequestedSpecificBlock", 10709}:   RouterErrorUserInvalidParams,

	// --- protocol/performance/errors.go ---
	{"Not Connected Error", 700}: RouterErrorConnectionRefused,

	// --- protocol/chainlib/common.go ---
	// ErrBatchRequestSizeExceeded uses sdkerrors codespace, code 1 — mapped by message instead
}

// ClassifyLegacyError extracts the sdkerrors (codespace, code) tuple from a
// legacy error and returns the corresponding RouterError. Returns the
// transport-scoped ClassifyError result if no mapping exists.
//
// transport is used for the message-based fallback when no sdkerrors
// codespace+code is found or the pair is not in our mapping. Use this for
// protocol-layer errors from the routersession/protocolerrors packages that
// carry an ABCI code via the sdkerrors interface.
func ClassifyLegacyError(err error, transport TransportType) *RouterError {
	if err == nil {
		return nil
	}

	// Try to extract sdkerrors (codespace, code) tuple
	if key, ok := extractLegacyKey(err); ok {
		if le, mapped := legacyCodeToRouterError[key]; mapped {
			return le
		}
	}

	// Fall back to message-based classification using the caller's transport
	return ClassifyError(DetectConnectionError(err), -1, transport, 0, err.Error())
}

// extractLegacyKey attempts to extract the (codespace, code) tuple from an
// sdkerrors error. cosmossdk.io/errors implements both Codespace() string
// and ABCICode() uint32 on *Error. Returns ok=false if either method is
// missing (e.g. plain errors.New, or an older ABCIError-only interface).
func extractLegacyKey(err error) (legacyKey, bool) {
	type codespaceCoder interface {
		Codespace() string
		ABCICode() uint32
	}
	if c, ok := err.(codespaceCoder); ok {
		return legacyKey{codespace: c.Codespace(), code: c.ABCICode()}, true
	}
	return legacyKey{}, false
}
