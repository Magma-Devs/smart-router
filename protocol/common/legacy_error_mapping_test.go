package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyErrorMapping_NilError(t *testing.T) {
	assert.Nil(t, ClassifyLegacyError(nil, TransportJsonRPC))
}

func TestLegacyErrorMapping_NonLegacyError(t *testing.T) {
	// A plain error without sdkerrors code falls back to message classification
	err := errors.New("nonce too low")
	result := ClassifyLegacyError(err, TransportJsonRPC)
	assert.Equal(t, RouterErrorChainNonceTooLow, result)
}

func TestLegacyErrorMapping_UnknownFallback(t *testing.T) {
	err := errors.New("completely unknown error")
	result := ClassifyLegacyError(err, TransportJsonRPC)
	assert.Equal(t, RouterErrorUnknown, result)
}

func TestLegacyErrorMapping_FallbackUsesTransport(t *testing.T) {
	// A gRPC "not implemented" message must be classified via gRPC matchers,
	// not JSON-RPC matchers — this is the bug the transport parameter fixes.
	err := errors.New("not implemented")
	grpcResult := ClassifyLegacyError(err, TransportGRPC)
	assert.Equal(t, RouterErrorNodeUnimplemented, grpcResult, "gRPC transport should match 'not implemented'")

	jsonRPCResult := ClassifyLegacyError(err, TransportJsonRPC)
	assert.Equal(t, RouterErrorUnknown, jsonRPCResult, "JSON-RPC transport should not match gRPC-only message")
}

// mockSDKError mimics the (codespace, code) interface from cosmossdk.io/errors.
// The legacy mapping keys on the tuple so every mock must supply both fields.
type mockSDKError struct {
	codespace string
	code      uint32
	msg       string
}

func (e *mockSDKError) Error() string     { return e.msg }
func (e *mockSDKError) Codespace() string { return e.codespace }
func (e *mockSDKError) ABCICode() uint32  { return e.code }

type legacyCase struct {
	name      string
	codespace string
	code      uint32
	expected  *RouterError
}

func runLegacyCases(t *testing.T, cases []legacyCase) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := &mockSDKError{codespace: tt.codespace, code: tt.code, msg: "test"}
			result := ClassifyLegacyError(err, TransportJsonRPC)
			require.NotNil(t, result)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLegacyErrorMapping_SessionErrors(t *testing.T) {
	runLegacyCases(t, []legacyCase{
		{"PairingListEmpty", "pairingListEmpty Error", 665, RouterErrorNoProviders},
		{"AllEndpointsDisabled", "AllProviderEndpointsDisabled Error", 667, RouterErrorAllEndpointsDisabled},
		{"MaxSessions", "MaximumNumberOfSessionsExceeded Error", 668, RouterErrorSessionNotFound},
		{"MaxCU", "MaxComputeUnitsExceeded Error", 669, RouterErrorMaxCUExceeded},
		{"EpochMismatch", "ReportingAnOldEpoch Error", 670, RouterErrorEpochMismatch},
		{"ConsumerNotRegistered", "AddressIndexWasNotFound Error", 671, RouterErrorConsumerNotRegistered},
		{"ConsumerBlocked", "SessionIsAlreadyBlockListed Error", 673, RouterErrorConsumerBlocked},
		{"SessionOutOfSync", "SessionOutOfSync Error", 677, RouterErrorSessionOutOfSync},
		{"ContextDeadline", "ContextDoneNoNeedToLockSelection Error", 687, RouterErrorContextDeadline},
		{"ConsistencyPreValidation", "ConsistencyPreValidation Error", 699, RouterErrorConsistencyError},
	})
}

func TestLegacyErrorMapping_ProviderErrors(t *testing.T) {
	runLegacyCases(t, []legacyCase{
		{"InvalidEpoch", "InvalidEpoch Error", 881, RouterErrorEpochMismatch},
		{"RelayNumMismatch", "NewSessionWithRelayNum Error", 882, RouterErrorRelayNumberMismatch},
		{"ConsumerBlockListed", "ConsumerIsBlockListed Error", 883, RouterErrorConsumerBlocked},
		{"ConsumerNotRegistered", "ConsumerNotActive Error", 884, RouterErrorConsumerNotRegistered},
		{"SessionNotExist", "SessionDoesNotExist Error", 885, RouterErrorSessionNotFound},
		{"MaxCU", "MaximumCULimitReachedByConsumer Error", 886, RouterErrorMaxCUExceeded},
		{"CuMismatch", "ProviderConsumerCuMisMatch Error", 887, RouterErrorSessionOutOfSync},
		{"RelayNumberMismatch", "RelayNumberMismatch Error", 888, RouterErrorRelayNumberMismatch},
		{"SubscriptionInit", "SubscriptionInitiationError Error", 889, RouterErrorSubscriptionInitFailed},
		{"EpochNotRegistered", "EpochIsNotRegisteredError Error", 890, RouterErrorEpochMismatch},
		{"ConsumerNotRegistered2", "ConsumerIsNotRegisteredError Error", 891, RouterErrorConsumerNotRegistered},
		{"SubscriptionAlreadyExists", "SubscriptionAlreadyExists Error", 892, RouterErrorSubscriptionAlreadyExists},
		// SubscriptionPointerIsNil is a provider-side invariant violation, so it
		// surfaces as RelayProcessingFailed rather than the misleading NotFound
		// classification that preceded the fix.
		{"SubscriptionPointerIsNil", "SubscriptionPointerIsNil Error", 896, RouterErrorRelayProcessingFailed},
		{"SessionIdNotFound", "SessionIdNotFound Error", 899, RouterErrorSessionNotFound},
	})
}

func TestLegacyErrorMapping_ProtocolErrors(t *testing.T) {
	runLegacyCases(t, []legacyCase{
		{"FinalizationData", "ProviderFinalizationData Error", 3365, RouterErrorFinalizationError},
		{"FinalizationAccountability", "ProviderFinalizationDataAccountability Error", 3366, RouterErrorFinalizationError},
		{"HashesConsensus", "HashesConsensus Error", 3367, RouterErrorHashConsensusError},
		{"Consistency", "Consistency Error", 3368, RouterErrorConsistencyError},
		{"UnhandledRelay", "UnhandledRelayReceiver Error", 3369, RouterErrorNodeMethodNotFound},
		{"DisabledRelay", "DisabledRelayReceiverError Error", 3370, RouterErrorNodeMethodNotSupported},
		{"NoResponseTimeout", "NoResponseTimeout Error", 685, RouterErrorNoResponseTimeout},
	})
}

func TestLegacyErrorMapping_CommonErrors(t *testing.T) {
	runLegacyCases(t, []legacyCase{
		{"ContextDeadline", "ContextDeadlineExceeded Error", 300, RouterErrorContextDeadline},
		{"StatusCode504", "Disallowed StatusCode Error", 504, RouterErrorNodeGatewayTimeout},
		{"StatusCode429", "Disallowed StatusCode Error", 429, RouterErrorNodeRateLimited},
		{"StatusCodeStrict", "Disallowed StatusCode Error", 800, RouterErrorNodeServerError},
		{"APINotSupported", "APINotSupported Error", 900, RouterErrorNodeMethodNotFound},
		{"SubscriptionNotFound", "SubscriptionNotFoundError Error", 901, RouterErrorSubscriptionNotFound},
	})
}

func TestLegacyErrorMapping_ChainTrackerErrors(t *testing.T) {
	runLegacyCases(t, []legacyCase{
		{"InvalidLatestBlock", "Invalid value for latestBlockNum", 10703, RouterErrorChainBlockNotFound},
		{"FailedFetchLatest", "Error FailedToFetchLatestBlock", 10705, RouterErrorChainBlockNotFound},
		// 10706/10707/10709 are request-input validation failures — the consumer
		// asked for an impossible range. Must classify as USER_INVALID_PARAMS so
		// they don't inflate CHAIN_DATA_NOT_AVAILABLE and don't trigger retries
		// on requests that can never succeed.
		{"InvalidRequested", "Error InvalidRequestedBlocks", 10706, RouterErrorUserInvalidParams},
		{"OutOfRange", "RequestedBlocksOutOfRange", 10707, RouterErrorUserInvalidParams},
		{"TooEarlyBlock", "Error ErrorFailedToFetchTooEarlyBlock", 10708, RouterErrorChainBlockTooOld},
		{"InvalidSpecific", "Error InvalidRequestedSpecificBlock", 10709, RouterErrorUserInvalidParams},
	})
}

func TestLegacyErrorMapping_PerformanceErrors(t *testing.T) {
	err := &mockSDKError{codespace: "Not Connected Error", code: 700, msg: "No Connection To grpc server"}
	result := ClassifyLegacyError(err, TransportJsonRPC)
	assert.Equal(t, RouterErrorConnectionRefused, result)
}

func TestLegacyErrorMapping_UnmappedCode(t *testing.T) {
	// An sdkerrors code that isn't in our mapping should fall back to message classification
	err := &mockSDKError{codespace: "unknown", code: 99999, msg: "some unknown legacy error"}
	result := ClassifyLegacyError(err, TransportJsonRPC)
	assert.Equal(t, RouterErrorUnknown, result)
}

func TestLegacyErrorMapping_ForeignCodespaceDoesNotCollide(t *testing.T) {
	// A foreign Cosmos module error with the same numeric code as a legacy
	// Legacy-codespace error MUST NOT match — this is the reason the map is keyed on
	// the (codespace, code) tuple. 885 is RouterErrorSessionNotFound only
	// under the "SessionDoesNotExist Error" codespace.
	foreign := &mockSDKError{codespace: "sdk", code: 885, msg: "unrelated foreign module error"}
	result := ClassifyLegacyError(foreign, TransportJsonRPC)
	assert.Equal(t, RouterErrorUnknown, result,
		"foreign codespace with matching code must not collide with the legacy mapping")
}

func TestLegacyMappingCoversAllCategories(t *testing.T) {
	// Verify the mapping covers both internal and external error categories
	hasInternal := false
	hasExternal := false
	for _, le := range legacyCodeToRouterError {
		if le.Category == CategoryInternal {
			hasInternal = true
		}
		if le.Category == CategoryExternal {
			hasExternal = true
		}
	}
	assert.True(t, hasInternal, "mapping should include internal errors")
	assert.True(t, hasExternal, "mapping should include external errors")
}
