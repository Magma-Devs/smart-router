package routersession

import "errors"

var ( // Consumer Side Errors
	PairingListEmptyError                 = errors.New("No pairings available.") // client could not connect to any provider.
	UnreachableCodeError                  = errors.New("Should not get here.")
	AllProviderEndpointsDisabledError     = errors.New("All endpoints are not available.") // a provider is completely unresponsive all endpoints are not available
	MaximumNumberOfSessionsExceededError  = errors.New("Provider reached maximum number of active sessions.")
	MaxComputeUnitsExceededError          = errors.New("Consumer is trying to exceed the maximum number of compute units available.")
	EpochMismatchError                    = errors.New("Tried to Report to an older epoch")
	AddressIndexWasNotFoundError          = errors.New("address index was not found in list")
	LockMisUseDetectedError               = errors.New("Faulty use of locks detected")
	SessionIsAlreadyBlockListedError      = errors.New("Session is already in block list")
	NegativeComputeUnitsAmountError       = errors.New("Tried to subtract to negative compute units amount")
	ReportAndBlockProviderError           = errors.New("Report and block the provider")
	BlockProviderError                    = errors.New("Block the provider")
	SessionOutOfSyncError                 = errors.New("Session went out of sync with the provider") // do no change the name, before also fixing the consumerSessionManager.ts file as it relies on the error message
	SendRelayError                        = errors.New("Failed To Send Relay")
	ContextDoneNoNeedToLockSelectionError = errors.New("Context deadline exceeded while trying to lock selection")
	BlockEndpointError                    = errors.New("Block the endpoint")
	ConsistencyPreValidationError         = errors.New("endpoint failed pre-request consistency validation")
	// The two outcomes of `router-select-provider`, kept apart because the message
	// reaches the caller verbatim: "not a provider here" and "already failed"
	// call for different reactions.
	SelectedProviderUnavailableError   = errors.New("Header-selected provider is not a valid provider for this request")
	SelectedProviderAlreadyFailedError = errors.New("Header-selected provider has already failed for this request")
	// SelectedProviderNotConfiguredError marks the one SelectedProviderUnavailableError that is the
	// caller's mistake: the router-select-provider name is none of the router's providers at all,
	// primary or backup, in any letter case (MAG-3800). GetSessions returns it wrapped together with
	// SelectedProviderUnavailableError, so everything that stops on a pinned refusal still does. Its
	// text is the whole sentence the listeners give the caller.
	SelectedProviderNotConfiguredError = errors.New("Selected provider is not configured")
)

// SessionOutOfSyncGRPCCode is the gRPC status code used when wrapping SessionOutOfSyncError
// as a gRPC status error. This value was historically derived from the cosmos SDK error code (677).
const SessionOutOfSyncGRPCCode = 677
