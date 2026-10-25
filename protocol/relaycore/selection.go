package relaycore

import "errors"

type Selection int

const (
	// noCrossValidationRequirement is the value every cross-validation knob (agreement threshold, max
	// participants, min groups) collapses to when CrossValidation is inactive: a single relay with no
	// agreement, fan-out, or group-diversity requirement.
	noCrossValidationRequirement = 1
)

var RelayRetryLimit = 2

// DisableBatchRequestRetry prevents batch requests from being retried when set to true.
// Batch requests (JSON-RPC batches) cannot be hashed for caching, so retries may be unnecessary.
// This is controlled via the --disable-batch-request-retry flag.
// Disabled by default (true) because batch retries can cause issues with stateful operations.
var DisableBatchRequestRetry = true

// Selection Enum - defines relay behavior modes
const (
	Stateless       Selection = iota // Single provider with retries on failure, sequential provider attempts until success
	Stateful                         // All top providers at once, waits for best result (no retries) or for all providers to return a response
	CrossValidation                  // maxParticipants providers at once, no retries, waits for agreementThreshold matching responses
)

// StopReasonProcessingTimeout marks a request that ended because its budget expired. Availability
// scoring uses it to tell an endpoint that ran out of road from one we cut short (MAG-2648).
const StopReasonProcessingTimeout = "ProcessingTimeout"

// StopReasonCallerGone marks a request whose context was cancelled from outside rather than by its
// own deadline — a websocket or gRPC client that hung up mid-request.
//
// It exists so that ending is not spelled the same way as running out of budget. Availability
// scoring reads the stop reason, and only the budget case may blame an endpoint: a caller closing
// its connection says nothing about the endpoint still working on the answer. The HTTP listeners
// build their request context from Background, so only the streaming transports reach this.
const StopReasonCallerGone = "CallerGone"

// StopReasonCrossValidationGroupsQuiet marks a cross-validation request that stopped at its attempt
// window because enough providers agreed without meeting the quorum rule, and the only providers that
// could still complete a quorum belong to under-staffed groups (smaller than the agreement threshold)
// and had not answered (MAG-3993). Cross-validation has no retry and nothing to hedge to, so nothing
// else could complete it. Waiting out the whole budget for them turned one quiet group into a fixed
// 30s stall on every request.
//
// It is not the budget running out, so availability scoring does not blame an endpoint for it. The
// quiet provider's relay is detached and still ends on its own deadline, and that is what marks it.
const StopReasonCrossValidationGroupsQuiet = "CrossValidationGroupsQuiet"

// ErrCrossValidationGroupsQuiet is the state machine's error for a StopReasonCrossValidationGroupsQuiet
// stop. The caller does not see it: the responses already collected go through final evaluation, which
// reports the unmet quorum (diversity-unmet or group-quorum-unmet) as it would at the end of the budget.
var ErrCrossValidationGroupsQuiet = errors.New("cross-validation quorum can only be completed by under-staffed provider groups whose providers did not answer within the attempt window")
