package relaycore

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

// StopReasonProcessingTimeout is the stop reason recorded when a request ended because its whole
// budget expired, rather than because an attempt answered or the policy gave up.
//
// It is the only evidence that an attempt still in flight had run out of road rather than being cut
// short, and availability scoring turns on that distinction: an endpoint we cancelled while it was
// still working, still inside its budget, has not been shown to be unavailable — it was merely
// slower than whoever won. Blaming that endpoint is the structural penalty MAG-2648 removed.
const StopReasonProcessingTimeout = "ProcessingTimeout"
