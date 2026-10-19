package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/internal/chainqueries"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	zerolog "github.com/rs/zerolog"
)

// relay_end is the one line written when a relay ends, on every exit path and at every log level.
// docs/RELAY-END-EVENT.md is its contract: readers parse it by schema, so a change they would have
// to know about bumps relayEndSchema and the document together.
const (
	relayEndEvent  = "relay_end"
	relayEndSchema = 1
)

// What the client got.
const (
	relayOutcomeOK                     = "ok"
	relayOutcomeNodeError              = "node_error"
	relayOutcomeNoAnswer               = "no_answer"
	relayOutcomeWriteUnknown           = "write_unknown"
	relayOutcomeRejectedBeforeDispatch = "rejected_before_dispatch"
	relayOutcomeRouterError            = "router_error"
)

// What one upstream did.
const (
	attemptOutcomeOK             = "ok"
	attemptOutcomeNodeError      = "node_error"
	attemptOutcomeTransportError = "transport_error"
	attemptOutcomeTimeout        = "timeout"
	attemptOutcomeRateLimited    = "rate_limited"
	attemptOutcomeCancelled      = "cancelled"
)

// relayEndTextLimit caps error and attempts[].message: a node may answer with a whole body.
const relayEndTextLimit = 512

// relayEndCachedUpstream names a cache hit, as the provider-address reply header does.
const relayEndCachedUpstream = "Cached"

// routerFault marks a failure of the router's own machinery, as opposed to the request, the
// provider pool or an upstream. A relay that ends on one is reported as router_error.
type routerFault struct{ error }

func (f routerFault) Unwrap() error { return f.error }

func isRouterFault(err error) bool {
	var fault routerFault
	return errors.As(err, &fault)
}

type relayEnd struct {
	guid         string
	connection   string
	chain        string
	stateful     bool
	method       string
	apiInterface string
	selection    string
	outcome      string
	stopReason   string
	servedBy     string
	status       int
	hasReply     bool
	errorCode    string
	errorText    string
	latency      time.Duration
	isBatch      bool
	skipped      relayEndSkips
	attempts     relayEndAttempts
}

type relayEndAttempt struct {
	upstream         string
	outcome          string
	errorCode        string
	message          string
	latency          time.Duration // zero when unknown
	returnedToClient bool
	sent             bool // went out as a dispatch, so batch and sentAfter are known
	batch            int
	sentAfter        time.Duration // from the relay's start
	staleFallback    bool          // sent although the consistency check had passed its upstream over

	// Not written: what the relay-level fields are derived from.
	reply     *pairingtypes.RelayReply
	lavaError *common.LavaError
	transport bool // a protocol error: no node reply came back
}

type relayEndAttempts []relayEndAttempt

func (e *relayEnd) MarshalZerologObject(event *zerolog.Event) {
	event.Str("event", relayEndEvent).
		Int("schema", relayEndSchema).
		Str("GUID", e.guid)
	if e.connection != "" {
		event.Str("connection", e.connection)
	}
	event.Bool("stateful", e.stateful).
		Str("method", e.method).
		Str("chain", e.chain).
		Str("api_interface", e.apiInterface).
		Str("selection", e.selection).
		Str("outcome", e.outcome).
		Str("stop_reason", e.stopReason).
		Str("served_by", e.servedBy).
		Int("status", e.status).
		Bool("has_reply", e.hasReply).
		Str("error_code", e.errorCode).
		Str("error", e.errorText).
		Int64("latency_ms", e.latency.Milliseconds()).
		Bool("is_batch", e.isBatch)
	if len(e.skipped) > 0 {
		event.Array("skipped", e.skipped)
	}
	if e.listsAttempts() {
		event.Array("attempts", e.attempts)
	}
}

// listsAttempts says whether the event carries its attempts: on a write, on a relay that did not end
// ok, on one that took more than one attempt or whose only attempt failed, so a read that recovered
// on another upstream shows what it recovered from, and on one that passed an upstream over. A read
// its first attempt answered, the common case, leaves them out.
func (e *relayEnd) listsAttempts() bool {
	if e.stateful || e.outcome != relayOutcomeOK || len(e.attempts) > 1 || len(e.skipped) > 0 {
		return true
	}
	return len(e.attempts) == 1 && e.attempts[0].outcome != attemptOutcomeOK
}

// relayEndSkips are the upstreams the consistency check passed over.
type relayEndSkips []lavasession.ConsistencySkip

func (skips relayEndSkips) MarshalZerologArray(array *zerolog.Array) {
	for i := range skips {
		array.Object(relayEndSkip(skips[i]))
	}
}

type relayEndSkip lavasession.ConsistencySkip

func (s relayEndSkip) MarshalZerologObject(event *zerolog.Event) {
	event.Str("upstream", s.Provider).
		Int64("lag", s.Lag).
		Int64("threshold", s.Threshold).
		Int("batch", s.Batch)
}

func (attempts relayEndAttempts) MarshalZerologArray(array *zerolog.Array) {
	for i := range attempts {
		array.Object(&attempts[i])
	}
}

func (a *relayEndAttempt) MarshalZerologObject(event *zerolog.Event) {
	event.Str("upstream", a.upstream).
		Str("outcome", a.outcome).
		Str("error_code", a.errorCode).
		Str("message", a.message)
	if a.errorCode != "" {
		event.Bool("retryable", a.retryable())
	}
	if a.sent {
		event.Int("batch", a.batch).
			Int64("sent_ms", a.sentAfter.Milliseconds())
	}
	if a.staleFallback {
		event.Bool("stale_fallback", true)
	}
	if a.latency > 0 {
		event.Int64("latency_ms", a.latency.Milliseconds())
	}
	event.Bool("returned_to_client", a.returnedToClient)
}

// relayEndInput is what a relay's exit hands over. Every field may be empty: a request that did not
// parse has no protocol message, one rejected before selection has no processor.
type relayEndInput struct {
	protocolMessage chainlib.ProtocolMessage
	processor       *relaycore.RelayProcessor
	result          *common.RelayResult // what the listener receives
	err             error               // what the listener receives
	cause           error               // the error the relay's processing ended with
	start           time.Time
}

// The WebSocket manager finds the reporter by type assertion, so a drifted signature would
// silently stop it being called.
var _ chainlib.RelayRejectionReporter = (*RPCSmartRouterServer)(nil)

// writeRelayEnd writes the relay's relay_end event. Each exit calls it once.
func (rpcss *RPCSmartRouterServer) writeRelayEnd(ctx context.Context, in relayEndInput) {
	utils.LogEvent(relayEndEvent, rpcss.buildRelayEnd(ctx, in))
}

// ReportRejectedRelay writes the relay_end of a request a listener parsed itself and could not hand
// to SendParsedRelay (chainlib.RelayRejectionReporter).
func (rpcss *RPCSmartRouterServer) ReportRejectedRelay(ctx context.Context, start time.Time, err error) {
	rpcss.writeRelayEnd(ctx, relayEndInput{err: err, cause: err, start: start})
}

func (rpcss *RPCSmartRouterServer) buildRelayEnd(ctx context.Context, in relayEndInput) *relayEnd {
	end := &relayEnd{latency: time.Since(in.start)}
	if rpcss.listenEndpoint != nil {
		end.chain = rpcss.listenEndpoint.ChainID
		end.apiInterface = rpcss.listenEndpoint.ApiInterface
	}
	if guid, found := utils.GetUniqueIdentifier(ctx); found {
		end.guid = strconv.FormatUint(guid, 10)
	}
	if connection, found := utils.GetConnectionIdentifier(ctx); found {
		end.connection = strconv.FormatUint(connection, 10)
	}

	if in.protocolMessage != nil {
		if api := in.protocolMessage.GetApi(); api != nil {
			end.stateful = chainlib.GetStateful(in.protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS
			end.method = relayEndMethod(api.Name, relayEndNamedByURL(in.protocolMessage, end.apiInterface))
		}
		end.isBatch = chainqueries.IsBatchRequest(in.protocolMessage)
	} else {
		// The two refusals of a batch happen while it is parsed, before there is a message to ask.
		end.isBatch = errors.Is(in.err, chainlib.ErrBatchRequestSizeExceeded) || errors.Is(in.err, rpcInterfaceMessages.ErrJsonrpcBatchRefused)
	}

	dispatched := 0
	if in.processor != nil {
		end.selection = relayEndSelection(in.processor.GetSelection())
		end.stopReason = in.processor.GetStopReason()
		var dispatches []lavasession.Dispatch
		if usedProviders := in.processor.GetUsedProviders(); usedProviders != nil {
			dispatches = usedProviders.Dispatches()
			end.skipped = usedProviders.ConsistencySkips()
		}
		dispatched = len(dispatches)
		successes, nodeErrors, protocolErrors := in.processor.GetResultsDetail()
		end.attempts = relayEndAttemptsOf(dispatches, in.start, successes, nodeErrors, protocolErrors, requestRanOutOfRoad(end.stopReason))
	}

	served := in.err == nil && in.result != nil && in.result.Reply != nil
	var returned *relayEndAttempt
	if served {
		for i := range end.attempts {
			if end.attempts[i].reply == in.result.Reply {
				end.attempts[i].returnedToClient = true
				returned = &end.attempts[i]
				break
			}
		}
		end.servedBy = in.result.GetProvider()
		if servedFromCache(in.result) {
			end.servedBy = relayEndCachedUpstream
		}
	}
	end.hasReply = served
	end.status = in.result.GetStatusCode()

	switch {
	case errors.Is(in.err, errUnknownWriteOutcome):
		end.outcome = relayOutcomeWriteUnknown
	case in.err == nil && !served:
		end.outcome = relayOutcomeRouterError // neither an answer nor an error: the router lost the result
	case in.err == nil && (in.result.IsNodeError || (returned != nil && returned.outcome != attemptOutcomeOK)):
		end.outcome = relayOutcomeNodeError
	case in.err == nil:
		end.outcome = relayOutcomeOK
	case isRouterFault(in.err) || isRouterFault(in.cause):
		end.outcome = relayOutcomeRouterError
	case dispatched == 0:
		end.outcome = relayOutcomeRejectedBeforeDispatch
	default:
		end.outcome = relayOutcomeNoAnswer
	}

	switch end.outcome {
	case relayOutcomeOK:
	case relayOutcomeNodeError:
		if returned != nil {
			end.errorCode, end.errorText = returned.errorCode, returned.message
		}
	case relayOutcomeWriteUnknown:
		// What the listener sends, whether or not the router returned a result to carry it.
		end.status = http.StatusInternalServerError
		end.errorCode = relayEndWriteUnknownCode(in.cause, end.attempts, requestRanOutOfRoad(end.stopReason))
		end.errorText = relayEndText(errUnknownWriteOutcome.Error())
	default:
		cause := in.cause
		if cause == nil {
			cause = in.err
		}
		end.errorCode = relayEndErrorCode(cause, in.result, end.attempts)
		if cause != nil {
			end.errorText = relayEndText(cause.Error())
		}
	}
	return end
}

// relayEndAttemptsOf lists one attempt per dispatched session, in dispatch order, each paired with
// the result its upstream recorded and carrying its batch and when it went out, measured from start;
// an attempt with no result has not answered by the time the relay ends. Results without a dispatch
// (a cache hit) follow. Within one upstream, results pair in the header's order: protocol errors,
// node errors, successes.
func relayEndAttemptsOf(dispatched []lavasession.Dispatch, start time.Time, successes []common.RelayResult, nodeErrors, protocolErrors []relaycore.RelayError, ranOutOfRoad bool) relayEndAttempts {
	recorded := make([]relayEndAttempt, 0, len(successes)+len(nodeErrors)+len(protocolErrors))
	for i := range protocolErrors {
		recorded = append(recorded, relayEndProtocolErrorAttempt(protocolErrors[i], ranOutOfRoad))
	}
	for i := range nodeErrors {
		recorded = append(recorded, relayEndNodeErrorAttempt(nodeErrors[i]))
	}
	for i := range successes {
		recorded = append(recorded, relayEndAttempt{
			upstream: relayEndUpstream(&successes[i]),
			outcome:  attemptOutcomeOK,
			latency:  successes[i].AttemptLatency,
			reply:    successes[i].Reply,
		})
	}

	byUpstream := make(map[string][]int, len(recorded))
	for i := range recorded {
		byUpstream[recorded[i].upstream] = append(byUpstream[recorded[i].upstream], i)
	}
	used := make([]bool, len(recorded))
	attempts := make(relayEndAttempts, 0, max(len(dispatched), len(recorded)))
	for _, dispatch := range dispatched {
		var attempt relayEndAttempt
		if queue := byUpstream[dispatch.Provider]; len(queue) > 0 {
			byUpstream[dispatch.Provider] = queue[1:]
			used[queue[0]] = true
			attempt = recorded[queue[0]]
		} else {
			attempt = relayEndAttempt{upstream: dispatch.Provider, outcome: attemptOutcomeCancelled}
			if ranOutOfRoad {
				attempt.outcome = attemptOutcomeTimeout
			}
		}
		attempt.sent = true
		attempt.batch = dispatch.Batch
		attempt.sentAfter = max(dispatch.At.Sub(start), 0)
		attempt.staleFallback = dispatch.StaleFallback
		attempts = append(attempts, attempt)
	}
	for i := range recorded {
		if !used[i] {
			attempts = append(attempts, recorded[i])
		}
	}
	return attempts
}

// retryable is the router's verdict on the attempt's error, the registry's Retryable flag the
// narrative lines print: whether another upstream may succeed where this one failed. An error the
// registry did not classify takes UNKNOWN_ERROR's, as its error_code does.
func (a *relayEndAttempt) retryable() bool {
	if a.lavaError == nil {
		return common.LavaErrorUnknown.Retryable
	}
	return a.lavaError.Retryable
}

func relayEndNodeErrorAttempt(nodeError relaycore.RelayError) relayEndAttempt {
	attempt := relayEndAttempt{outcome: attemptOutcomeNodeError, lavaError: nodeError.LavaError}
	if nodeError.Response != nil {
		result := &nodeError.Response.RelayResult
		attempt.upstream = relayEndUpstream(result)
		attempt.latency = result.AttemptLatency
		attempt.reply = result.Reply
		if result.IsRateLimited {
			attempt.outcome = attemptOutcomeRateLimited
		}
	} else {
		attempt.upstream = nodeError.ProviderInfo.ProviderAddress
	}
	if nodeError.LavaError.IsRateLimited() {
		attempt.outcome = attemptOutcomeRateLimited
	}
	attempt.errorCode = relayEndCode(nodeError.LavaError)
	if nodeError.Err != nil {
		attempt.message = relayEndText(nodeError.Err.Error())
	}
	return attempt
}

func relayEndProtocolErrorAttempt(protocolError relaycore.RelayError, ranOutOfRoad bool) relayEndAttempt {
	attempt := relayEndAttempt{upstream: protocolError.ProviderInfo.ProviderAddress, lavaError: protocolError.LavaError, transport: true}
	if protocolError.Response != nil {
		attempt.latency = protocolError.Response.RelayResult.AttemptLatency
	}
	err := protocolError.GetError()
	switch {
	case protocolError.LavaError.IsRateLimited() || errors.Is(err, common.StatusCodeError429):
		attempt.outcome = attemptOutcomeRateLimited
	case errors.Is(err, context.Canceled) || protocolError.LavaError == common.LavaErrorContextCanceled:
		// Cancelled because the budget ran out is a timeout; otherwise the router stopped waiting.
		attempt.outcome = attemptOutcomeCancelled
		if ranOutOfRoad {
			attempt.outcome = attemptOutcomeTimeout
		}
	case errors.Is(err, context.DeadlineExceeded) || protocolError.LavaError == common.LavaErrorContextDeadline ||
		protocolError.LavaError == common.LavaErrorConnectionTimeout || protocolError.LavaError == common.LavaErrorNoResponseTimeout:
		attempt.outcome = attemptOutcomeTimeout
	default:
		attempt.outcome = attemptOutcomeTransportError
	}
	attempt.errorCode = relayEndCode(protocolError.LavaError)
	if err != nil {
		attempt.message = relayEndText(err.Error())
	}
	return attempt
}

// relayEndErrorCode names, from the error registry, why a relay that did not end on an answer
// ended. The first rule that applies wins; docs/RELAY-END-EVENT.md lists them in this order.
func relayEndErrorCode(cause error, result *common.RelayResult, attempts relayEndAttempts) string {
	// First: a cross-validation staffing refusal also wraps PairingListEmptyError.
	if result != nil {
		switch result.CrossValidationFailureReason {
		case common.CrossValidationReasonInsufficientCapacity, common.CrossValidationReasonInsufficientGroups:
			return common.LavaErrorInsufficientProviders.Name
		case common.CrossValidationReasonNoAgreement, common.CrossValidationReasonDiversityUnmet, common.CrossValidationReasonGroupQuorumUnmet:
			return common.LavaErrorHashConsensusError.Name
		}
	}
	switch {
	case cause == nil:
	case isRouterFault(cause):
		return common.LavaErrorRelayProcessingFailed.Name
	case errors.Is(cause, lavasession.PairingListEmptyError):
		return common.LavaErrorNoProviders.Name
	case errors.Is(cause, lavasession.AllProviderEndpointsDisabledError):
		return common.LavaErrorAllEndpointsDisabled.Name
	case errors.Is(cause, lavasession.SelectedProviderUnavailableError), errors.Is(cause, lavasession.SelectedProviderAlreadyFailedError):
		return common.LavaErrorProviderUnavailable.Name
	case errors.Is(cause, chainlib.ErrBatchRequestSizeExceeded):
		return common.LavaErrorBatchSizeExceeded.Name
	}
	if connectionError := common.DetectConnectionError(cause); connectionError != nil {
		return connectionError.Name
	}
	for i := len(attempts) - 1; i >= 0; i-- {
		if attempts[i].errorCode != "" {
			return attempts[i].errorCode
		}
	}
	return ""
}

// relayEndWriteUnknownCode names why a write's outcome is unknown: the budget ran out with an
// upstream still silent, or an upstream's connection died after the request went out.
func relayEndWriteUnknownCode(cause error, attempts relayEndAttempts, ranOutOfRoad bool) string {
	if ranOutOfRoad {
		return common.LavaErrorContextDeadline.Name
	}
	// Only a transport error can leave a write unsettled; a node's reply settles its upstream, as in
	// writeOutcomeIsUnknown.
	for i := range attempts {
		if attempts[i].transport && attempts[i].lavaError != nil && attempts[i].lavaError.MayHaveReachedNode {
			return attempts[i].errorCode
		}
	}
	if connectionError := common.DetectConnectionError(cause); connectionError != nil {
		return connectionError.Name
	}
	return ""
}

func relayEndCode(lavaError *common.LavaError) string {
	if lavaError == nil {
		return common.LavaErrorUnknown.Name
	}
	return lavaError.Name
}

func relayEndUpstream(result *common.RelayResult) string {
	if result.ProviderInfo.ProviderAddress == "" && servedFromCache(result) {
		return relayEndCachedUpstream
	}
	return result.ProviderInfo.ProviderAddress
}

func servedFromCache(result *common.RelayResult) bool {
	tier := result.CacheLookup.ServedTier
	return tier == common.CacheTierPrimary || tier == common.CacheTierSecondary
}

// relayEndMethodLimit caps method: an unlisted name is whatever the client sent, and a batch joins
// every member's.
const relayEndMethodLimit = 256

// relayEndMethod is the parsed api name without a leading slash: the fleet's log collector rewrites
// a value that starts with "/" as /REDACTED, which would erase every REST path. A listed name is the
// spec's. An unlisted one is what the client sent. When the request named its api by URL, that is
// the client's path and query, which may carry a credential the "Default-" prefix would carry past
// the collector, so it is withheld.
func relayEndMethod(apiName string, namedByURL bool) string {
	members := strings.Split(apiName, chainlib.SEP)
	for i, member := range members {
		if namedByURL && strings.HasPrefix(member, chainlib.DefaultApiName) {
			members[i] = chainlib.DefaultApiName + "REDACTED"
		}
	}
	return utils.FormatLongString(strings.TrimLeft(strings.Join(members, chainlib.SEP), "/"), relayEndMethodLimit)
}

// relayEndNamedByURL reports whether the request named its api by URL. JSON-RPC and GraphQL name it
// in the body and gRPC names a service method. Tendermint does either: its parser reads a request
// with a body as JSON-RPC, over POST or WebSocket, and one without as a URI. REST, and any interface
// not listed here, counts as a URL, so an interface added later withholds rather than leaks.
func relayEndNamedByURL(protocolMessage chainlib.ProtocolMessage, apiInterface string) bool {
	switch apiInterface {
	case spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceGraphQL, spectypes.APIInterfaceGrpc:
		return false
	case spectypes.APIInterfaceTendermintRPC:
		return len(protocolMessage.RelayPrivateData().GetData()) == 0
	}
	return true
}

func relayEndSelection(selection relaycore.Selection) string {
	switch selection {
	case relaycore.Stateless:
		return "stateless"
	case relaycore.Stateful:
		return "stateful"
	case relaycore.CrossValidation:
		return "cross_validation"
	}
	return ""
}

func relayEndText(text string) string {
	return utils.FormatLongString(utils.RedactSecrets(text), relayEndTextLimit)
}
