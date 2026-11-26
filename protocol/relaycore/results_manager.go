package relaycore

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	common "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// transportFromProtocolMessage derives the TransportType from the protocol message's API collection.
func transportFromProtocolMessage(pm chainlib.ProtocolMessage) common.TransportType {
	return common.ApiInterfaceToTransport(apiInterfaceOf(pm))
}

// apiInterfaceOf is the protocol message's api interface, "" when it has no collection.
func apiInterfaceOf(pm chainlib.ProtocolMessage) string {
	if collection := pm.GetApiCollection(); collection != nil {
		return collection.CollectionData.ApiInterface
	}
	return ""
}

// isWrite reports whether protocolMessage is a stateful request: a write, relayed to every provider
// at once and never again.
func isWrite(protocolMessage chainlib.ProtocolMessage) bool {
	return protocolMessage != nil && chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS
}

// upstreamAnswerBody is the body an upstream answered a failed attempt with: the one the attempt's
// error carries (routersession.HTTPStatusError, a 429 or 5xx the sender failed the attempt on), else
// the result's own reply; nil when the attempt got no answer.
func upstreamAnswerBody(response *RelayResponse) []byte {
	var httpErr *routersession.HTTPStatusError
	if errors.As(response.Err, &httpErr) && len(httpErr.Body) > 0 {
		return httpErr.Body
	}
	if reply := response.RelayResult.Reply; reply != nil {
		return reply.Data
	}
	return nil
}

// replyStatusClassifier is a request message whose interface answers its errors inside a 2xx and
// can say which HTTP status the error registry should classify a reply under
// (rpcInterfaceMessages.GraphQLMessage). It stands in for the reply's status the way the body's
// error code does for JSON-RPC.
type replyStatusClassifier interface {
	// ReplyVerdict reads the reply once: whether it is an error, its message, the status it stands
	// for, and the registry entry the interface's own rows file it under, or nil when the shared
	// rows decide (GraphQL: a write, MAG-5121). Each reading decodes the whole reply.
	ReplyVerdict(data []byte, httpStatusCode int) (hasError bool, errorMessage string, classificationStatus int, classified *common.RouterError)
}

// chainRowVerdictReader is a request whose chain's list files some node errors under a verdict of its
// own (rpcInterfaceMessages.GrpcMessage, a copy of the chain's write: MAG-5186). The sender applied
// the same verdict to the attempt, so the record and the endpoint's health agree.
type chainRowVerdictReader interface {
	ChainRowVerdict(data []byte) *common.RouterError
}

type ResultsManager interface {
	String() string
	NodeResults() []common.RelayResult
	RequiredResults(requiredSuccesses int, selection Selection) bool
	ProtocolErrors() uint64
	HasResults() bool
	GetResults() (success int, nodeErrors int, specialNodeErrors int, protocolErrors int)
	GetResultsData() (successResults []common.RelayResult, nodeErrors []common.RelayResult, protocolErrors []RelayError)
	GetResultsDetail() (successResults []common.RelayResult, nodeErrors []RelayError, protocolErrors []RelayError)
	SetResponse(response *RelayResponse, protocolMessage chainlib.ProtocolMessage) (nodeError error)
	GetBestNodeErrorMessageForUser() RelayError
	GetBestProtocolErrorMessageForUser() RelayError
	NodeErrors() (ret []common.RelayResult)
}

type ResultsManagerInst struct {
	nodeResponseErrors     RelayErrors
	protocolResponseErrors RelayErrors
	successResults         []common.RelayResult
	lock                   sync.RWMutex
	guid                   uint64
	chainID                string
}

func NewResultsManager(guid uint64, chainID string) ResultsManager {
	return &ResultsManagerInst{
		guid:                   guid,
		chainID:                chainID,
		nodeResponseErrors:     RelayErrors{RelayErrors: []RelayError{}},
		protocolResponseErrors: RelayErrors{RelayErrors: []RelayError{}, OnFailureMergeAll: true},
	}
}

func (rp *ResultsManagerInst) setErrorResponse(response *RelayResponse, protocolMessage chainlib.ProtocolMessage) {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	// Protocol errors are plain Go errors (nothing in this fork carries an sdkerrors ABCI code any
	// more), so they are classified by connection shape and message text. TransportGRPC is the
	// historical choice for the message-based fallback and is kept so the verdicts do not move.
	//
	// One exception, for what that text cannot name: a REST attempt that failed on the upstream's
	// own status (relayInnerDirect's status branch: a 5xx not read as the node's reply). Its text is
	// "HTTP 507 Insufficient Storage", and the text rows name only the statuses httpStatusRows lists,
	// so a 505 to 599 was UNKNOWN_ERROR here while the sender and the health gate filed it as
	// NODE_UNLISTED_SERVER_ERROR. Such an error is read by its status on the REST table, which is
	// what the log line, the error metric and relay_end report (MAG-5490). Only an UNKNOWN_ERROR
	// verdict is re-read, so every status the text rows name keeps its name.
	var classified *common.RouterError
	if response.Err != nil {
		classified = common.ClassifyError(common.DetectConnectionError(response.Err), common.ChainFamilyUnknown, common.TransportGRPC, 0, response.Err.Error())
		if status, ok := restUpstreamStatus(response.Err, protocolMessage); ok && classified == common.RouterErrorUnknown {
			classified = common.ClassifyError(nil, common.GetChainFamilyOrDefault(rp.chainID), common.TransportREST, status, "")
		}
	}
	// A write's copy the sender failed on an upstream's status still carries the upstream's answer,
	// and that answer is read for the slot-already-used signatures (PRD §5.2, MAG-5593): a gateway's
	// 503 or a 429 can wrap the node's own "nonce too low", which says this very transaction may have
	// landed. The code is the body's JSON-RPC error code when it has one; the text rows read the body
	// wherever the words sit.
	if isWrite(protocolMessage) {
		if body := upstreamAnswerBody(response); len(body) > 0 {
			response.RelayResult.IsSlotAlreadyUsed = common.IsSlotAlreadyUsed(common.GetChainFamilyOrDefault(rp.chainID), apiInterfaceOf(protocolMessage), common.ExtractJSONRPCErrorCode(body), string(body))
		}
	}
	// This is a protocol-layer error (session not found, consumer blocked, etc.),
	// not a node-side error. Pass "" as chainErrorMessage so the chain_error_message
	// structured field doesn't get polluted with sdkerrors strings. The full
	// error still surfaces via the err argument.
	common.LogCodedError("could not send relay to provider", response.Err, classified, rp.chainID, 0, "",
		utils.Attribute{Key: "GUID", Value: rp.guid},
		utils.Attribute{Key: "provider", Value: response.RelayResult.ProviderInfo.ProviderAddress},
		utils.Attribute{Key: "statusCode", Value: response.RelayResult.StatusCode},
		utils.Attribute{Key: "providerTrailer", Value: response.RelayResult.ProviderTrailer},
	)
	rp.protocolResponseErrors.AddError(RelayError{Err: response.Err, ProviderInfo: response.RelayResult.ProviderInfo, Response: response, RouterError: classified})
}

// restUpstreamStatus is the status a REST upstream answered with, when err is a REST attempt's
// failure on that status (routersession.HTTPStatusError). GraphQL, which classifies by the REST
// table too, is left out: a write's failure on its status has the same gap, outside MAG-5490.
func restUpstreamStatus(err error, protocolMessage chainlib.ProtocolMessage) (int, bool) {
	if protocolMessage == nil {
		return 0, false
	}
	if collection := protocolMessage.GetApiCollection(); collection == nil || collection.CollectionData.ApiInterface != spectypes.APIInterfaceRest {
		return 0, false
	}
	var statusErr *routersession.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr == nil {
		return 0, false
	}
	return statusErr.StatusCode, true
}

// only when locked
func (rp *ResultsManagerInst) nodeResultsInner() []common.RelayResult {
	// start with results and add to them node results
	nodeResults := rp.successResults
	nodeResults = append(nodeResults, rp.NodeErrors()...)
	return nodeResults
}

// only when locked
func (rp *ResultsManagerInst) NodeErrors() (ret []common.RelayResult) {
	for _, relayError := range rp.nodeResponseErrors.RelayErrors {
		ret = append(ret, relayError.Response.RelayResult)
	}
	return ret
}

// returns an error if and only if it was a parsed node error
func (rp *ResultsManagerInst) setValidResponse(response *RelayResponse, protocolMessage chainlib.ProtocolMessage) (nodeError error) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	// future relay requests and data reliability requests need to ask for the same specific block height to get consensus on the reply
	// we do not modify the chain message data on the consumer, only it's requested block, so we let the provider know it can't put any block height it wants by setting a specific block height
	reqBlock, _ := protocolMessage.RequestedBlock()
	if reqBlock == spectypes.LATEST_BLOCK {
		// TODO: when we turn on dataReliability on latest call UpdateLatest, until then we turn it off always
		// modifiedOnLatestReq := rp.chainMessage.UpdateLatestBlockInMessage(response.RelayResult.Reply.LatestBlock, false)
		// if !modifiedOnLatestReq {
		response.RelayResult.Finalized = false // shut down data reliability
		// }
	}

	if response.RelayResult.Reply == nil {
		utils.FormatError("got to setValidResponse with nil Reply",
			response.Err,
			utils.LogAttr("ProviderInfo", response.RelayResult.ProviderInfo),
			utils.LogAttr("StatusCode", response.RelayResult.StatusCode),
			utils.LogAttr("Finalized", response.RelayResult.Finalized),
			utils.LogAttr("CrossValidation", response.RelayResult.CrossValidation),
		)
		return nil
	}

	// on subscribe results, we just append to successful results instead of parsing results because we already have a validation.
	if chainlib.IsFunctionTagOfType(protocolMessage, spectypes.FUNCTION_TAG_SUBSCRIBE) {
		rp.successResults = append(rp.successResults, response.RelayResult)
		return nil
	}

	// check response error
	foundError, errorMessage := protocolMessage.CheckResponseError(response.RelayResult.Reply.Data, response.RelayResult.StatusCode)
	if foundError {
		// this is a node error, meaning we still didn't get a good response.
		// we may choose to wait until there will be a response or timeout happens
		// if we decide to wait and timeout happens we will take the majority of response messages
		//
		// A CheckResponseError implementation may report hasError WITHOUT a message:
		// GrpcMessage decides purely from the status code and always returns "". Classifying on
		// the empty string reduces the transport's registry to its code matchers and leaves every
		// message-based row unreachable — a RESOURCE_EXHAUSTED whose body says "rate limit" would
		// be scored as an availability failure instead of exempted as healthy-but-busy — and it
		// also logs the node error below with an empty error string. Fall back to the node's own
		// reply body, which is where the evidence actually is, and only then to a synthetic
		// description so `err` is never blank.
		if errorMessage == "" {
			errorMessage = string(response.RelayResult.Reply.Data)
		}
		if errorMessage == "" {
			errorMessage = fmt.Sprintf("node error with no message (status %d)", response.RelayResult.StatusCode)
		}
		err := fmt.Errorf("%s", errorMessage)
		// Log node error payload and headers for troubleshooting
		// also log the original request payload and request headers if available
		reqPayload := ""
		var reqHeaders interface{}
		if response.RelayResult.Request != nil && response.RelayResult.Request.RelayData != nil {
			reqPayload = string(response.RelayResult.Request.RelayData.Data)
			reqHeaders = common.RedactMetadata(response.RelayResult.Request.RelayData.Metadata)
		}
		// Get request URL safely
		requestUrl := ""
		if protocolMessage.RelayPrivateData() != nil {
			requestUrl = protocolMessage.RelayPrivateData().ApiUrl
		}

		// Derive transport from the protocol message's API collection
		transport := transportFromProtocolMessage(protocolMessage)
		// Use the HTTP status code for classification; for JSON-RPC, the HTTP status is 200
		// even on errors, so extract the actual JSON-RPC error code from the response body.
		errorCode := response.RelayResult.StatusCode
		jsonRPCErrorCode := 0
		if transport == common.TransportJsonRPC && response.RelayResult.Reply.Data != nil {
			if code := common.ExtractJSONRPCErrorCode(response.RelayResult.Reply.Data); code != 0 {
				errorCode, jsonRPCErrorCode = code, code
			}
		}
		// A gRPC node error's status code is the status the node sent (handleGRPCError). Outside
		// 1..16 it is the router's own stand-in for a failure that carried no status.
		grpcStatus := 0
		if transport == common.TransportGRPC && errorCode >= int(codes.Canceled) && errorCode <= int(codes.Unauthenticated) {
			grpcStatus = errorCode
		}
		// GraphQL answers its node faults inside a 200 as well, so the status says nothing there
		// either. The message's own rule maps the error's code onto the status the registry's
		// rows know (RESOURCE_EXHAUSTED a 429, INTERNAL_SERVER_ERROR a 500, a caller's error on a
		// mutation a 400), which is how the sender already classified this reply for its retry
		// decision (sendGraphQLRelay). Classified by the 200 instead, every GraphQL node fault was
		// UNKNOWN_ERROR, logged at DEBUG and never blamed on the endpoint, while the same fault
		// behind a 503 was an ERROR line with its own name (MAG-4375).
		var ownClassification *common.RouterError
		if statusClassifier, ok := protocolMessage.GetRPCMessage().(replyStatusClassifier); ok {
			_, _, errorCode, ownClassification = statusClassifier.ReplyVerdict(response.RelayResult.Reply.Data, response.RelayResult.StatusCode)
		}
		// Resolve chain family so Tier-2 chain-specific matchers win over
		// Tier-1 generic ones. Without this, chain-native codes like Solana's
		// -32004 (block-not-yet-propagated, retryable) would get classified
		// through the JSON-RPC generic bucket as NODE_METHOD_NOT_SUPPORTED
		// (zero CU, no retry) — a silent retry-starvation bug.
		chainFamily := common.GetChainFamilyOrDefault(rp.chainID)
		// err is the node's reply text, so the transport-only rows stay out of it (MAG-4242).
		nodeClassified := ownClassification
		if rowReader, ok := protocolMessage.GetRPCMessage().(chainRowVerdictReader); ok && nodeClassified == nil {
			nodeClassified = rowReader.ChainRowVerdict(response.RelayResult.Reply.Data)
		}
		if nodeClassified == nil {
			nodeClassified = common.ClassifyError(common.DetectConnectionErrorInReplyText(errorMessage), chainFamily, transport, errorCode, err.Error())
		}
		// A write's copy whose reply says the slot is already used may be this very transaction landed
		// (PRD §5.2, MAG-5593). Read beside the row, not from it: whatever the row says, the copy is
		// never refused and the write is not sent again (relaypolicy.Decide). The client still gets the
		// node's reply as it came: a match never by itself makes the router speak (PRD §5.2).
		if isWrite(protocolMessage) {
			response.RelayResult.IsSlotAlreadyUsed = common.IsSlotAlreadyUsed(chainFamily, apiInterfaceOf(protocolMessage), errorCode, err.Error())
		}
		// ERROR only when the endpoint is to blame. A not-found, a pruned height, a rejected
		// transaction is the node answering the question it was asked, and on REST those are
		// routine — see LogCodedNodeAnswer. The metric fires either way.
		logNodeError := common.LogCodedNodeAnswer
		if nodeClassified.EndpointAtFault() {
			logNodeError = func(description string, err error, routerError *common.RouterError, chainID string, chainErrorCode int, chainErrorMessage string, attributes ...utils.Attribute) {
				common.LogCodedError(description, err, routerError, chainID, chainErrorCode, chainErrorMessage, attributes...)
			}
		}
		logNodeError("received node error reply from provider", err, nodeClassified, rp.chainID, errorCode, err.Error(),
			utils.LogAttr("GUID", rp.guid),
			utils.LogAttr("provider", response.RelayResult.ProviderInfo),
			utils.LogAttr("statusCode", response.RelayResult.StatusCode),
			utils.LogAttr("api", protocolMessage.GetApi().Name),
			utils.LogAttr("requestUrl", requestUrl),
			utils.LogAttr("payload", parser.CapStringLen(string(response.RelayResult.Reply.Data))),
			utils.LogAttr("headers", common.RedactMetadata(response.RelayResult.Reply.Metadata)),
			utils.LogAttr("requestPayload", parser.CapStringLen(reqPayload)),
			utils.LogAttr("requestHeaders", reqHeaders),
		)
		rp.nodeResponseErrors.AddError(RelayError{Err: err, ProviderInfo: response.RelayResult.ProviderInfo, Response: response, RouterError: nodeClassified, JSONRPCErrorCode: jsonRPCErrorCode, GRPCStatus: grpcStatus})
		return err
	}
	rp.successResults = append(rp.successResults, response.RelayResult)
	return nil
}

func (rm *ResultsManagerInst) GetResults() (success int, nodeErrors int, specialNodeErrors int, protocolErrors int) {
	rm.lock.RLock()
	defer rm.lock.RUnlock()

	specialErrorPatterns := []string{"The node does not track the shard ID"}
	for _, err := range rm.nodeResponseErrors.RelayErrors {
		if err.Response != nil && err.Response.RelayResult.Reply != nil && err.Response.RelayResult.Reply.Data != nil {
			for _, specialErrorPattern := range specialErrorPatterns {
				if strings.Contains(string(err.Response.RelayResult.Reply.Data), specialErrorPattern) {
					specialNodeErrors++
				}
			}
		}
	}
	nodeErrors = len(rm.nodeResponseErrors.RelayErrors) - specialNodeErrors
	protocolErrors = len(rm.protocolResponseErrors.RelayErrors)
	success = len(rm.successResults)
	return success, nodeErrors, specialNodeErrors, protocolErrors
}

func (rm *ResultsManagerInst) String() string {
	results, nodeErrors, specialNodeErrors, protocolErrors := rm.GetResults()
	return fmt.Sprintf("resultsManager {success %d, nodeErrors:%d, specialNodeErrors:%d, protocolErrors:%d}", results, nodeErrors, specialNodeErrors, protocolErrors)
}

// this function returns all results that came from a node, meaning success, and node errors
func (rp *ResultsManagerInst) NodeResults() []common.RelayResult {
	if rp == nil {
		return nil
	}
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.nodeResultsInner()
}

func (rp *ResultsManagerInst) ProtocolErrors() uint64 {
	if rp == nil {
		return 0
	}
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return uint64(len(rp.protocolResponseErrors.RelayErrors))
}

func (rp *ResultsManagerInst) RequiredResults(requiredSuccesses int, selection Selection) bool {
	if rp == nil {
		return false
	}
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	resultsCount := len(rp.successResults)
	if resultsCount >= requiredSuccesses {
		// we have enough successes, we can return
		utils.FormatDebug("Reached RequiredResults", utils.LogAttr("resultsCount", resultsCount), utils.LogAttr("requiredSuccesses", requiredSuccesses), utils.LogAttr("GUID", rp.guid))
		return true
	}
	// Only count successful results for cross-validation
	return false
}

// this function defines if we should use the manager to return the result (meaning it has some insight and responses) or just return to the user
func (rp *ResultsManagerInst) HasResults() bool {
	if rp == nil {
		return false
	}
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	resultsCount := len(rp.successResults)
	nodeErrors := len(rp.nodeResponseErrors.RelayErrors)
	protocolErrors := len(rp.protocolResponseErrors.RelayErrors)
	return resultsCount+nodeErrors+protocolErrors > 0
}

func (rp *ResultsManagerInst) SetResponse(response *RelayResponse, protocolMessage chainlib.ProtocolMessage) (nodeError error) {
	if response == nil {
		return nil
	}
	if response.Err != nil {
		rp.setErrorResponse(response, protocolMessage)
	} else {
		return rp.setValidResponse(response, protocolMessage)
	}
	return nil //nolint:nilerr // a protocol error is recorded on the processor for the policy to weigh; it is not a failure of this call
}

func (rp *ResultsManagerInst) GetResultsData() (successResults []common.RelayResult, nodeErrors []common.RelayResult, protocolErrors []RelayError) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.successResults, rp.NodeErrors(), rp.protocolResponseErrors.RelayErrors
}

// GetResultsDetail is GetResultsData with each node error kept as its RelayError, so the error and
// its classification come along with the result.
func (rp *ResultsManagerInst) GetResultsDetail() (successResults []common.RelayResult, nodeErrors []RelayError, protocolErrors []RelayError) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.successResults, rp.nodeResponseErrors.RelayErrors, rp.protocolResponseErrors.RelayErrors
}

func (rp *ResultsManagerInst) GetBestNodeErrorMessageForUser() RelayError {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.nodeResponseErrors.GetBestErrorMessageForUser()
}

func (rp *ResultsManagerInst) GetBestProtocolErrorMessageForUser() RelayError {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return rp.protocolResponseErrors.GetBestErrorMessageForUser()
}
