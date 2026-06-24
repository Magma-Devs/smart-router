package endpointstate

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// EndpointPoller implements chaintracker.ChainFetcher for direct RPC endpoints.
// It enables per-endpoint ChainTracker to continuously poll block data.
type EndpointPoller struct {
	endpoint         *lavasession.Endpoint
	directConnection lavasession.DirectRPCConnection
	chainParser      chainlib.ChainParser
	chainID          string
	apiInterface     string
	latestBlock      int64

	// Metadata for requests
	endpointURL string

	// onPollObservation, if set, is invoked after every FetchLatestBlockNum round-trip
	// (success or failure) with the parsed block, the transport round-trip latency, the
	// poll error (nil on success), and the completion time. The EndpointMonitor sets it
	// to record the per-endpoint observation (Topic A). Nil in standalone/test use.
	onPollObservation func(block int64, latency time.Duration, err error, at time.Time)
}

// NewEndpointPoller creates a new ChainFetcher for a direct RPC endpoint.
func NewEndpointPoller(
	endpoint *lavasession.Endpoint,
	directConnection lavasession.DirectRPCConnection,
	chainParser chainlib.ChainParser,
	chainID string,
	apiInterface string,
) *EndpointPoller {
	return &EndpointPoller{
		endpoint:         endpoint,
		directConnection: directConnection,
		chainParser:      chainParser,
		chainID:          chainID,
		apiInterface:     apiInterface,
		endpointURL:      endpoint.NetworkAddress,
	}
}

// blockNumRequestBody returns the request body for the GET_BLOCKNUM poll and whether the
// (apiInterface, functionTemplate) pair is pollable at all.
//
// For gRPC the spec's api_name carries the method and the function_template is only the request
// PAYLOAD — which is legitimately empty for a no-argument call (e.g. cosmos
// Service/GetLatestBlock). So an empty gRPC template means "empty body", not "undefined method",
// and we send "{}" (the canonical empty JSON message the gRPC codec marshals to an empty proto).
// This mirrors the relay path, which already serves gRPC GetLatestBlock with an empty body.
//
// REST (needs a URL path) and Tendermint (needs a method) genuinely cannot poll without a
// template, so an empty one there is a real spec gap and stays a hard error — otherwise the
// per-endpoint ChainTracker for a misconfigured chain would silently poll garbage.
func blockNumRequestBody(apiInterface, functionTemplate string) (body []byte, ok bool) {
	if functionTemplate != "" {
		return []byte(functionTemplate), true
	}
	if apiInterface == spectypes.APIInterfaceGrpc {
		return []byte("{}"), true
	}
	return nil, false
}

// hydrateGrpcChainMessage attaches the gRPC method descriptor (resolved and cached by the
// DirectRPCConnection during the send) to a freshly-crafted chain message so its BINARY-protobuf
// response can be decoded. No-op for non-gRPC interfaces and when the connection exposes no descriptor.
// MUST be called after a successful send, when the descriptor is in the connection's cache. Both
// spec-driven poll paths (latest block AND block-hash-by-number) parse gRPC responses, so both call it.
func (ecf *EndpointPoller) hydrateGrpcChainMessage(chainMessage chainlib.ChainMessageForSend, apiName string) error {
	if ecf.apiInterface != spectypes.APIInterfaceGrpc {
		return nil
	}
	provider, ok := ecf.directConnection.(lavasession.GRPCDescriptorProvider)
	if !ok {
		return nil
	}
	methodDesc := provider.GetCachedMethodDescriptor(apiName)
	if methodDesc == nil {
		return nil
	}
	return chainlib.HydrateGrpcResponseParsing(chainMessage, methodDesc)
}

// FetchLatestBlockNum fetches the latest block number from the endpoint.
// Uses spec-driven parsing to support any chain type (EVM, Tendermint, REST, etc.).
func (ecf *EndpointPoller) FetchLatestBlockNum(ctx context.Context) (blockNum int64, err error) {
	// Record a per-endpoint observation for this poll on every return path (Topic A).
	// pollLatency captures only the transport round-trip (set around sendRawRequest);
	// the observation is success iff err == nil && block > 0 (see recordPollObservation).
	// Routed through ObserveLatestBlockPoll (nil-safe) so the poll and SVM paths share a
	// single recording chokepoint.
	var pollLatency time.Duration
	defer func() {
		ecf.ObserveLatestBlockPoll(blockNum, pollLatency, err)
	}()

	parsing, apiCollection, ok := ecf.chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	tagName := spectypes.FUNCTION_TAG_GET_BLOCKNUM.String()
	if !ok {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatError(tagName+" tag function not found", nil,
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("apiInterface", ecf.apiInterface),
		)
	}

	collectionData := apiCollection.CollectionData

	// Build the request body from the function template.
	requestData, ok := blockNumRequestBody(ecf.apiInterface, parsing.FunctionTemplate)
	if !ok {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatError(tagName+" missing function template", nil,
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("apiInterface", ecf.apiInterface),
		)
	}

	// Send request via direct RPC connection (measure the transport round-trip)
	reqStart := time.Now()
	responseData, err := ecf.sendRawRequest(ctx, requestData, collectionData.Type, parsing.ApiName)
	pollLatency = time.Since(reqStart)
	if err != nil {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatDebug(tagName+" failed sending request",
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("apiInterface", ecf.apiInterface),
			utils.LogAttr("endpoint", ecf.endpointURL),
			utils.LogAttr("error", err),
		)
	}

	// Craft chain message for response parsing (needed for FormatResponseForParsing)
	craftData := &chainlib.CraftData{
		Path:           parsing.ApiName,
		Data:           requestData,
		ConnectionType: collectionData.Type,
	}
	chainMessage, err := chainlib.CraftChainMessage(parsing, collectionData.Type, ecf.chainParser, craftData, ecf.chainFetcherMetadata())
	if err != nil {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatError(tagName+" failed creating chainMessage for parsing", err,
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("apiInterface", ecf.apiInterface),
		)
	}

	// gRPC responses are binary protobuf; the crafted message has no method descriptor because
	// reflection runs INSIDE the DirectRPCConnection during the send (which already succeeded above),
	// not in CraftChainMessage. Wire the connection's just-cached descriptor in so the response can be
	// decoded — otherwise FormatResponseForParsing fails "does not have a methodDescriptor set in
	// grpcMessage" and the per-endpoint gRPC ChainTracker never completes.
	if err := ecf.hydrateGrpcChainMessage(chainMessage, parsing.ApiName); err != nil {
		return spectypes.NOT_APPLICABLE, err
	}

	// Parse the response using spec-driven rules
	parserInput, err := chainlib.FormatResponseForParsing(&pairingtypes.RelayReply{Data: responseData}, chainMessage)
	if err != nil {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatDebug(tagName+" failed formatResponseForParsing",
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("endpoint", ecf.endpointURL),
			utils.LogAttr("method", parsing.ApiName),
			utils.LogAttr("response", parser.CapStringLen(string(responseData))),
			utils.LogAttr("error", err),
		)
	}

	parsedInput := parser.ParseBlockFromReply(parserInput, parsing.ResultParsing, parsing.Parsers)
	blockNum = parsedInput.GetBlock()
	if blockNum == spectypes.NOT_APPLICABLE {
		return spectypes.NOT_APPLICABLE, utils.LavaFormatDebug(tagName+" failed to parse response",
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("endpoint", ecf.endpointURL),
			utils.LogAttr("method", parsing.ApiName),
			utils.LogAttr("response", parser.CapStringLen(string(responseData))),
		)
	}

	atomic.StoreInt64(&ecf.latestBlock, blockNum)
	return blockNum, nil
}

// FetchBlockHashByNum fetches the block hash for a given block number.
// Used by ChainTracker for fork detection.
//
// For Solana-family chains, if the endpoint returns error code -32004
// ("Block not available for slot X"), this method retries with previous slot
// numbers (blockNum-1, blockNum-2, ...) up to maxBlockNotAvailableRetries times.
// This handles both propagation delays (the latest slot data hasn't reached the
// node yet) and skipped slots (Solana occasionally produces no block for a slot).
func (ecf *EndpointPoller) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	parsing, apiCollection, ok := ecf.chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM)
	tagName := spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM.String()
	if !ok {
		return "", utils.LavaFormatError(tagName+" tag function not found", nil,
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("apiInterface", ecf.apiInterface),
		)
	}

	collectionData := apiCollection.CollectionData

	if parsing.FunctionTemplate == "" {
		return "", utils.LavaFormatError(tagName+" missing function template", nil,
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("apiInterface", ecf.apiInterface),
		)
	}

	if blockNum < 0 {
		return "", utils.LavaFormatError(tagName+" invalid negative block number", nil,
			utils.LogAttr("blockNum", blockNum),
			utils.LogAttr("chainID", ecf.chainID),
		)
	}

	if !common.IsSolanaFamily(ecf.chainID) {
		hash, _, err := ecf.fetchSingleBlockHash(ctx, blockNum, parsing, collectionData.Type, tagName)
		return hash, err
	}

	fetchFn := func(fCtx context.Context, block int64) (string, []byte, error) {
		return ecf.fetchSingleBlockHash(fCtx, block, parsing, collectionData.Type, tagName)
	}
	hash, fetchedBlock, err := chainlib.FetchBlockHashWithSolanaRetry(ctx, blockNum, chainlib.SameSlotRetryDelay, fetchFn)
	if err != nil {
		return "", utils.LavaFormatError(tagName+" all block-not-available retries exhausted", err,
			utils.LogAttr("originalBlock", blockNum),
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("endpoint", ecf.endpointURL),
		)
	}
	if fetchedBlock != blockNum {
		utils.LavaFormatWarning("Chain Tracker fetched previous slot after block-not-available",
			nil,
			utils.LogAttr("originalBlock", blockNum),
			utils.LogAttr("fetchedBlock", fetchedBlock),
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("endpoint", ecf.endpointURL),
		)
	}
	return hash, nil
}

// fetchSingleBlockHash fetches the block hash for a single block number.
// Returns the hash, the raw response data (for error inspection), and any error.
func (ecf *EndpointPoller) fetchSingleBlockHash(
	ctx context.Context,
	blockNum int64,
	parsing *spectypes.ParseDirective,
	connectionType string,
	tagName string,
) (string, []byte, error) {
	requestData := []byte(fmt.Sprintf(parsing.FunctionTemplate, blockNum))

	start := time.Now()
	responseData, err := ecf.sendRawRequest(ctx, requestData, connectionType, parsing.ApiName)
	if err != nil {
		timeTaken := time.Since(start)
		return "", nil, utils.LavaFormatDebug(tagName+" failed sending request",
			utils.LogAttr("sendTime", timeTaken),
			utils.LogAttr("error", err),
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("endpoint", ecf.endpointURL),
		)
	}

	craftData := &chainlib.CraftData{
		Path:           parsing.ApiName,
		Data:           requestData,
		ConnectionType: connectionType,
	}
	chainMessage, err := chainlib.CraftChainMessage(parsing, connectionType, ecf.chainParser, craftData, ecf.chainFetcherMetadata())
	if err != nil {
		return "", responseData, utils.LavaFormatError(tagName+" failed CraftChainMessage", err,
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("apiInterface", ecf.apiInterface),
		)
	}

	// gRPC block-hash responses are binary protobuf too — hydrate the connection's cached descriptor
	// so this path decodes like FetchLatestBlockNum (the ChainTracker fetches hashes for fork detection).
	if err := ecf.hydrateGrpcChainMessage(chainMessage, parsing.ApiName); err != nil {
		return "", responseData, err
	}

	parserInput, err := chainlib.FormatResponseForParsing(&pairingtypes.RelayReply{Data: responseData}, chainMessage)
	if err != nil {
		return "", responseData, utils.LavaFormatDebug(tagName+" failed formatResponseForParsing",
			utils.LogAttr("error", err),
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("endpoint", ecf.endpointURL),
			utils.LogAttr("method", parsing.ApiName),
			utils.LogAttr("response", parser.CapStringLen(string(responseData))),
		)
	}

	res, err := parser.ParseBlockHashFromReplyAndDecode(parserInput, parsing.ResultParsing, parsing.Parsers)
	if err != nil {
		return "", responseData, utils.LavaFormatDebug(tagName+" failed ParseBlockHashFromReplyAndDecode",
			utils.LogAttr("error", err),
			utils.LogAttr("chainID", ecf.chainID),
			utils.LogAttr("endpoint", ecf.endpointURL),
			utils.LogAttr("method", parsing.ApiName),
			utils.LogAttr("response", parser.CapStringLen(string(responseData))),
		)
	}

	return res, responseData, nil
}

// FetchEndpoint returns the endpoint information for this fetcher.
// Required by chaintracker.ChainFetcher interface.
func (ecf *EndpointPoller) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{
		ChainID:      ecf.chainID,
		ApiInterface: ecf.apiInterface,
		NodeUrls:     []common.NodeUrl{{Url: ecf.endpointURL}},
	}
}

// CustomMessage sends a custom JSON-RPC / REST message to the endpoint.
// Used by SVMChainTracker to call getLatestBlockhash (which returns slot + block
// hash + block height together — a single call that has no equivalent in the
// generic FetchLatestBlockNum path). Returning an error here disables the per-
// endpoint ChainTracker on Solana, which in turn starves every per-endpoint
// metric that depends on OnNewBlock (latest_block, fetch_latest_success, …).
//
// The `path` argument is accepted for interface compatibility with
// chainlib.ChainFetcher.CustomMessage but is not needed here: POST callers
// (like SVMChainTracker) pass the body in `data` with `path=""`, and GET
// callers already encode the URL suffix in `data` per sendRawRequest's REST
// convention (see connectionType == "GET" branch below).
func (ecf *EndpointPoller) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	return ecf.sendRawRequest(ctx, data, connectionType, apiName)
}

// ObserveLatestBlockPoll records a single latest-block poll observation for this
// endpoint (Topic A / MAG-2158). It implements chaintracker.PollObserver so the SVM
// wrapper — whose latest-block poll uses CustomMessage and therefore bypasses
// FetchLatestBlockNum's own instrumentation — can record exactly one observation per
// poll. The non-SVM path also funnels through here from FetchLatestBlockNum's defer, so
// every poll path records through a single chokepoint.
//
// block is the parsed latest block (0 on failure), transportLatency is the request
// round-trip only, and err is the poll error (nil on success). Nil-safe: a no-op in
// standalone/test use where no observation sink is wired.
func (ecf *EndpointPoller) ObserveLatestBlockPoll(block int64, transportLatency time.Duration, err error) {
	if ecf.onPollObservation == nil {
		return
	}
	ecf.onPollObservation(block, transportLatency, err, time.Now())
}

// sendRawRequest sends a raw request to the endpoint and returns the response.
// For REST/GET requests, requestData is a URL path that must be appended to the base URL.
// For JSON-RPC/POST requests, requestData is the JSON body.
func (ecf *EndpointPoller) sendRawRequest(ctx context.Context, requestData []byte, connectionType string, apiName string) ([]byte, error) {
	if ecf.directConnection == nil {
		return nil, fmt.Errorf("no direct connection for endpoint %s", ecf.endpointURL)
	}

	// REST GET: requestData is a URL path (e.g. "/cosmos/base/tendermint/v1beta1/blocks/latest")
	// Must be appended to the base URL and sent as an HTTP GET.
	if connectionType == "GET" {
		httpDoer, ok := ecf.directConnection.(lavasession.HTTPDirectRPCDoer)
		if !ok {
			return nil, fmt.Errorf("connection does not support HTTP requests for endpoint %s", ecf.endpointURL)
		}

		fullURL, err := common.JoinURLPath(ecf.directConnection.GetURL(), string(requestData))
		if err != nil {
			return nil, fmt.Errorf("failed to build REST URL: %w", err)
		}

		resp, err := httpDoer.DoHTTPRequest(ctx, lavasession.HTTPRequestParams{
			Method: "GET",
			URL:    fullURL,
		})
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 400 {
			return nil, &lavasession.HTTPStatusError{
				StatusCode: resp.StatusCode,
				Status:     fmt.Sprintf("%d", resp.StatusCode),
				Body:       resp.Body,
			}
		}
		return resp.Body, nil
	}

	// JSON-RPC / Tendermint RPC / gRPC / POST: send requestData as the body.
	headers := map[string]string{"Content-Type": "application/json"}
	// gRPC carries the method PATH in a header, not in the URL or body: the connection dials apiName
	// (e.g. cosmos.base.tendermint.v1beta1.Service/GetLatestBlock) and sends requestData as the request
	// payload. Without this header GRPCDirectRPCConnection.SendRequest rejects every call with "gRPC
	// method path not provided", so the per-endpoint gRPC poll never completes. The relay path sets
	// this header already; the poll path must too.
	if ecf.apiInterface == spectypes.APIInterfaceGrpc {
		headers[lavasession.GRPCMethodHeader] = apiName
	}
	response, err := ecf.directConnection.SendRequest(ctx, requestData, headers)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// chainFetcherMetadata returns metadata for constructing chain messages.
func (ecf *EndpointPoller) chainFetcherMetadata() []pairingtypes.Metadata {
	return nil // No special metadata needed for block tracking
}

// GetLatestBlock returns the last known latest block number.
func (ecf *EndpointPoller) GetLatestBlock() int64 {
	return atomic.LoadInt64(&ecf.latestBlock)
}
