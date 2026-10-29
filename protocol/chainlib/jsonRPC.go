package chainlib

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/parser"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	SEP = "&"
	// ethCallArchiveBlockDepth: an eth_call targeting a block deeper than this many blocks
	// behind the head is routed to archive (most full nodes prune state past ~128 blocks).
	ethCallArchiveBlockDepth = 126
)

var MaximumNumberOfParallelWebsocketConnectionsPerIp int64 = 0

type JsonRPCChainParser struct {
	BaseChainParser
}

// NewJrpcChainParser creates a new instance of JsonRPCChainParser
func NewJrpcChainParser() (chainParser *JsonRPCChainParser, err error) {
	parser := &JsonRPCChainParser{}
	parser.skipWebsocketVerification = SkipWebsocketVerificationDefault
	return parser, nil
}

func (apip *JsonRPCChainParser) getApiCollection(connectionType, internalPath, addon string) (*spectypes.ApiCollection, error) {
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	return apip.BaseChainParser.getApiCollection(connectionType, internalPath, addon)
}

func (apip *JsonRPCChainParser) getSupportedApi(name, connectionType string, internalPath string) (*ApiContainer, error) {
	// Guard that the JsonRPCChainParser instance exists
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	apiKey := ApiKey{Name: name, ConnectionType: connectionType, InternalPath: internalPath}
	return apip.BaseChainParser.getSupportedApi(apiKey)
}

func (apip *JsonRPCChainParser) CraftMessage(parsing *spectypes.ParseDirective, connectionType string, craftData *CraftData, metadata []pairingtypes.Metadata) (ChainMessageForSend, error) {
	if craftData != nil {
		return parseCraftedJsonrpcMessage(apip, craftData, metadata)
	}

	msg := newCraftedJsonrpcMessage(parsing.ApiName, metadata)
	apiCont, err := apip.getSupportedApi(parsing.ApiName, connectionType, "")
	if err != nil {
		return nil, err
	}
	apiCollection, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
	if err != nil {
		return nil, err
	}
	return apip.newChainMessage(apiCont.api, spectypes.NOT_APPLICABLE, nil, &msg, apiCollection, false), nil
}

// this func parses message data into chain message object
func (apip *JsonRPCChainParser) ParseMsg(url string, data []byte, connectionType string, metadata []pairingtypes.Metadata, extensionInfo extensionslib.ExtensionInfo) (ChainMessage, error) {
	// Guard that the JsonRPCChainParser instance exists
	if apip == nil {
		return nil, errors.New("JsonRPCChainParser not defined")
	}

	// connectionType is currently only used in rest API.
	// Unmarshal request
	msgs, isBatch, err := rpcInterfaceMessages.ParseJsonRPCMsgWithBatchFlag(data)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, errors.New("empty unmarshaled json")
	}
	// Check batch size limit
	if MaxBatchRequestSize > 0 && len(msgs) > MaxBatchRequestSize {
		return nil, utils.LavaFormatWarning("batch request size exceeded", ErrBatchRequestSizeExceeded,
			utils.LogAttr("batchSize", len(msgs)),
			utils.LogAttr("maxAllowed", MaxBatchRequestSize),
		)
	}
	if isBatch {
		if err := rpcInterfaceMessages.CheckJsonrpcBatch(apip.chainFamily(), msgs); err != nil {
			return nil, utils.LavaFormatWarning("JSON-RPC batch refused", err, utils.LogAttr("batchSize", len(msgs)))
		}
	}
	var api *spectypes.Api
	var apiCollection *spectypes.ApiCollection
	var blockFold requestedBlockFold
	blockHashes := []string{}
	parsedDefault := true
	for idx, msg := range msgs {
		parsedInput := parser.NewParsedInput()
		internalPath := ""
		if apip.isValidInternalPath(url) {
			internalPath = url
		}
		// Check api is supported and save it in nodeMsg
		apiCont, err := apip.getSupportedApi(msg.Method, connectionType, internalPath)
		if err != nil {
			utils.LavaFormatError("getSupportedApi jsonrpc failed",
				err,
				utils.LogAttr("method", msg.Method),
				utils.LogAttr("connectionType", connectionType),
				utils.LogAttr("internalPath", internalPath),
				utils.LogAttr("error", err),
			)

			return nil, err
		}

		apiCollectionForMessage, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
		if err != nil {
			return nil, fmt.Errorf("could not find the interface %s in the service %s, %w", connectionType, apiCont.api.Name, err)
		}

		metadata, overwriteReqBlock, _ := apip.HandleHeaders(metadata, apiCollectionForMessage, spectypes.Header_pass_send)
		settingHeaderDirective, _, _ := apip.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)
		msg.BaseMessage = chainproxy.BaseMessage{Headers: metadata, LatestBlockHeaderSetter: settingHeaderDirective}

		if overwriteReqBlock == "" {
			// Fetch requested block, it is used for data reliability
			parsedInput = parser.ParseBlockFromParams(msg, apiCont.api.BlockParsing, apiCont.api.Parsers)
			if hashes, err := parsedInput.GetBlockHashes(); err == nil {
				blockHashes = append(blockHashes, hashes...)
			}
			if !parsedInput.UsedDefaultValue {
				parsedDefault = false
			}
		} else {
			parsedBlock, err := msg.ParseBlock(overwriteReqBlock)
			parsedInput.SetBlock(parsedBlock)
			if err != nil {
				utils.LavaFormatError("failed parsing block from an overwrite header", err,
					utils.LogAttr("chain", apip.spec.Name),
					utils.LogAttr("overwriteReqBlock", overwriteReqBlock),
				)
				parsedInput.SetBlock(spectypes.NOT_APPLICABLE)
			} else {
				parsedInput.UsedDefaultValue = false
			}
		}

		parsedBlock := parsedInput.GetBlock()

		// Route eth_call requests for blocks deeper than ethCallArchiveBlockDepth behind the head to
		// archive. Guard the subtraction: when the head is unknown (LatestBlock == 0) or the chain is
		// younger than the depth, LatestBlock-ethCallArchiveBlockDepth would underflow (uint64) and force
		// archive for EVERY eth_call. Negative parsedBlock values are latest/pending sentinels, never
		// historical, so they are excluded too.
		if msg.Method == "eth_call" && parsedBlock >= 0 &&
			extensionInfo.LatestBlock > ethCallArchiveBlockDepth &&
			uint64(parsedBlock) < extensionInfo.LatestBlock-ethCallArchiveBlockDepth {
			utils.LavaFormatInfo("adding extension requirement to archive since block is 127 before lastest block")
			// change to archive. The router asks for it, not the caller, so a fleet without an
			// archive node serves the call without it and does not report it back (MAG-3935).
			extensionInfo.RouterExtensions = append(extensionInfo.RouterExtensions, extensionslib.ArchiveExtension)
		}

		// What this element adds to a batch's earliest block. RequestedBlock reads an earliest block
		// of 0 as "unset", so an element at block 0, the chain's first block, joins it as
		// EARLIEST_BLOCK, as a range lower bound of 0 does in foldParsedEarliest. Kept as 0, it
		// either lowered the earliest block to "unset" or lost to a "latest"-style sentinel, and
		// the archive rule judged the batch without its genesis element.
		elementEarliest := parsedBlock
		if isBatch && elementEarliest == 0 {
			elementEarliest = spectypes.EARLIEST_BLOCK
		}

		// The earliest block starts at the first element that names a block, not at the "unset" 0:
		// CompareRequestedBlockInBatch only lowers it, and 0 made every later combination come out
		// 0, so RequestedBlock reported the newest block as the earliest and the archive rule never
		// saw a batch's deep element (MAG-4242). An element that names no block (eth_chainId) does
		// not decide the batch's block at all; see requestedBlockFold (MAG-4381).
		blockFold.add(parsedBlock, elementEarliest, parsedInput, overwriteReqBlock == "" && apiNamesNoBlock(apiCont.api))

		if idx == 0 {
			// on the first entry store them.
			api = apiCont.api
			apiCollection = apiCollectionForMessage
		} else {
			// on next entries we need to compare to existing data
			if api == nil {
				utils.LavaFormatFatal("invalid parsing, api is nil", nil)
			}
			// on a batch request we need to do the following:
			// 1. create a new api object, since it's not a single one
			// 2. we need to add the compute units
			// 3. we need to set the requested block to be the latest of them all or not_applicable
			// 4. we need to take the most comprehensive apiCollection (addon)
			// 5. take the strictest category
			category := api.GetCategory()
			category = category.Combine(apiCont.api.GetCategory())
			if apiCollectionForMessage.CollectionData.AddOn != "" && apiCollectionForMessage.CollectionData.AddOn != apiCollection.CollectionData.AddOn {
				if apiCollection.CollectionData.AddOn != "" {
					return nil, utils.LavaFormatError("unable to parse batch request with api from multiple addons", nil,
						utils.Attribute{Key: "first addon", Value: apiCollection.CollectionData.AddOn},
						utils.Attribute{Key: "second addon", Value: apiCollectionForMessage.CollectionData.AddOn})
				}
				apiCollection = apiCollectionForMessage // overwrite apiColleciton to take the addon
			}
			api = &spectypes.Api{
				Enabled:      api.Enabled && apiCont.api.Enabled,
				Name:         api.Name + SEP + apiCont.api.Name,
				ComputeUnits: api.ComputeUnits + apiCont.api.ComputeUnits,
				Category:     category,
				BlockParsing: spectypes.BlockParser{
					ParserArg:    []string{},
					ParserFunc:   spectypes.PARSER_FUNC_EMPTY,
					DefaultValue: "",
					Encoding:     "",
				},
			}
		}
	}
	latestRequestedBlock, earliestRequestedBlock := blockFold.blocks()

	var nodeMsg *baseChainMessageContainer
	if len(msgs) == 1 && !isBatch {
		nodeMsg = apip.newChainMessage(api, latestRequestedBlock, blockHashes, &msgs[0], apiCollection, parsedDefault)
		// A single request has no earliest of its own unless its parsers read a range lower bound
		// (trace_filter's fromBlock); then the archive rule must judge the range on it.
		if earliestRequestedBlock != latestRequestedBlock {
			nodeMsg.earliestRequestedBlock = earliestRequestedBlock
		}
	} else {
		nodeMsg, err = apip.newBatchChainMessage(api, latestRequestedBlock, earliestRequestedBlock, blockHashes, msgs, apiCollection, parsedDefault)
		if err != nil {
			return nil, err
		}
	}
	apip.BaseChainParser.ExtensionParsing(apiCollection.CollectionData.AddOn, nodeMsg, extensionInfo)
	return nodeMsg, nil
}

func (*JsonRPCChainParser) newBatchChainMessage(serviceApi *spectypes.Api, requestedBlock int64, earliestRequestedBlock int64, requestedBlockHashes []string, msgs []rpcInterfaceMessages.JsonrpcMessage, apiCollection *spectypes.ApiCollection, usedDefaultValue bool) (*baseChainMessageContainer, error) {
	batchMessage, err := rpcInterfaceMessages.NewBatchMessage(msgs)
	if err != nil {
		return nil, err
	}
	nodeMsg := &baseChainMessageContainer{
		api:                      serviceApi,
		apiCollection:            apiCollection,
		latestRequestedBlock:     requestedBlock,
		requestedBlockHashes:     requestedBlockHashes,
		msg:                      &batchMessage,
		earliestRequestedBlock:   earliestRequestedBlock,
		resultErrorParsingMethod: rpcInterfaceMessages.CheckResponseErrorForJsonRpcBatch,
		parseDirective:           nil,
		usedDefaultValue:         usedDefaultValue,
	}
	return nodeMsg, err
}

func (apip *JsonRPCChainParser) newChainMessage(serviceApi *spectypes.Api, requestedBlock int64, requestedBlockHashes []string, msg *rpcInterfaceMessages.JsonrpcMessage, apiCollection *spectypes.ApiCollection, usedDefaultValue bool) *baseChainMessageContainer {
	return newSingleChainMessage(serviceApi, apiCollection, requestedBlock, requestedBlockHashes, msg, rpcInterfaceMessages.JsonrpcResponseErrorChecker(apip.chainFamily(), msg), usedDefaultValue)
}

// SetSpec sets the spec for the JsonRPCChainParser
func (apip *JsonRPCChainParser) SetSpec(spec spectypes.Spec) {
	// Guard that the JsonRPCChainParser instance exists
	if apip == nil {
		return
	}

	// Add a read-write lock to ensure thread safety
	apip.rwLock.Lock()
	defer apip.rwLock.Unlock()

	// extract server and tagged apis from spec
	internalPaths, serverApis, taggedApis, apiCollections, headers, verifications := getServiceApis(spec, spectypes.APIInterfaceJsonRPC)
	apip.BaseChainParser.Construct(spec, internalPaths, taggedApis, serverApis, apiCollections, headers, verifications)
}

// ChainBlockStats returns block stats from spec
// (spec.AllowedBlockLagForQosSync, spec.AverageBlockTime, spec.BlockDistanceForFinalizedData)
func (apip *JsonRPCChainParser) ChainBlockStats() (allowedBlockLagForQosSync int64, averageBlockTime time.Duration, blockDistanceForFinalizedData, blocksInFinalizationProof uint32) {
	// Guard that the JsonRPCChainParser instance exists
	if apip == nil {
		return 0, 0, 0, 0
	}

	// Acquire read lock
	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()

	// Convert average block time from int64 -> time.Duration
	averageBlockTime = time.Duration(apip.spec.AverageBlockTime) * time.Millisecond

	// Return allowedBlockLagForQosSync, averageBlockTime, blockDistanceForFinalizedData from spec
	return apip.spec.AllowedBlockLagForQosSync, averageBlockTime, apip.spec.BlockDistanceForFinalizedData, apip.spec.BlocksInFinalizationProof
}

type JsonRPCChainListener struct {
	chainListenerBase
	wsListenerBase
	app *fiber.App // captured during Serve so Shutdown can drain HTTP
}

// NewJrpcChainListener creates a new instance of JsonRPCChainListener
func NewJrpcChainListener(ctx context.Context, listenEndpoint *lavasession.RPCEndpoint,
	relaySender RelaySender, healthReporter HealthReporter,
	rpcConsumerLogs *metrics.RPCConsumerLogs,
	refererData *RefererData,
	wsSubscriptionManager WSSubscriptionManager,
) (chainListener *JsonRPCChainListener) {
	return &JsonRPCChainListener{
		chainListenerBase: chainListenerBase{
			endpoint:       listenEndpoint,
			relaySender:    relaySender,
			healthReporter: healthReporter,
			logger:         rpcConsumerLogs,
		},
		wsListenerBase: newWSListenerBase(refererData, wsSubscriptionManager),
	}
}

// Serve http server for JsonRPCChainListener
func (apil *JsonRPCChainListener) Serve(ctx context.Context, cmdFlags common.ConsumerCmdFlags) {
	// Guard that the JsonRPCChainListener instance exists
	if apil == nil {
		return
	}
	// Setup HTTP Server
	// true: this listener registers a GET catch-all that upgrades, so the health
	// route may hand an upgrade on its own path past the health handler.
	app := createAndSetupBaseAppListener(cmdFlags, apil.endpoint.HealthCheckPath, apil.healthReporter, true)
	apil.app = app

	// ConnectionType POST: ParseMsg needs it to find the subscription method in the spec.
	wsUpgradeMiddleware, websocketCallbackWithDappID := apil.websocketRoutes(ctx, app, &apil.chainListenerBase, cmdFlags, "jsonrpc", fiber.MethodPost)

	chainID := apil.endpoint.ChainID
	app.Post("/*", apil.relayHTTP(httpRelaySpec{
		testMode: common.IsTestMode(ctx),
		request: func(fiberCtx *fiber.Ctx) httpRelayRequest {
			path := "/" + fiberCtx.Params("*")
			return httpRelayRequest{
				path: path, url: path, body: string(fiberCtx.Body()), connectionType: http.MethodPost,
				logMethod: "POST", logPath: fiberCtx.Request().URI().String(),
			}
		},
		logReceived: func(ctx context.Context, guid uint64, msgSeed, dappID string, req httpRelayRequest, headers []pairingtypes.Metadata) {
			utils.LavaFormatInfo("Consumer received a new JSON-RPC request",
				utils.LogAttr("GUID", guid),
				utils.LogAttr(utils.KEY_REQUEST_ID, ctx),
				utils.LogAttr(utils.KEY_TASK_ID, ctx),
				utils.LogAttr(utils.KEY_TRANSACTION_ID, ctx),
				utils.LogAttr("path", req.path),
				utils.LogAttr("seed", msgSeed),
				utils.LogAttr("body", truncatedRelayBody(cmdFlags, req.body)),
				utils.LogAttr("dappID", dappID),
				utils.LogAttr("headers", common.RedactMetadata(headers)),
			)
		},
		logModule: "jsonrpc http",
		answerError: func(_ context.Context, fiberCtx *fiber.Ctx, err error, reply *pairingtypes.RelayReply) (bool, error) {
			if handled, sendErr := answerJsonRpcBatchTooLarge(fiberCtx, err); handled {
				return true, sendErr
			}
			if handled, sendErr := answerJsonRpcBatchRefused(fiberCtx, err); handled {
				return true, sendErr
			}
			if handled, sendErr := answerJsonRpcMethodNotFound(fiberCtx, err, reply); handled {
				return true, sendErr
			}
			if _, ok := err.(*json.SyntaxError); ok {
				errorResponse, _ := json.Marshal(common.JsonRpcParseError)
				return true, addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), errorResponse)
			}
			return false, nil
		},
		// A spec-compliant JSON-RPC 2.0 error envelope (see convertToJsonRpcError for the
		// rationale — `error` must be an Object with code/message per §5.1, not a stringified
		// envelope).
		errorBody: func(fiberCtx *fiber.Ctx, relayResult *common.RelayResult, errMasking, msgSeed string) []byte {
			if cf := relayResult.GetClientFacing(); cf != nil && metrics.ReturnMaskedErrors == "false" {
				// MAG-4218: the upstream's own answer, or one honest sentence + details in data.
				return convertUpstreamErrorToJsonRpc(cf, msgSeed, errMasking, fiberCtx.Body())
			}
			return convertToJsonRpcError(errMasking, fiberCtx.Body())
		},
		replyBody: func(data []byte) []byte {
			return checkUTXOResponseAndFixReply(chainID, data)
		},
	}))
	// A caller that guesses the bare endpoint URL (wss://host/) sends a GET
	// carrying the upgrade headers — the ws/wss scheme never reaches the server,
	// so that header is the only signal. Serve any such GET from the handler
	// /ws and /websocket already use; a GET without it still answers 405.
	app.Get("/*", func(fiberCtx *fiber.Ctx) error {
		if !websocket.IsWebSocketUpgrade(fiberCtx) {
			// Registering a GET route means fiber stops reaching its own
			// method-mismatch path, and that path is what used to attach Allow to
			// the 405. RFC 9110 §15.5.6 requires the header on a 405, so set it
			// here or the answer silently loses it. POST is the value fiber
			// produced before: it is the only non-upgrade method this listener
			// serves on /*, registered directly above.
			fiberCtx.Set(fiber.HeaderAllow, fiber.MethodPost)
			return fiber.ErrMethodNotAllowed
		}
		return fiberCtx.Next()
	}, wsUpgradeMiddleware, websocketCallbackWithDappID)

	apil.listen(ctx, app)
}

type JrpcChainProxy struct {
	BaseChainProxy
	conn *chainproxy.Connector
}

func NewJrpcChainProxy(ctx context.Context, nConns uint, rpcProviderEndpoint lavasession.RPCProviderEndpoint, chainParser ChainParser) (ChainProxy, error) {
	if err := requireNodeUrl(rpcProviderEndpoint); err != nil {
		return nil, err
	}
	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()

	// look for the first node url that has no internal path, otherwise take first node url
	nodeUrl := rpcProviderEndpoint.NodeUrls[0]

	cp := &JrpcChainProxy{
		BaseChainProxy: BaseChainProxy{
			averageBlockTime: averageBlockTime,
			NodeUrl:          nodeUrl,
			ErrorHandler:     newNodeErrorHandler(common.TransportJsonRPC, rpcProviderEndpoint.ChainID),
			ChainID:          rpcProviderEndpoint.ChainID,
		},
		conn: nil,
	}

	validateEndpoints(rpcProviderEndpoint.NodeUrls, spectypes.APIInterfaceJsonRPC)

	return cp, cp.start(ctx, nConns, nodeUrl)
}

func (cp *JrpcChainProxy) start(ctx context.Context, nConns uint, nodeUrl common.NodeUrl) error {
	conn, err := chainproxy.NewConnector(ctx, nConns, nodeUrl)
	if err != nil {
		isWs, parseErr := IsUrlWebSocket(nodeUrl.Url)
		if parseErr == nil && isWs {
			newUrl := strings.TrimSuffix(nodeUrl.Url, "/") + "/ws"
			originalUrl := nodeUrl.Url
			utils.LavaFormatWarning("Failed creating connector for ws, trying to create with /ws path", err, utils.LogAttr("url", nodeUrl.UrlStr()), utils.LogAttr("newUrl", newUrl))
			nodeUrl.Url = newUrl
			conn, newConnErr := chainproxy.NewConnector(ctx, nConns, nodeUrl)
			if newConnErr == nil {
				cp.conn = conn
				return nil
			}
			nodeUrl.Url = originalUrl
		}
		return err
	}

	cp.conn = conn
	return nil
}

func (cp *JrpcChainProxy) sendBatchMessage(ctx context.Context, nodeMessage *rpcInterfaceMessages.JsonrpcBatchMessage, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	rpc, err := cp.conn.GetRpc(ctx, true)
	if err != nil {
		return nil, err
	}
	defer cp.conn.ReturnRpc(rpc)
	if len(nodeMessage.GetHeaders()) > 0 {
		for _, metadata := range nodeMessage.GetHeaders() {
			rpc.SetHeader(metadata.Name, metadata.Value)
			// clear this header upon function completion so it doesn't last in the next usage from the rpc pool
			defer rpc.SetHeader(metadata.Name, "")
		}
	}
	// set context with timeout
	connectCtx, cancel := cp.CapTimeoutForSend(ctx, chainMessage)
	defer cancel()

	cp.NodeUrl.SetIpForwardingIfNecessary(ctx, rpc.SetHeader)
	batch := nodeMessage.GetBatch()
	err = rpc.BatchCallContext(connectCtx, batch, nodeMessage.GetDisableErrorHandling())
	if err != nil {
		// Validate if the error is related to the provider connection to the node or it is a valid error
		// in case the error is valid (e.g. bad input parameters) the error will return in the form of a valid error reply
		if parsedError := cp.HandleNodeError(ctx, err); parsedError != nil {
			return nil, parsedError
		}
		return nil, err
	}

	// Convert and release memory immediately to prevent OOM on large responses
	// keeping all responses in batch[].Result causes 4x memory duplication:
	//   1. HTTP response buffer
	//   2. Decoded JsonrpcMessage.Result (RawMessage)
	//   3. batch[].Result (stored reference)
	//   4. Final marshaled output
	// By releasing batch[].Result immediately after conversion, we reduce peak memory usage.
	replyMsgs := make([]rpcInterfaceMessages.JsonrpcMessage, len(batch))
	for idx := range batch {
		// convert them because batch elements can't be marshaled back to the user, they are missing tags and fields
		replyMsgs[idx], err = rpcInterfaceMessages.ConvertBatchElement(batch[idx])
		if err != nil {
			return nil, err
		}
		// Release the large Result field immediately after conversion to allow GC
		batch[idx].Result = nil
	}

	retData, err := json.Marshal(replyMsgs)
	if err != nil {
		return nil, err
	}
	// Clear replyMsgs Result fields after successful marshaling to release memory
	for idx := range replyMsgs {
		replyMsgs[idx].Result = nil
	}

	reply := &RelayReplyWrapper{
		StatusCode: http.StatusOK, // status code is used only for rest at the moment

		RelayReply: &pairingtypes.RelayReply{
			Data: retData,
		},
	}
	return reply, nil
}

func (cp *JrpcChainProxy) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	rpcInputMessage := chainMessage.GetRPCMessage()
	nodeMessage, ok := rpcInputMessage.(*rpcInterfaceMessages.JsonrpcMessage)
	if !ok {
		// this could be a batch message
		batchMessage, ok := rpcInputMessage.(*rpcInterfaceMessages.JsonrpcBatchMessage)
		if !ok {
			return nil, utils.LavaFormatError("invalid message type in jsonrpc failed to cast JsonrpcMessage or JsonrpcBatchMessage from chainMessage", nil, traceAttrs(ctx, utils.Attribute{Key: "rpcMessage", Value: rpcInputMessage})...)
		}
		return cp.sendBatchMessage(ctx, batchMessage, chainMessage)
	}

	rpc, err := cp.conn.GetRpc(ctx, true)
	if err != nil {
		return nil, err
	}
	defer cp.conn.ReturnRpc(rpc)

	// appending hashed url
	grpc.SetTrailer(ctx, metadata.Pairs(RPCProviderNodeAddressHash, cp.conn.GetUrlHash()))

	// Call our node
	var rpcMessage *rpcclient.JsonrpcMessage
	// support setting headers
	if len(nodeMessage.GetHeaders()) > 0 {
		for _, metadata := range nodeMessage.GetHeaders() {
			if metadata.Value == "" {
				rpc.DelHeader(metadata.Name)
			} else {
				rpc.SetHeader(metadata.Name, metadata.Value)
				// clear this header upon function completion so it doesn't last in the next usage from the rpc pool
				defer rpc.SetHeader(metadata.Name, "")
			}
		}
	}
	// we use the minimum timeout between the two, spec or context. to prevent the node from hanging
	// we don't use the context alone so it won't be hanging forever by an attack
	connectCtx, cancel := cp.CapTimeoutForSend(ctx, chainMessage)
	defer cancel()

	cp.NodeUrl.SetIpForwardingIfNecessary(ctx, rpc.SetHeader)
	rpcMessage, nodeErr := rpc.CallContext(connectCtx, nodeMessage.ID, nodeMessage.Method, nodeMessage.Params, true, nodeMessage.GetDisableErrorHandling())
	if nodeErr != nil {
		// here we are getting an error for every code that is not 200-300
		if errors.Is(nodeErr, common.StatusCodeError504) || errors.Is(nodeErr, common.StatusCodeError429) || errors.Is(nodeErr, common.StatusCodeErrorStrict) {
			return nil, utils.LavaFormatWarning("Received invalid status code", nodeErr, utils.Attribute{Key: "chainID", Value: cp.BaseChainProxy.ChainID}, utils.Attribute{Key: "apiName", Value: chainMessage.GetApi().Name})
		}
		// Validate if the error is related to the connection to the node or it is a valid error
		// in case the error is valid (e.g. bad input parameters) the error will return in the form of a valid error reply
		if parsedError := cp.HandleNodeError(ctx, nodeErr); parsedError != nil {
			return nil, parsedError
		}
	}

	var replyMsg *rpcInterfaceMessages.JsonrpcMessage
	// the error check here would only wrap errors not from the rpc
	if nodeErr != nil {
		// try to parse node error as json message
		rpcMessage = TryRecoverNodeErrorFromClientError(nodeErr)
		if rpcMessage == nil {
			utils.LavaFormatDebug("got error from node", utils.LogAttr("GUID", ctx), utils.LogAttr("nodeErr", nodeErr), utils.LogAttr("nodeUrl", cp.NodeUrl.Url))
			return nil, nodeErr
		}
	}

	replyMsg, err = rpcInterfaceMessages.ConvertJsonRPCMsg(rpcMessage)
	if err != nil {
		return nil, utils.LavaFormatError("jsonRPC error", err, traceAttrs(ctx)...)
	}

	err = cp.ValidateRequestAndResponseIds(nodeMessage.ID, replyMsg.ID)
	if err != nil {
		// When there's a response error (e.g., "Apikey is expired"), the node may return
		// an empty/invalid ID, causing ID mismatch. Include the response error to clarify the root cause.
		responseError := ""
		if replyMsg.Error != nil {
			responseError = replyMsg.Error.Message
		}
		return nil, utils.LavaFormatError("jsonRPC ID mismatch error", err, traceAttrs(ctx,
			utils.Attribute{Key: "nodeMessageRequestId", Value: nodeMessage.ID},
			utils.Attribute{Key: "responseId", Value: rpcMessage.ID},
			utils.Attribute{Key: "responseError", Value: responseError},
			utils.Attribute{Key: "reply", Value: replyMsg},
			utils.Attribute{Key: "request", Value: nodeMessage},
		)...)
	}

	// Release rpcMessage's Result after all references to it are complete
	rpcMessage.Result = nil

	retData, err := json.Marshal(replyMsg)
	if err != nil {
		return nil, err
	}

	reply := &RelayReplyWrapper{
		StatusCode: http.StatusOK, // status code is used only for rest at the moment

		RelayReply: &pairingtypes.RelayReply{
			Data: retData,
		},
	}

	// Clear replyMsg's Result after all references to it are complete
	replyMsg.Result = nil

	return reply, err
}

// Shutdown stops accepting new connections, drains in-flight HTTP requests,
// and then waits for active WebSocket goroutines to finish. See
// drainHTTPThenWS for the full rationale on the ordering of HTTP-then-WS.
func (apil *JsonRPCChainListener) Shutdown(ctx context.Context) error {
	if apil == nil {
		return nil
	}
	return drainHTTPThenWS(ctx, apil.app, &apil.wsWG, "JSONRPC")
}
