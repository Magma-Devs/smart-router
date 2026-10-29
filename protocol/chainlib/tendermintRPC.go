package chainlib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type TendermintChainParser struct {
	BaseChainParser
}

// NewTendermintRpcChainParser creates a new instance of TendermintChainParser
func NewTendermintRpcChainParser() (chainParser *TendermintChainParser, err error) {
	parser := &TendermintChainParser{}
	parser.skipWebsocketVerification = SkipWebsocketVerificationDefault
	return parser, nil
}

func (apip *TendermintChainParser) getApiCollection(connectionType, internalPath, addon string) (*spectypes.ApiCollection, error) {
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	return apip.BaseChainParser.getApiCollection(connectionType, internalPath, addon)
}

func (apip *TendermintChainParser) getSupportedApi(name, connectionType string) (*ApiContainer, error) {
	// Guard that the TendermintChainParser instance exists
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	apiKey := ApiKey{Name: name, ConnectionType: connectionType}
	return apip.BaseChainParser.getSupportedApi(apiKey)
}

func (apip *TendermintChainParser) CraftMessage(parsing *spectypes.ParseDirective, connectionType string, craftData *CraftData, metadata []pairingtypes.Metadata) (ChainMessageForSend, error) {
	if craftData != nil {
		return parseCraftedJsonrpcMessage(apip, craftData, metadata)
	}

	msg := newCraftedJsonrpcMessage(parsing.ApiName, metadata)

	apiCont, err := apip.getSupportedApi(parsing.ApiName, connectionType)
	if err != nil {
		return nil, err
	}
	apiCollection, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
	if err != nil {
		return nil, err
	}
	tenderMsg := rpcInterfaceMessages.TendermintrpcMessage{JsonrpcMessage: msg, Path: parsing.ApiName}
	return apip.newChainMessage(apiCont.api, spectypes.NOT_APPLICABLE, nil, &tenderMsg, apiCollection, false), nil
}

// ParseMsg parses message data into chain message object
func (apip *TendermintChainParser) ParseMsg(urlPath string, data []byte, connectionType string, metadata []pairingtypes.Metadata, extensionInfo extensionslib.ExtensionInfo) (ChainMessage, error) {
	utils.LavaFormatTrace("[Archive Debug] TendermintChainParser.ParseMsg called", utils.LogAttr("urlPath", urlPath), utils.LogAttr("extensionInfo", extensionInfo))
	// Guard that the TendermintChainParser instance exists
	if apip == nil {
		return nil, errors.New("TendermintChainParser not defined")
	}

	// connectionType is currently only used in rest api
	// Unmarshal request
	var msgs []rpcInterfaceMessages.JsonrpcMessage
	var isBatch bool
	isJsonrpc := string(data) != ""
	if isJsonrpc {
		// Fetch pointer to message and error
		var err error
		msgs, isBatch, err = rpcInterfaceMessages.ParseJsonRPCMsgWithBatchFlag(data)
		if err != nil {
			return nil, err
		}
	} else {
		// assuming URI
		urlObj, err := url.Parse(urlPath)
		if err != nil {
			return nil, err
		}
		urlObj.Query()

		msg := rpcInterfaceMessages.JsonrpcMessage{
			ID:      []byte("1"),
			Version: "2.0",
			Method:  urlObj.Path,
		}
		params := make(map[string]interface{})
		queryValues := urlObj.Query()
		for key, values := range queryValues {
			params[key] = strings.Join(values, ",")
		}
		msg.Params = params
		msgs = []rpcInterfaceMessages.JsonrpcMessage{msg}
	}

	msgsLength := len(msgs)

	if msgsLength == 0 {
		return nil, errors.New("empty unmarshaled json")
	}
	// Check batch size limit
	if MaxBatchRequestSize > 0 && msgsLength > MaxBatchRequestSize {
		return nil, utils.LavaFormatWarning("batch request size exceeded", ErrBatchRequestSizeExceeded,
			utils.LogAttr("batchSize", msgsLength),
			utils.LogAttr("maxAllowed", MaxBatchRequestSize),
		)
	}

	var api *spectypes.Api
	var apiCollection *spectypes.ApiCollection
	var blockFold requestedBlockFold
	blockHashes := []string{}
	parsedDefault := true
	for idx, msg := range msgs {
		parsedInput := parser.NewParsedInput()
		// Check api is supported and save it in nodeMsg
		apiCont, err := apip.getSupportedApi(msg.Method, connectionType)
		if err != nil {
			utils.LavaFormatDebug("getSupportedApi tendermintrpc failed",
				utils.LogAttr("method", msg.Method),
				utils.LogAttr("connectionType", connectionType),
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

		// An element that names no block (status) does not decide the batch's block; see
		// requestedBlockFold (MAG-4381).
		parsedBlock := parsedInput.GetBlock()
		blockFold.add(parsedBlock, parsedBlock, parsedInput, overwriteReqBlock == "" && apiNamesNoBlock(apiCont.api))

		if idx == 0 {
			// on the first entry store them
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
	if msgsLength == 1 && !isBatch {
		tenderMsg := rpcInterfaceMessages.TendermintrpcMessage{JsonrpcMessage: msgs[0], Path: ""}
		if !isJsonrpc {
			tenderMsg.Path = urlPath // add path
		}
		nodeMsg = apip.newChainMessage(api, latestRequestedBlock, blockHashes, &tenderMsg, apiCollection, parsedDefault)
		// A single request has no earliest of its own unless its parsers read a range lower bound;
		// then the archive rule must judge the range on it.
		if earliestRequestedBlock != latestRequestedBlock {
			nodeMsg.earliestRequestedBlock = earliestRequestedBlock
		}
	} else {
		var err error
		nodeMsg, err = apip.newBatchChainMessage(api, latestRequestedBlock, earliestRequestedBlock, blockHashes, msgs, apiCollection, parsedDefault)
		if err != nil {
			return nil, err
		}
	}

	utils.LavaFormatTrace("[Archive Debug] TendermintRPC ParseMsg calling ExtensionParsing", utils.LogAttr("addon", apiCollection.CollectionData.AddOn), utils.LogAttr("extensionInfo", extensionInfo), utils.LogAttr("urlPath", urlPath))
	apip.BaseChainParser.ExtensionParsing(apiCollection.CollectionData.AddOn, nodeMsg, extensionInfo)
	return nodeMsg, nil
}

func (*TendermintChainParser) newBatchChainMessage(serviceApi *spectypes.Api, requestedBlock int64, earliestRequestedBlock int64, requestedHashes []string, msgs []rpcInterfaceMessages.JsonrpcMessage, apiCollection *spectypes.ApiCollection, usedDefaultValue bool) (*baseChainMessageContainer, error) {
	batchMessage, err := rpcInterfaceMessages.NewBatchMessage(msgs)
	if err != nil {
		return nil, err
	}
	nodeMsg := &baseChainMessageContainer{
		api:                      serviceApi,
		apiCollection:            apiCollection,
		latestRequestedBlock:     requestedBlock,
		requestedBlockHashes:     requestedHashes,
		msg:                      &batchMessage,
		earliestRequestedBlock:   earliestRequestedBlock,
		resultErrorParsingMethod: rpcInterfaceMessages.CheckResponseErrorForTendermintBatch,
		parseDirective:           GetParseDirective(serviceApi, apiCollection),
		usedDefaultValue:         usedDefaultValue,
	}
	return nodeMsg, err
}

func (*TendermintChainParser) newChainMessage(serviceApi *spectypes.Api, requestedBlock int64, requestedHashes []string, msg *rpcInterfaceMessages.TendermintrpcMessage, apiCollection *spectypes.ApiCollection, usedDefaultValue bool) *baseChainMessageContainer {
	return newSingleChainMessage(serviceApi, apiCollection, requestedBlock, requestedHashes, msg, msg.CheckResponseError, usedDefaultValue)
}

// SetSpec sets the spec for the TendermintChainParser
func (apip *TendermintChainParser) SetSpec(spec spectypes.Spec) {
	// Guard that the TendermintChainParser instance exists
	if apip == nil {
		return
	}

	// Add a read-write lock to ensure thread safety
	apip.rwLock.Lock()
	defer apip.rwLock.Unlock()

	// extract server and tagged apis from spec
	internalPaths, serverApis, taggedApis, apiCollections, headers, verifications := getServiceApis(spec, spectypes.APIInterfaceTendermintRPC)
	apip.BaseChainParser.Construct(spec, internalPaths, taggedApis, serverApis, apiCollections, headers, verifications)
}

// ChainBlockStats returns block stats from spec
// (spec.AllowedBlockLagForQosSync, spec.AverageBlockTime, spec.BlockDistanceForFinalizedData, spec.BlocksInFinalizationProof)
func (apip *TendermintChainParser) ChainBlockStats() (allowedBlockLagForQosSync int64, averageBlockTime time.Duration, blockDistanceForFinalizedData, blocksInFinalizationProof uint32) {
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

type TendermintRpcChainListener struct {
	chainListenerBase
	wsListenerBase
	app *fiber.App // captured during Serve so Shutdown can drain HTTP
}

// NewTendermintRpcChainListener creates a new instance of TendermintRpcChainListener
func NewTendermintRpcChainListener(ctx context.Context, listenEndpoint *lavasession.RPCEndpoint,
	relaySender RelaySender, healthReporter HealthReporter,
	rpcConsumerLogs *metrics.RPCConsumerLogs,
	refererData *RefererData,
	wsSubscriptionManager WSSubscriptionManager,
) (chainListener *TendermintRpcChainListener) {
	return &TendermintRpcChainListener{
		chainListenerBase: chainListenerBase{
			endpoint:       listenEndpoint,
			relaySender:    relaySender,
			healthReporter: healthReporter,
			logger:         rpcConsumerLogs,
		},
		wsListenerBase: newWSListenerBase(refererData, wsSubscriptionManager),
	}
}

// Serve http server for TendermintRpcChainListener
func (apil *TendermintRpcChainListener) Serve(ctx context.Context, cmdFlags common.ConsumerCmdFlags) {
	// Guard that the TendermintChainParser instance exists
	if apil == nil {
		return
	}

	// Setup HTTP Server
	// true: this listener registers a GET catch-all that upgrades, so the health
	// route may hand an upgrade on its own path past the health handler.
	app := createAndSetupBaseAppListener(cmdFlags, apil.endpoint.HealthCheckPath, apil.healthReporter, true)
	apil.app = app

	// ConnectionType "": ParseMsg looks a Tendermint subscription method up without one.
	wsUpgradeMiddleware, websocketCallbackWithDappID := apil.websocketRoutes(ctx, app, &apil.chainListenerBase, cmdFlags, "tendermintrpc", "")

	testMode := common.IsTestMode(ctx)
	handlerPost := apil.relayHTTP(httpRelaySpec{
		testMode: testMode,
		request: func(fiberCtx *fiber.Ctx) httpRelayRequest {
			return httpRelayRequest{body: string(fiberCtx.Body()), logMethod: "POST", logPath: fiberCtx.Request().URI().String()}
		},
		logReceived: func(ctx context.Context, guid uint64, msgSeed, dappID string, req httpRelayRequest, headers []pairingtypes.Metadata) {
			utils.LavaFormatInfo("in <<<",
				utils.LogAttr("GUID", ctx),
				utils.LogAttr(utils.KEY_REQUEST_ID, ctx),
				utils.LogAttr(utils.KEY_TASK_ID, ctx),
				utils.LogAttr(utils.KEY_TRANSACTION_ID, ctx),
				utils.LogAttr("seed", msgSeed),
				utils.LogAttr("_msg", truncatedRelayBody(cmdFlags, req.body)),
				utils.LogAttr("dappID", dappID),
				utils.LogAttr("headers", common.RedactMetadata(headers)),
			)
		},
		logModule: "tendermint http in/out",
		answerError: func(_ context.Context, fiberCtx *fiber.Ctx, err error, reply *pairingtypes.RelayReply) (bool, error) {
			if handled, sendErr := answerJsonRpcBatchTooLarge(fiberCtx, err); handled {
				return true, sendErr
			}
			return answerJsonRpcMethodNotFound(fiberCtx, err, reply)
		},
		errorBody: func(fiberCtx *fiber.Ctx, _ *common.RelayResult, errMasking, _ string) []byte {
			return []byte(rpcInterfaceMessages.ConvertToTendermintError(errMasking, fiberCtx.Body()))
		},
	})

	handlerGet := apil.relayHTTP(httpRelaySpec{
		testMode: testMode,
		request: func(fiberCtx *fiber.Ctx) httpRelayRequest {
			path := fiberCtx.Params("*")
			query := "?" + string(fiberCtx.Request().URI().QueryString())
			return httpRelayRequest{path: path, url: path + query, logMethod: "GET", logPath: fiberCtx.Request().URI().String()}
		},
		logReceived: func(ctx context.Context, _ uint64, _, dappID string, req httpRelayRequest, headers []pairingtypes.Metadata) {
			utils.LavaFormatInfo("urirpc in <<<",
				utils.LogAttr("GUID", ctx),
				utils.LogAttr(utils.KEY_REQUEST_ID, ctx),
				utils.LogAttr(utils.KEY_TASK_ID, ctx),
				utils.LogAttr(utils.KEY_TRANSACTION_ID, ctx),
				utils.LogAttr("_msg", req.path),
				utils.LogAttr("dappID", dappID),
				utils.LogAttr("headers", common.RedactMetadata(headers)),
			)
		},
		logModule: "tendermint http in/out",
		errorBody: func(fiberCtx *fiber.Ctx, _ *common.RelayResult, errMasking, _ string) []byte {
			if string(fiberCtx.Body()) != "" {
				errMasking = addAttributeToError("recommendation", "For jsonRPC use POST", errMasking)
			}
			return convertToJsonError(errMasking)
		},
	})

	app.Post("/*", handlerPost)
	// A caller that guesses the bare endpoint URL (wss://host/) sends a GET
	// carrying the upgrade headers — the ws/wss scheme never reaches the server,
	// so that header is the only signal. Serve any such GET from the handler
	// /ws and /websocket already use; every other GET is a URI-style query.
	app.Get("/*", func(fiberCtx *fiber.Ctx) error {
		if !websocket.IsWebSocketUpgrade(fiberCtx) {
			return handlerGet(fiberCtx)
		}
		return fiberCtx.Next()
	}, wsUpgradeMiddleware, websocketCallbackWithDappID)

	apil.listen(ctx, app)
}

type tendermintRpcChainProxy struct {
	// embedding the jrpc chain proxy because the only diff is on parse message
	JrpcChainProxy
	httpClient *http.Client
}

func NewtendermintRpcChainProxy(ctx context.Context, nConns uint, rpcProviderEndpoint lavasession.RPCProviderEndpoint, chainParser ChainParser) (ChainProxy, error) {
	if err := requireNodeUrl(rpcProviderEndpoint); err != nil {
		return nil, err
	}
	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()

	validateEndpoints(rpcProviderEndpoint.NodeUrls, spectypes.APIInterfaceTendermintRPC)

	nodeUrl := rpcProviderEndpoint.NodeUrls[0]
	cp := &tendermintRpcChainProxy{
		JrpcChainProxy: JrpcChainProxy{
			BaseChainProxy: BaseChainProxy{
				averageBlockTime: averageBlockTime,
				NodeUrl:          nodeUrl,
				ErrorHandler:     newNodeErrorHandler(common.TransportJsonRPC, rpcProviderEndpoint.ChainID),
				ChainID:          rpcProviderEndpoint.ChainID,
			},
			conn: nil,
		},
	}

	return cp, cp.start(ctx, nConns, nodeUrl)
}

func (cp *tendermintRpcChainProxy) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	rpcInputMessage := chainMessage.GetRPCMessage()
	nodeMessage, ok := rpcInputMessage.(*rpcInterfaceMessages.TendermintrpcMessage)
	if !ok {
		batchMessage, ok := rpcInputMessage.(*rpcInterfaceMessages.JsonrpcBatchMessage)
		if !ok {
			return nil, utils.LavaFormatError("invalid message type in tendermintrpc failed to cast RPCInput from chainMessage", nil, utils.Attribute{Key: "rpcMessage", Value: rpcInputMessage}, utils.Attribute{Key: "ptrCast", Value: ok})
		}
		return cp.sendBatchMessage(ctx, batchMessage, chainMessage)
	}
	if nodeMessage.Path != "" {
		return cp.SendURI(ctx, nodeMessage, chainMessage)
	}

	// Else do RPC call
	return cp.SendRPC(ctx, nodeMessage, chainMessage)
}

func (cp *tendermintRpcChainProxy) SendURI(ctx context.Context, nodeMessage *rpcInterfaceMessages.TendermintrpcMessage, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	if cp.httpClient == nil {
		cp.httpClient = common.OptimizedHttpClient()
	}

	httpClient := cp.httpClient

	// appending hashed url
	grpc.SetTrailer(ctx, metadata.Pairs(RPCProviderNodeAddressHash, cp.conn.GetUrlHash()))

	// construct the url by concatenating the node url with the path variable
	url := cp.NodeUrl.Url + "/" + nodeMessage.Path

	// set context with timeout
	connectCtx, cancel := cp.CapTimeoutForSend(ctx, chainMessage)
	defer cancel()

	// create a new http request
	req, err := http.NewRequestWithContext(connectCtx, http.MethodGet, cp.NodeUrl.AuthConfig.AddAuthPath(url), nil)
	if err != nil {
		// Validate if the error is related to the provider connection to the node or it is a valid error
		// in case the error is valid (e.g. bad input parameters) the error will return in the form of a valid error reply
		if parsedError := cp.HandleNodeError(ctx, err); parsedError != nil {
			return nil, parsedError
		}
		// TODO: if not parsed return error as reply or as error?
		return nil, err
	}

	if len(nodeMessage.GetHeaders()) > 0 {
		for _, metadata := range nodeMessage.GetHeaders() {
			if metadata.Value == "" {
				req.Header.Del(metadata.Name)
			} else {
				req.Header.Set(metadata.Name, metadata.Value)
			}
		}
	}

	cp.NodeUrl.SetAuthHeaders(ctx, req.Header.Set)

	cp.NodeUrl.SetIpForwardingIfNecessary(ctx, req.Header.Set)
	// send the http request and get the response
	res, err := httpClient.Do(req)
	if res != nil {
		// resp can be non nil on error
		grpc.SetTrailer(ctx, metadata.Pairs(common.StatusCodeMetadataKey, strconv.Itoa(res.StatusCode))) // we ignore this error here since this code can be triggered not from grpc
	}
	if err != nil {
		// Classify the transport error the way the REST and GraphQL proxies do, so the router
		// gets a coded LavaError (LavaErrorConnectionRefused for a refused dial) instead of the
		// raw *url.Error. Nothing is masked: like theirs, the classified error keeps the raw
		// text, node address included, as its context.
		if parsedError := cp.HandleNodeError(ctx, err); parsedError != nil {
			return nil, parsedError
		}
		return nil, err
	}
	// close the response body
	if res.Body != nil {
		defer res.Body.Close()
	}

	err = cp.HandleStatusError(res.StatusCode, nodeMessage.GetDisableErrorHandling())
	if err != nil {
		err = common.WithRetryAfter(err, res.Header, time.Now())
		return nil, utils.LavaFormatWarning("Received invalid status code", err, utils.Attribute{Key: "Status Code", Value: res.StatusCode}, utils.Attribute{Key: "chainID", Value: cp.BaseChainProxy.ChainID}, utils.Attribute{Key: "apiName", Value: chainMessage.GetApi().Name})
	}

	// read the response body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	// create a new relay reply struct with the response body as the data
	reply := &RelayReplyWrapper{
		StatusCode: http.StatusOK, // status code is used only for rest at the moment
		RelayReply: &pairingtypes.RelayReply{
			Data: body,
		},
	}

	// checking if rest reply data is in json format
	err = cp.HandleJSONFormatError(reply.RelayReply.Data)
	if err != nil {
		return nil, utils.LavaFormatError("Tendermint reply is neither a JSON object nor a JSON array of objects", nil, utils.Attribute{Key: "reply.Data", Value: string(reply.RelayReply.Data)})
	}

	return reply, nil
}

// SendRPC sends Tendermint HTTP or WebSockets call
func (cp *tendermintRpcChainProxy) SendRPC(ctx context.Context, nodeMessage *rpcInterfaceMessages.TendermintrpcMessage, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	// Get rpc connection from the connection pool
	var rpc *rpcclient.Client
	rpc, err = cp.conn.GetRpc(ctx, true)
	if err != nil {
		return nil, err
	}
	// return the rpc connection to the websocket pool after the function completes
	defer cp.conn.ReturnRpc(rpc)

	// appending hashed url
	grpc.SetTrailer(ctx, metadata.Pairs(RPCProviderNodeAddressHash, cp.conn.GetUrlHash()))

	// create variables for the rpc message and reply message
	var rpcMessage *rpcclient.JsonrpcMessage
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
	// perform the rpc call
	rpcMessage, nodeErr := rpc.CallContext(connectCtx, nodeMessage.ID, nodeMessage.Method, nodeMessage.Params, false, nodeMessage.GetDisableErrorHandling())
	if nodeErr != nil {
		if errors.Is(nodeErr, common.StatusCodeError504) || errors.Is(nodeErr, common.StatusCodeError429) || errors.Is(nodeErr, common.StatusCodeErrorStrict) {
			return nil, utils.LavaFormatWarning("Received invalid status code", nodeErr, utils.Attribute{Key: "chainID", Value: cp.BaseChainProxy.ChainID}, utils.Attribute{Key: "apiName", Value: chainMessage.GetApi().Name})
		}
		// Validate if the error is related to the connection to the node or it is a valid error
		// in case the error is valid (e.g. bad input parameters) the error will return in the form of a valid error reply
		if parsedError := cp.HandleNodeError(ctx, nodeErr); parsedError != nil {
			return nil, parsedError
		}
	}

	var replyMsg *rpcInterfaceMessages.RPCResponse
	// the error check here would only wrap errors not from the rpc
	if nodeErr != nil {
		rpcMessage = TryRecoverNodeErrorFromClientError(nodeErr)
		if rpcMessage == nil {
			utils.LavaFormatDebug("got error from node", utils.LogAttr("GUID", ctx), utils.LogAttr("nodeErr", nodeErr))
			return nil, nodeErr
		}
	}

	replyMsg, err = rpcInterfaceMessages.ConvertTendermintMsg(rpcMessage)
	if err != nil {
		return nil, utils.LavaFormatError("tendermintRPC error", err)
	}

	err = cp.ValidateRequestAndResponseIds(nodeMessage.ID, rpcMessage.ID)
	if err != nil {
		// When there's a response error, the node may return an empty/invalid ID,
		// causing ID mismatch. Include the response error to clarify the root cause.
		responseError := ""
		if replyMsg.Error != nil {
			responseError = replyMsg.Error.Message
		}
		return nil, utils.LavaFormatError("tendermintRPC ID mismatch error", err, traceAttrs(ctx,
			utils.Attribute{Key: "requestId", Value: nodeMessage.ID},
			utils.Attribute{Key: "responseId", Value: rpcMessage.ID},
			utils.Attribute{Key: "responseError", Value: responseError},
		)...)
	}

	// marshal the jsonrpc message to json
	data, err := json.Marshal(replyMsg)
	if err != nil {
		return nil, err
	}

	// create a new relay reply struct
	reply := &RelayReplyWrapper{
		StatusCode: http.StatusOK, // status code is used only for rest at the moment
		RelayReply: &pairingtypes.RelayReply{
			Data: data,
		},
	}

	return reply, err
}

// Shutdown stops accepting new connections, drains in-flight HTTP requests,
// and then waits for active WebSocket goroutines to finish. See
// drainHTTPThenWS for the full rationale on the ordering of HTTP-then-WS.
func (apil *TendermintRpcChainListener) Shutdown(ctx context.Context) error {
	if apil == nil {
		return nil
	}
	return drainHTTPThenWS(ctx, apil.app, &apil.wsWG, "TendermintRPC")
}
