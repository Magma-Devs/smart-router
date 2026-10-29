package chainlib

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

type RestChainParser struct {
	BaseChainParser
}

// NewRestChainParser creates a new instance of RestChainParser
func NewRestChainParser() (chainParser *RestChainParser, err error) {
	parser := &RestChainParser{}
	parser.skipWebsocketVerification = SkipWebsocketVerificationDefault
	return parser, nil
}

func (apip *RestChainParser) CraftMessage(parsing *spectypes.ParseDirective, connectionType string, craftData *CraftData, metadata []pairingtypes.Metadata) (ChainMessageForSend, error) {
	if craftData != nil {
		var data []byte = nil
		urlPath := string(craftData.Data)
		if RestMethodCarriesBody(craftData.ConnectionType) {
			// A body method sends the template as its body, to the api name as the path. Any other
			// method's template is the request URL.
			data = craftData.Data
			urlPath = craftData.Path
		}
		// chain fetcher sends the replaced request inside data
		return parseCraftedMessage(apip, urlPath, data, craftData.ConnectionType, metadata)
	}

	restMessage := &rpcInterfaceMessages.RestMessage{
		Msg:         nil,
		Path:        parsing.ApiName,
		BaseMessage: chainproxy.BaseMessage{Headers: metadata},
	}

	apiCont, err := apip.getSupportedApi(parsing.ApiName, connectionType)
	if err != nil {
		return nil, err
	}
	apiCollection, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
	if err != nil {
		return nil, err
	}
	parsedInput := parser.NewParsedInput()
	parsedInput.SetBlock(spectypes.NOT_APPLICABLE)
	return newParsedChainMessage(apiCont.api, apiCollection, parsedInput, nil, restMessage, restMessage.CheckResponseError), nil
}

// ParseMsg parses message data into chain message object
func (apip *RestChainParser) ParseMsg(urlPath string, data []byte, connectionType string, metadata []pairingtypes.Metadata, extensionInfo extensionslib.ExtensionInfo) (ChainMessage, error) {
	// Guard that the RestChainParser instance exists
	if apip == nil {
		return nil, errors.New("RestChainParser not defined")
	}
	urlObj, err := url.Parse(urlPath)
	if err != nil {
		return nil, err
	}
	urlWithNoQuery := urlObj.Path

	// Check api is supported and save it in nodeMsg
	apiCont, err := apip.getSupportedApi(urlWithNoQuery, connectionType)
	if err != nil {
		return nil, err
	}

	// Extract default block parser
	api := apiCont.api

	apiCollection, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
	if err != nil {
		return nil, err
	}
	metadata = normalizeGenericContentType(metadata, data)
	metadata, overwriteReqBlock, _ := apip.HandleHeaders(metadata, apiCollection, spectypes.Header_pass_send)

	settingHeaderDirective, _, _ := apip.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)
	// Construct restMessage
	restMessage := rpcInterfaceMessages.RestMessage{
		Msg:         data,
		Path:        urlPath,
		BaseMessage: chainproxy.BaseMessage{Headers: metadata, LatestBlockHeaderSetter: settingHeaderDirective},
	}
	// add spec path to rest message so we can extract the requested block.
	restMessage.SpecPath = apiCont.api.Name
	parsedInput := parser.NewParsedInput()
	if overwriteReqBlock == "" {
		// Fetch requested block, it is used for data reliability
		parsedInput = parser.ParseBlockFromParams(restMessage, api.BlockParsing, api.Parsers)
	} else {
		parsedBlock, err := restMessage.ParseBlock(overwriteReqBlock)
		parsedInput.SetBlock(parsedBlock)
		if err != nil {
			utils.LavaFormatError("failed parsing block from an overwrite header", err,
				utils.LogAttr("chain", apip.spec.Name),
				utils.LogAttr("overwriteRequestedBlock", overwriteReqBlock),
			)
			parsedInput.SetBlock(spectypes.NOT_APPLICABLE)
		} else {
			parsedInput.UsedDefaultValue = false
		}
	}

	nodeMsg := newParsedChainMessage(apiCont.api, apiCollection, parsedInput, nil, &restMessage, restMessage.CheckResponseError)
	nodeMsg.applyParsedRange(parsedInput)
	apip.BaseChainParser.ExtensionParsing(apiCollection.CollectionData.AddOn, nodeMsg, extensionInfo)
	return nodeMsg, nil
}

func (apip *RestChainParser) getApiCollection(connectionType, internalPath, addon string) (*spectypes.ApiCollection, error) {
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	return apip.BaseChainParser.getApiCollection(connectionType, internalPath, addon)
}

// overwrites the base class match for a supported api
func (apip *RestChainParser) getSupportedApi(name, connectionType string) (*ApiContainer, error) {
	// Guard that the RestChainParser instance exists
	if apip == nil {
		return nil, errors.New("RestChainParser not defined")
	}

	// Acquire read lock
	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()

	// Fetch server apiCont by name
	apiCont, ok := matchSpecApiByName(name, connectionType, apip.serverApis)

	// Return an api container does not exist, return a default one
	if !ok {
		apiKey := ApiKey{Name: name, ConnectionType: connectionType, InternalPath: ""}
		return apip.defaultApiContainer(apiKey)
	}
	api := apiCont.api

	// Return an error if api is disabled
	if !api.Enabled {
		return nil, errors.New("api is disabled")
	}

	return apiCont, nil
}

// SetSpec sets the spec for the TendermintChainParser
func (apip *RestChainParser) SetSpec(spec spectypes.Spec) {
	// Guard that the TendermintChainParser instance exists
	if apip == nil {
		return
	}

	// Add a read-write lock to ensure thread safety
	apip.rwLock.Lock()
	defer apip.rwLock.Unlock()

	// extract server and tagged apis from spec
	internalPaths, serverApis, taggedApis, apiCollections, headers, verifications := getServiceApis(spec, spectypes.APIInterfaceRest)
	apip.BaseChainParser.Construct(spec, internalPaths, taggedApis, serverApis, apiCollections, headers, verifications)
}

// ChainBlockStats returns block stats from spec
// (spec.AllowedBlockLagForQosSync, spec.AverageBlockTime, spec.BlockDistanceForFinalizedData)
func (apip *RestChainParser) ChainBlockStats() (allowedBlockLagForQosSync int64, averageBlockTime time.Duration, blockDistanceForFinalizedData, blocksInFinalizationProof uint32) {
	// Guard that the JsonRPCChainParser instance exists
	if apip == nil {
		return 0, 0, 0, 0
	}

	// Acquire read lock
	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()

	// Convert average block time from int64 -> time.Duration
	averageBlockTime = time.Duration(apip.spec.AverageBlockTime) * time.Millisecond

	// Return values
	return apip.spec.AllowedBlockLagForQosSync, averageBlockTime, apip.spec.BlockDistanceForFinalizedData, apip.spec.BlocksInFinalizationProof
}

type RestChainListener struct {
	chainListenerBase
	app *fiber.App // captured during Serve so Shutdown can drain HTTP
}

// RestMethodCarriesBody reports whether the REST listener relays the request body for
// an HTTP method. POST, PUT and PATCH carry one, and the node receives the body as
// Fiber's Body() returns it: the client's bytes, decoded first when the client set a
// Content-Encoding (that header is not forwarded). Every other method is relayed
// without a body. The same set decides which requests forward the client's
// Content-Type (forwardsClientBodyHeader), so the router never forwards a Content-Type
// for bytes it did not send.
func RestMethodCarriesBody(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return true
	default:
		return false
	}
}

// restRequestPath is the client's path as the parser and the sender must both read it.
// The parser matches it against the spec with url.Parse; the sender appends it to the
// configured node URL. url.Parse reads "//cosmos/tx/v1beta1/txs" (a client whose base
// URL ends in "/") as host "cosmos" and path "/tx/v1beta1/txs", so a write fell to the
// Default read API while the node received the whole path (MAG-3970, MAG-3973). Collapsing
// the leading slashes before either side sees the path keeps the two in agreement.
func restRequestPath(fiberCtx *fiber.Ctx) string {
	return "/" + strings.TrimLeft(fiberCtx.Params("*"), "/")
}

// NewRestChainListener creates a new instance of RestChainListener
func NewRestChainListener(ctx context.Context, listenEndpoint *lavasession.RPCEndpoint,
	relaySender RelaySender, healthReporter HealthReporter,
	rpcConsumerLogs *metrics.RPCConsumerLogs,
) (chainListener *RestChainListener) {
	// Create a new instance of JsonRPCChainListener
	chainListener = &RestChainListener{
		chainListenerBase: chainListenerBase{
			endpoint:       listenEndpoint,
			relaySender:    relaySender,
			healthReporter: healthReporter,
			logger:         rpcConsumerLogs,
		},
	}

	return chainListener
}

// Serve http server for RestChainListener
func (apil *RestChainListener) Serve(ctx context.Context, cmdFlags common.ConsumerCmdFlags) {
	// Guard that the RestChainListener instance exists
	if apil == nil {
		return
	}

	// Setup HTTP Server
	// false: rest serves no websockets and registers no GET route, so the next
	// handler after the health route is the relay catch-all below. Handing an
	// upgrade on here would turn a health probe into a chain request.
	app := createAndSetupBaseAppListener(cmdFlags, apil.endpoint.HealthCheckPath, apil.healthReporter, false)
	apil.app = app

	chainID := apil.endpoint.ChainID
	// A verb the spec does not declare is a 501, not a masked 500, on both handlers.
	answerMethodNotFound := func(ctx context.Context, fiberCtx *fiber.Ctx, err error, _ *pairingtypes.RelayReply) (bool, error) {
		if !errors.Is(err, common.APINotSupportedError) {
			return false, nil
		}
		utils.LavaFormatError("api method is not supported", err, utils.LogAttr("GUID", ctx))
		return true, common.CreateRestMethodNotFoundError(fiberCtx, chainID)
	}
	errorBody := func(_ *fiber.Ctx, _ *common.RelayResult, errMasking, _ string) []byte {
		return convertToJsonError(errMasking)
	}
	testMode := common.IsTestMode(ctx)

	// Catch Post
	app.Post("/*", apil.relayHTTP(httpRelaySpec{
		testMode: testMode,
		request: func(fiberCtx *fiber.Ctx) httpRelayRequest {
			path := restRequestPath(fiberCtx)
			query := "?" + string(fiberCtx.Request().URI().QueryString())
			return httpRelayRequest{
				path: path, url: path + query, body: string(fiberCtx.Body()), connectionType: http.MethodPost,
				logMethod: http.MethodPost, logPath: path,
			}
		},
		logReceived: func(ctx context.Context, guid uint64, msgSeed, dappID string, req httpRelayRequest, headers []pairingtypes.Metadata) {
			utils.LavaFormatInfo("Consumer received a new REST POST request",
				utils.LogAttr("GUID", guid),
				utils.LogAttr(utils.KEY_REQUEST_ID, ctx),
				utils.LogAttr(utils.KEY_TASK_ID, ctx),
				utils.LogAttr(utils.KEY_TRANSACTION_ID, ctx),
				utils.LogAttr("path", req.path),
				utils.LogAttr("dappID", dappID),
				utils.LogAttr("msgSeed", msgSeed),
				utils.LogAttr("body", req.body),
				utils.LogAttr("headers", common.RedactMetadata(headers)),
			)
		},
		logModule:   "http in/out",
		answerError: answerMethodNotFound,
		errorBody:   errorBody,
	}))
	// Catch the others
	app.Use("/*", apil.relayHTTP(httpRelaySpec{
		testMode: testMode,
		request: func(fiberCtx *fiber.Ctx) httpRelayRequest {
			path := restRequestPath(fiberCtx)
			query := "?" + string(fiberCtx.Request().URI().QueryString())
			// POST has its own handler above; PUT and PATCH arrive here and carry a body too.
			body := ""
			if RestMethodCarriesBody(fiberCtx.Method()) {
				body = string(fiberCtx.Body())
			}
			return httpRelayRequest{
				path: path, url: path + query, body: body, connectionType: fiberCtx.Method(),
				logMethod: fiberCtx.Method(), logPath: path,
			}
		},
		logReceived: func(ctx context.Context, guid uint64, msgSeed, dappID string, req httpRelayRequest, headers []pairingtypes.Metadata) {
			utils.LavaFormatInfo("Consumer received a new REST non-POST request",
				utils.LogAttr("GUID", guid),
				utils.LogAttr(utils.KEY_REQUEST_ID, ctx),
				utils.LogAttr(utils.KEY_TASK_ID, ctx),
				utils.LogAttr(utils.KEY_TRANSACTION_ID, ctx),
				utils.LogAttr("path", req.path),
				utils.LogAttr("seed", msgSeed),
				utils.LogAttr("dappID", dappID),
				utils.LogAttr("body", req.body),
				utils.LogAttr("headers", common.RedactMetadata(headers)),
			)
		},
		logModule:   "http in/out",
		answerError: answerMethodNotFound,
		errorBody:   errorBody,
	}))

	apil.listen(ctx, app)
}

type RestChainProxy struct {
	BaseChainProxy
}

func NewRestChainProxy(ctx context.Context, nConns uint, rpcProviderEndpoint lavasession.RPCProviderEndpoint, chainParser ChainParser) (ChainProxy, error) {
	if err := requireNodeUrl(rpcProviderEndpoint); err != nil {
		return nil, err
	}

	validateEndpoints(rpcProviderEndpoint.NodeUrls, spectypes.APIInterfaceRest)

	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()
	nodeUrl := rpcProviderEndpoint.NodeUrls[0]
	nodeUrl.Url = strings.TrimSuffix(rpcProviderEndpoint.NodeUrls[0].Url, "/")
	rcp := &RestChainProxy{
		BaseChainProxy: BaseChainProxy{averageBlockTime: averageBlockTime, NodeUrl: rpcProviderEndpoint.NodeUrls[0], HashedNodeUrl: chainproxy.HashURL(nodeUrl.Url), ErrorHandler: newNodeErrorHandler(common.TransportREST, rpcProviderEndpoint.ChainID), ChainID: rpcProviderEndpoint.ChainID},
	}
	return rcp, nil
}

func (rcp *RestChainProxy) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	// appending hashed url
	grpc.SetTrailer(ctx, metadata.Pairs(RPCProviderNodeAddressHash, rcp.BaseChainProxy.HashedNodeUrl))

	rpcInputMessage := chainMessage.GetRPCMessage()
	nodeMessage, ok := rpcInputMessage.(*rpcInterfaceMessages.RestMessage)
	if !ok {
		return nil, utils.LavaFormatError("invalid message type in rest, failed to cast RPCInput from chainMessage", nil, traceAttrs(ctx, utils.Attribute{Key: "rpcMessage", Value: rpcInputMessage})...)
	}
	var connectionTypeSlected string = http.MethodGet
	// if ConnectionType is default value or empty we will choose http.MethodGet otherwise choosing the header type provided
	if chainMessage.GetApiCollection().CollectionData.Type != "" {
		connectionTypeSlected = chainMessage.GetApiCollection().CollectionData.Type
	}

	msgBuffer := bytes.NewBuffer(nodeMessage.Msg)
	urlPath := rcp.NodeUrl.Url + nodeMessage.Path

	// set context with timeout
	connectCtx, cancel := rcp.CapTimeoutForSend(ctx, chainMessage)
	defer cancel()

	req, err := http.NewRequestWithContext(connectCtx, connectionTypeSlected, rcp.NodeUrl.AuthConfig.AddAuthPath(urlPath), msgBuffer)
	if err != nil {
		return nil, err
	}

	// setting the content-type to be application/json instead of Go's defult http.DefaultClient
	if connectionTypeSlected == http.MethodPost || connectionTypeSlected == http.MethodPut {
		req.Header.Set("Content-Type", common.JSONContentType)
	}

	res, err := rcp.doHTTP(ctx, req, nodeMessage.GetHeaders(), nodeMessage.GetDisableErrorHandling(), chainMessage.GetApi().Name, func(header http.Header) {
		utils.LavaFormatInfo("Sending request to node from provider",
			utils.LogAttr("_method", nodeMessage.Path),
			utils.LogAttr("headers", utils.RedactHeaders(header)),
			utils.LogAttr("apiInterface", spectypes.APIInterfaceRest),
		)
	})
	if err != nil {
		return nil, err
	}

	reply := &RelayReplyWrapper{
		StatusCode: res.status,
		RelayReply: &pairingtypes.RelayReply{
			Data:     res.body,
			Metadata: convertToMetadataMapOfSlices(res.header),
		},
	}

	if strings.Split(nodeMessage.Path, "?")[0] != "/" {
		// checking if rest reply data is in json format
		err = rcp.HandleJSONFormatError(reply.RelayReply.Data)
		if err != nil {
			return nil, utils.LavaFormatError("Rest reply is neither a JSON object nor a JSON array of objects", nil, utils.Attribute{Key: "reply.Data", Value: string(reply.RelayReply.Data)})
		}
	}

	return reply, nil
}

// Shutdown drains in-flight HTTP requests and closes the listener.
// REST has no client-facing WebSockets, so no 1001 close frames are needed.
func (apil *RestChainListener) Shutdown(ctx context.Context) error {
	if apil == nil || apil.app == nil {
		return nil
	}
	return apil.app.ShutdownWithContext(ctx)
}
