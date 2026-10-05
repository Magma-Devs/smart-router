package chainlib

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// GraphQL is served as its own api_interface rather than as a REST POST collection because a
// REST api's identity is its URL path, and every GraphQL operation on a chain shares one path.
// Under a REST spec the api name would be "/graphql" for every request ever sent, and the four
// subsystems keyed on api name would all collapse onto that single key: one cross-validation
// policy for the entire surface, a stateful-write guard that cannot see a mutation, a GET_BLOCKNUM
// parse directive that matches arbitrary user traffic and feeds it to the endpoint tip store, and
// one set of compute units for every operation.
//
// Deriving identity from the request body instead — the way JSON-RPC already derives its method —
// gives each GraphQL root field its own api, and all four subsystems become correct without
// changing anything outside this interface.

type GraphQLChainParser struct {
	BaseChainParser
}

// NewGraphQLChainParser creates a new instance of GraphQLChainParser
func NewGraphQLChainParser() (chainParser *GraphQLChainParser, err error) {
	parser := &GraphQLChainParser{}
	// GraphQL-over-HTTP is POST only, so there is no websocket route to verify. Sui's schema
	// does declare a Subscription type as of server 1.80.1 (checkpoints, transactions, events),
	// but subscriptions need a websocket or SSE transport that GraphQL-over-HTTP POST cannot
	// carry, and the production streaming path on this chain is gRPC, which already has its
	// own SUBSCRIBE directives.
	parser.skipWebsocketVerification = true
	return parser, nil
}

func (apip *GraphQLChainParser) GetUniqueName() string {
	return "graphql_chain_parser"
}

func (apip *GraphQLChainParser) getApiCollection(connectionType, internalPath, addon string) (*spectypes.ApiCollection, error) {
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	return apip.BaseChainParser.getApiCollection(connectionType, internalPath, addon)
}

func (apip *GraphQLChainParser) getSupportedApi(name, connectionType, internalPath string) (*ApiContainer, error) {
	if apip == nil {
		return nil, errors.New("GraphQLChainParser not defined")
	}
	apiKey := ApiKey{Name: name, ConnectionType: connectionType, InternalPath: internalPath}
	return apip.BaseChainParser.getSupportedApi(apiKey)
}

// CraftMessage crafts a chain message from a spec parse directive, for verifications and the
// head poll. The directive's function_template is the GraphQL request body.
func (apip *GraphQLChainParser) CraftMessage(parsing *spectypes.ParseDirective, connectionType string, craftData *CraftData, metadata []pairingtypes.Metadata) (ChainMessageForSend, error) {
	if craftData != nil {
		// GraphQL has no request path, so craftData.Path is ignored: the operation is named in
		// the body, which is what ParseMsg resolves the api from.
		chainMessage, err := apip.ParseMsg(craftData.InternalPath, craftData.Data, craftData.ConnectionType, metadata, extensionslib.ExtensionInfo{LatestBlock: 0})
		if err != nil {
			return nil, err
		}
		// A directive's template must invoke the operation the directive names. When it does
		// not, ParseMsg resolves the api from the template's own root field and the crafted
		// message carries a different api than the caller asked for — and for a tagged
		// directive, GetParseDirective then attaches nothing. The poll path happens not to read
		// the directive back off the crafted message today, so a mismatch would be invisible:
		// refuse it here rather than let a spec typo degrade silently.
		if parsing != nil && parsing.ApiName != "" && chainMessage.GetApi().Name != parsing.ApiName {
			return nil, utils.LavaFormatError("graphql parse directive template does not invoke the operation it names", nil,
				utils.LogAttr("apiName", parsing.ApiName),
				utils.LogAttr("templateOperation", chainMessage.GetApi().Name),
				utils.LogAttr("functionTag", parsing.FunctionTag),
			)
		}
		chainMessage.AppendHeader(metadata)
		return chainMessage, nil
	}

	apiCont, err := apip.getSupportedApi(parsing.ApiName, connectionType, "")
	if err != nil {
		return nil, err
	}
	apiCollection, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
	if err != nil {
		return nil, err
	}

	// A directive with no template has no request body to send. GraphQL names its operation in
	// the body and nowhere else, so unlike gRPC — where an empty payload is a legitimate
	// no-argument call — an empty template here is a spec gap, not an empty request.
	if parsing.FunctionTemplate == "" {
		return nil, utils.LavaFormatError("graphql parse directive has no function template", nil,
			utils.LogAttr("apiName", parsing.ApiName),
			utils.LogAttr("functionTag", parsing.FunctionTag),
		)
	}

	graphqlMessage, err := rpcInterfaceMessages.ParseGraphQLMsg([]byte(parsing.FunctionTemplate))
	if err != nil {
		return nil, utils.LavaFormatError("failed parsing graphql function template", err,
			utils.LogAttr("apiName", parsing.ApiName),
			utils.LogAttr("functionTemplate", parsing.FunctionTemplate),
		)
	}
	graphqlMessage.BaseMessage = chainproxy.BaseMessage{Headers: metadata}

	parsedInput := parser.NewParsedInput()
	parsedInput.SetBlock(spectypes.NOT_APPLICABLE)
	return apip.newChainMessage(apiCont.api, parsedInput, nil, graphqlMessage, apiCollection), nil
}

// ParseMsg parses a GraphQL request body into a chain message.
//
// A single GraphQL request may select several root fields — `{ chainIdentifier checkpoint {...} }`
// is one request naming two operations — which is structurally a JSON-RPC batch and is combined
// the same way: compute units summed, categories combined to the strictest, the requested block
// taken across all roots. That combination is what keeps a mutation from riding in on a read's
// policy: a request pairing executeTransaction with a query resolves to a stateful api, so the
// cross-validation stateful guard still sees the write.
func (apip *GraphQLChainParser) ParseMsg(url string, data []byte, connectionType string, metadata []pairingtypes.Metadata, extensionInfo extensionslib.ExtensionInfo) (ChainMessage, error) {
	if apip == nil {
		return nil, errors.New("GraphQLChainParser not defined")
	}

	graphqlMessage, err := rpcInterfaceMessages.ParseGraphQLMsg(data)
	if err != nil {
		return nil, err
	}

	internalPath := ""
	if apip.isValidInternalPath(url) {
		internalPath = url
	}

	var combinedApi *spectypes.Api
	var apiCollection *spectypes.ApiCollection
	var latestRequestedBlock, earliestRequestedBlock int64 = 0, 0
	blockHashes := []string{}
	parsedDefault := true

	for index, rootField := range graphqlMessage.RootFields {
		apiCont, err := apip.getSupportedApi(rootField.Name, connectionType, internalPath)
		if err != nil {
			utils.LavaFormatError("getSupportedApi graphql failed", err,
				utils.LogAttr("operation", rootField.Name),
				utils.LogAttr("connectionType", connectionType),
				utils.LogAttr("internalPath", internalPath),
			)
			return nil, err
		}

		apiCollectionForRootField, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
		if err != nil {
			return nil, utils.LavaFormatError("could not find the graphql api collection", err,
				utils.LogAttr("connectionType", connectionType),
				utils.LogAttr("operation", apiCont.api.Name),
			)
		}

		filteredMetadata, overwriteReqBlock, _ := apip.HandleHeaders(metadata, apiCollectionForRootField, spectypes.Header_pass_send)
		settingHeaderDirective, _, _ := apip.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)
		graphqlMessage.BaseMessage = chainproxy.BaseMessage{Headers: filteredMetadata, LatestBlockHeaderSetter: settingHeaderDirective}

		parsedInput := parser.NewParsedInput()
		if overwriteReqBlock == "" {
			// Parse the block against this root field's own arguments, so a multi-root request
			// does not resolve every root's block from the first root's arguments.
			rootFieldInput := graphqlMessage.RootFieldInput(index)
			parsedInput = parser.ParseBlockFromParams(rootFieldInput, apiCont.api.BlockParsing, apiCont.api.Parsers)
			if hashes, err := parsedInput.GetBlockHashes(); err == nil {
				blockHashes = append(blockHashes, hashes...)
			}
			if !parsedInput.UsedDefaultValue {
				parsedDefault = false
			}
		} else {
			parsedBlock, err := graphqlMessage.ParseBlock(overwriteReqBlock)
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

		parsedBlock := parsedInput.GetBlock()
		if index == 0 {
			combinedApi = apiCont.api
			apiCollection = apiCollectionForRootField
			latestRequestedBlock = parsedBlock
			earliestRequestedBlock = parsedBlock
			continue
		}

		if apiCollectionForRootField.CollectionData.AddOn != "" && apiCollectionForRootField.CollectionData.AddOn != apiCollection.CollectionData.AddOn {
			if apiCollection.CollectionData.AddOn != "" {
				return nil, utils.LavaFormatError("unable to parse a graphql request selecting root fields from multiple addons", nil,
					utils.LogAttr("first addon", apiCollection.CollectionData.AddOn),
					utils.LogAttr("second addon", apiCollectionForRootField.CollectionData.AddOn),
				)
			}
			apiCollection = apiCollectionForRootField
		}

		category := combinedApi.GetCategory().Combine(apiCont.api.GetCategory())
		combinedApi = &spectypes.Api{
			Enabled:      combinedApi.Enabled && apiCont.api.Enabled,
			Name:         combinedApi.Name + rpcInterfaceMessages.GraphQLMethodSeparator + apiCont.api.Name,
			ComputeUnits: combinedApi.ComputeUnits + apiCont.api.ComputeUnits,
			Category:     category,
			BlockParsing: spectypes.BlockParser{
				ParserArg:    []string{},
				ParserFunc:   spectypes.PARSER_FUNC_EMPTY,
				DefaultValue: "",
				Encoding:     "",
			},
		}
		latestRequestedBlock, earliestRequestedBlock = CompareRequestedBlockInBatch(latestRequestedBlock, earliestRequestedBlock, parsedBlock)
	}

	// ParseGraphQLMsg rejects an operation that selects no root field, so this is unreachable
	// today. It is guarded anyway because the loop below it dereferences both values, and the
	// guarantee lives in another file.
	if combinedApi == nil || apiCollection == nil {
		return nil, utils.LavaFormatError("graphql request resolved to no api", nil,
			utils.LogAttr("method", graphqlMessage.GetMethod()),
		)
	}

	parsedInput := parser.NewParsedInput()
	parsedInput.SetBlock(latestRequestedBlock)
	parsedInput.UsedDefaultValue = parsedDefault

	var earliest *int64
	if len(graphqlMessage.RootFields) > 1 {
		earliest = &earliestRequestedBlock
	}
	nodeMsg := apip.newChainMessage(combinedApi, parsedInput, blockHashes, graphqlMessage, apiCollection)
	if earliest != nil {
		nodeMsg.earliestRequestedBlock = *earliest
	}
	apip.BaseChainParser.ExtensionParsing(apiCollection.CollectionData.AddOn, nodeMsg, extensionInfo)
	return nodeMsg, nil
}

func (*GraphQLChainParser) newChainMessage(api *spectypes.Api, parsedInput *parser.ParsedInput, requestedBlockHashes []string, graphqlMessage *rpcInterfaceMessages.GraphQLMessage, apiCollection *spectypes.ApiCollection) *baseChainMessageContainer {
	if requestedBlockHashes == nil {
		requestedBlockHashes, _ = parsedInput.GetBlockHashes()
	}
	return &baseChainMessageContainer{
		api:                      api,
		msg:                      graphqlMessage,
		latestRequestedBlock:     parsedInput.GetBlock(),
		requestedBlockHashes:     requestedBlockHashes,
		apiCollection:            apiCollection,
		resultErrorParsingMethod: graphqlMessage.CheckResponseError,
		// A multi-root request has a combined api name that matches no parse directive, so
		// GetParseDirective returns nil for it — which is the safe direction: a request that
		// merely includes the GET_BLOCKNUM operation alongside others must not be recorded as a
		// tip observation.
		parseDirective:   GetParseDirective(api, apiCollection),
		usedDefaultValue: parsedInput.UsedDefaultValue,
	}
}

// SetSpec sets the spec for the GraphQLChainParser
func (apip *GraphQLChainParser) SetSpec(spec spectypes.Spec) {
	if apip == nil {
		return
	}
	apip.rwLock.Lock()
	defer apip.rwLock.Unlock()

	internalPaths, serverApis, taggedApis, apiCollections, headers, verifications := getServiceApis(spec, spectypes.APIInterfaceGraphQL)
	apip.BaseChainParser.Construct(spec, internalPaths, taggedApis, serverApis, apiCollections, headers, verifications)
}

// ChainBlockStats returns block stats from spec
// (spec.AllowedBlockLagForQosSync, spec.AverageBlockTime, spec.BlockDistanceForFinalizedData)
func (apip *GraphQLChainParser) ChainBlockStats() (allowedBlockLagForQosSync int64, averageBlockTime time.Duration, blockDistanceForFinalizedData, blocksInFinalizationProof uint32) {
	if apip == nil {
		return 0, 0, 0, 0
	}
	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()

	averageBlockTime = time.Duration(apip.spec.AverageBlockTime) * time.Millisecond
	return apip.spec.AllowedBlockLagForQosSync, averageBlockTime, apip.spec.BlockDistanceForFinalizedData, apip.spec.BlocksInFinalizationProof
}

type GraphQLChainListener struct {
	endpoint         *lavasession.RPCEndpoint
	relaySender      RelaySender
	healthReporter   HealthReporter
	logger           *metrics.RPCConsumerLogs
	listeningAddress atomic.Pointer[string]
	app              *fiber.App // captured during Serve so Shutdown can drain HTTP
}

// NewGraphQLChainListener creates a new instance of GraphQLChainListener
func NewGraphQLChainListener(ctx context.Context, listenEndpoint *lavasession.RPCEndpoint,
	relaySender RelaySender, healthReporter HealthReporter,
	rpcConsumerLogs *metrics.RPCConsumerLogs,
) (chainListener *GraphQLChainListener) {
	return &GraphQLChainListener{
		endpoint:       listenEndpoint,
		relaySender:    relaySender,
		healthReporter: healthReporter,
		logger:         rpcConsumerLogs,
	}
}

// Serve http server for GraphQLChainListener
func (apil *GraphQLChainListener) Serve(ctx context.Context, cmdFlags common.ConsumerCmdFlags) {
	if apil == nil {
		return
	}

	// false: this listener serves no websockets and registers no GET route, so handing an
	// upgrade on here would turn a health probe into a chain request. GraphQL subscriptions
	// would need their own transport; gRPC carries streaming on this chain.
	app := createAndSetupBaseAppListener(cmdFlags, apil.endpoint.HealthCheckPath, apil.healthReporter, false)
	apil.app = app

	chainID := apil.endpoint.ChainID
	apiInterface := apil.endpoint.ApiInterface

	handlerPost := func(fiberCtx *fiber.Ctx) error {
		fiberCtx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSONCharsetUTF8)
		startTime := time.Now()

		msgSeed := apil.logger.GetMessageSeed()
		path := "/" + fiberCtx.Params("*")

		metadataValues := fiberCtx.GetReqHeaders()
		graphqlHeaders := convertToMetadataMap(metadataValues)
		ctx, cancel := context.WithCancel(context.Background())
		ctx = utils.WithUniqueIdentifier(ctx, utils.GenerateUniqueIdentifier())
		ctx = utils.ExtractWantedHeadersFromCachedMap(metadataValues, ctx)
		defer cancel() // incase there's a problem make sure to cancel the connection
		guid, found := utils.GetUniqueIdentifier(ctx)
		if found {
			msgSeed = strconv.FormatUint(guid, 10)
		}
		dappID := extractDappIDFromFiberContext(fiberCtx)
		analytics := metrics.NewRelayAnalytics(dappID, chainID, apiInterface)
		analytics.SetProcessingTimestampBeforeRelay(startTime)
		userIp := GetHeaderFromCachedMap(metadataValues, common.IP_FORWARDING_HEADER_NAME, fiberCtx.IP())
		requestBody := string(fiberCtx.Body())
		utils.LavaFormatInfo("Consumer received a new GraphQL request",
			utils.LogAttr("GUID", guid),
			utils.LogAttr(utils.KEY_REQUEST_ID, ctx),
			utils.LogAttr(utils.KEY_TASK_ID, ctx),
			utils.LogAttr(utils.KEY_TRANSACTION_ID, ctx),
			utils.LogAttr("path", path),
			utils.LogAttr("dappID", dappID),
			utils.LogAttr("msgSeed", msgSeed),
			utils.LogAttr("body", requestBody),
			utils.LogAttr("headers", common.RedactMetadata(graphqlHeaders)),
		)

		// The url argument is the internal path, not a route: a GraphQL endpoint serves every
		// operation on one path, and the operation is named in the body.
		relayResult, err := apil.relaySender.SendRelay(ctx, "", requestBody, http.MethodPost, dappID, userIp, analytics, graphqlHeaders)
		reply := relayResult.GetReply()
		go apil.logger.AddMetricForHttp(analytics, err, metadataValues)
		if err != nil {
			errMasking := apil.logger.GetUniqueGuidResponseForError(err, msgSeed)
			apil.logger.LogRequestAndResponse("graphql in/out", true, http.MethodPost, path, requestBody, errMasking, msgSeed, time.Since(startTime), err)
			if relayResult.GetStatusCode() != 0 {
				fiberCtx.Status(relayResult.StatusCode)
			} else {
				fiberCtx.Status(fiber.StatusInternalServerError)
			}
			return addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), convertToJsonError(errMasking))
		}
		apil.logger.LogRequestAndResponse("graphql in/out", false, http.MethodPost, path, requestBody, string(reply.Data), msgSeed, time.Since(startTime), nil)
		if relayResult.GetStatusCode() != 0 {
			fiberCtx.Status(relayResult.StatusCode)
		}
		return addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), reply.Data)
	}

	// GraphQL-over-HTTP is POST only here. A GET carries its query in the URL, which is the very
	// identity collapse this interface exists to avoid, so it is refused rather than served.
	app.Post("/*", handlerPost)
	app.Use("/*", func(fiberCtx *fiber.Ctx) error {
		fiberCtx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSONCharsetUTF8)
		fiberCtx.Status(fiber.StatusMethodNotAllowed)
		return addHeadersAndSendBytes(fiberCtx, nil, convertToJsonError("graphql requests must be sent as HTTP POST"))
	})

	addrChannel := make(chan string)
	addrChannelSafe := common.NewSafeChannelSender(ctx, addrChannel)
	go func() {
		addr := <-addrChannel
		apil.listeningAddress.Store(&addr)
	}()

	ListenWithRetry(ctx, app, apil.endpoint.NetworkAddress, addrChannelSafe)
}

func (apil *GraphQLChainListener) GetListeningAddress() string {
	if p := apil.listeningAddress.Load(); p != nil {
		return *p
	}
	return ""
}

// Shutdown drains in-flight HTTP requests and closes the listener.
// GraphQL has no client-facing WebSockets, so no 1001 close frames are needed.
func (apil *GraphQLChainListener) Shutdown(ctx context.Context) error {
	if apil == nil || apil.app == nil {
		return nil
	}
	return apil.app.ShutdownWithContext(ctx)
}

type GraphQLChainProxy struct {
	BaseChainProxy
	httpClient *http.Client
}

func NewGraphQLChainProxy(ctx context.Context, nConns uint, rpcProviderEndpoint lavasession.RPCProviderEndpoint, chainParser ChainParser) (ChainProxy, error) {
	if len(rpcProviderEndpoint.NodeUrls) == 0 {
		return nil, utils.LavaFormatError("rpcProviderEndpoint.NodeUrl list is empty missing node url", nil,
			utils.Attribute{Key: "chainID", Value: rpcProviderEndpoint.ChainID},
			utils.Attribute{Key: "ApiInterface", Value: rpcProviderEndpoint.ApiInterface},
		)
	}

	validateEndpoints(rpcProviderEndpoint.NodeUrls, spectypes.APIInterfaceGraphQL)

	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()
	nodeUrl := rpcProviderEndpoint.NodeUrls[0]
	nodeUrl.Url = strings.TrimSuffix(rpcProviderEndpoint.NodeUrls[0].Url, "/")
	return &GraphQLChainProxy{
		BaseChainProxy: BaseChainProxy{
			averageBlockTime: averageBlockTime,
			NodeUrl:          nodeUrl,
			HashedNodeUrl:    chainproxy.HashURL(nodeUrl.Url),
			ErrorHandler:     &RestErrorHandler{chainFamily: common.GetChainFamilyOrDefault(rpcProviderEndpoint.ChainID), chainID: rpcProviderEndpoint.ChainID},
			ChainID:          rpcProviderEndpoint.ChainID,
		},
	}, nil
}

// SendNodeMsg posts the GraphQL document to the endpoint's base URL.
//
// Unlike REST, the URL never varies: the operation is named in the body, so every request on a
// chain goes to the one endpoint URL the spec configures.
func (gcp *GraphQLChainProxy) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	if gcp.httpClient == nil {
		gcp.httpClient = common.OptimizedHttpClient()
	}
	httpClient := gcp.httpClient

	// appending hashed url
	grpc.SetTrailer(ctx, metadata.Pairs(RPCProviderNodeAddressHash, gcp.BaseChainProxy.HashedNodeUrl))

	rpcInputMessage := chainMessage.GetRPCMessage()
	nodeMessage, ok := rpcInputMessage.(*rpcInterfaceMessages.GraphQLMessage)
	if !ok {
		return nil, utils.LavaFormatError("invalid message type in graphql, failed to cast RPCInput from chainMessage", nil,
			utils.Attribute{Key: "GUID", Value: ctx},
			utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx},
			utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx},
			utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx},
			utils.Attribute{Key: "rpcMessage", Value: rpcInputMessage},
		)
	}

	connectCtx, cancel := gcp.CapTimeoutForSend(ctx, chainMessage)
	defer cancel()

	msgBuffer := bytes.NewBuffer(nodeMessage.Msg)
	req, err := http.NewRequestWithContext(connectCtx, http.MethodPost, gcp.NodeUrl.AuthConfig.AddAuthPath(gcp.NodeUrl.Url), msgBuffer)
	if err != nil {
		return nil, err
	}

	// The router prints the request body itself (it forwards the caller's document verbatim as
	// JSON), so the content type is ours to declare, as it is for JSON-RPC.
	req.Header.Set("Content-Type", "application/json")

	for _, header := range nodeMessage.GetHeaders() {
		if header.Value == "" {
			req.Header.Del(header.Name)
		} else {
			req.Header.Set(header.Name, header.Value)
		}
	}
	gcp.NodeUrl.SetAuthHeaders(ctx, req.Header.Set)
	gcp.NodeUrl.SetIpForwardingIfNecessary(ctx, req.Header.Set)

	utils.LavaFormatInfo("Sending request to node from provider",
		utils.LogAttr("_method", nodeMessage.GetMethod()),
		utils.LogAttr("headers", utils.RedactHeaders(req.Header)),
		utils.LogAttr("apiInterface", spectypes.APIInterfaceGraphQL),
	)

	res, err := httpClient.Do(req)
	if res != nil {
		// resp can be non nil on error
		grpc.SetTrailer(ctx, metadata.Pairs(common.StatusCodeMetadataKey, strconv.Itoa(res.StatusCode)))
	}
	if err != nil {
		if parsedError := gcp.HandleNodeError(ctx, err); parsedError != nil {
			return nil, parsedError
		}
		return nil, err
	}
	if res.Body != nil {
		defer res.Body.Close()
	}

	err = gcp.HandleStatusError(res.StatusCode, nodeMessage.GetDisableErrorHandling())
	if err != nil {
		err = common.WithRetryAfter(err, res.Header, time.Now())
		return nil, utils.LavaFormatWarning("Received invalid status code", err,
			utils.Attribute{Key: "Status Code", Value: res.StatusCode},
			utils.Attribute{Key: "chainID", Value: gcp.BaseChainProxy.ChainID},
			utils.Attribute{Key: "apiName", Value: chainMessage.GetApi().Name},
		)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	reply := &RelayReplyWrapper{
		StatusCode: res.StatusCode,
		RelayReply: &pairingtypes.RelayReply{
			Data:     body,
			Metadata: convertToMetadataMapOfSlices(res.Header),
		},
	}

	// Every GraphQL reply is a JSON document, errors included — the interface has no other
	// representation — so a non-JSON body did not come from a GraphQL endpoint.
	if err := gcp.HandleJSONFormatError(reply.RelayReply.Data); err != nil {
		return nil, utils.LavaFormatError("GraphQL reply is neither a JSON object nor a JSON array of objects", nil,
			utils.Attribute{Key: "reply.Data", Value: string(reply.RelayReply.Data)},
		)
	}

	return reply, nil
}
