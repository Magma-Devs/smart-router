package chainlib

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	dyncodec "github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/version"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc/status"
)

const GRPCStatusCodeOnFailedMessages = 32

type GrpcNodeErrorResponse struct {
	ErrorMessage string `json:"error_message"`
	ErrorCode    uint32 `json:"error_code"`
}

type GrpcChainParser struct {
	BaseChainParser

	registry *dyncodec.Registry
	codec    *dyncodec.Codec

	// descriptorConfig records which node-url's grpc-config produced registry/codec,
	// so a second node-url asking for a different one is caught rather than silently
	// winning. See setupForProvider.
	descriptorConfig *grpcDescriptorConfig
}

// grpcDescriptorConfig is the descriptor resolution a single node-url asks for.
type grpcDescriptorConfig struct {
	source string
	path   string
}

func (c grpcDescriptorConfig) String() string {
	if c.path == "" {
		return c.source
	}
	return c.source + " (" + c.path + ")"
}

// NewGrpcChainParser creates a new instance of GrpcChainParser
func NewGrpcChainParser() (chainParser *GrpcChainParser, err error) {
	parser := &GrpcChainParser{}
	parser.skipWebsocketVerification = SkipWebsocketVerificationDefault
	return parser, nil
}

// cloneForValidation returns a *GrpcChainParser with isolated registry/codec
// fields so callers (one-shot verification flows) can safely pass the result
// into NewGrpcChainProxy → setupForProvider without mutating the live parser.
//
// Read-only-after-init data on BaseChainParser is aliased — including the
// inner contents of `spec` (a protobuf with nested slices/maps), which is
// copied by value but whose nested fields remain shared. This is sound only
// because the live serving path treats spec as immutable post-init; the rare
// SetSpec / SetPolicy / UpdateBlockTime mutators run at init or at known
// quiescent moments.
//
// The new BaseChainParser is constructed via struct literal, so its rwLock
// is a fresh zero-value — no mutex is copied. Note the trade-off: because the
// clone has a *separate* mutex from the live parser, the two cannot
// synchronize. If a live-path SetSpec / UpdateBlockTime were to race with a
// validation reader on the clone, the clone could observe a torn write of
// nested spec fields. We accept that risk since the alternative — sharing the
// parser instance — corrupts the live registry/codec when validation's bounded
// context is cancelled, panicking gRPC relays on a nil connector.
//
// registry/codec are aliased initially; setupForProvider on the clone will
// replace the *clone's* fields, leaving the live parser untouched.
//
// descriptorConfig is deliberately NOT carried over: it records which node-url
// produced the clone's registry, and the clone is built for a verification
// endpoint whose node-url set is chosen by the caller. Starting it at nil lets
// the clone record what it is actually given, and still catches a conflict
// within that set.
func (apip *GrpcChainParser) cloneForValidation() *GrpcChainParser {
	return &GrpcChainParser{
		BaseChainParser: BaseChainParser{
			internalPaths:     apip.internalPaths,
			taggedApis:        apip.taggedApis,
			spec:              apip.spec,
			serverApis:        apip.serverApis,
			apiCollections:    apip.apiCollections,
			headers:           apip.headers,
			verifications:     apip.verifications,
			allowedAddons:     apip.allowedAddons,
			allowedExtensions: apip.allowedExtensions,
			extensionParser:   apip.extensionParser,
			active:            apip.active,

			// Carried, not re-seeded from the package default: the clone must verify
			// under the same ws policy as the parser it stands in for.
			skipWebsocketVerification: apip.SkipWebsocketVerification(),
		},
		registry: apip.registry,
		codec:    apip.codec,
	}
}

func (bcp *GrpcChainParser) GetUniqueName() string {
	return "grpc_chain_parser"
}

func (apip *GrpcChainParser) getApiCollection(connectionType, internalPath, addon string) (*spectypes.ApiCollection, error) {
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	return apip.BaseChainParser.getApiCollection(connectionType, internalPath, addon)
}

func (apip *GrpcChainParser) getSupportedApi(name, connectionType string) (*ApiContainer, error) {
	// Guard that the GrpcChainParser instance exists
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	apiKey := ApiKey{Name: name, ConnectionType: connectionType}
	return apip.BaseChainParser.getSupportedApi(apiKey)
}

func (apip *GrpcChainParser) setupForConsumer(relayer grpcproxy.ProxyCallBack) {
	apip.registry = dyncodec.NewRegistry(dyncodec.NewRelayerRemote(relayer))
	apip.codec = dyncodec.NewCodec(apip.registry)
}

// setupForProvider builds the registry that backs GrpcMessage.dynamicResolve —
// the path GetParams takes for binary-proto requests, and therefore what block
// parsing depends on. It honours grpc-config for the same reason SendNodeMsg does:
// a node that does not serve reflection would otherwise boot and then fail here,
// at parse time, instead of at connect time (MAG-2350).
func (apip *GrpcChainParser) setupForProvider(reflectionConnection *grpc.ClientConn, grpcConfig *common.GrpcConfig) error {
	// registry/codec are per chain; grpc-config is per node-url. newChainRouter
	// builds one proxy per node-url batch entry and hands every one of them THIS
	// parser, so each call overwrites the last — and since the batch is a map, "last"
	// is whatever Go's randomized iteration order picks that boot. That was benign
	// while every iteration produced an equivalent reflection registry; now that the
	// registry follows the node-url's grpc-config, a chain whose gRPC node-urls
	// disagree would parse blocks against a different descriptor set on each restart.
	// Refuse the config rather than pick one of them at random (MAG-2350).
	wanted := grpcDescriptorConfig{source: grpcConfig.GetDescriptorSource(), path: grpcConfig.DescriptorSetPath}
	if apip.descriptorConfig != nil && *apip.descriptorConfig != wanted {
		return utils.LavaFormatError("conflicting gRPC descriptor-source across a chain's node-urls", nil,
			utils.LogAttr("configured", apip.descriptorConfig.String()),
			utils.LogAttr("conflicting", wanted.String()),
			utils.LogAttr("resolution", "block parsing resolves through one registry per chain, so every gRPC node-url on a chain must declare the same descriptor-source and descriptor-set-path"))
	}

	var remote dyncodec.ProtoFileRegistry = dyncodec.NewGRPCReflectionProtoFileRegistryFromConn(reflectionConnection)
	remote, err := dyncodec.ProtoFileRegistryForGrpcConfig(grpcConfig, remote)
	if err != nil {
		return err
	}
	apip.registry = dyncodec.NewRegistry(remote)
	apip.codec = dyncodec.NewCodec(apip.registry)
	apip.descriptorConfig = &wanted
	return nil
}

func (apip *GrpcChainParser) CraftMessage(parsing *spectypes.ParseDirective, connectionType string, craftData *CraftData, metadata []pairingtypes.Metadata) (ChainMessageForSend, error) {
	if craftData != nil {
		chainMessage, err := apip.ParseMsg(craftData.Path, craftData.Data, craftData.ConnectionType, metadata, extensionslib.ExtensionInfo{LatestBlock: 0})
		if err == nil {
			chainMessage.AppendHeader(metadata)
		}
		return chainMessage, err
	}

	grpcMessage := &rpcInterfaceMessages.GrpcMessage{
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
	parsedInput := &parser.ParsedInput{}
	parsedInput.SetBlock(spectypes.NOT_APPLICABLE)
	return apip.newChainMessage(apiCont.api, parsedInput, grpcMessage, apiCollection), nil
}

// HydrateGrpcResponseParsing attaches the protobuf method descriptor (and a JSON formatter built
// from it) to a crafted gRPC chain message so its BINARY-protobuf response can be parsed.
//
// The chain proxy's relay path resolves the descriptor via live reflection during the send
// (see the SendNodeMsg path) and calls SetParsingData itself. But a caller that sends a gRPC poll
// through a lavasession.DirectRPCConnection — the per-endpoint ChainTracker poller — gets the
// descriptor resolved INSIDE the connection (it caches it and exposes it via GetCachedMethodDescriptor)
// and a raw protobuf response back, so the crafted message has no descriptor. This wires the
// connection's descriptor into that message.
//
// The formatter is grpcurl FormatJSON — the SAME formatter the relay path uses (RequestParserAndFormatter
// above), so the JSON it emits has identical (camelCase) field names and the spec's result_parsing
// parser_arg navigation behaves identically on poll and relay responses.
func HydrateGrpcResponseParsing(chainMessage ChainMessageForSend, methodDescriptor *desc.MethodDescriptor) error {
	if methodDescriptor == nil {
		return utils.LavaFormatError("HydrateGrpcResponseParsing: nil method descriptor", nil)
	}
	grpcMessage, ok := chainMessage.GetRPCMessage().(*rpcInterfaceMessages.GrpcMessage)
	if !ok {
		return utils.LavaFormatError("HydrateGrpcResponseParsing: chain message is not a gRPC message", nil)
	}
	descriptorSource, err := grpcurl.DescriptorSourceFromFileDescriptors(methodDescriptor.GetFile())
	if err != nil {
		return utils.LavaFormatError("HydrateGrpcResponseParsing: failed building descriptor source", err)
	}
	_, formatter, err := grpcurl.RequestParserAndFormatter(grpcurl.FormatJSON, descriptorSource, nil, grpcurl.FormatOptions{
		EmitJSONDefaultFields: false,
		IncludeTextSeparator:  false,
		AllowUnknownFields:    true,
	})
	if err != nil {
		return utils.LavaFormatError("HydrateGrpcResponseParsing: failed building formatter", err)
	}
	grpcMessage.SetParsingData(methodDescriptor, formatter)
	return nil
}

// ParseMsg parses message data into chain message object
func (apip *GrpcChainParser) ParseMsg(url string, data []byte, connectionType string, metadata []pairingtypes.Metadata, extensionInfo extensionslib.ExtensionInfo) (ChainMessage, error) {
	// Guard that the GrpcChainParser instance exists
	if apip == nil {
		return nil, errors.New("GrpcChainParser not defined")
	}

	// Check API is supported and save it in nodeMsg.
	apiCont, err := apip.getSupportedApi(url, connectionType)
	if err != nil {
		return nil, utils.LavaFormatError("failed to getSupportedApi gRPC", err, utils.LogAttr("url", url), utils.LogAttr("connectionType", connectionType))
	}

	apiCollection, err := apip.getApiCollection(connectionType, apiCont.collectionKey.InternalPath, apiCont.collectionKey.Addon)
	if err != nil {
		return nil, utils.LavaFormatError("failed to getApiCollection gRPC", err)
	}

	// handle headers
	metadata, overwriteReqBlock, _ := apip.HandleHeaders(metadata, apiCollection, spectypes.Header_pass_send)

	settingHeaderDirective, _, _ := apip.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)

	// Construct grpcMessage
	grpcMessage := rpcInterfaceMessages.GrpcMessage{
		Msg:         data,
		Path:        url,
		Codec:       apip.codec,
		Registry:    apip.registry,
		BaseMessage: chainproxy.BaseMessage{Headers: metadata, LatestBlockHeaderSetter: settingHeaderDirective},
	}

	// // Fetch requested block, it is used for data reliability
	// // Extract default block parser
	api := apiCont.api
	parsedInput := parser.NewParsedInput()
	if overwriteReqBlock == "" {
		parsedInput = parser.ParseBlockFromParams(grpcMessage, api.BlockParsing, api.Parsers)
	} else {
		parsedBlock, err := grpcMessage.ParseBlock(overwriteReqBlock)
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

	nodeMsg := apip.newChainMessage(apiCont.api, parsedInput, &grpcMessage, apiCollection)
	apip.BaseChainParser.ExtensionParsing(apiCollection.CollectionData.AddOn, nodeMsg, extensionInfo)
	return nodeMsg, nil
}

func (*GrpcChainParser) newChainMessage(api *spectypes.Api, parsedInput *parser.ParsedInput, grpcMessage *rpcInterfaceMessages.GrpcMessage, apiCollection *spectypes.ApiCollection) *baseChainMessageContainer {
	requestedBlock := parsedInput.GetBlock()
	requestedHashes, _ := parsedInput.GetBlockHashes()
	nodeMsg := &baseChainMessageContainer{
		api:                      api,
		msg:                      grpcMessage, // setting the grpc message as a pointer so we can set descriptors for parsing
		latestRequestedBlock:     requestedBlock,
		requestedBlockHashes:     requestedHashes,
		apiCollection:            apiCollection,
		resultErrorParsingMethod: grpcMessage.CheckResponseError,
		parseDirective:           GetParseDirective(api, apiCollection),
		usedDefaultValue:         parsedInput.UsedDefaultValue,
	}
	return nodeMsg
}

// SetSpec sets the spec for the GrpcChainParser
func (apip *GrpcChainParser) SetSpec(spec spectypes.Spec) {
	// Guard that the GrpcChainParser instance exists
	if apip == nil {
		return
	}

	// Add a read-write lock to ensure thread safety
	apip.rwLock.Lock()
	defer apip.rwLock.Unlock()

	// extract server and tagged apis from spec
	internalPaths, serverApis, taggedApis, apiCollections, headers, verifications := getServiceApis(spec, spectypes.APIInterfaceGrpc)
	apip.BaseChainParser.Construct(spec, internalPaths, taggedApis, serverApis, apiCollections, headers, verifications)
}

// ChainBlockStats returns block stats from spec
// (spec.AllowedBlockLagForQosSync, spec.AverageBlockTime, spec.BlockDistanceForFinalizedData)
func (apip *GrpcChainParser) ChainBlockStats() (allowedBlockLagForQosSync int64, averageBlockTime time.Duration, blockDistanceForFinalizedData, blocksInFinalizationProof uint32) {
	// Guard that the GrpcChainParser instance exists
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

type GrpcChainListener struct {
	endpoint         *lavasession.RPCEndpoint
	relaySender      RelaySender
	logger           *metrics.RPCConsumerLogs
	chainParser      *GrpcChainParser
	healthReporter   HealthReporter
	listeningAddress atomic.Pointer[string]
	httpServer       *http.Server // captured during Serve so Shutdown can call httpServer.Shutdown
}

func NewGrpcChainListener(
	ctx context.Context,
	listenEndpoint *lavasession.RPCEndpoint,
	relaySender RelaySender,
	healthReporter HealthReporter,
	rpcConsumerLogs *metrics.RPCConsumerLogs,
	chainParser ChainParser,
) (chainListener *GrpcChainListener) {
	// Create a new instance of GrpcChainListener
	chainListener = &GrpcChainListener{
		endpoint:       listenEndpoint,
		relaySender:    relaySender,
		logger:         rpcConsumerLogs,
		chainParser:    chainParser.(*GrpcChainParser),
		healthReporter: healthReporter,
	}
	return chainListener
}

// Serve http server for GrpcChainListener
func (apil *GrpcChainListener) Serve(ctx context.Context, cmdFlags common.ConsumerCmdFlags) {
	// Guard that the GrpcChainListener instance exists
	if apil == nil {
		return
	}

	lis := GetListenerWithRetryGrpc("tcp", apil.endpoint.NetworkAddress)
	addr := lis.Addr().String()
	apil.listeningAddress.Store(&addr)
	apiInterface := apil.endpoint.ApiInterface
	sendRelayCallback := func(ctx context.Context, method string, reqBody []byte) ([]byte, metadata.MD, error) {
		if method == "grpc.reflection.v1.ServerReflection/ServerReflectionInfo" {
			return nil, nil, status.Error(codes.Unimplemented, "v1 reflection currently not supported by cosmos-sdk")
		}

		guid := utils.GenerateUniqueIdentifier()
		ctx = utils.WithUniqueIdentifier(ctx, guid)
		msgSeed := strconv.FormatUint(guid, 10)
		metadataValues, _ := metadata.FromIncomingContext(ctx)
		startTime := time.Now()
		// Extract dappID from grpc header
		dappID := extractDappIDFromGrpcHeader(metadataValues)

		grpcHeaders := convertToMetadataMapOfSlices(metadataValues)
		utils.LavaFormatDebug("in <<< GRPC Relay ",
			utils.LogAttr("GUID", ctx),
			utils.LogAttr("_method", method),
			utils.LogAttr("headers", grpcHeaders),
		)
		metricsData := metrics.NewRelayAnalytics(dappID, apil.endpoint.ChainID, apiInterface)
		metricsData.SetProcessingTimestampBeforeRelay(startTime)
		consumerIp := common.GetIpFromGrpcContext(ctx)
		relayResult, err := apil.relaySender.SendRelay(ctx, method, string(reqBody), "", dappID, consumerIp, metricsData, grpcHeaders)
		relayReply := relayResult.GetReply()
		go apil.logger.AddMetricForGrpc(metricsData, err, &metadataValues)

		if err != nil {
			errMasking := apil.logger.GetUniqueGuidResponseForError(err, msgSeed)
			apil.logger.LogRequestAndResponse("grpc in/out", true, method, string(reqBody), "", errMasking, msgSeed, time.Since(startTime), err)
			// Even on error the relay result may carry response metadata — notably the
			// lava-cross-validation-* failure headers synthesized on a quorum/structural failure. Surface it
			// (the proxy attaches it as gRPC trailers on the error path) so gRPC clients get the same
			// structured cross-validation signal as the HTTP interfaces. nil/empty when there is nothing to
			// propagate, preserving prior behavior for ordinary errors.
			return nil, convertRelayMetaDataToMDMetaData(relayReply.GetMetadata()), utils.LavaFormatError("Failed to SendRelay", fmt.Errorf("%s", errMasking))
		}
		apil.logger.LogRequestAndResponse("grpc in/out", false, method, string(reqBody), "", "", msgSeed, time.Since(startTime), nil)

		// try checking for node errors.
		nodeError := &GrpcNodeErrorResponse{}
		unMarshalingError := json.Unmarshal(relayReply.Data, nodeError)
		metadataToReply := relayReply.Metadata
		if unMarshalingError == nil {
			return nil, convertRelayMetaDataToMDMetaData(metadataToReply), status.Error(codes.Code(nodeError.ErrorCode), nodeError.ErrorMessage)
		}
		return relayReply.Data, convertRelayMetaDataToMDMetaData(metadataToReply), nil
	}

	// Check if the relay sender supports gRPC reflection (optional interface)
	var reflectionCallback grpcproxy.ReflectionProxyCallback
	if reflectionProvider, ok := apil.relaySender.(GRPCReflectionProvider); ok {
		reflectionCallback = reflectionProvider.GetGRPCReflectionConnection
		utils.LavaFormatInfo("gRPC reflection support enabled",
			utils.LogAttr("address", apil.endpoint.NetworkAddress),
		)
	}

	// Same optional-interface pattern for server-streaming methods (MAG-2643). Left nil
	// unless BOTH the relay sender has a subscription manager and this chain's spec
	// carries a SUBSCRIBE directive — the callback parses every request it is offered,
	// and that parse is pure overhead on a chain with no streaming methods to find.
	// When it is nil, a streaming method is refused further down the relay path rather
	// than being invoked as a unary call.
	_, _, specHasSubscription := apil.chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_SUBSCRIBE)
	var streamCallback grpcproxy.StreamProxyCallBack
	if subscriptionProvider, ok := apil.relaySender.(GRPCSubscriptionProvider); ok && specHasSubscription {
		if subscriptionManager := subscriptionProvider.GetGRPCSubscriptionManager(); subscriptionManager != nil {
			streamCallback = apil.makeStreamRelayCallback(subscriptionManager)
			utils.LavaFormatInfo("gRPC server-streaming support enabled",
				utils.LogAttr("address", apil.endpoint.NetworkAddress),
				utils.LogAttr("chainID", apil.endpoint.ChainID),
			)
		}
	}

	_, httpServer, err := grpcproxy.NewGRPCProxyWithReflection(sendRelayCallback, apil.endpoint.HealthCheckPath, cmdFlags, apil.healthReporter, reflectionCallback, streamCallback)
	if err != nil {
		utils.LavaFormatFatal("provider failure RegisterServer", err, utils.Attribute{Key: "listenAddr", Value: apil.endpoint.NetworkAddress})
	}
	apil.httpServer = httpServer

	// setup chain parser
	apil.chainParser.setupForConsumer(sendRelayCallback)

	utils.LavaFormatInfo("Server listening", utils.Attribute{Key: "Address", Value: lis.Addr()})

	var serveExecutor func() error
	if apil.endpoint.TLSEnabled {
		utils.LavaFormatInfo("Running with self signed TLS certificate")
		var certificateErr error
		httpServer.TLSConfig, certificateErr = lavasession.GetSelfSignedConfig()
		if certificateErr != nil {
			utils.LavaFormatFatal("failed getting a self signed certificate", certificateErr)
		}
		serveExecutor = func() error { return httpServer.ServeTLS(lis, "", "") }
	} else {
		utils.LavaFormatInfo("Running with disabled TLS configuration")
		serveExecutor = func() error { return httpServer.Serve(lis) }
	}

	fmt.Printf(`
 ┌───────────────────────────────────────────────────┐
 │               Lava's Grpc Server                  │
 │               %s│
 │               Version: %s│
 └───────────────────────────────────────────────────┘

`, truncateAndPadString(apil.endpoint.NetworkAddress, 36), truncateAndPadString(version.Version, 27))
	if err := serveExecutor(); !errors.Is(err, http.ErrServerClosed) {
		utils.LavaFormatFatal("Portal failed to serve", err, utils.Attribute{Key: "Address", Value: lis.Addr()}, utils.Attribute{Key: "ChainID", Value: apil.endpoint.ChainID})
	}
}

// makeStreamRelayCallback builds the gRPC listener's server-streaming entry point
// (MAG-2643). It mirrors what ConsumerWebsocketManager does for eth_subscribe: parse
// the request, recognise it as a subscription, start (or join) the upstream stream,
// and hand back the per-client channel — except the client connection here is the
// gRPC stream itself, which grpcproxy holds open and pumps.
//
// Returns (nil, nil) for anything that is not a spec-declared gRPC subscription, which
// puts the call back on the unary path unchanged.
func (apil *GrpcChainListener) makeStreamRelayCallback(subscriptionManager GRPCSubscriptionManager) grpcproxy.StreamProxyCallBack {
	return func(ctx context.Context, method string, reqBody []byte) (*grpcproxy.StreamResponse, error) {
		metadataValues, _ := metadata.FromIncomingContext(ctx)
		dappID := extractDappIDFromGrpcHeader(metadataValues)
		consumerIp := common.GetIpFromGrpcContext(ctx)

		protocolMessage, err := apil.relaySender.ParseRelay(ctx, method, string(reqBody), "", dappID, consumerIp, convertToMetadataMapOfSlices(metadataValues))
		if err != nil {
			// Not this callback's error to report: an unknown or malformed method must
			// keep producing exactly the error the unary path already produces for it.
			return nil, nil
		}
		if !IsGrpcSubscription(protocolMessage) {
			return nil, nil
		}

		guid := utils.GenerateUniqueIdentifier()
		ctx = utils.WithUniqueIdentifier(ctx, guid)

		// One gRPC stream is one subscriber. The WebSocket path keys clients by
		// connection UID because a single socket multiplexes many subscriptions; here
		// the stream is the connection, so the GUID identifies it for its whole life.
		//
		// It has to be per-stream: the manager holds one reply channel per client key
		// per subscription, so two streams sharing a key would overwrite each other's
		// channel, and releasing one on disconnect would release the other. The cost is
		// that the manager's per-client limits (subscribe rate, max subscriptions per
		// client) see every stream as a new client and so never bind for gRPC — the
		// global subscription cap is what bounds this interface.
		connectionUniqueId := strconv.FormatUint(guid, 10)
		clientKey := subscriptionManager.ClientKey(dappID, consumerIp, connectionUniqueId)

		startTime := time.Now()
		metricsData := metrics.NewRelayAnalytics(dappID, apil.endpoint.ChainID, apil.endpoint.ApiInterface)
		metricsData.SetProcessingTimestampBeforeRelay(startTime)

		utils.LavaFormatDebug("in <<< GRPC stream subscribe",
			utils.LogAttr("GUID", ctx),
			utils.LogAttr("_method", method),
			utils.LogAttr("dappID", dappID),
		)

		firstReply, repliesChan, err := subscriptionManager.StartSubscription(ctx, protocolMessage, dappID, consumerIp, connectionUniqueId, metricsData)

		// Snapshot before the emit goroutine starts. AddMetricForGrpc writes Success and
		// Origin on the struct it is handed, and the per-delivery emits work from these
		// same fields for the life of the subscription — so each side needs its own
		// copy. ConsumerWebsocketManager copies for the same reason.
		subscriptionFields := *metricsData
		go apil.logger.AddMetricForGrpc(metricsData, err, &metadataValues)
		if err != nil {
			apil.logger.LogRequestAndResponse("grpc stream in/out", true, method, string(reqBody), "", err.Error(), connectionUniqueId, time.Since(startTime), err)
			return nil, utils.LavaFormatError("failed to start gRPC subscription", err,
				utils.LogAttr("GUID", ctx),
				utils.LogAttr("_method", method),
				utils.LogAttr("dappID", dappID),
			)
		}

		return &grpcproxy.StreamResponse{
			Replies: apil.forwardSubscriptionReplies(ctx, repliesChan, subscriptionFields, snapshotMetricsHeaders(metadataValues)),
			// firstReply's payload is a JSON acknowledgement, which would not decode as
			// the method's output type — only its subscription id is carried, as headers.
			Metadata: streamResponseHeaders(firstReply.GetMetadata()),
			Close: func() {
				if err := subscriptionManager.UnsubscribeAll(context.Background(), clientKey); err != nil {
					utils.LavaFormatWarning("failed to release gRPC subscription on stream close", err,
						utils.LogAttr("GUID", ctx),
						utils.LogAttr("_method", method),
					)
				}
				apil.logger.LogRequestAndResponse("grpc stream in/out", false, method, string(reqBody), "", "", connectionUniqueId, time.Since(startTime), nil)
			},
		}, nil
	}
}

// forwardSubscriptionReplies adapts the manager's reply channel to the raw payload
// channel grpcproxy pumps, and emits one analytics record per pushed message.
//
// The billing shape matches the WebSocket path: the subscribe itself is billed at the
// spec CU under its real method, and every pushed message is a separate operation at
// the flat delivery default, so downstream billing stays a plain SUM(cu).
//
// subscriptionFields is taken by value because the per-message emits would otherwise
// race each other on the shared RelayMetrics pointer.
func (apil *GrpcChainListener) forwardSubscriptionReplies(ctx context.Context, repliesChan <-chan *pairingtypes.RelayReply, subscriptionFields metrics.RelayMetrics, metricsHeaders metadata.MD) <-chan []byte {
	payloads := make(chan []byte)
	go func() {
		// Closing tells grpcproxy the upstream ended, which closes the client stream
		// with OK.
		defer close(payloads)
		for reply := range repliesChan {
			select {
			case payloads <- reply.GetData():
			case <-ctx.Done():
				// The client is gone and grpcproxy has stopped reading, so this send
				// would block forever. Leaving repliesChan undrained is safe: the
				// manager's sender never blocks on a full channel, and the teardown
				// StreamResponse.Close triggers closes it.
				return
			}

			perMessage := subscriptionFields
			perMessage.Timestamp = time.Now()
			perMessage.ApiMethod = SubscriptionDeliveryMethod
			perMessage.ComputeUnits = DefaultSubscriptionDeliveryCU
			go apil.logger.AddMetricForGrpc(&perMessage, nil, &metricsHeaders)
		}
	}()
	return payloads
}

// transportOwnedGRPCHeaders are response headers the HTTP/2 gRPC transport writes
// itself. Relay metadata describes a relay payload, so on a streaming response — where
// the payload never goes on the wire at all — these would either duplicate or contradict
// what grpc-go already emitted. content-type is the live case: the acknowledgement is
// JSON and says so, while every frame the client actually receives is application/grpc.
var transportOwnedGRPCHeaders = map[string]struct{}{
	"content-type":         {},
	"content-length":       {},
	"grpc-encoding":        {},
	"grpc-accept-encoding": {},
	"grpc-status":          {},
	"grpc-message":         {},
	"te":                   {},
}

// streamResponseHeaders converts the acknowledgement's relay metadata into gRPC response
// headers, dropping the ones the transport owns. In practice what survives is the
// subscription id — the one value that has to reach the client, since the ack body it
// would otherwise have arrived in cannot be sent.
func streamResponseHeaders(md []pairingtypes.Metadata) metadata.MD {
	headers := make(metadata.MD, len(md))
	for _, entry := range md {
		key := strings.ToLower(entry.Name)
		if _, reserved := transportOwnedGRPCHeaders[key]; reserved {
			continue
		}
		headers[key] = append(headers[key], entry.Value)
	}
	return headers
}

// snapshotMetricsHeaders detaches the headers AddMetricForGrpc reads from the request
// metadata. gRPC metadata strings can alias the transport receive buffer, and a
// subscription's per-delivery emits keep referring to them for as long as the stream
// lives — far past the point where the unary path would have released them.
func snapshotMetricsHeaders(metadataValues metadata.MD) metadata.MD {
	snapshot := metadata.MD{}
	for _, key := range []string{metrics.RefererHeaderKey, metrics.UserAgentHeaderKey, metrics.OriginHeaderKey} {
		if values := metadataValues.Get(key); len(values) > 0 {
			snapshot.Set(key, strings.Clone(values[0]))
		}
	}
	return snapshot
}

func (apil *GrpcChainListener) GetListeningAddress() string {
	if p := apil.listeningAddress.Load(); p != nil {
		return *p
	}
	return ""
}

type GrpcChainProxy struct {
	BaseChainProxy
	conn             grpcConnectorInterface
	descriptorsCache *common.SafeSyncMap[string, *desc.MethodDescriptor]
}
type grpcConnectorInterface interface {
	Close()
	GetRpc(ctx context.Context, block bool) (*grpc.ClientConn, error)
	ReturnRpc(rpc *grpc.ClientConn)
}

func NewGrpcChainProxy(ctx context.Context, nConns uint, rpcProviderEndpoint lavasession.RPCProviderEndpoint, parser ChainParser) (ChainProxy, error) {
	if len(rpcProviderEndpoint.NodeUrls) == 0 {
		return nil, utils.LavaFormatError("rpcProviderEndpoint.NodeUrl list is empty missing node url", nil, utils.Attribute{Key: "chainID", Value: rpcProviderEndpoint.ChainID}, utils.Attribute{Key: "ApiInterface", Value: rpcProviderEndpoint.ApiInterface})
	}
	_, averageBlockTime, _, _ := parser.ChainBlockStats()
	nodeUrl := rpcProviderEndpoint.NodeUrls[0]
	nodeUrl.Url = strings.TrimSuffix(nodeUrl.Url, "/") // remove suffix if exists
	// Same context for lifetime and dial: this is a startup path, and ctx is the
	// process's. There is nothing to distinguish — unlike the direct-RPC path,
	// which builds its pool inside a relay (MAG-2808).
	conn, err := chainproxy.NewGRPCConnector(ctx, ctx, nConns, nodeUrl)
	if err != nil {
		return nil, err
	}
	return newGrpcChainProxy(ctx, averageBlockTime, parser, conn, rpcProviderEndpoint)
}

func newGrpcChainProxy(ctx context.Context, averageBlockTime time.Duration, parser ChainParser, conn grpcConnectorInterface, rpcProviderEndpoint lavasession.RPCProviderEndpoint) (ChainProxy, error) {
	cp := &GrpcChainProxy{
		// NodeUrl is what carries grpc-config onto the request path; without it
		// cp.NodeUrl.GrpcConfig is the zero value and descriptor-source is invisible
		// here no matter what the config says (MAG-2350).
		BaseChainProxy:   BaseChainProxy{averageBlockTime: averageBlockTime, ErrorHandler: &GRPCErrorHandler{chainFamily: common.GetChainFamilyOrDefault(rpcProviderEndpoint.ChainID), chainID: rpcProviderEndpoint.ChainID}, ChainID: rpcProviderEndpoint.ChainID, NodeUrl: rpcProviderEndpoint.NodeUrls[0], HashedNodeUrl: chainproxy.HashURL(rpcProviderEndpoint.NodeUrls[0].Url)},
		descriptorsCache: &common.SafeSyncMap[string, *desc.MethodDescriptor]{},
	}
	cp.conn = conn
	if cp.conn == nil {
		return nil, utils.LavaFormatError("g_conn == nil", nil)
	}

	reflectionConnection, err := conn.GetRpc(context.Background(), true)
	if err != nil {
		return nil, utils.LavaFormatError("reflectionConnection Error", err)
	}
	// this connection is kept open so it needs to be closed on teardown
	go func() {
		<-ctx.Done()
		utils.LavaFormatInfo("tearing down reflection connection, context done")
		conn.ReturnRpc(reflectionConnection)
	}()

	grpcParser, ok := parser.(*GrpcChainParser)
	if !ok {
		return nil, fmt.Errorf("grpc chain proxy: parser is not a GrpcChainParser")
	}
	err = grpcParser.setupForProvider(reflectionConnection, &cp.NodeUrl.GrpcConfig)
	if err != nil {
		return nil, fmt.Errorf("grpc chain proxy: failed to setup parser: %w", err)
	}
	return cp, nil
}

func (cp *GrpcChainProxy) SendNodeMsg(ctx context.Context, chainMessage ChainMessageForSend) (relayReply *RelayReplyWrapper, err error) {
	conn, err := cp.conn.GetRpc(ctx, true)
	if err != nil {
		return nil, utils.LavaFormatError("grpc get connection failed ", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
	}
	defer cp.conn.ReturnRpc(conn)

	// appending hashed url
	grpc.SetTrailer(ctx, metadata.Pairs(RPCProviderNodeAddressHash, cp.BaseChainProxy.HashedNodeUrl))

	rpcInputMessage := chainMessage.GetRPCMessage()
	nodeMessage, ok := rpcInputMessage.(*rpcInterfaceMessages.GrpcMessage)
	if !ok {
		return nil, utils.LavaFormatError("invalid message type in grpc failed to cast RPCInput from chainMessage", nil, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx}, utils.Attribute{Key: "rpcMessage", Value: rpcInputMessage})
	}

	metadataMap := make(map[string]string, 0)
	for _, metaData := range nodeMessage.GetHeaders() {
		if metaData.Value != "" {
			metadataMap[metaData.Name] = metaData.Value
		}
	}

	if len(metadataMap) > 0 {
		md := metadata.New(metadataMap)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	cl := grpcreflect.NewClientAuto(ctx, conn)
	descriptorSource, err := rpcInterfaceMessages.DescriptorSourceForGrpcConfig(&cp.NodeUrl.GrpcConfig, rpcInterfaceMessages.DescriptorSourceFromServer(cl))
	if err != nil {
		return nil, utils.LavaFormatError("failed resolving grpc descriptor source", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
	}
	svc, methodName := rpcInterfaceMessages.ParseSymbol(nodeMessage.Path)

	// Check if we have method descriptor already cached.
	// The reason we do Load and then Store here, instead of LoadOrStore:
	// On the worst case scenario, where 2 threads are accessing the map at the same time, the same descriptor will be stored twice.
	// It is better than the alternative, which is always creating the descriptor, since the outcome is the same.
	methodDescriptor, found, _ := cp.descriptorsCache.Load(methodName)
	if !found { // method descriptor not cached yet, need to fetch it and add to cache
		var descriptor desc.Descriptor
		if descriptor, err = descriptorSource.FindSymbol(svc); err != nil {
			return nil, utils.LavaFormatError("descriptorSource.FindSymbol", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
		}
		serviceDescriptor, ok := descriptor.(*desc.ServiceDescriptor)
		if !ok {
			return nil, utils.LavaFormatError("serviceDescriptor, ok := descriptor.(*desc.ServiceDescriptor)", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx}, utils.Attribute{Key: "descriptor", Value: descriptor})
		}
		methodDescriptor = serviceDescriptor.FindMethodByName(methodName)
		if methodDescriptor == nil {
			return nil, utils.LavaFormatError("serviceDescriptor.FindMethodByName returned nil", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx}, utils.Attribute{Key: "methodName", Value: methodName})
		}

		// add the descriptor to the chainProxy cache
		cp.descriptorsCache.Store(methodName, methodDescriptor)
	}

	msgFactory := dynamic.NewMessageFactoryWithDefaults()

	// grpcurl hands this reader straight to json.NewDecoder, which panics on a
	// nil io.Reader (encoding/json is v2-backed since Go 1.27). An empty request
	// message therefore gets an empty reader rather than a nil one.
	var reader io.Reader = bytes.NewReader(nil)
	msg := msgFactory.NewMessage(methodDescriptor.GetInputType())
	formatMessage := false
	if len(nodeMessage.Msg) > 0 {
		// guess if json or binary
		if nodeMessage.Msg[0] != '{' && nodeMessage.Msg[0] != '[' {
			msgLocal := msgFactory.NewMessage(methodDescriptor.GetInputType())
			err = proto.Unmarshal(nodeMessage.Msg, msgLocal)
			if err != nil {
				return nil, utils.LavaFormatError("Failed to unmarshal proto.Unmarshal(nodeMessage.Msg, msgLocal)", err)
			}
			jsonBytes, err := marshalJSON(msgLocal)
			if err != nil {
				return nil, utils.LavaFormatError("Failed to unmarshal marshalJSON(msgLocal)", err)
			}
			reader = bytes.NewReader(jsonBytes)
		} else {
			reader = bytes.NewReader(nodeMessage.Msg)
		}
		formatMessage = true
	}

	rp, formatter, err := grpcurl.RequestParserAndFormatter(grpcurl.FormatJSON, descriptorSource, reader, grpcurl.FormatOptions{
		EmitJSONDefaultFields: false,
		IncludeTextSeparator:  false,
		AllowUnknownFields:    true,
	})
	if err != nil {
		return nil, utils.LavaFormatError("Failed to create formatter", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
	}

	// used when parsing the grpc result
	nodeMessage.SetParsingData(methodDescriptor, formatter)

	if formatMessage {
		err = rp.Next(msg)
		if err != nil {
			return nil, utils.LavaFormatError("rp.Next(msg) Failed", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
		}
	}

	utils.LavaFormatTrace("provider sending node message",
		utils.LogAttr("_method", nodeMessage.Path),
		utils.LogAttr("headers", metadataMap),
		utils.LogAttr("apiInterface", "grpc"),
	)

	var respHeaders metadata.MD
	response := msgFactory.NewMessage(methodDescriptor.GetOutputType())
	connectCtx, cancel := cp.CapTimeoutForSend(ctx, chainMessage)
	defer cancel()
	err = conn.Invoke(connectCtx, "/"+nodeMessage.Path, msg, response, grpc.Header(&respHeaders))
	if err != nil {
		// A corroborated rate limit fails the send with the typed sentinel — matching the
		// HTTP transports — instead of being converted into a node-error reply. gRPC has
		// no status code to check, so common.RateLimitFromGRPC keys on the known texts,
		// or on RESOURCE_EXHAUSTED corroborated by a retry delay in the metadata.
		if st, ok := status.FromError(err); ok {
			if retryAfter, limited := common.RateLimitFromGRPC(uint32(st.Code()), st.Message(), respHeaders); limited {
				return nil, utils.LavaFormatWarning("gRPC node rate-limited the request", common.RateLimited(err, retryAfter),
					utils.Attribute{Key: "GUID", Value: ctx},
					utils.Attribute{Key: "chainID", Value: cp.BaseChainProxy.ChainID},
					utils.Attribute{Key: "apiName", Value: nodeMessage.Path},
				)
			}
		}
		// Validate if the error is related to the provider connection to the node or it is a valid error
		// in case the error is valid (e.g. bad input parameters) the error will return in the form of a valid error reply
		if parsedError := cp.HandleNodeError(ctx, err); parsedError != nil {
			return nil, parsedError
		}
		// return the node's error back to the client as the error type is a invalid request which is cu deductible
		respBytes, statusCode, handlingError := parseGrpcNodeErrorToReply(ctx, err)
		if handlingError != nil {
			return nil, handlingError
		}
		// set status code for user header
		grpc.SetTrailer(ctx, metadata.Pairs(common.StatusCodeMetadataKey, strconv.Itoa(int(statusCode)))) // we ignore this error here since this code can be triggered not from grpc
		reply := &RelayReplyWrapper{
			StatusCode: int(statusCode),
			RelayReply: &pairingtypes.RelayReply{
				Data:     respBytes,
				Metadata: convertToMetadataMapOfSlices(respHeaders),
			},
		}
		return reply, nil
	}

	var respBytes []byte
	respBytes, err = proto.Marshal(response)
	if err != nil {
		return nil, utils.LavaFormatError("proto.Marshal(response) Failed", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
	}
	// set response status code
	validResponseStatus := http.StatusOK
	grpc.SetTrailer(ctx, metadata.Pairs(common.StatusCodeMetadataKey, strconv.Itoa(validResponseStatus))) // we ignore this error here since this code can be triggered not from grpc
	// create reply wrapper
	reply := &RelayReplyWrapper{
		StatusCode: validResponseStatus, // status code is used only for rest at the moment
		RelayReply: &pairingtypes.RelayReply{
			Data:     respBytes,
			Metadata: convertToMetadataMapOfSlices(respHeaders),
		},
	}
	return reply, nil
}

// This method assumes that the error is due to misuse of the request arguments, meaning the user would like to get
// the response from the server to fix the request arguments. this method will make sure the user will get the response
// from the node in the same format as expected.
func parseGrpcNodeErrorToReply(ctx context.Context, err error) ([]byte, uint32, error) {
	var respBytes []byte
	var marshalingError error
	var errorCode uint32 = GRPCStatusCodeOnFailedMessages
	// try fetching status code from error or otherwise use the GRPCStatusCodeOnFailedMessages
	if statusError, ok := status.FromError(err); ok {
		errorCode = uint32(statusError.Code())
		respBytes, marshalingError = json.Marshal(&GrpcNodeErrorResponse{ErrorMessage: statusError.Message(), ErrorCode: errorCode})
		if marshalingError != nil {
			return nil, errorCode, utils.LavaFormatError("json.Marshal(&GrpcNodeErrorResponse Failed 1", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
		}
	} else {
		respBytes, marshalingError = json.Marshal(&GrpcNodeErrorResponse{ErrorMessage: err.Error(), ErrorCode: errorCode})
		if marshalingError != nil {
			return nil, errorCode, utils.LavaFormatError("json.Marshal(&GrpcNodeErrorResponse Failed 2", err, utils.Attribute{Key: "GUID", Value: ctx}, utils.Attribute{Key: utils.KEY_REQUEST_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TASK_ID, Value: ctx}, utils.Attribute{Key: utils.KEY_TRANSACTION_ID, Value: ctx})
		}
	}
	return respBytes, errorCode, nil
}

func marshalJSON(msg proto.Message) ([]byte, error) {
	if dyn, ok := msg.(*dynamic.Message); ok {
		return dyn.MarshalJSON()
	}
	buf := new(bytes.Buffer)
	err := (&jsonpb.Marshaler{}).Marshal(buf, msg)
	return buf.Bytes(), err
}

// Shutdown drains in-flight gRPC unary requests and closes the listener.
// http.Server.Shutdown sends HTTP/2 GOAWAY frames automatically — that is
// the gRPC "going away" equivalent for the server-streaming subscriptions the
// listener now serves. Each of those ends when its stream context is cancelled,
// which releases the client from the upstream subscription via
// grpcproxy.StreamResponse.Close.
func (apil *GrpcChainListener) Shutdown(ctx context.Context) error {
	if apil == nil || apil.httpServer == nil {
		return nil
	}
	return apil.httpServer.Shutdown(ctx)
}
