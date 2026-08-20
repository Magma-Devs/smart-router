package chainlib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	epochstorage "github.com/magma-Devs/smart-router/types/epoch"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/lavaslices"
	"github.com/magma-Devs/smart-router/utils/maps"
)

var (
	// SkipWebsocketVerificationDefault seeds every parser built by NewChainParser and
	// is bound to --skip-websocket-verification. It is written once during flag parsing,
	// before any parser or goroutine exists, and is read-only from then on.
	//
	// It is deliberately NOT consulted at the point of use. The `health` command probes
	// each direct-rpc entry concurrently and needs a different answer per entry (ws
	// augmentation only routes for an entry that actually has a ws:// URL), so it used to
	// flip a package global under a mutex around ValidateCollect. That missed the second
	// reader — newChainRouter — which runs outside that mutex, so entries raced each other
	// into the wrong ws enforcement and healthy legs reported red (MAG-2333). Per-parser
	// state removes the shared cell entirely.
	SkipWebsocketVerificationDefault = false

	// SkipAllVerifications is the process-wide off switch behind --skip-all-verifications.
	// Read through chainlib.skipVerification, so it suppresses the latest-block probe as well
	// as the verifications themselves. Bound by the rpcsmartrouter command only — the health
	// command deliberately does not expose it, since reporting verification results is its job.
	SkipAllVerifications = false

	DefaultApiName = "Default-"
)

type PolicyInf interface {
	GetSupportedAddons(specID string) (addons []string, err error)
	GetSupportedExtensions(specID string) (extensions []epochstorage.EndpointService, err error)
}

type InternalPath struct {
	Path           string
	Enabled        bool
	ApiInterface   string
	ConnectionType string
	Addon          string
}

type BaseChainParser struct {
	internalPaths     map[string]InternalPath
	taggedApis        map[spectypes.FUNCTION_TAG]TaggedContainer
	spec              spectypes.Spec
	rwLock            sync.RWMutex
	serverApis        map[ApiKey]ApiContainer
	apiCollections    map[CollectionKey]*spectypes.ApiCollection
	headers           map[ApiKey]*spectypes.Header
	verifications     map[VerificationKey]map[string][]VerificationContainer // map[VerificationKey]map[InternalPath][]VerificationContainer
	allowedAddons     map[string]bool
	allowedExtensions map[string]struct{}
	extensionParser   extensionslib.ExtensionParser
	active            bool

	// skipWebsocketVerification is this parser's own answer to "should verifications be
	// augmented with the websocket extension, and should the router enforce ws support".
	// Seeded from SkipWebsocketVerificationDefault; overridden per-endpoint by callers
	// that probe several endpoints concurrently. Guarded by rwLock like every other
	// mutable field here.
	skipWebsocketVerification bool
}

// SkipWebsocketVerification reports whether this parser's endpoint opts out of
// websocket augmentation and enforcement.
func (bcp *BaseChainParser) SkipWebsocketVerification() bool {
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()
	return bcp.skipWebsocketVerification
}

// SetSkipWebsocketVerification overrides the process default for this parser only.
// Callers probing multiple endpoints concurrently must build one parser per endpoint
// and set it here rather than reaching for the package-level default.
func (bcp *BaseChainParser) SetSkipWebsocketVerification(skip bool) {
	bcp.rwLock.Lock()
	defer bcp.rwLock.Unlock()
	bcp.skipWebsocketVerification = skip
}

func (bcp *BaseChainParser) Activate() {
	bcp.active = true
}

func (bcp *BaseChainParser) Active() bool {
	return bcp.active
}

func (bcp *BaseChainParser) UpdateBlockTime(newBlockTime time.Duration) {
	bcp.rwLock.Lock()
	defer bcp.rwLock.Unlock()
	utils.LavaFormatInfo("chainParser updated block time", utils.Attribute{Key: "newTime", Value: newBlockTime}, utils.Attribute{Key: "oldTime", Value: time.Duration(bcp.spec.AverageBlockTime) * time.Millisecond}, utils.Attribute{Key: "specID", Value: bcp.spec.Index})
	bcp.spec.AverageBlockTime = newBlockTime.Milliseconds()
}

func (bcp *BaseChainParser) HandleHeaders(metadata []pairingtypes.Metadata, apiCollection *spectypes.ApiCollection, headersDirection spectypes.Header_HeaderType) (filteredHeaders []pairingtypes.Metadata, overwriteRequestedBlock string, ignoredMetadata []pairingtypes.Metadata) {
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()
	if len(metadata) == 0 {
		return []pairingtypes.Metadata{}, "", []pairingtypes.Metadata{}
	}
	retMetadata := []pairingtypes.Metadata{}
	for _, header := range metadata {
		headerName := strings.ToLower(header.Name)
		apiKey := ApiKey{Name: headerName, ConnectionType: apiCollection.CollectionData.Type}
		headerDirective, ok := bcp.headers[apiKey]
		if !ok {
			// this header is not handled
			continue
		}
		if headerDirective.Kind == headersDirection || headerDirective.Kind == spectypes.Header_pass_both {
			retMetadata = append(retMetadata, header)
			if headerDirective.FunctionTag == spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA {
				// this header sets the latest requested block
				overwriteRequestedBlock = header.Value
			}
		}
	}

	// iterate over the headers defined in spec file to handle any nullified headers
	for _, bcpHeader := range bcp.headers {
		// handle nullified headers
		if bcpHeader.Kind == spectypes.Header_pass_nullify {
			retMetadata = append(retMetadata, pairingtypes.Metadata{Name: bcpHeader.Name, Value: ""})
		}

		if bcpHeader.Kind == spectypes.Header_pass_override {
			retMetadata = append(retMetadata, pairingtypes.Metadata{Name: bcpHeader.Name, Value: bcpHeader.Value})
		}
	}

	return retMetadata, overwriteRequestedBlock, ignoredMetadata
}

func (bcp *BaseChainParser) isAddon(addon string) bool {
	_, ok := bcp.allowedAddons[addon]
	return ok
}

func (bcp *BaseChainParser) isExtension(extension string) bool {
	_, ok := bcp.allowedExtensions[extension]
	return ok
}

// ValidateMessage validates the chain message against the consumer's policy (allowed addons).
// Should be called after ParseMsg by consumers/providers that enforce policy.
// Smart-router does not call this since it uses static providers without on-chain policy.
func (bcp *BaseChainParser) ValidateMessage(chainMsg ChainMessage) error {
	nodeMessage, ok := chainMsg.(*baseChainMessageContainer)
	if !ok {
		return nil
	}
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()
	if addon := GetAddon(nodeMessage); addon != "" {
		if allowed := bcp.allowedAddons[addon]; !allowed {
			return utils.LavaFormatError("consumer policy does not allow addon", nil,
				utils.LogAttr("addon", addon),
			)
		}
	}
	return nil
}

func (bcp *BaseChainParser) BuildMapFromPolicyQuery(policy PolicyInf, chainId string, apiInterface string) (map[string]struct{}, error) {
	addons, err := policy.GetSupportedAddons(chainId)
	if err != nil {
		return nil, err
	}
	extensions, err := policy.GetSupportedExtensions(chainId)
	if err != nil {
		return nil, err
	}
	services := make(map[string]struct{})
	for _, addon := range addons {
		services[addon] = struct{}{}
	}
	for _, consumerExtension := range extensions {
		// store only relevant apiInterface extensions; empty ApiInterface matches all
		if consumerExtension.ApiInterface == "" || consumerExtension.ApiInterface == apiInterface {
			services[consumerExtension.Extension] = struct{}{}
		}
	}
	return services, nil
}

func (bcp *BaseChainParser) SetPolicyFromAddonAndExtensionMap(policyInformation map[string]struct{}) {
	bcp.rwLock.Lock()
	defer bcp.rwLock.Unlock()
	// reset the current one in case we configured it previously
	configuredExtensions := make(map[extensionslib.ExtensionKey]*spectypes.Extension)
	for collectionKey, apiCollection := range bcp.apiCollections {
		// manage extensions
		for _, extension := range apiCollection.Extensions {
			if extension.Name == "" {
				// skip empty extensions
				continue
			}
			if _, ok := policyInformation[extension.Name]; ok {
				extensionKey := extensionslib.ExtensionKey{
					Extension:      extension.Name,
					ConnectionType: apiCollection.CollectionData.ApiInterface,
					InternalPath:   collectionKey.InternalPath,
					Addon:          collectionKey.Addon,
				}
				configuredExtensions[extensionKey] = extension
			}
		}
	}
	bcp.extensionParser.SetConfiguredExtensions(configuredExtensions)
	// manage allowed addons
	for addon := range bcp.allowedAddons {
		_, bcp.allowedAddons[addon] = policyInformation[addon]
	}
}

// policy information contains all configured services (extensions and addons) allowed to be used by the consumer
func (bcp *BaseChainParser) SetPolicy(policy PolicyInf, chainId string, apiInterface string) error {
	policyInformation, err := bcp.BuildMapFromPolicyQuery(policy, chainId, apiInterface)
	if err != nil {
		return err
	}
	bcp.SetPolicyFromAddonAndExtensionMap(policyInformation)
	return nil
}

// this function errors if it meets a value that is neither a n addon or an extension
func (bcp *BaseChainParser) SeparateAddonsExtensions(ctx context.Context, supported []string) (addons, extensions []string, err error) {
	checked := map[string]struct{}{}
	for _, supportedToCheck := range supported {
		// ignore repeated occurrences
		if _, ok := checked[supportedToCheck]; ok {
			continue
		}
		checked[supportedToCheck] = struct{}{}

		if bcp.isAddon(supportedToCheck) {
			addons = append(addons, supportedToCheck)
		} else if supportedToCheck == "" {
			continue
		} else {
			isExtensionResult := bcp.isExtension(supportedToCheck)
			isWebSocket := supportedToCheck == WebSocketExtension

			if isExtensionResult || isWebSocket {
				extensions = append(extensions, supportedToCheck)
				continue
			}
			// neither is an error
			err = utils.LavaFormatError("invalid supported to check, is neither an addon or an extension", err,
				utils.Attribute{Key: "spec", Value: bcp.spec.Index},
				utils.Attribute{Key: "supported", Value: supportedToCheck})
		}
	}

	return addons, extensions, err
}

// gets all verifications for an endpoint supporting multiple addons and extensions
func (bcp *BaseChainParser) GetVerifications(supported []string, internalPath string, apiInterface string) (retVerifications []VerificationContainer, err error) {
	// addons will contains extensions and addons,
	// extensions must exist in all verifications, addons must be split because they are separated
	addons, extensions, err := bcp.SeparateAddonsExtensions(context.Background(), supported)
	if err != nil {
		return nil, err
	}
	if len(extensions) == 0 {
		extensions = []string{""}
	}
	addons = append(addons, "") // always add the empty addon

	for _, addon := range addons {
		for _, extension := range extensions {
			verificationKey := VerificationKey{
				Extension: extension,
				Addon:     addon,
			}
			collectionVerifications, ok := bcp.verifications[verificationKey]
			if ok {
				if verifications, ok := collectionVerifications[internalPath]; ok {
					retVerifications = append(retVerifications, verifications...)
				}
			}
		}
	}
	return retVerifications, nil
}

func (bcp *BaseChainParser) Construct(spec spectypes.Spec, internalPaths map[string]InternalPath, taggedApis map[spectypes.FUNCTION_TAG]TaggedContainer,
	serverApis map[ApiKey]ApiContainer, apiCollections map[CollectionKey]*spectypes.ApiCollection, headers map[ApiKey]*spectypes.Header,
	verifications map[VerificationKey]map[string][]VerificationContainer,
) {
	bcp.spec = spec
	bcp.internalPaths = internalPaths
	bcp.serverApis = serverApis
	bcp.taggedApis = taggedApis
	bcp.headers = headers
	bcp.apiCollections = apiCollections
	bcp.verifications = verifications
	allowedAddons := map[string]bool{}
	allowedExtensions := map[string]struct{}{}
	for _, apiCollection := range apiCollections {
		for _, extension := range apiCollection.Extensions {
			allowedExtensions[extension.Name] = struct{}{}
		}
		// if addon was already existing (happens on spec update), use the existing policy, otherwise set it to false by default
		allowedAddons[apiCollection.CollectionData.AddOn] = bcp.allowedAddons[apiCollection.CollectionData.AddOn]
	}
	bcp.allowedAddons = allowedAddons
	bcp.allowedExtensions = allowedExtensions
	bcp.extensionParser = extensionslib.NewExtensionParser(bcp.extensionParser.GetConfiguredExtensions())
}

func (bcp *BaseChainParser) ParseDirectiveEnabled() bool {
	_, _, ok := bcp.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM)
	if !ok {
		return false
	}
	_, _, ok = bcp.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	return ok
}

func (bcp *BaseChainParser) GetParsingByTag(tag spectypes.FUNCTION_TAG) (parsing *spectypes.ParseDirective, apiCollection *spectypes.ApiCollection, existed bool) {
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()

	val, ok := bcp.taggedApis[tag]
	if !ok {
		return nil, nil, false
	}
	return val.Parsing, val.ApiCollection, ok
}

func (bcp *BaseChainParser) IsTagInCollection(tag spectypes.FUNCTION_TAG, collectionKey CollectionKey) bool {
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()

	apiCollection, ok := bcp.apiCollections[collectionKey]
	return ok && lavaslices.ContainsPredicate(apiCollection.ParseDirectives, func(elem *spectypes.ParseDirective) bool {
		return elem.FunctionTag == tag
	})
}

func (bcp *BaseChainParser) GetAllInternalPaths() []string {
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()
	return lavaslices.Map(maps.ValuesSlice(bcp.internalPaths), func(internalPath InternalPath) string {
		return internalPath.Path
	})
}

func (bcp *BaseChainParser) IsInternalPathEnabled(internalPath string, apiInterface string, addon string) bool {
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()
	internalPathObj, ok := bcp.internalPaths[internalPath]
	return ok && internalPathObj.Enabled && internalPathObj.ApiInterface == apiInterface && internalPathObj.Addon == addon
}

func (bcp *BaseChainParser) ExtensionParsing(addon string, parsedMessageArg *baseChainMessageContainer, extensionInfo extensionslib.ExtensionInfo) {
	// Only emit archive debug traces for user relays (LatestBlock > 0), not internal chain tracker polls
	debugLog := extensionInfo.LatestBlock > 0
	if debugLog {
		utils.LavaFormatTrace("[Archive Debug] ExtensionParsing called", utils.LogAttr("addon", addon), utils.LogAttr("extensionInfo", extensionInfo))
	}
	if extensionInfo.ExtensionOverride == nil {
		if debugLog {
			utils.LavaFormatTrace("[Archive Debug] Using extensionParsingInner", utils.LogAttr("latestBlock", extensionInfo.LatestBlock))
		}
		bcp.extensionParsingInner(addon, parsedMessageArg, extensionInfo.LatestBlock)
	} else {
		if debugLog {
			utils.LavaFormatTrace("[Archive Debug] Using OverrideExtensions", utils.LogAttr("extensionOverride", extensionInfo.ExtensionOverride))
		}
		parsedMessageArg.OverrideExtensions(extensionInfo.ExtensionOverride, &bcp.extensionParser)
	}
	if extensionInfo.AdditionalExtensions != nil {
		if debugLog {
			utils.LavaFormatTrace("[Archive Debug] Using AdditionalExtensions", utils.LogAttr("additionalExtensions", extensionInfo.AdditionalExtensions))
		}
		parsedMessageArg.OverrideExtensions(extensionInfo.AdditionalExtensions, &bcp.extensionParser)
	}
}

func (bcp *BaseChainParser) extensionParsingInner(addon string, parsedMessageArg *baseChainMessageContainer, latestBlock uint64) {
	bcp.rwLock.RLock()
	defer bcp.rwLock.RUnlock()
	bcp.extensionParser.ExtensionParsing(addon, parsedMessageArg, latestBlock)
}

func (apip *BaseChainParser) defaultApiContainer(apiKey ApiKey) (*ApiContainer, error) {
	// Guard that the GrpcChainParser instance exists
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}
	utils.LavaFormatDebug("api not supported", utils.Attribute{Key: "apiKey", Value: apiKey})
	apiCont := &ApiContainer{
		api: &spectypes.Api{
			Enabled:      true,
			Name:         DefaultApiName + apiKey.Name, // do not change this name
			ComputeUnits: 20,                           // set 20 compute units by default
			Category: spectypes.SpecCategory{
				Deterministic: true,
			},
			BlockParsing: spectypes.BlockParser{
				ParserFunc: spectypes.PARSER_FUNC_DEFAULT,
				ParserArg:  []string{spectypes.ParserArgLatest},
			},
			TimeoutMs: 0,
			Parsers:   []spectypes.GenericParser{},
		},
		collectionKey: CollectionKey{
			ConnectionType: apiKey.ConnectionType,
			InternalPath:   apiKey.InternalPath,
			Addon:          "",
		},
	}

	return apiCont, nil
}

// getSupportedApi fetches service api from spec by name
func (apip *BaseChainParser) getSupportedApi(apiKey ApiKey) (*ApiContainer, error) {
	// Guard that the GrpcChainParser instance exists
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}

	// Acquire read lock
	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()

	// Fetch server api by name
	apiCont, ok := apip.serverApis[apiKey]

	// Return an api container does not exist, return a default one
	if !ok {
		return apip.defaultApiContainer(apiKey)
	}

	// Return an error if api is disabled
	if !apiCont.api.Enabled {
		return nil, utils.LavaFormatInfo("api is disabled", utils.Attribute{Key: "apiKey", Value: apiKey})
	}

	return &apiCont, nil
}

// ApiHasStatefulCategory reports whether any supported API named `name` is a write / stateful method
// (Category.Stateful == CONSISTENCY_SELECT_ALL_PROVIDERS). Used by the cross-validation policy guard to
// reject an enabled per-method CV policy on a write method, where cross-validating the response is a
// no-op (the response is a deterministic acknowledgement, not an observation of chain state).
func (apip *BaseChainParser) ApiHasStatefulCategory(name string) bool {
	if apip == nil {
		return false
	}
	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()
	for apiKey, apiCont := range apip.serverApis {
		if apiKey.Name == name && apiCont.api != nil && apiCont.api.Category.Stateful == common.CONSISTENCY_SELECT_ALL_PROVIDERS {
			return true
		}
	}
	return false
}

func (apip *BaseChainParser) isValidInternalPath(path string) bool {
	if apip == nil || len(apip.internalPaths) == 0 {
		return false
	}

	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()
	_, ok := apip.internalPaths[path]
	return ok
}

// take an http request and direct it through the consumer
func (apip *BaseChainParser) ExtractDataFromRequest(request *http.Request) (url string, data string, connectionType string, metadata []pairingtypes.Metadata, err error) {
	// Extract relative URL path
	url = request.URL.Path
	// Extract connection type
	connectionType = request.Method

	// Extract metadata
	for key, values := range request.Header {
		for _, value := range values {
			metadata = append(metadata, pairingtypes.Metadata{
				Name:  key,
				Value: value,
			})
		}
	}

	// Extract data
	if request.Body != nil {
		bodyBytes, err := io.ReadAll(request.Body)
		if err != nil {
			return "", "", "", nil, err
		}
		data = string(bodyBytes)
	}

	return url, data, connectionType, metadata, nil
}

func (apip *BaseChainParser) SetResponseFromRelayResult(relayResult *common.RelayResult) (*http.Response, error) {
	if relayResult == nil {
		return nil, errors.New("relayResult is nil")
	}
	response := &http.Response{
		StatusCode: relayResult.StatusCode,
		Header:     make(http.Header),
	}

	for _, values := range relayResult.Reply.Metadata {
		response.Header.Add(values.Name, values.Value)
	}

	if relayResult.Reply != nil && relayResult.Reply.Data != nil {
		response.Body = io.NopCloser(strings.NewReader(string(relayResult.Reply.Data)))
	}

	return response, nil
}

// getSupportedApi fetches service api from spec by name
func (apip *BaseChainParser) getApiCollection(connectionType, internalPath, addon string) (*spectypes.ApiCollection, error) {
	// Guard that the GrpcChainParser instance exists
	if apip == nil {
		return nil, errors.New("ChainParser not defined")
	}

	// Acquire read lock
	apip.rwLock.RLock()
	defer apip.rwLock.RUnlock()

	// Fetch server api by name
	api, ok := apip.apiCollections[CollectionKey{
		ConnectionType: connectionType,
		InternalPath:   internalPath,
		Addon:          addon,
	}]

	// Return an error if spec does not exist
	if !ok {
		utils.LavaFormatDebug("api not supported", utils.Attribute{Key: "connectionType", Value: connectionType})
		return nil, common.APINotSupportedError
	}

	// Return an error if api is disabled
	if !api.Enabled {
		return nil, utils.LavaFormatError("api is disabled", nil, utils.Attribute{Key: "connectionType", Value: connectionType})
	}

	return api, nil
}

func getServiceApis(
	spec spectypes.Spec,
	rpcInterface string,
) (
	retInternalPaths map[string]InternalPath,
	retServerApis map[ApiKey]ApiContainer,
	retTaggedApis map[spectypes.FUNCTION_TAG]TaggedContainer,
	retApiCollections map[CollectionKey]*spectypes.ApiCollection,
	retHeaders map[ApiKey]*spectypes.Header,
	retVerifications map[VerificationKey]map[string][]VerificationContainer,
) {
	retInternalPaths = map[string]InternalPath{}
	serverApis := map[ApiKey]ApiContainer{}
	taggedApis := map[spectypes.FUNCTION_TAG]TaggedContainer{}
	headers := map[ApiKey]*spectypes.Header{}
	apiCollections := map[CollectionKey]*spectypes.ApiCollection{}
	verifications := map[VerificationKey]map[string][]VerificationContainer{}
	if spec.Enabled {
		for _, apiCollection := range spec.ApiCollections {
			if !apiCollection.Enabled {
				continue
			}
			if apiCollection.CollectionData.ApiInterface != rpcInterface {
				continue
			}
			collectionKey := CollectionKey{
				ConnectionType: apiCollection.CollectionData.Type,
				InternalPath:   apiCollection.CollectionData.InternalPath,
				Addon:          apiCollection.CollectionData.AddOn,
			}

			// add as a valid internal path
			retInternalPaths[apiCollection.CollectionData.InternalPath] = InternalPath{
				Path:           apiCollection.CollectionData.InternalPath,
				Enabled:        apiCollection.Enabled,
				ApiInterface:   apiCollection.CollectionData.ApiInterface,
				ConnectionType: apiCollection.CollectionData.Type,
				Addon:          apiCollection.CollectionData.AddOn,
			}

			for _, parsing := range apiCollection.ParseDirectives {
				// We do this because some specs may have multiple parse directives
				// with the same tag - SUBSCRIBE (like in Solana).
				//
				// Since the function tag is not used for handling the subscription flow,
				// we can ignore the extra parse directives and take only the first one. The
				// subscription flow is handled by the consumer websocket manager and the chain router
				// that uses the api collection to fetch the correct parse directive.
				//
				// The only place the SUBSCRIBE tag is checked against the taggedApis map is in the chain parser with GetParsingByTag.
				// But there, we're not interested in the parse directive, only if the tag is present.
				if _, ok := taggedApis[parsing.FunctionTag]; !ok {
					taggedApis[parsing.FunctionTag] = TaggedContainer{
						Parsing:       parsing,
						ApiCollection: apiCollection,
					}
				}
			}

			for _, api := range apiCollection.Apis {
				if !api.Enabled {
					continue
				}

				if rpcInterface == spectypes.APIInterfaceRest {
					apiKey, apiContainer, err := newRestApiContainer(api, collectionKey)
					if err != nil {
						utils.LavaFormatError("regex Compile api", err, utils.Attribute{Key: "apiName", Value: api.Name})
						continue
					}
					serverApis[apiKey] = apiContainer
				} else {
					// add another internal path entry so it can specifically be referenced
					if apiCollection.CollectionData.InternalPath != "" {
						serverApis[ApiKey{
							Name:           api.Name,
							ConnectionType: collectionKey.ConnectionType,
							InternalPath:   apiCollection.CollectionData.InternalPath,
						}] = ApiContainer{
							api:           api,
							collectionKey: collectionKey,
						}
						// if it does not exist set it
						if _, ok := serverApis[ApiKey{Name: api.Name, ConnectionType: collectionKey.ConnectionType}]; !ok {
							serverApis[ApiKey{
								Name:           api.Name,
								ConnectionType: collectionKey.ConnectionType,
							}] = ApiContainer{
								api:           api,
								collectionKey: collectionKey,
							}
						}
					} else {
						serverApis[ApiKey{
							Name:           api.Name,
							ConnectionType: collectionKey.ConnectionType,
						}] = ApiContainer{
							api:           api,
							collectionKey: collectionKey,
						}
					}
				}
			}
			for _, header := range apiCollection.Headers {
				headers[ApiKey{
					Name:           header.Name,
					ConnectionType: collectionKey.ConnectionType,
				}] = header
			}
			for _, verification := range apiCollection.Verifications {
				if verification.ParseDirective.FunctionTag != spectypes.FUNCTION_TAG_VERIFICATION {
					if _, ok := taggedApis[verification.ParseDirective.FunctionTag]; ok {
						verification.ParseDirective = taggedApis[verification.ParseDirective.FunctionTag].Parsing
					} else {
						utils.LavaFormatError("Bad verification definition", fmt.Errorf("verification function tag is not defined in the collections parse directives"), utils.LogAttr("function_tag", verification.ParseDirective.FunctionTag))
						continue
					}
				}

				for _, parseValue := range verification.Values {
					verificationKey := VerificationKey{
						Extension: parseValue.Extension,
						Addon:     apiCollection.CollectionData.AddOn,
					}

					verCont := VerificationContainer{
						InternalPath:    apiCollection.CollectionData.InternalPath,
						ConnectionType:  apiCollection.CollectionData.Type,
						Name:            verification.Name,
						ParseDirective:  *verification.ParseDirective,
						Value:           parseValue.ExpectedValue,
						LatestDistance:  parseValue.LatestDistance,
						VerificationKey: verificationKey,
						Severity:        parseValue.Severity,
					}

					internalPath := apiCollection.CollectionData.InternalPath
					if extensionVerifications, ok := verifications[verificationKey]; !ok {
						verifications[verificationKey] = map[string][]VerificationContainer{internalPath: {verCont}}
					} else if collectionVerifications, ok := extensionVerifications[internalPath]; !ok {
						verifications[verificationKey][internalPath] = []VerificationContainer{verCont}
					} else {
						verifications[verificationKey][internalPath] = append(collectionVerifications, verCont)
					}
				}
			}
			apiCollections[collectionKey] = apiCollection
		}
	}
	return retInternalPaths, serverApis, taggedApis, apiCollections, headers, verifications
}

func (bcp *BaseChainParser) ExtensionsParser() *extensionslib.ExtensionParser {
	return &bcp.extensionParser
}

// restPlaceholderRegex finds the {placeholder} segments of a REST spec api name, and the
// two patterns each one compiles into: an inner segment must not be empty, a trailing one may.
var restPlaceholderRegex = regexp.MustCompile(`{[^}]+}`)

const (
	restSegmentPattern = `[^\/\s]+`
	restTailPattern    = `[^\/\s]*`
)

// restApiNameToRegex turns a REST spec api name into the pattern stored on its ApiKey:
// each {placeholder} becomes a single path segment, everything else is literal. A trailing
// placeholder may match empty, an inner one may not.
func restApiNameToRegex(apiName string) string {
	processedName := string(restPlaceholderRegex.ReplaceAll([]byte(apiName), []byte("replace-me-with-regex")))
	processedName = regexp.QuoteMeta(processedName)
	processedName = strings.ReplaceAll(processedName, "replace-me-with-regex/", restSegmentPattern+"/")
	processedName = strings.ReplaceAll(processedName, "replace-me-with-regex", restTailPattern)
	return processedName
}

// trimOptionalTrailingSlash drops one trailing slash, leaving a bare "/" untouched —
// several specs (ARWEAVE, APT1, XLM) name a real api exactly "/".
func trimOptionalTrailingSlash(path string) string {
	if len(path) > 1 && strings.HasSuffix(path, "/") {
		return path[:len(path)-1]
	}
	return path
}

// newRestApiContainer keys a REST api by the pattern its name compiles to — the name is a
// path template, so it is matched per request rather than looked up — and precompiles that
// pattern so a lookup never calls regexp.Compile.
func newRestApiContainer(api *spectypes.Api, collectionKey CollectionKey) (ApiKey, ApiContainer, error) {
	apiPattern := restApiNameToRegex(api.Name)
	matcher, err := buildRestApiMatcher(api.Name, apiPattern)
	if err != nil {
		return ApiKey{}, ApiContainer{}, err
	}
	return ApiKey{
			Name:           apiPattern,
			ConnectionType: collectionKey.ConnectionType,
		}, ApiContainer{
			api:           api,
			collectionKey: collectionKey,
			restMatcher:   matcher,
		}, nil
}

// restApiMatcher holds everything matchSpecApiByName needs for one REST api. It is built
// once per api at spec load, so a lookup costs matches rather than compiles, and it carries
// the rank that decides between apis whose patterns both cover the requested path.
type restApiMatcher struct {
	name         string
	pattern      *regexp.Regexp // ^name$
	trimmed      *regexp.Regexp // ^name without its trailing slash$, or pattern when it has none
	placeholders int            // {…} segments in the spec name — fewer is more specific
	literalLen   int            // characters outside those segments — longer is more specific
}

// buildRestApiMatcher compiles the patterns for one REST spec api name.
func buildRestApiMatcher(apiName, apiPattern string) (*restApiMatcher, error) {
	pattern, err := regexp.Compile("^" + apiPattern + "$")
	if err != nil {
		return nil, err
	}
	trimmed := pattern
	if trimmedPattern := trimOptionalTrailingSlash(apiPattern); trimmedPattern != apiPattern {
		trimmed, err = regexp.Compile("^" + trimmedPattern + "$")
		if err != nil {
			return nil, err
		}
	}
	// Ranked off the compiled pattern rather than the spec name: a serverApis map built by
	// hand carries only the pattern, and ranking both the same way keeps the two paths from
	// disagreeing.
	literal := strings.ReplaceAll(apiPattern, restSegmentPattern, "")
	literal = strings.ReplaceAll(literal, restTailPattern, "")
	return &restApiMatcher{
		name:         apiName,
		pattern:      pattern,
		trimmed:      trimmed,
		placeholders: strings.Count(apiPattern, restSegmentPattern) + strings.Count(apiPattern, restTailPattern),
		literalLen:   len(literal),
	}, nil
}

// moreSpecificThan orders two apis that both cover the requested path.
//
// A match on the path exactly as sent beats a slash-insensitive one, so relaxing the slash
// can never re-route a request that resolves today. Beyond that the most specific name
// wins: a literal segment describes a path better than a {placeholder} that happens to
// swallow it, which is what keeps ARWEAVE /info on the 10-CU /info instead of the 20-CU
// /{id}.
//
// Two distinct names can still rank equal: /a/{x}/c and /a/b/{y} both cover /a/b/c with one
// placeholder and five literal characters, and they compile to different patterns, so they
// do not collapse into one ApiKey the way {address_bytes} and {address_string} do. No shape
// in the catalog reaches that today, but it is reachable, so the last resort is the api name
// — without a total order the winner is Go map iteration order: a coin flip per call between
// apis that can carry different compute units and different block parsing.
//
// Comparing names lexicographically is also the right answer rather than merely a stable one:
// '{' sorts above every character a path segment holds, so it reads as "literal beats
// placeholder at the first segment where the two names differ" — the precedence an HTTP
// router applies.
func (m *restApiMatcher) moreSpecificThan(exact bool, other *restApiMatcher, otherExact bool) bool {
	if exact != otherExact {
		return exact
	}
	if m.placeholders != other.placeholders {
		return m.placeholders < other.placeholders
	}
	if m.literalLen != other.literalLen {
		return m.literalLen > other.literalLen
	}
	return m.name < other.name
}

// matchSpecApiByName returns the service api that best matches the given name.
//
// A trailing slash is optional on both sides: specs name apis both ways (TEZOS omits it,
// STACKS carries it) and clients send either form, while the compiled name is anchored
// "^...$" so the slash alone used to decide the match. A path that misses here falls
// through to defaultApiContainer, which bills a flat 20 compute units and pins block
// parsing to latest, so a miss is a routing error and not only a metrics one.
//
// More than one api can cover the same path — a literal name and the {placeholder} sibling
// that swallows it, or two placeholders over one path — so every candidate is ranked and
// the most specific one wins (see moreSpecificThan). The scan only stops early on a literal
// name matching the path as sent, which nothing else can outrank.
func matchSpecApiByName(name, connectionType string, serverApis map[ApiKey]ApiContainer) (*ApiContainer, bool) {
	foundNameOnDifferentConnectionType := ""
	trimmedName := trimOptionalTrailingSlash(name)

	var best *ApiContainer
	var bestMatcher *restApiMatcher
	bestExact := false

	for apiKey, apiCont := range serverApis {
		matcher := apiCont.restMatcher
		if matcher == nil {
			// Every REST api is keyed through newRestApiContainer, which always compiles a
			// matcher, so this is unreachable. Matching without one would mean compiling in
			// the loop — the per-lookup cost the precompilation exists to remove — so an
			// entry that arrived some other way is dropped loudly rather than absorbed.
			utils.LavaFormatError("REST api container has no compiled matcher", nil, utils.Attribute{Key: "apiKey", Value: apiKey})
			continue
		}
		exact := matcher.pattern.MatchString(name)
		// When the spec name carries no trailing slash, trimmed is pattern, so this second
		// match only ever succeeds for a request that carried one.
		if !exact && !matcher.trimmed.MatchString(trimmedName) {
			continue
		}
		if apiKey.ConnectionType != connectionType {
			// its hard to notice when we have an API on only one connection type.
			foundNameOnDifferentConnectionType = apiKey.ConnectionType
			continue
		}
		if exact && matcher.placeholders == 0 {
			// A literal name matching the path as sent cannot be beaten: two distinct
			// literal names cannot match one string, and every other candidate ranks lower.
			matched := apiCont
			return &matched, true
		}
		if best == nil || matcher.moreSpecificThan(exact, bestMatcher, bestExact) {
			matched := apiCont
			best, bestMatcher, bestExact = &matched, matcher, exact
		}
	}

	if best != nil {
		return best, true
	}
	if foundNameOnDifferentConnectionType != "" { // its hard to notice when we have an API on only one connection type.
		utils.LavaFormatWarning("API was found on a different connection type", nil,
			utils.Attribute{Key: "connection_type_found", Value: foundNameOnDifferentConnectionType},
			utils.Attribute{Key: "connection_type_requested", Value: connectionType},
			utils.LogAttr("requested_api", name),
		)
	}
	return nil, false
}
