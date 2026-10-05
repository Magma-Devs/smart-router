package chainlib

import (
	"math"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

type updatableRPCInput interface {
	rpcInterfaceMessages.GenericMessage
	UpdateLatestBlockInMessage(latestBlock uint64, modifyContent bool) (success bool)
	AppendHeader(metadata []pairingtypes.Metadata)
	SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string
	GetRawRequestHash() ([]byte, error)
}

type baseChainMessageContainer struct {
	api                    *spectypes.Api
	latestRequestedBlock   int64
	requestedBlockHashes   []string
	earliestRequestedBlock int64
	msg                    updatableRPCInput
	apiCollection          *spectypes.ApiCollection
	extensions             []*spectypes.Extension
	// unavailableExtensions are extensions the caller asked for (lava-extension) that the spec
	// knows but no node on this router offers. The request is served without them; they are kept
	// so the reply can say so (MAG-3935).
	unavailableExtensions []string
	timeoutOverride       time.Duration
	forceCacheRefresh     bool
	parseDirective        *spectypes.ParseDirective // setting the parse directive related to the api, can be nil
	usedDefaultValue      bool

	// resultErrorParsingMethod passed by each api interface message to parse the result of the message
	// and validate it doesn't contain a node error
	resultErrorParsingMethod func(data []byte, httpStatusCode int) (hasError bool, errorMessage string)
}

func (bcmc *baseChainMessageContainer) UpdateEarliestInMessage(incomingEarliest int64) bool {
	updatedSuccessfully := false
	if bcmc.earliestRequestedBlock != spectypes.EARLIEST_BLOCK {
		// check earliest is not unset (0) or incoming is lower than current value
		if bcmc.earliestRequestedBlock == 0 || bcmc.earliestRequestedBlock > incomingEarliest {
			bcmc.earliestRequestedBlock = incomingEarliest
			updatedSuccessfully = true
		}
	}
	return updatedSuccessfully
}

func (bcnc *baseChainMessageContainer) GetRequestedBlocksHashes() []string {
	return bcnc.requestedBlockHashes
}

func (bcnc *baseChainMessageContainer) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return bcnc.msg.SubscriptionIdExtractor(reply)
}

// returning parse directive for the api. can be nil.
func (bcnc *baseChainMessageContainer) GetParseDirective() *spectypes.ParseDirective {
	return bcnc.parseDirective
}

func (pm *baseChainMessageContainer) GetRawRequestHash() ([]byte, error) {
	// Compute fresh each call rather than memoizing into a struct field: the relay processor runs
	// CONCURRENT provider attempts on the same protocol message, so a lazy cache write here raced the
	// other attempts' reads (data race under -race). A struct-level lock/Once is not an option — this
	// type has value-receiver methods, so any sync field would trip `go vet copylocks`. The underlying
	// msg.GetRawRequestHash() is a pure function of the message's immutable fields (method/params/
	// headers → sha256), so recomputing it is cheap and inherently race-free.
	return pm.msg.GetRawRequestHash()
}

// not necessary for base chain message.
func (bcnc *baseChainMessageContainer) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	if bcnc.resultErrorParsingMethod == nil {
		utils.LavaFormatError("tried calling resultErrorParsingMethod when it is not set", nil)
		return false, ""
	}
	return bcnc.resultErrorParsingMethod(data, httpStatusCode)
}

func (bcnc *baseChainMessageContainer) TimeoutOverride(override ...time.Duration) time.Duration {
	if len(override) > 0 {
		bcnc.timeoutOverride = override[0]
	}
	return bcnc.timeoutOverride
}

func (bcnc *baseChainMessageContainer) SetForceCacheRefresh(force bool) bool {
	bcnc.forceCacheRefresh = force
	return bcnc.forceCacheRefresh
}

func (bcnc *baseChainMessageContainer) GetForceCacheRefresh() bool {
	return bcnc.forceCacheRefresh
}

func (bcnc *baseChainMessageContainer) DisableErrorHandling() {
	bcnc.msg.DisableErrorHandling()
}

func (bcnc baseChainMessageContainer) AppendHeader(metadata []pairingtypes.Metadata) {
	bcnc.msg.AppendHeader(metadata)
}

func (bcnc baseChainMessageContainer) GetApi() *spectypes.Api {
	return bcnc.api
}

func (bcnc baseChainMessageContainer) GetApiCollection() *spectypes.ApiCollection {
	return bcnc.apiCollection
}

func (bcnc baseChainMessageContainer) RequestedBlock() (latest int64, earliest int64) {
	if bcnc.earliestRequestedBlock == 0 {
		// earliest is optional and not set here
		return bcnc.latestRequestedBlock, bcnc.latestRequestedBlock
	}
	return bcnc.latestRequestedBlock, bcnc.earliestRequestedBlock
}

func (bcnc baseChainMessageContainer) GetRPCMessage() rpcInterfaceMessages.GenericMessage {
	return bcnc.msg
}

// IsBatch returns true if this is a batch request (e.g., JSON-RPC batch)
func (bcnc baseChainMessageContainer) IsBatch() bool {
	_, isBatch := bcnc.msg.(*rpcInterfaceMessages.JsonrpcBatchMessage)
	return isBatch
}

func (bcnc *baseChainMessageContainer) UpdateLatestBlockInMessage(latestBlock int64, modifyContent bool) (modifiedOnLatestReq bool) {
	requestedBlock, _ := bcnc.RequestedBlock()
	if latestBlock <= spectypes.NOT_APPLICABLE || requestedBlock != spectypes.LATEST_BLOCK {
		return false
	}
	success := bcnc.msg.UpdateLatestBlockInMessage(uint64(latestBlock), modifyContent)
	if success {
		bcnc.latestRequestedBlock = latestBlock
		return true
	}
	return false
}

func (bcnc *baseChainMessageContainer) GetExtensions() []*spectypes.Extension {
	utils.LavaFormatTrace("[Archive Debug] GetExtensions called", utils.LogAttr("extensions", len(bcnc.extensions)))
	return bcnc.extensions
}

// OverrideExtensions adds the extensions the request's lava-extension directive names. One that
// no node on this router offers is recorded, so the reply can tell the caller it was dropped.
func (bcnc *baseChainMessageContainer) OverrideExtensions(extensionNames []string, extensionParser *extensionslib.ExtensionParser) {
	bcnc.addExtensions(extensionNames, extensionParser, true)
}

// addRouterExtensions adds extensions the router requires on its own, such as archive for an
// eth_call deep behind the head. The caller did not ask for them, so one that no node offers is
// not recorded: the reply names only what the caller requested (MAG-3935).
func (bcnc *baseChainMessageContainer) addRouterExtensions(extensionNames []string, extensionParser *extensionslib.ExtensionParser) {
	bcnc.addExtensions(extensionNames, extensionParser, false)
}

// adds the following extensions. callerRequested says whether the caller asked for them; only
// then is one that no node offers recorded as unavailable.
func (bcnc *baseChainMessageContainer) addExtensions(extensionNames []string, extensionParser *extensionslib.ExtensionParser, callerRequested bool) {
	utils.LavaFormatTrace("[Archive Debug] OverrideExtensions called", utils.LogAttr("extensionNames", extensionNames), utils.LogAttr("existingExtensions", len(bcnc.extensions)))
	existingExtensions := map[string]struct{}{}
	for _, extension := range bcnc.extensions {
		existingExtensions[extension.Name] = struct{}{}
	}
	// Already reported as unavailable by an earlier call (override, then additional): looking it
	// up again would only record it twice.
	for _, extensionName := range bcnc.unavailableExtensions {
		existingExtensions[extensionName] = struct{}{}
	}
	for _, extensionName := range extensionNames {
		if _, ok := existingExtensions[extensionName]; !ok {
			existingExtensions[extensionName] = struct{}{}
			extensionKey := extensionslib.ExtensionKey{
				Extension:      extensionName,
				ConnectionType: bcnc.apiCollection.CollectionData.ApiInterface,
				InternalPath:   bcnc.apiCollection.CollectionData.InternalPath,
				Addon:          bcnc.apiCollection.CollectionData.AddOn,
			}
			utils.LavaFormatTrace("[Archive Debug] Looking for extension", utils.LogAttr("extensionKey", extensionKey), utils.LogAttr("configuredExtensions", extensionParser.GetConfiguredExtensions()))
			extension := extensionParser.GetExtension(extensionKey)
			if extension != nil {
				bcnc.extensions = append(bcnc.extensions, extension)
				bcnc.updateCUForApi(extension)
				utils.LavaFormatTrace("[Archive Debug] Extension added", utils.LogAttr("extensionName", extensionName), utils.LogAttr("totalExtensions", len(bcnc.extensions)))
			} else {
				if callerRequested {
					// The name is the caller's lava-extension header value, which on the HTTP
					// listeners aliases fasthttp's per-connection header buffer: the next request on
					// the same keep-alive connection rewrites it in place. Readers of this list keep
					// it past the request (the warn-once register's keys, metric labels), so own it.
					bcnc.unavailableExtensions = append(bcnc.unavailableExtensions, strings.Clone(extensionName))
				}
				utils.LavaFormatTrace("[Archive Debug] Extension not found", utils.LogAttr("extensionName", extensionName), utils.LogAttr("extensionKey", extensionKey))
			}
		} else {
			utils.LavaFormatTrace("[Archive Debug] Extension already exists", utils.LogAttr("extensionName", extensionName))
		}
	}
	utils.LavaFormatTrace("[Archive Debug] OverrideExtensions completed", utils.LogAttr("finalExtensions", len(bcnc.extensions)))
}

// GetUnavailableExtensions returns the extensions the caller requested that no node on this
// router offers, in request order. The request was served without them.
func (bcnc *baseChainMessageContainer) GetUnavailableExtensions() []string {
	return bcnc.unavailableExtensions
}

func (bcnc *baseChainMessageContainer) GetUsedDefaultValue() bool {
	return bcnc.usedDefaultValue
}

func (bcnc *baseChainMessageContainer) SetExtension(extension *spectypes.Extension) {
	if len(bcnc.extensions) > 0 {
		for _, ext := range bcnc.extensions {
			if ext.Name == extension.Name {
				// already existing, no need to add
				return
			}
		}
		bcnc.extensions = append(bcnc.extensions, extension)
	} else {
		bcnc.extensions = []*spectypes.Extension{extension}
	}
	bcnc.updateCUForApi(extension)
}

func (bcnc *baseChainMessageContainer) updateCUForApi(extension *spectypes.Extension) {
	copyApi := *bcnc.api // we can't modify this because it points to an object inside the chainParser
	copyApi.ComputeUnits = uint64(math.Floor(float64(extension.GetCuMultiplier()) * float64(copyApi.ComputeUnits)))
	bcnc.api = &copyApi
}

type CraftData struct {
	Path           string
	Data           []byte
	ConnectionType string
	InternalPath   string
}

func CraftChainMessage(parsing *spectypes.ParseDirective, connectionType string, chainParser ChainParser, craftData *CraftData, metadata []pairingtypes.Metadata) (ChainMessageForSend, error) {
	return chainParser.CraftMessage(parsing, connectionType, craftData, metadata)
}
