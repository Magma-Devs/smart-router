package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
)

// httpRelayRequest is what one listener handler reads off the request: what it hands SendRelay
// and what its log lines print.
type httpRelayRequest struct {
	path           string // the request path, as the handler's received line names it
	url            string // SendRelay's url argument
	body           string // the relayed body, also the request on the debug-level success line
	loggedBody     string // the body as the received and failure lines print it (relayBodyForLog); relayHTTP sets it
	connectionType string // SendRelay's connection type
	logMethod      string // the method on the in/out line
	logPath        string // the path on the in/out line
}

// httpRelaySpec is what differs between the listeners' HTTP handlers. relayHTTP is the rest:
// detached headers, the GUID, analytics, the relay, the metric, the masked error envelope and
// the reply.
type httpRelaySpec struct {
	request func(fiberCtx *fiber.Ctx) httpRelayRequest
	// logReceived writes the handler's "request received" line.
	logReceived func(ctx context.Context, guid uint64, msgSeed, dappID string, req httpRelayRequest, headers []pairingtypes.Metadata)
	// logModule names the in/out lines LogRequestAndResponse writes.
	logModule string
	// answerError may answer a relay error before the masked envelope; handled false falls through.
	answerError func(ctx context.Context, fiberCtx *fiber.Ctx, err error, reply *pairingtypes.RelayReply) (handled bool, sendErr error)
	// refusal answers a request the router declined by one of its own limits (MAG-4577): the
	// status, the body, the reply text for the failure line, and the headers the reply carries
	// beside Router-Guid (Router-Write-Outcome on a refused write, MAG-5120); ok false falls through.
	// Unlike answerError, relayHTTP writes the failure line for it, with the cut body (MAG-4454).
	refusal func(err error, msgSeed string) (status int, body []byte, loggedReply string, headers map[string]string, ok bool)
	// noAnswer is the shape of the router's own 502/504 when no node answered (MAG-4481).
	noAnswer noAnswerShape
	// errorBody builds the masked error envelope.
	errorBody func(fiberCtx *fiber.Ctx, relayResult *common.RelayResult, errMasking, msgSeed string) []byte
	// replyBody rewrites a successful reply's body; nil sends it as the relay returned it.
	replyBody func(data []byte) []byte
	// testMode logs every request's URI and headers (common.IsTestMode).
	testMode bool
	// debugRelays logs whole request bodies instead of their cut form (--debug-relays).
	debugRelays bool
}

// relayHTTP is the handler every HTTP listener route relays through.
func (apil *chainListenerBase) relayHTTP(spec httpRelaySpec) fiber.Handler {
	chainID := apil.endpoint.ChainID
	apiInterface := apil.endpoint.ApiInterface
	return func(fiberCtx *fiber.Ctx) error {
		fiberCtx.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSONCharsetUTF8)
		startTime := time.Now()
		req := spec.request(fiberCtx)
		// A body holds what the client sent, a signed transaction included; the received and
		// failure lines carry its cut form unless --debug-relays (MAG-4454).
		req.loggedBody = relayBodyForLog(req.body, spec.debugRelays)
		dappID := extractDappIDFromFiberContext(fiberCtx)
		analytics := metrics.NewRelayAnalytics(dappID, chainID, apiInterface)
		analytics.SetProcessingTimestampBeforeRelay(startTime)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		guid := utils.GenerateUniqueIdentifier()
		ctx = utils.WithUniqueIdentifier(ctx, guid)
		msgSeed := strconv.FormatUint(guid, 10)
		if spec.testMode {
			apil.logger.LogTestMode(fiberCtx)
		}

		// Detached copies: fiber hands out strings that alias the per-connection buffer, and the
		// relay keeps some of these past the reply (MAG-3881).
		metadataValues := detachedReqHeaders(fiberCtx)
		ctx = utils.ExtractWantedHeadersFromCachedMap(metadataValues, ctx)
		userIp := GetHeaderFromCachedMap(metadataValues, common.IP_FORWARDING_HEADER_NAME, fiberCtx.IP())
		headers := convertToMetadataMap(metadataValues)
		spec.logReceived(ctx, guid, msgSeed, dappID, req, headers)

		relayResult, err := apil.relaySender.SendRelay(ctx, req.url, req.body, req.connectionType, dappID, userIp, analytics, headers)
		reply := relayResult.GetReply()
		apil.logger.AddMetricForHttp(analytics, err, metadataValues)
		if err != nil {
			if spec.answerError != nil {
				if handled, sendErr := spec.answerError(ctx, fiberCtx, err, reply); handled {
					return sendErr
				}
			}
			if spec.refusal != nil {
				if status, body, loggedReply, headers, ok := spec.refusal(err, msgSeed); ok {
					apil.logger.LogRequestAndResponse(spec.logModule, true, req.logMethod, req.logPath, req.loggedBody, loggedReply, msgSeed, time.Since(startTime), err)
					fiberCtx.Status(status)
					// A refusal is made before any provider is asked, so no relay ran to put the
					// request's headers on it. The request id goes on here, the one the body's guid
					// and the router's log lines carry (MAG-4351).
					fiberCtx.Set(common.GUID_HEADER_NAME, msgSeed)
					for name, value := range headers {
						fiberCtx.Set(name, value)
					}
					return addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), body)
				}
			}
			if message, ok := refusedRequestMessage(err); ok {
				// The router refused the request itself before asking any provider: the request is
				// the reason, so it is a client error, not the 500 that tells every alert rule and
				// retry policy the router failed (MAG-3800). The listener's own error body carries
				// one fixed sentence, never err's text, which lists the router's provider names.
				// Masked mode keeps the sentence too: the router wrote it, so it holds nothing to hide.
				response := spec.errorBody(fiberCtx, relayResult, refusalEnvelope(message, msgSeed), msgSeed)
				apil.logger.LogRequestAndResponse(spec.logModule, true, req.logMethod, req.logPath, req.loggedBody, loggedReply(response), msgSeed, time.Since(startTime), err)
				fiberCtx.Status(fiber.StatusBadRequest)
				fiberCtx.Set(common.GUID_HEADER_NAME, msgSeed)
				return addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), response)
			}
			errMasking := apil.logger.GetUniqueGuidResponseForError(err, msgSeed)
			// MAG-4481: the upstream's own answer as is, or the router's 502/504 when none answered.
			// The request log records what the client is sent.
			if failed, ok := buildFailedRelayReply(relayResult, msgSeed, fiberCtx.Body(), spec.noAnswer); ok {
				apil.logger.LogRequestAndResponse(spec.logModule, true, req.logMethod, req.logPath, req.loggedBody, loggedReply(failed.body), msgSeed, time.Since(startTime), err)
				return sendFailedRelayReply(fiberCtx, failed, reply.GetMetadata())
			}
			response := spec.errorBody(fiberCtx, relayResult, errMasking, msgSeed)
			apil.logger.LogRequestAndResponse(spec.logModule, true, req.logMethod, req.logPath, req.loggedBody, loggedReply(response), msgSeed, time.Since(startTime), err)
			fiberCtx.Status(failedRelayStatus(relayResult))
			return addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), response)
		}

		response := reply.Data
		if spec.replyBody != nil {
			response = spec.replyBody(response)
		}
		// LogRequestAndResponse writes a success at debug level only. Go evaluates arguments
		// eagerly, so without the gate every reply body would be copied into a string that is
		// never printed.
		var loggedResponse string
		if utils.IsDebugEnabled() {
			loggedResponse = string(response)
		}
		apil.logger.LogRequestAndResponse(spec.logModule, false, req.logMethod, req.logPath, req.body, loggedResponse, msgSeed, time.Since(startTime), nil)
		if relayResult.GetStatusCode() != 0 {
			fiberCtx.Status(relayResult.StatusCode)
		}
		return addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), response)
	}
}

// refusedRequestMessage reports whether err is a refusal of the request itself that every
// interface answers as a client error, and the one sentence the caller is given for it: a
// router-select-provider name that none of the router's providers carries (MAG-3800), and on gRPC a
// binary request that is not a protobuf message (MAG-5421) or a JSON request that is not JSON the
// method's message reads (MAG-5676).
func refusedRequestMessage(err error) (message string, ok bool) {
	switch {
	case errors.Is(err, routersession.SelectedProviderNotConfiguredError):
		return routersession.SelectedProviderNotConfiguredError.Error(), true
	case errors.Is(err, GrpcRequestNotProtobufError):
		return GrpcRequestNotProtobufError.Error(), true
	case errors.Is(err, routersession.ErrGRPCRequestNotValidJSON):
		return routersession.ErrGRPCRequestNotValidJSON.Error(), true
	}
	return "", false
}

// refusalEnvelope is the {"Error_GUID":…,"Error":…} text a listener's errorBody reads, built for
// a router-written sentence: unlike GetUniqueGuidResponseForError it keeps the sentence in masked
// mode as well.
func refusalEnvelope(message, msgSeed string) string {
	envelope, err := json.Marshal(struct {
		ErrorGUID string `json:"Error_GUID"`
		Error     string `json:"Error"`
	}{ErrorGUID: msgSeed, Error: message})
	if err != nil {
		return message
	}
	return string(envelope)
}

// receivedAttrs are a received line's GUID and the tracing ids ctx carries, followed by the
// handler's own attributes.
func receivedAttrs(ctx context.Context, guid uint64, attrs ...utils.Attribute) []utils.Attribute {
	return append([]utils.Attribute{
		utils.LogAttr("GUID", guid),
		utils.LogAttr(utils.KEY_REQUEST_ID, ctx),
		utils.LogAttr(utils.KEY_TASK_ID, ctx),
		utils.LogAttr(utils.KEY_TRANSACTION_ID, ctx),
	}, attrs...)
}

// logReceivedWithSeedAfterDapp writes the received line the REST POST and GraphQL handlers share.
func logReceivedWithSeedAfterDapp(message string) func(context.Context, uint64, string, string, httpRelayRequest, []pairingtypes.Metadata) {
	return func(ctx context.Context, guid uint64, msgSeed, dappID string, req httpRelayRequest, headers []pairingtypes.Metadata) {
		utils.FormatInfo(message, receivedAttrs(ctx, guid,
			utils.LogAttr("path", req.path),
			utils.LogAttr("dappID", dappID),
			utils.LogAttr("msgSeed", msgSeed),
			utils.LogAttr("body", req.loggedBody),
			utils.LogAttr("headers", common.RedactMetadata(headers)),
		)...)
	}
}

// listen binds app, records the address it bound and serves until ctx ends.
func (apil *chainListenerBase) listen(ctx context.Context, app *fiber.App) {
	addrChannel := make(chan string)
	addrChannelSafe := common.NewSafeChannelSender(ctx, addrChannel)
	go func() {
		addr := <-addrChannel
		apil.listeningAddress.Store(&addr)
	}()

	listenWithRetry(ctx, app, apil.endpoint.NetworkAddress, addrChannelSafe, &apil.gate)
}

// answerJsonRpcBatchTooLarge answers a batch over the operator's size limit with 429.
func answerJsonRpcBatchTooLarge(fiberCtx *fiber.Ctx, err error) (bool, error) {
	if !errors.Is(err, ErrBatchRequestSizeExceeded) {
		return false, nil
	}
	errorResponse, _ := json.Marshal(common.JsonRpcBatchSizeExceededError)
	return true, fiberCtx.Status(fiber.StatusTooManyRequests).SendString(string(errorResponse))
}

// answerJsonRpcMethodNotFound answers an api the spec does not declare with JSON-RPC's
// method-not-found error.
func answerJsonRpcMethodNotFound(fiberCtx *fiber.Ctx, err error, reply *pairingtypes.RelayReply) (bool, error) {
	if !errors.Is(err, common.APINotSupportedError) {
		return false, nil
	}
	errorResponse, _ := json.Marshal(common.JsonRpcMethodNotFoundError)
	return true, addHeadersAndSendBytes(fiberCtx, reply.GetMetadata(), errorResponse)
}

// wsListenerBase is what the two listeners that serve WebSocket subscriptions, JSON-RPC and
// Tendermint, add to chainListenerBase.
type wsListenerBase struct {
	refererData                *RefererData
	wsSubscriptionManager      WSSubscriptionManager
	websocketConnectionLimiter *WebsocketConnectionLimiter
	wsWG                       sync.WaitGroup // tracks active downstream WS goroutines for Shutdown drain
}

func newWSListenerBase(refererData *RefererData, wsSubscriptionManager WSSubscriptionManager) wsListenerBase {
	return wsListenerBase{
		refererData:                refererData,
		wsSubscriptionManager:      wsSubscriptionManager,
		websocketConnectionLimiter: &WebsocketConnectionLimiter{ipToNumberOfActiveConnections: make(map[string]int64)},
	}
}

// websocketRoutes registers /ws and /websocket on app and returns the upgrade middleware and the
// callback, for a listener that also hands a bare-URL upgrade to them. name prefixes the
// opened/closed log lines; connectionType is what ParseMsg looks the subscription method up with.
func (wsl *wsListenerBase) websocketRoutes(ctx context.Context, app *fiber.App, base *chainListenerBase, cmdFlags common.ConsumerCmdFlags, name, connectionType string) (upgrade, callback fiber.Handler) {
	// wsWG.Add must run synchronously inside the request handler so that
	// app.ShutdownWithContext (which waits for in-flight handlers but stops
	// tracking hijacked WS connections) is guaranteed to observe the Add
	// before wsWG.Wait runs. Doing the Add inside the websocket goroutine
	// would race: the goroutine is scheduled after the connection is hijacked
	// and may not have called Add by the time Wait sees counter == 0.
	//
	// Registered as per-route middleware (not app.Use) so /ws and /websocket
	// are covered uniformly without depending on fiber's mount-prefix matching.
	upgrade = func(c *fiber.Ctx) error {
		wsl.websocketConnectionLimiter.HandleFiberRateLimitFlags(c)
		// IsWebSocketUpgrade returns true if the client
		// requested upgrade to the WebSocket protocol.
		if !websocket.IsWebSocketUpgrade(c) {
			return fiber.ErrUpgradeRequired
		}
		wsl.wsWG.Add(1)
		c.Locals("allowed", true)
		err := c.Next()
		// If the upgrade did not succeed (anything other than 101 Switching
		// Protocols), the websocket goroutine will not run, so balance the Add.
		if c.Response().StatusCode() != fiber.StatusSwitchingProtocols {
			wsl.wsWG.Done()
		}
		return err
	}

	chainID := base.endpoint.ChainID
	apiInterface := base.endpoint.ApiInterface
	webSocketCallback := websocket.New(func(websocketConn *websocket.Conn) {
		defer wsl.wsWG.Done()
		canOpenConnection, decreaseIpConnection := wsl.websocketConnectionLimiter.CanOpenConnection(websocketConn)
		defer decreaseIpConnection()
		if !canOpenConnection {
			return
		}
		rateLimitInf := websocketConn.Locals(WebSocketRateLimitHeader)
		rateLimit, assertionSuccessful := rateLimitInf.(int64)
		if !assertionSuccessful || rateLimit < 0 {
			rateLimit = 0
		}

		utils.FormatDebug(name+" websocket opened", utils.LogAttr("consumerIp", websocketConn.LocalAddr().String()))
		defer utils.FormatDebug(name+" websocket closed", utils.LogAttr("consumerIp", websocketConn.LocalAddr().String()))

		consumerWebsocketManager := NewConsumerWebsocketManager(ConsumerWebsocketManagerOptions{
			WebsocketConn:          websocketConn,
			RpcConsumerLogs:        base.logger,
			RefererMatchString:     refererMatchString,
			CmdFlags:               cmdFlags,
			ChainID:                chainID,
			ApiInterface:           apiInterface,
			ConnectionType:         connectionType,
			RefererData:            wsl.refererData,
			RelaySender:            base.relaySender,
			WsSubscriptionManager:  wsl.wsSubscriptionManager,
			WebsocketConnectionUID: strconv.FormatUint(utils.GenerateUniqueIdentifier(), 10),
			headerRateLimit:        uint64(rateLimit),
		})

		consumerWebsocketManager.ListenToMessages(ctx)
	})
	callback = constructFiberCallbackWithHeaderAndParameterExtraction(webSocketCallback)
	app.Get("/ws", upgrade, callback)
	app.Get("/websocket", upgrade, callback) // catching http://HOST:PORT/1/websocket requests.
	return upgrade, callback
}

// The batch refusal answer is JSON-RPC only: Tendermint batches carry no chain rule that refuses them.
func answerJsonRpcBatchRefused(fiberCtx *fiber.Ctx, err error) (bool, error) {
	if !errors.Is(err, rpcInterfaceMessages.ErrJsonrpcBatchRefused) {
		return false, nil
	}
	// A batch the chain cannot take (an XRPL submit inside one). The node itself would refuse it
	// with a 400, so the caller gets the same verdict, as a JSON-RPC invalid request.
	refused := common.JsonRpcInvalidRequestError
	refused.Error.Data = rpcInterfaceMessages.ErrJsonrpcBatchRefused.Error()
	errorResponse, _ := json.Marshal(refused)
	return true, fiberCtx.Status(fiber.StatusBadRequest).SendString(string(errorResponse))
}
