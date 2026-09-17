package chainlib

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/websocket/v2"
	"github.com/magma-Devs/smart-router/protocol/chainlib/cacheformat"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/tidwall/gjson"
)

var (
	WebSocketRateLimit   = -1               // rate limit requests per second on websocket connection
	WebSocketBanDuration = time.Duration(0) // once rate limit is reached, will not allow new incoming message for a duration
)

// How long a connection may go without traffic in either direction before the
// router closes it. Same shape as the keep-alive interval below and for the same
// reason: it is written once from a startup flag but read by every live
// connection's idle reaper, so a test that shortens it would otherwise race the
// connections already running.
var maxIdleTimeInSeconds = func() *atomic.Int64 {
	idleTime := &atomic.Int64{}
	idleTime.Store(DefaultMaxIdleTimeInSeconds)
	return idleTime
}()

func SetMaxIdleTimeInSeconds(seconds int64) {
	maxIdleTimeInSeconds.Store(seconds)
}

func GetMaxIdleTimeInSeconds() int64 {
	return maxIdleTimeInSeconds.Load()
}

// The server pings a live connection every keep-alive interval. Proxies in front
// of the router (Cloudflare, the Envoy Gateway) close a silent WebSocket after
// ~2 minutes and send no close frame, so an idle subscription reaches the client
// as a transport error rather than an unsubscribe; pinging inside that window
// keeps it open. The interval is a startup flag, held in an atomic so tests can
// shorten it while other connections are running. Zero or less disables pings.
var webSocketKeepAliveInterval = func() *atomic.Int64 {
	interval := &atomic.Int64{}
	interval.Store(int64(DefaultWebSocketKeepAliveInterval))
	return interval
}()

func SetWebSocketKeepAliveInterval(interval time.Duration) {
	webSocketKeepAliveInterval.Store(int64(interval))
}

func GetWebSocketKeepAliveInterval() time.Duration {
	return time.Duration(webSocketKeepAliveInterval.Load())
}

// How long one frame write may take before the router gives up on the client. A
// client that stopped reading blocks the writer inside WriteMessage once the socket
// buffers fill; without a bound that pins the writer, every sender waiting on it,
// and the handler that waits for the writer on return. On the deadline the write
// fails, the writer exits, and the read loop tears the connection down. Same shape
// as the two tunables above. Zero or less disables the deadline, which removes the
// only bound on such a client.
var webSocketWriteTimeout = func() *atomic.Int64 {
	timeout := &atomic.Int64{}
	timeout.Store(int64(DefaultWebSocketWriteTimeout))
	return timeout
}()

func SetWebSocketWriteTimeout(timeout time.Duration) {
	webSocketWriteTimeout.Store(int64(timeout))
}

func GetWebSocketWriteTimeout() time.Duration {
	return time.Duration(webSocketWriteTimeout.Load())
}

// WebSocketKeepAliveOutlivesIdleReaper reports whether the two tunables together let
// a quiet connection live for as long as its client keeps the socket open: the
// keep-alive pings stop the proxy in front of the router from reaping it, and with
// the idle limit off the router never will either. That is what an operator who
// disables the idle limit asked for, but before the keep-alive existed the proxy
// reaped such a connection within minutes anyway, so the combination deserves a
// warning at startup rather than silence.
func WebSocketKeepAliveOutlivesIdleReaper(keepAlive time.Duration, maxIdleSeconds int64) bool {
	return keepAlive > 0 && maxIdleSeconds <= 0
}

const (
	DefaultMaxIdleTimeInSeconds       = int64(20 * 60) // 20 minutes of idle time will disconnect the websocket connection
	DefaultWebSocketKeepAliveInterval = 30 * time.Second
	DefaultWebSocketWriteTimeout      = 10 * time.Second

	WebSocketRateLimitHeader            = "x-lava-websocket-rate-limit"
	WebSocketOpenConnectionsLimitHeader = "x-lava-websocket-open-connections-limit"

	SubscriptionDeliveryMethod           = "subscription_delivery"
	DefaultSubscriptionDeliveryCU uint64 = 10
)

type ConsumerWebsocketManager struct {
	websocketConn          *websocket.Conn
	rpcConsumerLogs        *metrics.RPCConsumerLogs
	cmdFlags               common.ConsumerCmdFlags
	refererMatchString     string
	relayMsgLogMaxChars    int
	chainId                string
	apiInterface           string
	connectionType         string
	refererData            *RefererData
	relaySender            RelaySender
	wsSubscriptionManager  WSSubscriptionManager
	WebsocketConnectionUID string
	headerRateLimit        uint64
}

type ConsumerWebsocketManagerOptions struct {
	WebsocketConn          *websocket.Conn
	RpcConsumerLogs        *metrics.RPCConsumerLogs
	RefererMatchString     string
	CmdFlags               common.ConsumerCmdFlags
	RelayMsgLogMaxChars    int
	ChainID                string
	ApiInterface           string
	ConnectionType         string
	RefererData            *RefererData
	RelaySender            RelaySender
	WsSubscriptionManager  WSSubscriptionManager
	WebsocketConnectionUID string
	headerRateLimit        uint64
}

func NewConsumerWebsocketManager(options ConsumerWebsocketManagerOptions) *ConsumerWebsocketManager {
	cwm := &ConsumerWebsocketManager{
		websocketConn:          options.WebsocketConn,
		relaySender:            options.RelaySender,
		rpcConsumerLogs:        options.RpcConsumerLogs,
		cmdFlags:               options.CmdFlags,
		refererMatchString:     options.RefererMatchString,
		relayMsgLogMaxChars:    options.RelayMsgLogMaxChars,
		chainId:                options.ChainID,
		apiInterface:           options.ApiInterface,
		connectionType:         options.ConnectionType,
		refererData:            options.RefererData,
		wsSubscriptionManager:  options.WsSubscriptionManager,
		WebsocketConnectionUID: options.WebsocketConnectionUID,
		headerRateLimit:        options.headerRateLimit,
	}
	return cwm
}

func (cwm *ConsumerWebsocketManager) handleRateLimitReached(inpData []byte) ([]byte, error) {
	rateLimitError := common.JsonRpcRateLimitError
	id := 0
	result := gjson.GetBytes(inpData, "id")
	switch result.Type {
	case gjson.Number:
		id = int(result.Int())
	case gjson.String:
		idParsed, err := strconv.Atoi(result.Raw)
		if err == nil {
			id = idParsed
		}
	}
	rateLimitError.Id = id
	bytesRateLimitError, err := json.Marshal(rateLimitError)
	if err != nil {
		return []byte{}, utils.LavaFormatError("failed marshalling jsonrpc rate limit error", err)
	}
	return bytesRateLimitError, nil
}

// webSocketMsgWithType is one frame queued for the connection's writer goroutine.
type webSocketMsgWithType struct {
	messageType int
	msg         []byte
	// final marks the last frame of the connection: once it is on the wire the
	// writer tears the connection down itself instead of waiting for the peer to
	// act on it. Senders that decide a connection is over — the idle reaper — set
	// it, because they run on goroutines that must not touch the conn directly.
	final bool
}

// websocketFrameWriter is the slice of the connection the writer goroutine touches.
// *websocket.Conn satisfies it; tests hand in a fake whose writes fail on demand.
type websocketFrameWriter interface {
	WriteMessage(messageType int, data []byte) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// runWebsocketWriter is the connection's single writer. Single-writer discipline
// guarantees WriteMessage is never called concurrently, which the underlying
// gorilla/fasthttp websocket library does not allow.
//
// It serves regular frames, the keep-alive ping and the server-shutdown close frame,
// and closes writerDone on its way out. A failed write ends it, and that used to leave every later
// enqueueWebsocketFrame caller blocked forever: the read loop's own error path sends a
// frame before it can return, so the handler never returned, and fasthttp kept the
// 128 KiB read buffer, the handler goroutine and the per-IP limiter slot for the life
// of the process (MAG-3722). Now a failed write also sets an immediate read deadline so
// the read loop wakes and the handler tears the connection down.
func runWebsocketWriter(ctx, webSocketCtx context.Context, conn websocketFrameWriter, frames <-chan webSocketMsgWithType, writerDone chan<- struct{}) {
	defer close(writerDone)

	// The keep-alive fires only after a full interval with nothing written: every
	// frame that goes out resets the ticker, so a busy connection is never pinged
	// and a quiet one is pinged exactly once per interval of silence, which is the
	// silence the proxy measures. A nil channel blocks forever, so a non-positive
	// interval disables the keep-alive without a second select arm.
	//
	// Deliberately unlike the upstream client in chainproxy/rpcclient/websocket.go,
	// which pings on its own cadence and drops the node when a pong is late: that
	// side owns a connection to a node the router chose, whereas the peer here is a
	// customer on whatever network they have. A late pong does not end a customer's
	// connection. A peer that vanished is still caught, by the TCP retransmission
	// timeout once the pings stop being acknowledged, and a peer that stopped
	// reading by the write deadline below.
	var keepAlive <-chan time.Time
	var keepAliveTicker *time.Ticker
	interval := GetWebSocketKeepAliveInterval()
	if interval > 0 {
		keepAliveTicker = time.NewTicker(interval)
		defer keepAliveTicker.Stop()
		keepAlive = keepAliveTicker.C
	}

	// writeFrame is the only way to the connection. The deadline keeps a stalled
	// client from blocking the goroutine forever, which would in turn stall the
	// handler waiting on writerDone.
	writeTimeout := GetWebSocketWriteTimeout()
	writeFrame := func(messageType int, data []byte) error {
		if writeTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		}
		err := conn.WriteMessage(messageType, data)
		if err == nil && keepAliveTicker != nil {
			keepAliveTicker.Reset(interval)
		}
		return err
	}

	for {
		select {
		case <-keepAlive:
			// Ping from inside the writer goroutine to keep the single-writer
			// discipline. A failed write means the connection is already gone:
			// unblock the read loop with an immediate read deadline, the same way
			// the shutdown branch below does, and leave the Close to the handler.
			if err := writeFrame(websocket.PingMessage, nil); err != nil {
				utils.LavaFormatTrace("error writing keep-alive ping to the websocket", utils.LogAttr("err", err))
				_ = conn.SetReadDeadline(time.Now())
				return
			}
		case <-ctx.Done():
			// Server-wide shutdown — send a CloseGoingAway (1001) frame so the client can
			// distinguish an intentional shutdown from a crash, then unblock the read loop by
			// setting an immediate read deadline rather than Close()-ing the conn here.
			//
			// Close() from this goroutine raced gofiber's handler cleanup: it unblocks ReadMessage,
			// the handler returns, and gofiber recycles the Conn (fasthttp pooling) WHILE this
			// goroutine is still inside Close() — a use-after-recycle data race. SetReadDeadline
			// returns immediately (the read loop breaks on the resulting error) and lets the handler,
			// the conn's sole owner, do the single Close on return.
			_ = writeFrame(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"))
			_ = conn.SetReadDeadline(time.Now())
			return
		case <-webSocketCtx.Done():
			utils.LavaFormatTrace("websocket's context cancelled", utils.LogAttr("GUID", webSocketCtx))
			return
		case msg, ok := <-frames:
			if !ok {
				return
			}
			if err := writeFrame(msg.messageType, msg.msg); err != nil {
				utils.LavaFormatTrace("error writing msg to the websocket", utils.LogAttr("err", err))
				_ = conn.SetReadDeadline(time.Now())
				return
			}
			if msg.final {
				// The sender has ended the connection and its frame is on the wire.
				// Tear down exactly as the shutdown branch does rather than waiting
				// for a peer that may never act on the close frame: the read loop
				// wakes on the deadline and the handler does the single Close.
				_ = conn.SetReadDeadline(time.Now())
				return
			}
		}
	}
}

// enqueueWebsocketFrame hands a frame to the writer goroutine, but gives up as soon as
// ctx (server shutdown) or webSocketCtx (per-connection cleanup) cancels or the writer
// is gone: a plain send would park the caller for good.
func enqueueWebsocketFrame(ctx, webSocketCtx context.Context, writerDone <-chan struct{}, frames chan<- webSocketMsgWithType, msg webSocketMsgWithType) {
	select {
	case frames <- msg:
	case <-ctx.Done():
	case <-webSocketCtx.Done():
	case <-writerDone:
	}
}

func (cwm *ConsumerWebsocketManager) ListenToMessages(ctx context.Context) {
	// adding metrics for how many active connections we have.
	cwm.rpcConsumerLogs.SetWebSocketConnectionActive(cwm.chainId, cwm.apiInterface, true)
	defer cwm.rpcConsumerLogs.SetWebSocketConnectionActive(cwm.chainId, cwm.apiInterface, false)

	var (
		messageType int
		msg         []byte
		err         error
	)

	websocketConnWriteChan := make(chan webSocketMsgWithType)
	// Closed by the writer goroutine on its way out.
	writerDone := make(chan struct{})

	websocketConn := cwm.websocketConn
	logger := cwm.rpcConsumerLogs

	webSocketCtx, cancelWebSocketCtx := context.WithCancel(context.Background())
	guid := utils.GenerateUniqueIdentifier()
	guidString := strconv.FormatUint(guid, 10)
	webSocketCtx = utils.WithUniqueIdentifier(webSocketCtx, guid)
	utils.LavaFormatDebug("consumer websocket manager started", utils.LogAttr("GUID", webSocketCtx))
	defer func() {
		// The connection is over. Release every subscription this client still holds
		// right here, synchronously: reply channels close and the forwarder goroutines
		// below end now, not whenever a cancelled context happens to be noticed. dappID
		// and the remote address are per-connection, so this key matches every
		// subscribe the loop registered.
		if cwm.wsSubscriptionManager != nil {
			dappID, _ := websocketConn.Locals(ProjectIDHeader).(string)
			if err := cwm.wsSubscriptionManager.UnsubscribeAll(webSocketCtx, dappID, websocketConn.RemoteAddr().String(), cwm.WebsocketConnectionUID, nil); err != nil {
				utils.LavaFormatDebug("error releasing subscriptions on websocket close", utils.LogAttr("err", err), utils.LogAttr("GUID", webSocketCtx))
			}
		}
		cancelWebSocketCtx() // In case there's a problem make sure to cancel the connection
		// gofiber recycles the connection the moment this handler returns, so wait
		// for the writer goroutine to stop touching it — otherwise a keep-alive ping
		// can land on a released conn. websocketWriteTimeout bounds the wait.
		<-writerDone
		utils.LavaFormatDebug("consumer websocket manager stopped", utils.LogAttr("GUID", webSocketCtx))
	}()

	sendWS := func(msg webSocketMsgWithType) {
		enqueueWebsocketFrame(ctx, webSocketCtx, writerDone, websocketConnWriteChan, msg)
	}

	go runWebsocketWriter(ctx, webSocketCtx, cwm.websocketConn, websocketConnWriteChan, writerDone)

	// set up a routine to check for rate limits or idle time
	idleFor := atomic.Int64{}
	idleFor.Store(time.Now().Unix())
	requestsPerSecond := &atomic.Uint64{}
	go func() {
		if WebSocketRateLimit <= 0 && cwm.headerRateLimit <= 0 && GetMaxIdleTimeInSeconds() <= 0 {
			return
		}
		ticker := time.NewTicker(time.Second) // rate limit per second.
		defer ticker.Stop()
		for {
			select {
			case <-webSocketCtx.Done():
				utils.LavaFormatDebug("ctx done in time checker")
				return
			case <-ticker.C:
				if maxIdleTime := GetMaxIdleTimeInSeconds(); maxIdleTime > 0 {
					utils.LavaFormatDebug("checking idle time", utils.LogAttr("idleFor", idleFor.Load()), utils.LogAttr("maxIdleTime", maxIdleTime), utils.LogAttr("now", time.Now().Unix()))
					idleDuration := idleFor.Load() + maxIdleTime
					if time.Now().Unix() > idleDuration {
						// Route the idle-close frame through the single writer goroutine (sendWS), NOT a
						// direct WriteMessage: this goroutine is separate from the writer, and gofiber/
						// gorilla forbids concurrent writers — a direct write here raced the writer
						// goroutine's WriteMessage (data race).
						//
						// final tears the connection down once that frame is out rather than trusting
						// the client to close on it. A client that ignores it used to be reaped by the
						// proxy in front of the router, which closes a silent socket within ~2 minutes
						// and so woke the read loop for us; the keep-alive ping means the connection is
						// never silent, so that backstop is gone and nothing else would ever free the
						// handler goroutine, its read buffer or the per-IP limiter slot (MAG-3722).
						sendWS(webSocketMsgWithType{
							messageType: websocket.CloseMessage,
							msg:         websocket.FormatCloseMessage(websocket.CloseNormalClosure, fmt.Sprintf("Connection idle for too long, closing connection. Idle time: %d", idleDuration)),
							final:       true,
						})
						return
					}
				}
				if cwm.headerRateLimit > 0 || WebSocketRateLimit > 0 {
					// check if rate limit reached, and ban is required
					currentRequestsPerSecondLoad := requestsPerSecond.Load()
					if WebSocketBanDuration > 0 && (currentRequestsPerSecondLoad > cwm.headerRateLimit || currentRequestsPerSecondLoad > uint64(WebSocketRateLimit)) {
						// wait the ban duration before resetting the store.
						select {
						case <-webSocketCtx.Done():
							return
						case <-time.After(WebSocketBanDuration): // just continue
						}
					}
					requestsPerSecond.Store(0)
				}
			}
		}
	}()

	for {
		idleFor.Store(time.Now().Unix())
		startTime := time.Now()
		msgSeed := guidString + "_" + strconv.Itoa(rand.Intn(10000000000)) // use message seed with original guid and new int

		utils.LavaFormatTrace("listening for new message from the websocket")

		if messageType, msg, err = websocketConn.ReadMessage(); err != nil {
			utils.LavaFormatTrace("error reading msg from the websocket, probably websocket was closed by the user", utils.LogAttr("err", err))
			formatterMsg := logger.AnalyzeWebSocketErrorAndGetFormattedMessage(websocketConn.LocalAddr().String(), err, msgSeed, msg, cwm.apiInterface, time.Since(startTime))
			if formatterMsg != nil {
				sendWS(webSocketMsgWithType{messageType: messageType, msg: formatterMsg})
			}
			break
		}

		// Check rate limit is met
		currentRequestsPerSecond := requestsPerSecond.Add(1)
		if (cwm.headerRateLimit > 0 && currentRequestsPerSecond > cwm.headerRateLimit) ||
			(WebSocketRateLimit > 0 && currentRequestsPerSecond > uint64(WebSocketRateLimit)) {
			rateLimitResponse, err := cwm.handleRateLimitReached(msg)
			if err == nil {
				sendWS(webSocketMsgWithType{messageType: messageType, msg: rateLimitResponse})
			}
			continue
		}

		dappID, ok := websocketConn.Locals(ProjectIDHeader).(string)
		if !ok {
			// Log and remove the analyze
			formatterMsg := logger.AnalyzeWebSocketErrorAndGetFormattedMessage(websocketConn.LocalAddr().String(), nil, msgSeed, []byte("Unable to extract dappID"), cwm.apiInterface, time.Since(startTime))
			if formatterMsg != nil {
				sendWS(webSocketMsgWithType{messageType: messageType, msg: formatterMsg})
			}
		}

		userIp := websocketConn.RemoteAddr().String()

		logFormattedMsg := string(msg)
		if !cwm.cmdFlags.DebugRelays {
			logFormattedMsg = utils.FormatLongString(logFormattedMsg, cwm.relayMsgLogMaxChars)
		}

		utils.LavaFormatDebug("ws in <<<",
			utils.LogAttr("seed", msgSeed),
			utils.LogAttr("GUID", webSocketCtx),
			utils.LogAttr("msg", logFormattedMsg),
			utils.LogAttr("dappID", dappID),
		)

		metricsData := metrics.NewRelayAnalytics(dappID, cwm.chainId, cwm.apiInterface)

		protocolMessage, err := cwm.relaySender.ParseRelay(webSocketCtx, "", string(msg), cwm.connectionType, dappID, userIp, nil)
		if err != nil {
			utils.LavaFormatDebug("ws manager could not parse message", utils.LogAttr("message", msg), utils.LogAttr("err", err))
			formatterMsg := logger.AnalyzeWebSocketErrorAndGetFormattedMessage(websocketConn.LocalAddr().String(), err, msgSeed, msg, cwm.apiInterface, time.Since(startTime))
			if formatterMsg != nil {
				sendWS(webSocketMsgWithType{messageType: messageType, msg: formatterMsg})
			}
			continue
		}

		// check whether it's a normal relay / unsubscribe / unsubscribe_all otherwise its a subscription flow.
		if !IsFunctionTagOfType(protocolMessage, spectypes.FUNCTION_TAG_SUBSCRIBE) {
			if IsFunctionTagOfType(protocolMessage, spectypes.FUNCTION_TAG_UNSUBSCRIBE) {
				responseData, err := cwm.wsSubscriptionManager.Unsubscribe(webSocketCtx, protocolMessage, dappID, userIp, cwm.WebsocketConnectionUID, metricsData)
				if err != nil {
					utils.LavaFormatWarning("error unsubscribing from subscription", err, utils.LogAttr("GUID", webSocketCtx))
					if err == common.SubscriptionNotFoundError {
						// Echo the caller's id (JSON-RPC 2.0 §4.2) — the template's hardcoded
						// id is just a fallback for malformed requests with no id field.
						msgData, err := common.MarshalJsonRPCErrorWithRequestID(common.JsonRpcSubscriptionNotFoundError, msg)
						if err != nil {
							continue
						}
						sendWS(webSocketMsgWithType{messageType: messageType, msg: msgData})
					} else {
						// Send formatted error so client gets feedback (e.g. upstream timeout)
						formatterMsg := logger.AnalyzeWebSocketErrorAndGetFormattedMessage(websocketConn.LocalAddr().String(), err, msgSeed, msg, cwm.apiInterface, time.Since(startTime))
						if formatterMsg != nil {
							sendWS(webSocketMsgWithType{messageType: messageType, msg: formatterMsg})
						}
					}
				} else {
					if responseData == nil {
						// An implementation may report success with no payload, having no
						// synchronous ack frame from the node to forward. Synthesize a
						// §4.2-compliant reply so clients don't hang on recv() until they
						// time out (~15s). The
						// JSON-RPC envelope is safe here: ConsumerWebsocketManager is
						// only constructed by jsonRPC.go and tendermintRPC.go, both
						// JSON-RPC-shaped. Mirrors lavanet/lava#2296.
						responseData = buildUnsubscribeSuccessReply(msg)
						utils.LavaFormatTrace("synthesized unsubscribe ack",
							utils.LogAttr("GUID", webSocketCtx),
							utils.LogAttr("dappID", dappID),
						)
					}
					// Forward the node's response directly to the end user - no transformation or wrapping.
					sendWS(webSocketMsgWithType{messageType: messageType, msg: responseData})
				}
				continue
			} else if IsFunctionTagOfType(protocolMessage, spectypes.FUNCTION_TAG_UNSUBSCRIBE_ALL) {
				err := cwm.wsSubscriptionManager.UnsubscribeAll(webSocketCtx, dappID, userIp, cwm.WebsocketConnectionUID, metricsData)
				if err != nil {
					utils.LavaFormatWarning("error unsubscribing from all subscription", err, utils.LogAttr("GUID", webSocketCtx))
				}
				continue
			} else {
				// Normal relay over websocket. (not subscription related)
				relayResult, err := cwm.relaySender.SendParsedRelay(webSocketCtx, metricsData, protocolMessage)
				if err != nil {
					formatterMsg := logger.AnalyzeWebSocketErrorAndGetFormattedMessage(websocketConn.LocalAddr().String(), utils.LavaFormatError("could not send parsed relay", err), msgSeed, msg, cwm.apiInterface, time.Since(startTime))
					if formatterMsg != nil {
						sendWS(webSocketMsgWithType{messageType: messageType, msg: formatterMsg})
					}
				} else if relayResultReply := relayResult.GetReply(); relayResultReply != nil {
					// No need to verify signature since this is already happening inside the SendParsedRelay flow
					sendWS(webSocketMsgWithType{messageType: messageType, msg: relayResultReply.Data})
				} else {
					utils.LavaFormatError("Relay result is nil over websocket normal request flow, should not happen", err, utils.LogAttr("messageType", messageType))
				}
				// Emit the relay_usage event for every normal WS relay
				// (success or failure). The pre-existing flow `continue`d
				// before reaching AddMetricForWebSocket below, so normal WS
				// relays were silently dropped from the analytics pipeline.
				go logger.AddMetricForWebSocket(metricsData, err, websocketConn)
				continue
			}
		}

		// Subscription flow
		inputFormatter, outputFormatter := cacheformat.FormatterForRelayRequestAndResponse(protocolMessage.GetApiCollection().CollectionData.ApiInterface) // we use this to preserve the original jsonrpc id
		inputFormatter(protocolMessage.RelayPrivateData().Data)                                                                                            // set the extracted jsonrpc id

		reply, subscriptionMsgsChan, err := cwm.wsSubscriptionManager.StartSubscription(webSocketCtx, protocolMessage, dappID, userIp, cwm.WebsocketConnectionUID, metricsData)
		if err != nil {
			utils.LavaFormatWarning("StartSubscription returned an error", err,
				utils.LogAttr("GUID", webSocketCtx),
				utils.LogAttr("dappID", dappID),
				utils.LogAttr("userIp", userIp),
				utils.LogAttr("params", protocolMessage.GetRPCMessage().GetParams()),
			)

			formatterMsg := logger.AnalyzeWebSocketErrorAndGetFormattedMessage(websocketConn.LocalAddr().String(), utils.LavaFormatError("could not start subscription", err), msgSeed, msg, cwm.apiInterface, time.Since(startTime))
			if formatterMsg != nil {
				sendWS(webSocketMsgWithType{messageType: messageType, msg: formatterMsg}) // No need to use outputFormatter here since we are sending an error
				continue
			}

			// Handle the case when the error is a method not found error
			if errors.Is(err, common.APINotSupportedError) {
				msgData, err := json.Marshal(common.JsonRpcMethodNotFoundError)
				if err != nil {
					continue
				}

				sendWS(webSocketMsgWithType{messageType: messageType, msg: outputFormatter(msgData)})
				continue
			}
			continue
		}

		// Snapshot the populated RelayMetrics fields for per-delivery
		// emits. The start-of-subscription emit below races with the
		// per-message emits on the same pointer (AddMetricForWebSocket
		// mutates Success and Origin) — copy by value so each per-message
		// goroutine works on its own struct.
		subscriptionFields := *metricsData

		if subscriptionMsgsChan != nil { // if == nil, it means that we already have an active subscription running on this query
			go func() {
				utils.LavaFormatTrace("created go routine for new websocketSubMsgsChan",
					utils.LogAttr("GUID", webSocketCtx),
					utils.LogAttr("dappID", dappID),
					utils.LogAttr("userIp", userIp),
					utils.LogAttr("params", protocolMessage.GetRPCMessage().GetParams()),
				)

				for subscriptionMsgReply := range subscriptionMsgsChan {
					idleFor.Store(time.Now().Unix())
					sendWS(webSocketMsgWithType{messageType: messageType, msg: outputFormatter(subscriptionMsgReply.Data)})
					// Per-delivery emission. The originating subscribe is
					// billed at the spec CU under its real method; each
					// pushed notification is its own operation charged at
					// the flat delivery default, so downstream billing is
					// plain SUM(cu) without subscription-specific rules.
					perMessage := subscriptionFields
					perMessage.Timestamp = time.Now()
					perMessage.ApiMethod = SubscriptionDeliveryMethod
					perMessage.ComputeUnits = DefaultSubscriptionDeliveryCU
					go logger.AddMetricForWebSocket(&perMessage, nil, websocketConn)
				}

				utils.LavaFormatTrace("subscriptionMsgsChan was closed",
					utils.LogAttr("GUID", webSocketCtx),
					utils.LogAttr("dappID", dappID),
					utils.LogAttr("userIp", userIp),
					utils.LogAttr("params", protocolMessage.GetRPCMessage().GetParams()),
				)
			}()
		}

		go logger.AddMetricForWebSocket(metricsData, err, websocketConn)

		if reply != nil {
			reply.Data = outputFormatter(reply.Data) // use that id for the reply

			sendWS(webSocketMsgWithType{messageType: messageType, msg: reply.Data})

			logger.LogRequestAndResponse("jsonrpc ws msg", false, "ws", websocketConn.LocalAddr().String(), string(msg), string(reply.Data), msgSeed, time.Since(startTime), nil)
		}
	}
}

// buildUnsubscribeSuccessReply synthesizes a JSON-RPC 2.0 success response for an
// eth_unsubscribe / unsubscribe that the consumer satisfied locally — provider
// relays do not surface an unsubscribe acknowledgement frame, so we fabricate
// one here rather than letting clients hang. id is substituted as raw JSON to
// preserve the caller's exact type (§4.2).
//
// result is hard-coded to `true`, matching EVM upstream conventions. Tendermint
// upstreams typically return `{}` here; we optimize for "client unblocks"
// rather than exact upstream parity.
func buildUnsubscribeSuccessReply(requestBytes []byte) []byte {
	idRaw := "null"
	if r := gjson.GetBytes(requestBytes, "id"); r.Exists() {
		idRaw = r.Raw
	}
	return []byte(`{"jsonrpc":"2.0","id":` + idRaw + `,"result":true}`)
}
