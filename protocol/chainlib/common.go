package chainlib

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"errors"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/favicon"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	common "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc/metadata"
)

const (
	// ProjectIDHeader is the canonical wire-level key the consumer reads
	// to attribute a relay to a project for billing analytics. Used as the
	// HTTP header name, the gRPC metadata key (lower-cased per gRPC
	// convention), and the fiber Locals key passing the value from the
	// HTTP upgrade context to the websocket handler.
	ProjectIDHeader            = "project-id"
	RetryListeningInterval     = 10 // seconds
	relayMsgLogMaxChars        = 200
	RPCProviderNodeAddressHash = "Lava-Provider-Node-Address-Hash"
	RPCProviderNodeExtension   = "Lava-Provider-Node-Extension"
	WebSocketExtension         = "websocket"
)

var (
	TrailersToAddToHeaderResponse      = []string{RPCProviderNodeExtension}
	InvalidResponses                   = []string{"null", "", "nil", "undefined"}
	FailedSendingSubscriptionToClients = errors.New("Failed Sending Subscription To Clients connection might have been closed by the user")
	NoActiveSubscriptionFound          = errors.New("no active subscriptions for hashed params.")
	MaxBatchRequestSize                = 0 // configured via --max-batch-request-size flag, 0 means unlimited
	ErrBatchRequestSizeExceeded        = errors.New("batch request size exceeded the configured limit")
)

type RelayReplyWrapper struct {
	StatusCode int
	RelayReply *pairingtypes.RelayReply
}

type VerificationKey struct {
	Extension string
	Addon     string
}

type VerificationContainer struct {
	InternalPath   string
	ConnectionType string
	Name           string
	ParseDirective spectypes.ParseDirective
	Value          string
	LatestDistance uint64
	Severity       spectypes.ParseValue_VerificationSeverity
	VerificationKey
}

func (vc *VerificationContainer) IsActive() bool {
	if vc.Value == "" && vc.LatestDistance == 0 {
		return false
	}
	return true
}

type TaggedContainer struct {
	Parsing       *spectypes.ParseDirective
	ApiCollection *spectypes.ApiCollection
}

type ApiContainer struct {
	api           *spectypes.Api
	collectionKey CollectionKey
}

type ApiKey struct {
	Name           string
	ConnectionType string
	InternalPath   string
}

type CollectionKey struct {
	ConnectionType string
	InternalPath   string
	Addon          string
}

type BaseChainProxy struct {
	ErrorHandler
	averageBlockTime time.Duration
	NodeUrl          common.NodeUrl
	ChainID          string
	HashedNodeUrl    string
}

// returns the node url and chain id for that proxy.
func (bcp *BaseChainProxy) GetChainProxyInformation() (common.NodeUrl, string) {
	return bcp.NodeUrl, bcp.ChainID
}

func (bcp *BaseChainProxy) CapTimeoutForSend(ctx context.Context, chainMessage ChainMessageForSend) (context.Context, context.CancelFunc) {
	relayTimeout := GetRelayTimeout(chainMessage, bcp.averageBlockTime)
	processingTimeout := common.GetTimeoutForProcessing(relayTimeout, GetTimeoutInfo(chainMessage))
	connectCtx, cancel := bcp.NodeUrl.LowerContextTimeout(ctx, processingTimeout)
	return connectCtx, cancel
}

func extractDappIDFromFiberContext(c *fiber.Ctx) (dappID string) {
	// fiber.Ctx.Get returns a string aliased to fasthttp's per-request
	// header buffer (zero-copy via unsafe). The buffer is recycled when
	// the request completes and reused for subsequent requests on the
	// same worker. RelayMetrics.ProjectHash is enqueued asynchronously
	// into the OTel BatchLogProcessor and serialized after the request
	// returns — by then the backing array may already hold another
	// request's headers, prefix-overwriting the project hash. Clone to
	// detach from fasthttp's pool.
	dappID = strings.Clone(c.Get(ProjectIDHeader))
	if dappID == "" {
		dappID = generateNewDappID()
	}
	return dappID
}

// extractDappIDFromGrpcHeader extracts the project id from gRPC metadata.
func extractDappIDFromGrpcHeader(metadataValues metadata.MD) string {
	dappId := generateNewDappID()
	if values, ok := metadataValues[ProjectIDHeader]; ok && len(values) > 0 {
		// Same hazard as the HTTP path: gRPC metadata strings can alias
		// the receive buffer depending on the transport implementation.
		// Clone before retaining past the handler return.
		dappId = strings.Clone(values[0])
	}
	return dappId
}

// generateNewDappID generates default dappID
// In future we can also implement unique dappID generation
func generateNewDappID() string {
	return "DefaultDappID"
}

func constructFiberCallbackWithHeaderAndParameterExtraction(callbackToBeCalled fiber.Handler, isMetricEnabled bool) fiber.Handler {
	webSocketCallback := callbackToBeCalled
	handler := func(c *fiber.Ctx) error {
		// Extract project id from headers and stash it in the request-scoped
		// fiber context for the websocket handler to read after the upgrade.
		dappID := extractDappIDFromFiberContext(c)
		c.Locals(ProjectIDHeader, dappID)

		if isMetricEnabled {
			c.Locals(metrics.RefererHeaderKey, c.Get(metrics.RefererHeaderKey, ""))
			c.Locals(metrics.UserAgentHeaderKey, c.Get(metrics.UserAgentHeaderKey, ""))
			// Clone Origin: it crosses the request boundary into the
			// websocket handler and from there into RelayMetrics, which the
			// OTel sink serializes asynchronously after fasthttp has
			// recycled the request buffer.
			c.Locals(metrics.OriginHeaderKey, strings.Clone(c.Get(metrics.OriginHeaderKey, "")))
		}
		return webSocketCallback(c) // uses external dappID
	}
	return handler
}

// drainHTTPThenWS performs the graceful shutdown sequence shared by every
// chain listener: drain in-flight HTTP via app.ShutdownWithContext, then wait
// for the per-listener websocket WaitGroup to reach zero. The HTTP shutdown
// runs first so that (a) the listener stops accepting new /ws upgrades and
// (b) any in-flight upgrade handler — which performs wsWG.Add(1) synchronously
// in the upgrade middleware — has been observed before wsWG.Wait runs. If the
// caller's context expires before wsWG drains, we log a warning and return;
// the listener is already closed and remaining WS conns will tear down when
// the process exits. name is used to disambiguate the warning across listeners.
func drainHTTPThenWS(ctx context.Context, app *fiber.App, wg *sync.WaitGroup, name string) error {
	if app == nil {
		return nil
	}
	httpErr := app.ShutdownWithContext(ctx)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		utils.LavaFormatWarning(name+": WS goroutines did not finish within shutdown grace period", nil)
	}
	return httpErr
}

func isUTXOFamily(chainID string) bool {
	return chainID == "BTC" || chainID == "BTCT" || chainID == "LTC" || chainID == "LTCT" || chainID == "DOGE" || chainID == "DOGET" || chainID == "BCH" || chainID == "BCHT"
}

// checkUTXOResponseAndFixReply returns the reply body for UTXO-family chains after
// reformatting it for JSON-RPC 1.0 semantics, and returns replyData unchanged for all
// other chains. The non-UTXO path is zero-copy: hot chains (ETH, Cosmos, etc.) no
// longer pay a full string(replyData) allocation per response.
func checkUTXOResponseAndFixReply(chainID string, replyData []byte) []byte {
	if !isUTXOFamily(chainID) {
		return replyData
	}

	// Try single response first
	var jsonMsg *rpcclient.JsonrpcMessage
	if err := json.Unmarshal(replyData, &jsonMsg); err == nil {
		if marshaledRes, err := json.Marshal(convertToUTXOResponse(jsonMsg)); err == nil {
			return marshaledRes
		}
		return replyData
	}

	// Try batch response (JSON array)
	var jsonMsgs []rpcclient.JsonrpcMessage
	if err := json.Unmarshal(replyData, &jsonMsgs); err == nil && len(jsonMsgs) > 0 {
		btcBatch := make([]*rpcclient.BTCResponse, len(jsonMsgs))
		for i := range jsonMsgs {
			btcBatch[i] = convertToUTXOResponse(&jsonMsgs[i])
		}
		if marshaledRes, err := json.Marshal(btcBatch); err == nil {
			return marshaledRes
		}
	}

	// Reached only if batch unmarshal failed OR batch marshal failed: return the
	// original bytes unchanged so the caller still gets a well-formed reply.
	return replyData
}

// convertToUTXOResponse converts a JsonrpcMessage to BTCResponse format.
// UTXO-family chains use JSON-RPC 1.0 which omits the "jsonrpc" field and always
// includes the "error" field (even when null). The relay pipeline may add "jsonrpc":"2.0"
// and strip null errors during response reconstruction — this undoes those changes.
func convertToUTXOResponse(msg *rpcclient.JsonrpcMessage) *rpcclient.BTCResponse {
	return &rpcclient.BTCResponse{
		// Omit Version: UTXO-family nodes use JSON-RPC 1.0 which doesn't include the "jsonrpc" field.
		// The relay pipeline may inject "2.0" during reconstruction; leaving Version empty strips it
		// (BTCResponse.Version has omitempty).
		ID:     msg.ID,
		Method: msg.Method,
		Error:  msg.Error,
		Result: msg.Result,
	}
}

// addHeadersAndSendBytes writes response metadata headers and sends the raw body via
// fiber.Ctx.Send to avoid the []byte → string → []byte round-trip that fiber.Ctx.SendString
// imposes at every call site. Hot-path response bodies (already []byte from the relay
// pipeline) no longer pay an extra string-conversion allocation per request.
func addHeadersAndSendBytes(c *fiber.Ctx, metaData []pairingtypes.Metadata, data []byte) error {
	for _, value := range metaData {
		c.Set(value.Name, value.Value)
	}

	return c.Send(data)
}

// convertToJsonError returns a JSON-encoded `{"error": errorMsg}` body as raw bytes.
// Returning []byte (rather than string) lets every error-path caller hand the
// result straight to addHeadersAndSendBytes / fiber.Ctx.Send with no conversion.
//
// NOTE: This helper produces a non-spec error envelope (`error` is a string).
// It is retained for non-JSON-RPC chain interfaces (REST, Tendermint GET) whose
// clients do not expect JSON-RPC 2.0 §5.1 shape. For JSON-RPC interfaces use
// convertToJsonRpcError below.
func convertToJsonError(errorMsg string) []byte {
	jsonResponse, err := json.Marshal(fiber.Map{
		"error": errorMsg,
	})
	if err != nil {
		return []byte(`{"error": "Failed to marshal error response to json"}`)
	}

	return jsonResponse
}

// convertToJsonRpcError returns a JSON-RPC 2.0 §5.1 compliant error envelope:
//
//	{"jsonrpc":"2.0", "id": <reqID>, "error": {"code": -32000, "message": "...", "data": {...}}}
//
// Spec-compliant clients (Web3.py, ethers.js, viem) require `error` to be an
// Object with `code` (int) and `message` (string). Previously the JSON-RPC
// error path used convertToJsonError, which set `error` to a stringified
// envelope — clients crashed reading response.error.code on a string.
//
// rawErrorMsg: the JSON string previously produced by GetUniqueGuidResponseForError
// (shape: {"Error_GUID":"<guid>","Error":"<inner message>"}). We parse it to
// extract the GUID and the inner error message; the inner message becomes the
// JSON-RPC `message`, and full context lands under `data` for debugging.
//
// requestBody: the raw incoming JSON-RPC request body. We extract `id` from it
// so the response preserves request correlation. If parsing fails, id is null
// (still spec-valid).
//
// Code -32000 is from JSON-RPC 2.0 §5.1 implementation-defined server-error
// range (-32000 to -32099). It is the appropriate code for router-synthesized
// errors that are not parse/method/invalid-params/internal — e.g. "Selected
// provider not available", retry exhaustion, consistency pre-validation, etc.
func convertToJsonRpcError(rawErrorMsg string, requestBody []byte) []byte {
	// Extract id from the original request so the client can correlate.
	// Fall back to JSON null if the body is malformed or id is absent —
	// spec-compliant for notifications and unparseable requests.
	var reqID json.RawMessage
	if len(requestBody) > 0 {
		var probe struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(requestBody, &probe); err == nil && len(probe.ID) > 0 {
			reqID = probe.ID
		}
	}
	if len(reqID) == 0 {
		reqID = json.RawMessage("null")
	}

	// Parse the stringified envelope produced by GetUniqueGuidResponseForError.
	// Shape: {"Error_GUID":"<guid>","Error":"<inner-error-message>"}.
	// Fall back to the raw string if parsing fails — we still produce a valid
	// JSON-RPC error envelope.
	var parsed struct {
		ErrorGUID string `json:"Error_GUID"`
		Error     string `json:"Error"`
	}
	guid := ""
	message := rawErrorMsg
	if err := json.Unmarshal([]byte(rawErrorMsg), &parsed); err == nil {
		guid = parsed.ErrorGUID
		if parsed.Error != "" {
			message = parsed.Error
		} else if guid != "" {
			// GetUniqueGuidResponseForError elides the Error field via ,omitempty
			// when ReturnMaskedErrors == "true". In that mode rawErrorMsg is just
			// `{"Error_GUID":"<guid>"}` — surfacing it as the JSON-RPC message
			// would leak a raw JSON envelope into error.message, which is the
			// exact failure shape this helper was added to avoid.
			message = "Internal server error"
		}
	}
	if message == "" {
		message = "Internal server error"
	}

	data := map[string]any{}
	if guid != "" {
		data["guid"] = guid
	}
	// The inner error message from LavaFormatError already embeds provider
	// context (selectedProvider, validProviders, addon, extensions, GUID) in
	// its appended attribute block. Surface it under data.error so the full
	// context is preserved for debugging without brittle substring parsing.
	data["error"] = message

	envelope := fiber.Map{
		"jsonrpc": "2.0",
		"id":      reqID,
		"error": fiber.Map{
			"code":    -32000,
			"message": message,
			"data":    data,
		},
	}

	jsonResponse, err := json.Marshal(envelope)
	if err != nil {
		// Last-resort fallback — still spec-compliant.
		return []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Failed to marshal error response"}}`)
	}
	return jsonResponse
}

func addAttributeToError(key, value, errorMessage string) string {
	return errorMessage + fmt.Sprintf(`, "%v": "%v"`, key, value)
}

func validateEndpoints(endpoints []common.NodeUrl, apiInterface string) {
	for _, endpoint := range endpoints {
		common.ValidateEndpoint(endpoint.Url, apiInterface)
	}
}

func ListenWithRetry(ctx context.Context, app *fiber.App, address string, chosenAddrCh *common.SafeChannelSender[string]) {
	for {
		ln, err := net.Listen("tcp", address)
		if err != nil {
			utils.LavaFormatError("net.Listen(tcp, address)", err, utils.LogAttr("address", address))
		} else {
			chosenAddrCh.Send(ln.Addr().String())

			err = app.Listener(ln)
			if err != nil {
				utils.LavaFormatError("app.Listen(listenAddr)", err)
			}
		}

		// Stop retrying when the caller cancels (graceful shutdown). Without this,
		// after app.ShutdownWithContext returns from app.Listener, the loop would
		// sleep and rebind the port, undoing the shutdown.
		select {
		case <-ctx.Done():
			return
		case <-time.After(RetryListeningInterval * time.Second):
		}
	}
}

func GetListenerWithRetryGrpc(protocol, addr string) net.Listener {
	for {
		lis, err := net.Listen(protocol, addr)
		if err == nil {
			return lis
		}
		utils.LavaFormatError("failure setting up listener, net.Listen(protocol, addr)", err, utils.Attribute{Key: "listenAddr", Value: addr})
		time.Sleep(RetryListeningInterval * time.Second)
		utils.LavaFormatWarning("Attempting connection retry", nil)
	}
}

// GetHeaderFromCachedMap extracts a header value from a cached headers map.
// Returns the first value if present, or the defaultValue if not found.
// This avoids repeated calls to fiberCtx.Get() which has overhead.
func GetHeaderFromCachedMap(headers map[string][]string, key string, defaultValue string) string {
	if values, ok := headers[key]; ok && len(values) > 0 {
		return values[0]
	}
	return defaultValue
}

// rest request headers are formatted like map[string]string
func convertToMetadataMap(md map[string][]string) []pairingtypes.Metadata {
	metadata := make([]pairingtypes.Metadata, len(md))
	indexer := 0
	for k, v := range md {
		metadata[indexer] = pairingtypes.Metadata{Name: k, Value: strings.Join(v, ", ")}
		indexer += 1
	}
	return metadata
}

// rest response headers / grpc headers are formatted like map[string][]string
func convertToMetadataMapOfSlices(md map[string][]string) []pairingtypes.Metadata {
	metadata := make([]pairingtypes.Metadata, len(md))
	indexer := 0
	for k, v := range md {
		metadata[indexer] = pairingtypes.Metadata{Name: k, Value: v[0]}
		indexer += 1
	}
	return metadata
}

func convertRelayMetaDataToMDMetaData(md []pairingtypes.Metadata) metadata.MD {
	responseMetaData := make(metadata.MD)
	for _, v := range md {
		responseMetaData[v.Name] = append(responseMetaData[v.Name], v.Value)
	}
	return responseMetaData
}

// split two requested blocks to the most advanced and most behind
// the hierarchy is as follows:
// NOT_APPLICABLE
// LATEST_BLOCK
// PENDING_BLOCK
// SAFE
// FINALIZED
// numeric value (descending)
// EARLIEST
func CompareRequestedBlockInBatch(currentLatestRequestedBlock, currentEarliestRequestedBlock, parsedBlock int64) (latestCombinedBlock int64, earliestCombinedBlock int64) {
	latestCallback := func(currentLatest int64, parsedBlock int64) int64 {
		if currentLatest < 0 && parsedBlock < 0 {
			return utils.Max(currentLatest, parsedBlock)
		}

		if currentLatest > 0 && parsedBlock < 0 && parsedBlock != spectypes.EARLIEST_BLOCK {
			return parsedBlock
		}

		if currentLatest < 0 && parsedBlock > 0 && currentLatest != spectypes.EARLIEST_BLOCK {
			return currentLatest
		}

		return utils.Max(currentLatest, parsedBlock)
	}

	earliestCallback := func(currentEarliest int64, parsedBlock int64) int64 {
		if currentEarliest == spectypes.EARLIEST_BLOCK || parsedBlock == spectypes.EARLIEST_BLOCK {
			return spectypes.EARLIEST_BLOCK
		}

		if currentEarliest == spectypes.NOT_APPLICABLE || parsedBlock == spectypes.NOT_APPLICABLE {
			return spectypes.NOT_APPLICABLE
		}

		if currentEarliest < 0 && parsedBlock < 0 {
			return utils.Min(currentEarliest, parsedBlock)
		}

		if currentEarliest > 0 && parsedBlock < 0 {
			return currentEarliest
		}

		if currentEarliest < 0 && parsedBlock > 0 {
			return parsedBlock
		}

		return utils.Min(currentEarliest, parsedBlock)
	}

	return latestCallback(currentLatestRequestedBlock, parsedBlock), earliestCallback(currentEarliestRequestedBlock, parsedBlock)
}

func GetRelayTimeout(chainMessage ChainMessageForSend, averageBlockTime time.Duration) time.Duration {
	if chainMessage.TimeoutOverride() != 0 {
		return chainMessage.TimeoutOverride()
	}
	// Calculate extra RelayTimeout
	extraRelayTimeout := time.Duration(0)
	if IsHangingApi(chainMessage) {
		extraRelayTimeout = averageBlockTime * 2
	}
	relayTimeAddition := common.GetTimePerCu(GetComputeUnits(chainMessage))
	if chainMessage.GetApi().TimeoutMs > 0 {
		relayTimeAddition = time.Millisecond * time.Duration(chainMessage.GetApi().TimeoutMs)
	}
	// Set relay timout, increase it every time we fail a relay on timeout
	return extraRelayTimeout + relayTimeAddition
}

// applyResponseCompression wires the fiber compress middleware according to
// cmdFlags.ResponseCompression. Modes:
//
//	"off"    — no compression middleware registered. Cheapest on CPU; clients get
//	           raw bytes. Use when the process sits behind a compressing ingress.
//	"brotli" — current legacy behavior: fasthttp auto-negotiates br/gzip/deflate
//	           based on Accept-Encoding. Brotli in Go costs ~3x the CPU of gzip
//	           for similar wire savings, so this is the expensive choice.
//	"gzip"   — default. Strips `br` from Accept-Encoding before fasthttp's
//	           encoder picks, so the best encoding advertised falls back to gzip
//	           (or deflate). ~3x cheaper than brotli on the same workload.
//
// Any unknown value is treated as "gzip" but logs a warning — a deploy-time
// typo in the flag would otherwise silently downgrade compression config.
func applyResponseCompression(app *fiber.App, mode string) {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case common.ResponseCompressionOff:
		return
	case common.ResponseCompressionBrotli:
		app.Use(compress.New(compress.Config{Level: compress.LevelBestSpeed}))
	default: // "gzip", "", or anything else
		if normalized != common.ResponseCompressionGzip && normalized != "" {
			utils.LavaFormatWarning("unknown response-compression mode, falling back to gzip",
				nil, utils.LogAttr("mode", mode))
		}
		app.Use(stripBrotliAcceptEncoding)
		app.Use(compress.New(compress.Config{Level: compress.LevelBestSpeed}))
	}
}

// stripBrotliAcceptEncoding removes the `br` coding from Accept-Encoding before
// downstream middleware inspects it. Preserves q-values on remaining codings and
// is a no-op when the header is missing. Case-insensitive per RFC 7231 §5.3.4.
func stripBrotliAcceptEncoding(c *fiber.Ctx) error {
	ae := c.Get(fiber.HeaderAcceptEncoding)
	if ae == "" {
		return c.Next()
	}
	parts := strings.Split(ae, ",")
	kept := parts[:0]
	stripped := false
	for _, part := range parts {
		coding := part
		if semi := strings.IndexByte(coding, ';'); semi >= 0 {
			coding = coding[:semi]
		}
		if strings.EqualFold(strings.TrimSpace(coding), "br") {
			stripped = true
			continue
		}
		kept = append(kept, part)
	}
	if stripped {
		if len(kept) == 0 {
			// Client advertised only `br`. An absent Accept-Encoding is
			// semantically different from an empty-value one: many stacks
			// (fasthttp included) treat absent as "no client preference, pick
			// your default" while empty-value can short-circuit content
			// negotiation entirely. Delete the header to fall back to default.
			c.Request().Header.Del(fiber.HeaderAcceptEncoding)
		} else {
			c.Request().Header.Set(fiber.HeaderAcceptEncoding, strings.Join(kept, ","))
		}
	}
	return c.Next()
}

// setup a common preflight and cors configuration allowing wild cards and preflight caching.
func createAndSetupBaseAppListener(cmdFlags common.ConsumerCmdFlags, healthCheckPath string, healthReporter HealthReporter) *fiber.App {
	app := fiber.New(fiber.Config{
		JSONEncoder: json.Marshal,
		JSONDecoder: json.Unmarshal,
		// Fiber's default is 4 KiB, which mTLS X-Forwarded-Client-Cert headers (PEM-encoded
		// certs, often with chains) routinely exceed — fasthttp then 431s before any handler runs.
		// 128 KiB matches the Envoy Gateway ClientTrafficPolicy ceiling in deployment.
		ReadBufferSize:  128 * 1024,
		WriteBufferSize: 128 * 1024,
	})
	app.Use(favicon.New())
	applyResponseCompression(app, cmdFlags.ResponseCompression)
	app.Use(func(c *fiber.Ctx) error {
		// we set up wild card by default.
		c.Set("Access-Control-Allow-Origin", cmdFlags.OriginFlag)
		// Handle preflight requests directly
		if c.Method() == "OPTIONS" {
			// set up all allowed methods.
			c.Set("Access-Control-Allow-Methods", cmdFlags.MethodsFlag)
			// allow headers — the cors-headers flag defaults to "", which would
			// echo an empty allow-list and make the browser reject any preflight
			// that carries a non-simple header (e.g. Content-Type: application/json).
			// Treat empty as "*" so the default posture matches the flag's help text
			// ("* for all") and the wild-card origin we already set above.
			allowHeaders := cmdFlags.HeadersFlag
			if allowHeaders == "" {
				allowHeaders = "*"
			}
			c.Set("Access-Control-Allow-Headers", allowHeaders)
			// allow credentials
			c.Set("Access-Control-Allow-Credentials", cmdFlags.CredentialsFlag)
			// Cache preflight request for 24 hours (in seconds)
			c.Set("Access-Control-Max-Age", cmdFlags.CDNCacheDuration)
			return c.SendStatus(fiber.StatusNoContent)
		}
		if c.Method() == "DELETE" {
			return c.SendStatus(fiber.StatusNoContent)
		}
		return c.Next()
	})

	app.Get(healthCheckPath, func(fiberCtx *fiber.Ctx) error {
		if healthReporter.IsHealthy() {
			fiberCtx.Status(http.StatusOK)
			return fiberCtx.SendString("Health status OK")
		} else {
			fiberCtx.Status(http.StatusServiceUnavailable)
			return fiberCtx.SendString("Health status Failure")
		}
	})

	return app
}

func truncateAndPadString(s string, maxLength int) string {
	// Truncate to a maximum length
	if len(s) > maxLength {
		s = s[:maxLength]
	}

	// Pad with empty strings if the length is less than the specified maximum length
	s = fmt.Sprintf("%-*s", maxLength, s)

	return s
}

// return if response is valid or not - true
func ValidateNilResponse(responseString string) error {
	return nil // this feature was disabled in version 0.35.8 due to some nodes request this response.
	// after the timeout features we can add support for this filtering as it would be parsed and
	// returned to the user if multiple providers returned the same type of response

	// Removed on 0.35.8
	// if slices.Contains(InvalidResponses, responseString) {
	// 	return fmt.Errorf("response returned an empty value: %s", responseString)
	// }
	// return nil
}

func GetTimeoutInfo(chainMessage ChainMessageForSend) common.TimeoutInfo {
	return common.TimeoutInfo{
		CU:       chainMessage.GetApi().ComputeUnits,
		Hanging:  IsHangingApi(chainMessage),
		Stateful: GetStateful(chainMessage),
	}
}

func IsUrlWebSocket(urlToParse string) (bool, error) {
	u, err := url.Parse(urlToParse)
	if err != nil {
		return false, err
	}

	return u.Scheme == "ws" || u.Scheme == "wss", nil
}
