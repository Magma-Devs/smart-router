package metrics

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc/metadata"
)

var ReturnMaskedErrors = "false"

const (
	webSocketCloseMessage = "websocket: close "
	OriginHeaderKey       = "Origin"
)

type RPCConsumerLogs struct {
	consumerMetricsManager     ConsumerMetricsManagerInf
	usageSink                  UsageEventSink
	consumerOptimizerQoSClient *ConsumerOptimizerQoSClient
}

func NewRPCConsumerLogs(consumerMetricsManager ConsumerMetricsManagerInf, usageSink UsageEventSink, consumerOptimizerQoSClient *ConsumerOptimizerQoSClient) (*RPCConsumerLogs, error) {
	consumerMetricsManager = SafeMetrics(consumerMetricsManager)
	if usageSink == nil {
		usageSink = NoopUsageSink{}
	}
	return &RPCConsumerLogs{
		consumerMetricsManager:     consumerMetricsManager,
		usageSink:                  usageSink,
		consumerOptimizerQoSClient: consumerOptimizerQoSClient,
	}, nil
}

func (rpccl *RPCConsumerLogs) SetWebSocketConnectionActive(chainId string, apiInterface string, add bool) {
	rpccl.consumerMetricsManager.SetWebSocketConnectionActive(chainId, apiInterface, add)
}

func (rpccl *RPCConsumerLogs) SetRelaySentToProviderMetric(providerAddress, chainId, apiInterface string) {
	rpccl.consumerOptimizerQoSClient.SetRelaySentToProvider(providerAddress, chainId)
}

func (rpccl *RPCConsumerLogs) SetRelayNodeErrorMetric(chainId, apiInterface, providerAddress, method string) {
	if providerAddress == "" {
		// skip if provider address is empty
		return
	}

	rpccl.consumerMetricsManager.SetRelayNodeErrorMetric(chainId, apiInterface, providerAddress, method)
	rpccl.consumerOptimizerQoSClient.SetNodeErrorToProvider(providerAddress, chainId)
}

func (rpccl *RPCConsumerLogs) SetCrossValidationMetric(
	chainId, apiInterface, method string,
	success bool,
	agreeingProviders, disagreeingProviders []string,
) {
	if rpccl == nil {
		return
	}
	rpccl.consumerMetricsManager.SetCrossValidationMetric(chainId, apiInterface, method, success, agreeingProviders, disagreeingProviders)
}

// SetCrossValidationFailureMetric records a cross-validation failure broken down by reason (the bounded
// failures_total series). Sits alongside SetCrossValidationMetric so the server emits both at every failure
// site; a no-reason call is a no-op.
func (rpccl *RPCConsumerLogs) SetCrossValidationFailureMetric(chainId, apiInterface, method, reason string) {
	if rpccl == nil {
		return
	}
	rpccl.consumerMetricsManager.SetCrossValidationFailureMetric(chainId, apiInterface, method, reason)
}

func (rpccl *RPCConsumerLogs) GetMessageSeed() string {
	return "GUID_" + strconv.Itoa(rand.Intn(10000000000))
}

func (rpccl *RPCConsumerLogs) SetProtocolError(chainId string, apiInterface string, providerAddress string, method string) {
	rpccl.consumerMetricsManager.SetProtocolError(chainId, apiInterface, providerAddress, method)
}

func (rpccl *RPCConsumerLogs) RecordIncidentRetry(chainId string, apiInterface string, method string, count uint64, success bool) {
	rpccl.consumerMetricsManager.RecordIncidentRetry(chainId, apiInterface, method, count, success)
}

func (rpccl *RPCConsumerLogs) RecordIncidentConsistency(chainId string, apiInterface string, method string, success bool) {
	rpccl.consumerMetricsManager.RecordIncidentConsistency(chainId, apiInterface, method, success)
}

func (rpccl *RPCConsumerLogs) RecordIncidentHedgeResult(chainId string, apiInterface string, method string, count uint64, success bool) {
	rpccl.consumerMetricsManager.RecordIncidentHedgeResult(chainId, apiInterface, method, count, success)
}

// Input will be masked with a random GUID if returnMaskedErrors is set to true
func (rpccl *RPCConsumerLogs) GetUniqueGuidResponseForError(responseError error, msgSeed string) string {
	type ErrorData struct {
		Error_GUID string `json:"Error_GUID"`
		Error      string `json:"Error,omitempty"`
	}

	data := ErrorData{
		Error_GUID: msgSeed,
	}
	if ReturnMaskedErrors == "false" {
		// Every client-facing error envelope — jsonrpc, tendermint, rest, grpc and
		// ws — is built here, so this is the one place that has to strip upstream
		// credentials. net/http's *url.Error embeds the full request url and Go
		// masks only the userinfo password, so a transport failure would otherwise
		// hand the caller the node-url's api key.
		data.Error = utils.RedactSecrets(responseError.Error())
	}

	utils.LavaFormatError("UniqueGuidResponseForError", responseError, utils.Attribute{Key: "msgSeed", Value: msgSeed})

	ret, _ := json.Marshal(data)

	return string(ret)
}

// Websocket healthy disconnections throw "websocket: close 1005 (no status)" error,
// We don't want to alert error monitoring for that purpses.
func (rpccl *RPCConsumerLogs) AnalyzeWebSocketErrorAndGetFormattedMessage(webSocketAddr string, err error, msgSeed string, msg []byte, rpcType string, timeTaken time.Duration) []byte {
	if err != nil {
		errMessage := err.Error()
		if strings.Contains(errMessage, webSocketCloseMessage) {
			utils.LavaFormatDebug("Websocket connection closed by the user", utils.LogAttr("error", errMessage))
			return nil
		}
		rpccl.LogRequestAndResponse(rpcType+" ws msg", true, "ws", webSocketAddr, string(msg), "", msgSeed, timeTaken, err)

		jsonResponse, err := json.Marshal(fiber.Map{
			"Error_Received": rpccl.GetUniqueGuidResponseForError(err, msgSeed),
		})
		if err != nil {
			utils.LavaFormatError("AnalyzeWebSocketErrorAndGetFormattedMessage unexpected behavior, failed marshalling json response", err, utils.LogAttr("seed", msgSeed))
		}
		return jsonResponse
	}
	return nil
}

func (rpccl *RPCConsumerLogs) LogRequestAndResponse(module string, hasError bool, method, path, req, resp, msgSeed string, timeTaken time.Duration, err error) {
	if hasError && err != nil {
		utils.LavaFormatError(module, err, []utils.Attribute{{Key: "GUID", Value: msgSeed}, {Key: "timeTaken", Value: timeTaken}, {Key: "request", Value: req}, {Key: "response", Value: parser.CapStringLen(resp)}, {Key: "method", Value: method}, {Key: "path", Value: path}, {Key: "HasError", Value: hasError}}...)
		return
	}
	utils.LavaFormatDebug(module, []utils.Attribute{{Key: "GUID", Value: msgSeed}, {Key: "timeTaken", Value: timeTaken}, {Key: "request", Value: req}, {Key: "response", Value: parser.CapStringLen(resp)}, {Key: "method", Value: method}, {Key: "path", Value: path}, {Key: "HasError", Value: hasError}}...)
}

func (rpccl *RPCConsumerLogs) RecordEndToEndLatency(chainId string, apiInterface string, method string, latencyMs float64) {
	rpccl.consumerMetricsManager.RecordEndToEndLatency(chainId, apiInterface, method, latencyMs)
}

func (rpccl *RPCConsumerLogs) RecordCacheResult(chainId, apiInterface, method, cacheTier, outcome string, latencyMs float64) {
	rpccl.consumerMetricsManager.RecordCacheResult(chainId, apiInterface, method, cacheTier, outcome, latencyMs)
}

func (rpccl *RPCConsumerLogs) RecordProviderLatency(chainId string, apiInterface string, providerAddress string, method string, latencyMs float64) {
	rpccl.consumerMetricsManager.RecordProviderLatency(chainId, apiInterface, providerAddress, method, latencyMs)
}

func (rpccl *RPCConsumerLogs) AddMetricForHttp(data *RelayMetrics, err error, headers map[string][]string) {
	// Set OTel-bound fields before Emit. Success matters on the smart-router
	// path too, where consumerMetricsManager.SetRelayMetrics is a no-op.
	data.Success = err == nil
	rpccl.consumerMetricsManager.SetRelayMetrics(data, err)
	// headers must hold owned strings: this runs in a goroutine after the reply, and strings.Join
	// of a single value returns that value itself rather than a copy. The HTTP listeners pass
	// chainlib's detached map (MAG-3881).
	data.Origin = strings.Join(headers[OriginHeaderKey], ", ")
	rpccl.usageSink.Emit(NewRelayUsageEvent(data))
}

func (rpccl *RPCConsumerLogs) AddMetricForWebSocket(data *RelayMetrics, err error) {
	data.Success = err == nil
	rpccl.consumerMetricsManager.SetRelayMetrics(data, err)
	rpccl.usageSink.Emit(NewRelayUsageEvent(data))
}

func (rpccl *RPCConsumerLogs) AddMetricForGrpc(data *RelayMetrics, err error, metadataValues *metadata.MD) {
	getMetadataHeaderOrDefault := func(headerKey string) string {
		headerValues := metadataValues.Get(headerKey)
		headerValue := ""
		if len(headerValues) > 0 {
			headerValue = headerValues[0]
		}
		return headerValue
	}
	data.Success = err == nil
	rpccl.consumerMetricsManager.SetRelayMetrics(data, err)
	// Origin crosses into the async OTel emit path, so it must be an owned string. It is one
	// already on every listener: the HTTP listeners hand over chainlib's detached copy of fiber's
	// zero-copy headers, and grpc-go's transport allocates a string for every metadata value it
	// decodes. The clone stays as a cheap guard at the boundary (MAG-3881).
	data.Origin = strings.Clone(getMetadataHeaderOrDefault(OriginHeaderKey))
	rpccl.usageSink.Emit(NewRelayUsageEvent(data))
}

func (rpccl *RPCConsumerLogs) LogTestMode(fiberCtx *fiber.Ctx) {
	headers := fiberCtx.GetReqHeaders()
	st := "Test Mode Log: new request\n"
	st += "Full URI: " + fiberCtx.Request().URI().String() + "\n"
	for header, HeaderVal := range headers {
		st += fmt.Sprintf("Header %16s HeaderVal: %s\n", header, HeaderVal)
	}
	utils.LavaFormatInfo(st)
}
