package chainlib

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
)

// noAnswerShape is the error body a listener's router-made replies already use: a JSON-RPC 2.0
// error object (JSON-RPC, Tendermint POST), or {"error": "..."} (REST, Tendermint GET, GraphQL).
type noAnswerShape int

const (
	noAnswerJsonRpc noAnswerShape = iota
	noAnswerJsonError
)

// failedRelayReply is what the client reads for a failed relay.
type failedRelayReply struct {
	status      int
	body        []byte
	contentType string // empty: keep the listener's own Content-Type
	// fromUpstream is true when the body is the upstream's answer rather than the router's text.
	fromUpstream bool
}

// buildFailedRelayReply decides the reply for a failed relay that carries a client-facing verdict
// (MAG-4218, MAG-4481). The router is a proxy:
//
//   - An upstream answered: its status and body go back as is, on every interface, in masked mode
//     too. The body is not rewritten: no envelope, no added fields, and no GUID (that stays in
//     the Lava-Guid reply header). The router forwards the caller's own request bytes, so the id
//     in a JSON-RPC body is already the caller's.
//   - No upstream answered: the router's own 502 or 504 and one sentence, in the listener's own
//     error shape. Masked mode (ReturnMaskedErrors) masks both: ok is false and the listener sends
//     its masked reply with failedRelayStatus.
//
// ok is false when the relay carries no verdict, and the listener keeps its own reply.
func buildFailedRelayReply(relayResult *common.RelayResult, guid string, requestBody []byte, shape noAnswerShape) (reply failedRelayReply, ok bool) {
	cf := relayResult.GetClientFacing()
	if cf == nil {
		return failedRelayReply{}, false
	}
	if cf.UpstreamStatus != 0 {
		return failedRelayReply{
			status:       cf.UpstreamStatus,
			body:         cf.UpstreamBody,
			contentType:  cf.UpstreamContentType,
			fromUpstream: true,
		}, true
	}
	if metrics.ReturnMaskedErrors != "false" {
		return failedRelayReply{}, false
	}
	message := utils.RedactSecrets(cf.NoAnswerMessage())
	var body []byte
	switch shape {
	case noAnswerJsonRpc:
		data := map[string]any{}
		if guid != "" {
			data["guid"] = guid
		}
		if cf.RouterErrorName != "" {
			data["router_error_code"] = cf.RouterErrorCode
			data["router_error_name"] = cf.RouterErrorName
		}
		body = marshalOr(fiber.Map{
			"jsonrpc": "2.0",
			"id":      requestIDOrNull(requestBody),
			"error": fiber.Map{
				"code":    -32000,
				"message": message,
				"data":    data,
			},
		}, `{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Failed to marshal error response"}}`)
	default:
		body = convertToJsonError(message)
	}
	return failedRelayReply{status: relayResult.GetStatusCode(), body: body, contentType: fiber.MIMEApplicationJSONCharsetUTF8}, true
}

// failedRelayStatus is the status of a failed relay's reply when buildFailedRelayReply did not
// build it. A masked no-answer is masked in its status too, so it reads 500 like any other masked
// router error.
func failedRelayStatus(relayResult *common.RelayResult) int {
	if cf := relayResult.GetClientFacing(); cf != nil && cf.UpstreamStatus == 0 {
		return fiber.StatusInternalServerError
	}
	if status := relayResult.GetStatusCode(); status != 0 {
		return status
	}
	return fiber.StatusInternalServerError
}

// sendFailedRelayReply writes a reply buildFailedRelayReply built.
func sendFailedRelayReply(fiberCtx *fiber.Ctx, reply failedRelayReply, metadata []pairingtypes.Metadata) error {
	fiberCtx.Status(reply.status)
	if reply.contentType != "" {
		fiberCtx.Set(fiber.HeaderContentType, reply.contentType)
	} else if reply.fromUpstream {
		// The upstream sent no Content-Type: do not label its body as JSON on its behalf.
		fiberCtx.Response().Header.Del(fiber.HeaderContentType)
	}
	return addHeadersAndSendBytes(fiberCtx, metadata, reply.body)
}

// loggedReply is the reply as the request log records it: capped, and redacted, as any other
// log field.
func loggedReply(body []byte) string {
	return utils.RedactSecrets(parser.CapStringLen(string(body)))
}

func marshalOr(value any, fallback string) []byte {
	out, err := json.Marshal(value)
	if err != nil {
		return []byte(fallback)
	}
	return out
}

// requestIDOrNull extracts the caller's id so the reply correlates; null when absent or unparseable.
func requestIDOrNull(requestBody []byte) json.RawMessage {
	if len(requestBody) > 0 {
		var probe struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(requestBody, &probe); err == nil && len(probe.ID) > 0 {
			return probe.ID
		}
	}
	return json.RawMessage("null")
}
