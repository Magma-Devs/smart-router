package chainlib

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

const upstreamBodyCap = 256

// convertUpstreamErrorToJsonRpc renders a failed relay for the caller from the upstream's own
// answer (MAG-4218). A JSON-RPC error body is forwarded as is, under the caller's id.
// Anything else gets the standard envelope: one human sentence in message, the details in data.
func convertUpstreamErrorToJsonRpc(cf *common.ClientFacingError, guid, rawRouterMsg string, requestBody []byte) []byte {
	reqID := requestIDOrNull(requestBody)

	// 1. The node's own JSON-RPC error: forward it unchanged, id rewritten to the caller's.
	if len(cf.UpstreamBody) > 0 {
		var env map[string]json.RawMessage
		if json.Unmarshal(cf.UpstreamBody, &env) == nil {
			if errField, ok := env["error"]; ok {
				var probe struct {
					Code    *int64  `json:"code"`
					Message *string `json:"message"`
				}
				if json.Unmarshal(errField, &probe) == nil && probe.Code != nil && probe.Message != nil {
					env["id"] = reqID
					env["jsonrpc"] = json.RawMessage(`"2.0"`)
					if out, err := json.Marshal(env); err == nil {
						return out
					}
				}
			}
		}
	}

	// 2. Router-built envelope.
	data := map[string]any{}
	if guid != "" {
		data["guid"] = guid
	}
	var message string
	if cf.UpstreamStatus != 0 {
		statusText := cf.UpstreamStatusText
		if statusText == "" {
			statusText = fmt.Sprintf("%d", cf.UpstreamStatus)
		}
		message = "upstream returned HTTP " + statusText
		data["upstream_status"] = cf.UpstreamStatus
		if len(cf.UpstreamBody) == 0 {
			message += " with an empty body"
		} else {
			body := cf.UpstreamBody
			if len(body) > upstreamBodyCap {
				body = body[:upstreamBodyCap]
			}
			data["upstream_body"] = utils.RedactSecrets(string(body))
		}
	} else {
		message = "no answer from upstream: " + utils.RedactSecrets(cf.Cause)
	}
	if cf.RouterErrorName != "" {
		data["router_error_code"] = cf.RouterErrorCode
		data["router_error_name"] = cf.RouterErrorName
	}
	if detail := routerDetail(rawRouterMsg); detail != "" {
		data["router_detail"] = detail
	}
	envelope := fiber.Map{
		"jsonrpc": "2.0",
		"id":      reqID,
		"error": fiber.Map{
			"code":    -32000,
			"message": message,
			"data":    data,
		},
	}
	out, err := json.Marshal(envelope)
	if err != nil {
		return []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Failed to marshal error response"}}`)
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

var logAttrBlock = regexp.MustCompile(`\s*\{[^{}]*\}`)

// routerDetail keeps the router's own description chain for support, minus the leaf cause (which
// is now the message) and minus the log attributes that leak into the string today.
func routerDetail(rawRouterMsg string) string {
	var parsed struct {
		Error string `json:"Error"`
	}
	text := rawRouterMsg
	if json.Unmarshal([]byte(rawRouterMsg), &parsed) == nil && parsed.Error != "" {
		text = parsed.Error
	}
	text = logAttrBlock.ReplaceAllString(text, "")
	parts := strings.Split(text, " ErrMsg: ")
	if len(parts) > 1 {
		parts = parts[:len(parts)-1]
	}
	return utils.RedactSecrets(strings.TrimSpace(strings.Join(parts, ": ")))
}
