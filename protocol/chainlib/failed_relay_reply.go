package chainlib

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// noAnswerShape is the error body a listener's router-made replies already use: a JSON-RPC 2.0
// error object (JSON-RPC, Tendermint POST), {"error": "..."} (REST, Tendermint GET), or the
// router's own GraphQL error (GraphQL).
type noAnswerShape int

const (
	noAnswerJsonRpc noAnswerShape = iota
	noAnswerJsonError
	// noAnswerGraphQL is the router's own GraphQL error (MAG-5119): one error with the code
	// NO_PROVIDER_ANSWER, the guid in extensions and in Lava-Guid, no data key, and a sentence
	// that names no provider (noProviderAnswerMessage).
	noAnswerGraphQL
)

// graphQLNoProviderAnswerCode is the extensions.code of the GraphQL reply when no provider answered:
// 502 when a provider refused or dropped the connection, 504 when every provider timed out (the
// GraphQL chapter's cases T9 and T10).
const graphQLNoProviderAnswerCode = "NO_PROVIDER_ANSWER"

// failedRelayReply is what the client reads for a failed relay.
type failedRelayReply struct {
	status      int
	body        []byte
	contentType string // empty: keep the listener's own Content-Type
	// fromUpstream is true when the body is the upstream's answer rather than the router's text.
	fromUpstream bool
	// guid, when set, is also sent as Lava-Guid, after the relay's own reply headers, so the header
	// always equals the guid the body carries (the GraphQL shape).
	guid string
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
	if shape == noAnswerGraphQL {
		// The router speaks for itself here, so the sentence is its own and names no provider. The
		// transport cause carries a provider's url and address, and a url can carry an API key;
		// it stays in the router's log line, under the same guid (MAG-5119).
		return failedRelayReply{
			status:      relayResult.GetStatusCode(),
			body:        convertToGraphQLError(noProviderAnswerMessage(cf), graphQLNoProviderAnswerCode, guid),
			contentType: fiber.MIMEApplicationJSONCharsetUTF8,
			guid:        guid,
		}, true
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

// grpcNoAnswerStatus is the router's own gRPC status when no provider answered (MAG-5130):
// UNAVAILABLE, or DEADLINE_EXCEEDED when every provider timed out, with the GraphQL sentence. Like
// the HTTP reply it names no provider, no address and no transport error, which stay in the router's
// log under the request's guid. ok is false when the relay carries no such verdict, and in masked
// mode, where the listener keeps its masked reply, as the HTTP listeners do.
func grpcNoAnswerStatus(relayResult *common.RelayResult) (st *status.Status, ok bool) {
	cf := relayResult.GetClientFacing()
	if cf == nil || cf.UpstreamStatus != 0 || metrics.ReturnMaskedErrors != "false" {
		return nil, false
	}
	code := codes.Unavailable
	if cf.TimedOut {
		code = codes.DeadlineExceeded
	}
	return status.New(code, noProviderAnswerMessage(cf)), true
}

// noProviderAnswerMessage is the router's no-answer sentence on GraphQL and gRPC. It names the
// request's timeout when every provider timed out, and nothing else: no provider, no address, no
// transport error.
func noProviderAnswerMessage(cf *common.ClientFacingError) string {
	if cf.TimedOut {
		if cf.Timeout > 0 {
			return "no provider answered within " + cf.Timeout.String()
		}
		return "no provider answered within the request's timeout"
	}
	return "no provider answered the request"
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
	if reply.guid != "" {
		// Last, so it wins over a Lava-Guid among the relay's own reply headers.
		metadata = append(append([]pairingtypes.Metadata(nil), metadata...), pairingtypes.Metadata{Name: common.GUID_HEADER_NAME, Value: reply.guid})
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
