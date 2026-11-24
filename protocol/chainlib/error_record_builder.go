package chainlib

import (
	"encoding/json"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// RecordsFromReply builds the error records (common.ErrorRecord, PRD §3.1; MAG-5595) of a reply the
// interface's checker (CheckResponseError) judged a node error, or of an upstream's non-2xx body an
// attempt failed on. One record per batch element or GraphQL error when the body is in the
// interface's standard place; otherwise one record whose text is checkerText, what the checker found
// (a chain's reply check in router code: XRPL's engine result, Tendermint's response code, Cosmos'
// tx_response on REST; or the checker's own words for a malformed body), so the record says what the
// router acted on. policy is the chain's error policy, whose shapes say where its errors sit; nil
// until MAG-5606 resolves one per chain, so today only the standard places are read.
//
// The record is built beside today's classifier inputs, not in their place: the flags on the result
// keep their verdict, and ClassifyRecord gives the router's rows the same inputs from the record.
func RecordsFromReply(policy *spectypes.ErrorsSection, apiInterface, method string, status int, body []byte, checkerText string) []common.ErrorRecord {
	switch apiInterface {
	case spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC:
		kind, records := common.ReadJSONRPCReply(apiInterface, status, body)
		if kind == common.ReplyWellFormedError {
			if len(records) == 1 && records[0].Text == "" {
				records[0].Text = checkerText
			}
			return records
		}
		// The checker found an error the envelope does not show: inside a result (XRPL's engine
		// result, a Tendermint response code), or a body that is not JSON-RPC at all.
		return []common.ErrorRecord{{
			Interface: apiInterface,
			Status:    status,
			Text:      firstNonEmpty(checkerText, bareBodyText(records)),
			InSuccess: kind == common.ReplyAnswer && common.IsSuccessStatus(status),
		}}
	case spectypes.APIInterfaceRest:
		kind, records := common.ReadRESTReply(policyShapes(policy), method, false, status, body)
		if kind == common.ReplyWellFormedError {
			return records
		}
		// No shape read it: the text is what the REST checker found (extractErrorMessage on a
		// non-2xx; a Cosmos tx_response's raw_log inside a 2xx answer, which is an error in a success).
		// A 2xx that is not an answer at all (HTML, an empty body) is bare, not an error in a success.
		return []common.ErrorRecord{{
			Interface: spectypes.APIInterfaceRest,
			Status:    status,
			Text:      firstNonEmpty(checkerText, bareBodyText(records)),
			InSuccess: kind == common.ReplyAnswer && common.IsSuccessStatus(status),
		}}
	case spectypes.APIInterfaceGraphQL:
		kind, records := common.ReadGraphQLReply(status, body)
		if kind == common.ReplyWellFormedError {
			return records
		}
		return []common.ErrorRecord{{Interface: spectypes.APIInterfaceGraphQL, Status: status, Text: firstNonEmpty(checkerText, bareBodyText(records))}}
	case spectypes.APIInterfaceGrpc:
		// status is the gRPC status code. The body is the router's own rendering of the node's
		// status ({"error_message", "error_code"}, routersession.GRPCNodeErrorResponse); the text is
		// the node's message. Details reach the record once MAG-5129 carries them.
		text := checkerText
		if text == "" {
			text = grpcErrorMessage(body)
		}
		if status == 0 {
			return []common.ErrorRecord{{Interface: spectypes.APIInterfaceGrpc, Text: text}}
		}
		_, records := common.ReadGRPCReply(status, text, nil, false)
		return records
	default:
		return []common.ErrorRecord{{Interface: apiInterface, Status: status, Text: checkerText}}
	}
}

// RecordsFromUpstreamBody builds the records of an upstream's non-2xx answer an attempt failed on
// (a 429 or 5xx the sender turned into a routersession.HTTPStatusError, body kept): a JSON-RPC 5xx
// body is read as the node's error it carries (PRD §5.2: the signatures read every record, this one
// included); any other body is bare, its text the body's.
func RecordsFromUpstreamBody(policy *spectypes.ErrorsSection, apiInterface, method string, status int, body []byte) []common.ErrorRecord {
	if len(body) == 0 {
		return nil
	}
	return RecordsFromReply(policy, apiInterface, method, status, body, "")
}

// policyShapes is the policy's shapes, none for a nil policy.
func policyShapes(policy *spectypes.ErrorsSection) []*spectypes.ErrorShape {
	if policy == nil {
		return nil
	}
	return policy.Shapes
}

// bareBodyText is the text of the one bare record a standard reading returns, "" when there is none.
func bareBodyText(records []common.ErrorRecord) string {
	if len(records) == 1 {
		return records[0].Text
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// grpcErrorMessage is the node's status message out of the router's gRPC node-error body.
func grpcErrorMessage(body []byte) string {
	var rendered struct {
		ErrorMessage string `json:"error_message"`
	}
	if json.Unmarshal(body, &rendered) == nil {
		return rendered.ErrorMessage
	}
	return ""
}
