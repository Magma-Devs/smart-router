package common

import (
	"strings"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The matching order (PRD PER-CHAIN-ERROR-CLASSIFICATION §3.2; MAG-5597): for a reply, first match
// wins, in these stages. Steps 1 to 3 are the prefix (§5.1): router code that runs ahead of every
// spec row, whatever a row's text says, with the two exceptions the rulebook sets, a write's success
// and duplicate rows (step 2) and the chain's over-limit row on gRPC RESOURCE_EXHAUSTED (step 3).
// Then the router-owned XRP rows that come first (§5.5), the chain's spec rows, and the router's own
// tables (the XRP catch-all, the family rows, the transport rows, the bare status) last.
//
// The status a stage reads is the record's Status (§3.1), never a number inside the body: the
// HTTPStatusContains text rows (httpStatusMessageMappings) are transport rows at the last stage,
// where evm_http_429 and evm_http_503 keep their verdicts.
//
// What the prefix decides today is main's verdict on every interface: a 429 is a rate limit (the
// senders fail the attempt on that status alone before the body is read; MAG-3030), and a
// connection-level fault in the reply's text is no reply. The rulebook's other status rules (401,
// 402 and 403 and gRPC PERMISSION_DENIED and UNAUTHENTICATED as C6, whatever the body) are MAG-5248
// item 1's, MAG-5254 item 3's and MAG-5132's to add here; until they land those replies reach the
// rows, as on main.

// Stage is the stage of the order that decided a reply.
type Stage int

const (
	StageNone Stage = iota
	// StageNoReply is §3.2 step 1: a connection-level fault, no reply.
	StageNoReply
	// StageWriteRow is step 2: on a write copy whose chain's write rows have shipped, a success or
	// duplicate row said the copy took it.
	StageWriteRow
	// StageStatus is step 3: the status decided, or the chain's over-limit row on RESOURCE_EXHAUSTED.
	StageStatus
	// StageXRPFirst is step 4.1: a router-owned XRP row that comes first (§5.5).
	StageXRPFirst
	// StageSpecRow is step 4.2: one of the chain's spec rows.
	StageSpecRow
	// StageRouterRows is steps 4.3 and 6: the router's own tables, the XRP catch-all, the family
	// rows and the transport rows with their status rows (ClassifyRecord).
	StageRouterRows
)

func (s Stage) String() string {
	switch s {
	case StageNoReply:
		return "no_reply"
	case StageWriteRow:
		return "write_row"
	case StageStatus:
		return "status"
	case StageXRPFirst:
		return "xrp_first"
	case StageSpecRow:
		return "spec_row"
	case StageRouterRows:
		return "router_rows"
	default:
		return "none"
	}
}

// ReplyContext is what the order needs about the request the reply answers, beside the record.
type ReplyContext struct {
	// Method is the api's name as the spec writes it ("eth_sendRawTransaction",
	// "POST /cosmos/tx/v1beta1/txs"), which a row's methods name.
	Method string
	// IsWrite says the api is stateful: a write copy, which step 2 and a row's scope read.
	IsWrite bool
	// Body is the reply body, for a success row's `at` on a write copy (step 2). Decoded only then.
	Body []byte
	// ConnectionError is a connection-level fault the caller found in the reply's text
	// (DetectConnectionErrorInReplyText) or on the call (DetectConnectionError); nil when none.
	ConnectionError *RouterError
}

// Verdict is what the order decided and where.
type Verdict struct {
	Stage Stage
	// RouterError is the router's verdict. Nil only when a spec row decided with a class of its own
	// (Row set, no `error` name): what the router does with a chain's own class is MAG-5606's.
	RouterError *RouterError
	// Row is the spec row that decided, at StageWriteRow, StageStatus (the over-limit row) and
	// StageSpecRow; nil elsewhere.
	Row *spectypes.ErrorRow
	// TookIt says a write copy's reply was read as the write landing (step 2; PRD §5.2, R-L).
	TookIt bool
}

// Classified is the router's verdict, RouterErrorUnknown when a spec row's own class decided.
func (v Verdict) Classified() *RouterError {
	if v.RouterError == nil {
		return RouterErrorUnknown
	}
	return v.RouterError
}

// ClassifyInOrder runs the order on record, a reply to the request ctx describes, with the chain's
// policy (nil for a chain without one) and family.
func ClassifyInOrder(policy *spectypes.ErrorsSection, family ChainFamily, record ErrorRecord, ctx ReplyContext) Verdict {
	if verdict, ok := ClassifyPrefix(policy, family, record, ctx); ok {
		return verdict
	}
	if routerError, ok := classifyXRPFirst(family, record); ok {
		return Verdict{Stage: StageXRPFirst, RouterError: routerError}
	}
	if policy != nil {
		if row, ok := MatchErrorRow(policy.Rows, record, ctx.Method, ctx.IsWrite); ok {
			return specRowVerdict(StageSpecRow, row)
		}
	}
	return Verdict{Stage: StageRouterRows, RouterError: ClassifyRecord(family, record)}
}

// ClassifyPrefix is §3.2 steps 1 to 3 alone: ok is false when the prefix leaves the reply to the
// rows. A reader that still gives the router's rows the inputs it reads today (the results manager,
// until MAG-5248 moves the JSON-RPC rows onto the record) runs the prefix and then its own rows.
func ClassifyPrefix(policy *spectypes.ErrorsSection, family ChainFamily, record ErrorRecord, ctx ReplyContext) (Verdict, bool) {
	// Step 1: no reply.
	if ctx.ConnectionError != nil {
		return Verdict{Stage: StageNoReply, RouterError: ctx.ConnectionError}, true
	}
	// Step 2: a write copy's success and duplicate rows, once the chain's write rows have shipped.
	if ctx.IsWrite && writeRowsShipped(policy, record.Interface) {
		if row, ok := tookItRow(policy.Rows, record, ctx); ok {
			verdict := specRowVerdict(StageWriteRow, row)
			verdict.TookIt = true
			return verdict, true
		}
	}
	// Step 3: the status decides.
	if record.Interface == spectypes.APIInterfaceGrpc && record.GRPCCode == grpcResourceExhausted {
		if policy != nil {
			if row, ok := MatchErrorRow(overLimitRows(policy.Rows), record, ctx.Method, ctx.IsWrite); ok {
				return specRowVerdict(StageStatus, row), true
			}
		}
		// The rate-limit and quota rows, the pushback metadata and the router's own receive limit
		// are MAG-5132's; the text rows at the last stage read them today.
	}
	if record.Status == httpTooManyRequests {
		return Verdict{Stage: StageStatus, RouterError: RouterErrorNodeRateLimited}, true
	}
	return Verdict{}, false
}

const (
	httpTooManyRequests   = 429
	grpcResourceExhausted = 8 // codes.ResourceExhausted
)

// writeRowsShipped reports whether the chain's write rows are on for apiInterface (PRD §2.1
// write_rows): until then a write copy's reply is judged as a read's, and by the §5.2 signatures.
func writeRowsShipped(policy *spectypes.ErrorsSection, apiInterface string) bool {
	if policy == nil {
		return false
	}
	for _, shipped := range policy.WriteRows {
		if shipped == apiInterface {
			return true
		}
	}
	return false
}

// tookItRow is the first success or duplicate row (`write: took_it`) that reads a write copy's reply:
// an answer row through the reply's body (a success row with `at` reads it under any status, R-L),
// an error row through the record. A plain success beats a duplicate: the answer rows run first.
func tookItRow(rows []*spectypes.ErrorRow, record ErrorRecord, ctx ReplyContext) (*spectypes.ErrorRow, bool) {
	var answerRows, errorRows []*spectypes.ErrorRow
	for _, row := range rows {
		if row == nil || row.Disabled || row.EffectiveWrite() != spectypes.ErrorWriteTookIt {
			continue
		}
		if row.Answer {
			answerRows = append(answerRows, row)
		} else {
			errorRows = append(errorRows, row)
		}
	}
	if len(answerRows) > 0 && len(ctx.Body) > 0 {
		if doc, ok := DecodeJSONDocument(ctx.Body); ok {
			success := record.InSuccess || IsSuccessStatus(record.Status)
			if row, ok := MatchAnswerRow(answerRows, record.Interface, ctx.Method, true, success, doc); ok {
				return row, true
			}
		}
	}
	return MatchErrorRow(errorRows, record, ctx.Method, true)
}

// overLimitRows are the rows that say a reply is the request over a size or count limit (C2
// over_limit): the one spec verdict that beats the status at step 3, on gRPC RESOURCE_EXHAUSTED.
func overLimitRows(rows []*spectypes.ErrorRow) []*spectypes.ErrorRow {
	var over []*spectypes.ErrorRow
	for _, row := range rows {
		if row != nil && !row.Answer && row.Class == spectypes.ErrorClassC2 && row.EffectiveReason() == spectypes.ErrorReasonOverLimit {
			over = append(over, row)
		}
	}
	return over
}

// specRowVerdict is the verdict a spec row gives: the registry's error when the row names one,
// else the row alone with its class.
func specRowVerdict(stage Stage, row *spectypes.ErrorRow) Verdict {
	verdict := Verdict{Stage: stage, Row: row}
	if row.Error != "" {
		if routerError, ok := RouterErrorByName(row.Error); ok {
			verdict.RouterError = routerError
		}
	}
	return verdict
}

// classifyXRPFirst runs the router-owned XRP rows that come ahead of the chain's spec rows (PRD
// §5.5, §3.2 step 4.1) on an XRP reply's text, as ClassifyError runs them ahead of the family table.
func classifyXRPFirst(family ChainFamily, record ErrorRecord) (*RouterError, bool) {
	if family != ChainFamilyXRP {
		return nil, false
	}
	lowered := strings.ToLower(record.Text)
	for _, mapping := range xrpFirstRows {
		if matchMapping(mapping.Matcher, 0, record.Text, lowered) {
			return mapping.RouterError, true
		}
	}
	return nil, false
}
