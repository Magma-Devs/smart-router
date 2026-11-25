package common

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The PRD §8.2 "Order" cases, in one table (MAG-5597). Each case carries a tag: `rulebook` where the
// case is asserted in the rulebook's form, because main already gives that verdict; otherwise the
// ticket that changes it, with the case asserted in main's form (§8.2's first paragraph: the gates
// that assert the rulebook's order where main differs run once the task named there lands).
func TestClassificationOrder(t *testing.T) {
	jsonrpc := func(status int, code string, text string) ErrorRecord {
		record := ErrorRecord{Interface: spectypes.APIInterfaceJsonRPC, Status: status, Text: text}
		if code != "" {
			record.Code = json.Number(code)
		}
		return record
	}
	grpc := func(status, code int, text string) ErrorRecord {
		return ErrorRecord{Interface: spectypes.APIInterfaceGrpc, Status: status, GRPCCode: code, Text: text}
	}
	rest := func(status int, text string) ErrorRecord {
		return ErrorRecord{Interface: spectypes.APIInterfaceRest, Status: status, Text: text}
	}
	policyOf := func(writeRows []string, rows ...string) *spectypes.ErrorsSection {
		policy := &spectypes.ErrorsSection{SchemaVersion: 1, WriteRows: writeRows}
		for _, literal := range rows {
			policy.Rows = append(policy.Rows, rowOf(t, literal))
		}
		return policy
	}
	const gatewayStatus = "unexpected HTTP status code received from server: %d; transport: received unexpected content-type \"text/html\""
	gateway := func(status int) string { return fmt.Sprintf(gatewayStatus, status) }

	overLimit := `{"id":"sui.over-limit","interface":"grpc","status":"RESOURCE_EXHAUSTED","contains":"larger than max","class":"C2","reason":"over_limit"}`
	duplicate := `{"id":"eth1.already-known","interface":"jsonrpc","methods":["eth_sendRawTransaction"],"scope":"write","contains":"already known","write":"took_it"}`
	sent := `{"id":"eth1.sent","interface":"jsonrpc","methods":["eth_sendRawTransaction"],"scope":"write","answer":true,"at":"/result","present":true,"write":"took_it"}`
	write := ReplyContext{Method: "eth_sendRawTransaction", IsWrite: true}

	cases := []struct {
		name, tag string
		policy    *spectypes.ErrorsSection
		family    ChainFamily
		record    ErrorRecord
		ctx       ReplyContext
		stage     Stage
		want      *RouterError // nil: a spec row's own class decided
		row       string
		tookIt    bool
	}{
		// Step 3: the status decides, whatever the body. A 429 is main's verdict on every interface
		// already: the senders fail the attempt on that status before the body is read (MAG-3030).
		{name: "429 with a JSON-RPC -32603 body", tag: "rulebook", record: jsonrpc(429, "-32603", "internal error"), stage: StageStatus, want: RouterErrorNodeRateLimited},
		{name: "429 on Tendermint with a -32603 body", tag: "rulebook", record: ErrorRecord{Interface: spectypes.APIInterfaceTendermintRPC, Status: 429, Code: json.Number("-32603"), Text: "internal error"}, stage: StageStatus, want: RouterErrorNodeRateLimited},
		{name: "429 on REST with a route-not-found body", tag: "rulebook", record: rest(429, "route not found"), stage: StageStatus, want: RouterErrorNodeRateLimited},
		{name: "429 on GraphQL with an error entry", tag: "rulebook", record: ErrorRecord{Interface: spectypes.APIInterfaceGraphQL, Status: 429, Code: "RESOURCE_EXHAUSTED", Text: "slow down"}, stage: StageStatus, want: RouterErrorNodeRateLimited},
		{name: "a gateway's 429 inside a gRPC status (D12)", tag: "rulebook", record: grpc(429, 14, gateway(429)), stage: StageStatus, want: RouterErrorNodeRateLimited},
		// The rulebook's C6 by status is MAG-5254 item 3's (REST), MAG-5132's (gRPC) and MAG-5248
		// item 1's (JSON-RPC); until then the rows read these, text first, as on main.
		{name: "REST 401 with family text", tag: "MAG-5254 item 3: C6 by status", record: rest(401, "route not found"), stage: StageRouterRows, want: RouterErrorNodeEndpointNotFound},
		{name: "REST 402 with family text", tag: "MAG-5254 item 3: C6 by status", record: rest(402, "route not found"), stage: StageRouterRows, want: RouterErrorNodeEndpointNotFound},
		{name: "REST 402 with no family text", tag: "MAG-5254 item 3: C6 by status (today the 402 code row, the same verdict)", record: rest(402, "payment required"), stage: StageRouterRows, want: RouterErrorNodeAccessDenied},
		{name: "REST 403 with family text", tag: "MAG-5254 item 3: C6 by status", record: rest(403, "endpoint not found"), stage: StageRouterRows, want: RouterErrorNodeEndpointNotFound},
		{name: "gRPC PERMISSION_DENIED with a rate-limit text", tag: "MAG-5132: C6 by status", record: grpc(7, 7, "rate limit exceeded for this key"), stage: StageRouterRows, want: RouterErrorNodeRateLimited},
		{name: "gRPC UNAUTHENTICATED", tag: "MAG-5132: C6 by status", record: grpc(16, 16, "missing api key"), stage: StageRouterRows, want: RouterErrorUnknown},
		{name: "a gateway's 402 inside a gRPC status (D12)", tag: "MAG-5132: C6 by status", record: grpc(402, 2, gateway(402)), stage: StageRouterRows, want: RouterErrorUnknown},
		// Step 4: a well-formed error through rows, whatever the status.
		{name: "a well-formed JSON-RPC error under HTTP 500 takes its code row", tag: "rulebook", record: jsonrpc(500, "-32601", "the method eth_foo does not exist/is not available"), stage: StageRouterRows, want: RouterErrorNodeMethodNotFound},
		{name: "200 with an unmatched -32000", tag: "MAG-5248 items 2 and 6: unknown (the -32000 catch-all row reads it today)", record: jsonrpc(200, "-32000", "odd"), stage: StageRouterRows, want: RouterErrorNodeServerError},
		{name: "a 2xx with a 429 code", tag: "MAG-5248: unknown (the MAG-4165 mirror row reads it today)", record: jsonrpc(200, "429", "odd"), stage: StageRouterRows, want: RouterErrorNodeRateLimited},
		// Step 6: the bare status, which the transport tables' status rows give today.
		{name: "522 with an unmatched JSON-RPC body", tag: "rulebook", record: jsonrpc(522, "-32099", "odd"), stage: StageRouterRows, want: RouterErrorNodeGatewayTimeout},
		{name: "400 with an unmatched body on JSON-RPC", tag: "MAG-5248: C3", record: jsonrpc(400, "-32099", "odd"), stage: StageRouterRows, want: RouterErrorUnknown},
		{name: "400 with an unmatched body on REST", tag: "rulebook", record: rest(400, "odd"), stage: StageRouterRows, want: RouterErrorUserInvalidRequest},
		{name: "400 with an unmatched body on GraphQL", tag: "MAG-5121: C3 (the REST fold's 400 row reads it today)", record: ErrorRecord{Interface: spectypes.APIInterfaceGraphQL, Status: 400, Text: "odd"}, stage: StageRouterRows, want: RouterErrorUserInvalidRequest},
		// The status is the record's status, never a number in the text: the HTTPStatusContains rows
		// are transport rows at the last stage (evm_http_503 keeps its verdict there).
		{name: "a body text beginning HTTP 503: is never read as a status", tag: "rulebook", record: jsonrpc(200, "", "HTTP 503: upstream"), stage: StageRouterRows, want: RouterErrorNodeServiceUnavailable},
		// A row reads its own interface only (MAG-5253): an ETH1 jsonrpc row never reads a Hedera
		// rest reply, which the REST status rows take.
		{name: "an ETH1 jsonrpc row never reads a Hedera rest reply", tag: "rulebook", policy: policyOf(nil, `{"id":"eth1.not-found","interface":"jsonrpc","contains":"not found","error":"CHAIN_TX_NOT_FOUND"}`), record: rest(404, "not found"), stage: StageRouterRows, want: RouterErrorNodeDataNotHeld},
		{name: "the same row reads its own interface", tag: "rulebook", policy: policyOf(nil, `{"id":"eth1.not-found","interface":"jsonrpc","contains":"not found","error":"CHAIN_TX_NOT_FOUND"}`), record: jsonrpc(200, "-32000", "transaction not found"), stage: StageSpecRow, want: RouterErrorChainTxNotFound, row: "eth1.not-found"},
		// The two exceptions: hooks in the prefix, with fixture rows.
		{name: "gRPC RESOURCE_EXHAUSTED with an over-limit row beats the rate-limit text", tag: "rulebook", policy: policyOf(nil, overLimit), record: grpc(8, 8, "rate limit: grpc: received message larger than max (5242880 vs. 4194304)"), stage: StageStatus, row: "sui.over-limit"},
		{name: "gRPC RESOURCE_EXHAUSTED without the row is the text rows'", tag: "MAG-5132: C4 by the rate-limit row, C2 over_limit by grpc-go's text", record: grpc(8, 8, "rate limit: grpc: received message larger than max (5242880 vs. 4194304)"), stage: StageRouterRows, want: RouterErrorNodeRateLimited},
		{name: "a duplicate row's copy under a 429 took it once write_rows is on", tag: "rulebook", policy: policyOf([]string{"jsonrpc"}, duplicate), record: jsonrpc(429, "-32000", "already known"), ctx: write, stage: StageWriteRow, row: "eth1.already-known", tookIt: true},
		{name: "and is refused before write_rows is on", tag: "rulebook", policy: policyOf(nil, duplicate), record: jsonrpc(429, "-32000", "already known"), ctx: write, stage: StageStatus, want: RouterErrorNodeRateLimited},
		{name: "write_rows on for another interface is off for this one", tag: "rulebook", policy: policyOf([]string{"rest"}, duplicate), record: jsonrpc(429, "-32000", "already known"), ctx: write, stage: StageStatus, want: RouterErrorNodeRateLimited},
		{name: "a read's reply never reaches the write rows", tag: "rulebook", policy: policyOf([]string{"jsonrpc"}, duplicate), record: jsonrpc(429, "-32000", "already known"), ctx: ReplyContext{Method: "eth_sendRawTransaction"}, stage: StageStatus, want: RouterErrorNodeRateLimited},
		{name: "a success row with at reads a write copy's body under a 429", tag: "rulebook", policy: policyOf([]string{"jsonrpc"}, sent), record: jsonrpc(429, "", `{"jsonrpc":"2.0","id":1,"result":"0xabc"}`), ctx: ReplyContext{Method: "eth_sendRawTransaction", IsWrite: true, Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0xabc"}`)}, stage: StageWriteRow, row: "eth1.sent", tookIt: true},
		{name: "a plain success beats a duplicate", tag: "rulebook", policy: policyOf([]string{"jsonrpc"}, duplicate, sent), record: jsonrpc(429, "", "already known"), ctx: ReplyContext{Method: "eth_sendRawTransaction", IsWrite: true, Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0xabc"}`)}, stage: StageWriteRow, row: "eth1.sent", tookIt: true},
		// Step 1: no reply, ahead of everything.
		{name: "a connection fault in the reply's text is no reply", tag: "rulebook", record: jsonrpc(429, "", "connection reset by peer"), ctx: ReplyContext{ConnectionError: RouterErrorConnectionReset}, stage: StageNoReply, want: RouterErrorConnectionReset},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			verdict := ClassifyInOrder(c.policy, c.family, c.record, c.ctx)
			require.Equal(t, c.stage.String(), verdict.Stage.String(), "[%s] stage", c.tag)
			if c.want == nil {
				require.Nil(t, verdict.RouterError, "[%s] a spec row's own class decided", c.tag)
			} else {
				require.NotNil(t, verdict.RouterError, "[%s]", c.tag)
				require.Equal(t, c.want.Name, verdict.RouterError.Name, "[%s] verdict", c.tag)
			}
			if c.row == "" {
				require.Nil(t, verdict.Row, "[%s] no row", c.tag)
			} else {
				require.NotNil(t, verdict.Row, "[%s] row", c.tag)
				require.Equal(t, c.row, verdict.Row.ID, "[%s] row", c.tag)
			}
			require.Equal(t, c.tookIt, verdict.TookIt, "[%s] took it", c.tag)
		})
	}
}

// The keep-today cases (PRD §8.2 Order; MAG-5597): a number inside a node's text is not a status, so
// the prefix never reads it, and the row that decides today still decides.
func TestClassificationOrderKeepsTodaysVerdicts(t *testing.T) {
	keep := func(t *testing.T, family ChainFamily, record ErrorRecord) {
		t.Helper()
		verdict := ClassifyInOrder(nil, family, record, ReplyContext{})
		require.Equal(t, StageRouterRows.String(), verdict.Stage.String(), "the status stage never read the text's number")
		require.Equal(t, ClassifyRecord(family, record).Name, verdict.RouterError.Name)
	}
	t.Run("Bitcoin -26 with a 429 inside the fee text", func(t *testing.T) {
		for _, text := range []string{"min relay fee not met, 429 < 1000", "mempool min fee not met, 429 < 1000"} {
			record := ErrorRecord{Interface: spectypes.APIInterfaceJsonRPC, Status: 200, Code: json.Number("-26"), Text: text}
			keep(t, ChainFamilyBitcoin, record)
			require.NotEqual(t, RouterErrorNodeRateLimited.Name, ClassifyInOrder(nil, ChainFamilyBitcoin, record, ReplyContext{}).RouterError.Name)
		}
	})
	t.Run("Agave -32005 behind by 429 slots", func(t *testing.T) {
		record := ErrorRecord{Interface: spectypes.APIInterfaceJsonRPC, Status: 200, Code: json.Number("-32005"), Text: "Node is behind by 429 slots"}
		keep(t, ChainFamilySolana, record)
		require.NotEqual(t, RouterErrorNodeRateLimited.Name, ClassifyInOrder(nil, ChainFamilySolana, record, ReplyContext{}).RouterError.Name)
	})
	t.Run("a rate-limit phrase inside gRPC 3, 5, 10, 11 and 12 stays with its code row", func(t *testing.T) {
		for code, want := range map[int]*RouterError{3: RouterErrorUserInvalidParams, 5: RouterErrorNodeDataNotHeld, 10: RouterErrorNodeAborted, 11: RouterErrorNodeDataNotHeld, 12: RouterErrorNodeUnimplemented} {
			record := ErrorRecord{Interface: spectypes.APIInterfaceGrpc, Status: code, GRPCCode: code, Text: "rate limit exceeded; too many requests"}
			keep(t, ChainFamilyUnknown, record)
			require.Equal(t, want.Name, ClassifyInOrder(nil, ChainFamilyUnknown, record, ReplyContext{}).RouterError.Name, "code %d", code)
		}
	})
}

// The router-owned XRP rows (PRD §5.5; MAG-5597): three run ahead of the chain's spec rows, the
// te[fmlr] catch-all after them, and the signature and the rate limit hold with no policy at all
// (reduced coverage, whole-source fallback; §8.3).
func TestClassificationOrderXRPRows(t *testing.T) {
	xrp := func(text string) ErrorRecord {
		return ErrorRecord{Interface: spectypes.APIInterfaceJsonRPC, Status: 200, Text: text, InSuccess: true}
	}
	first := map[string]*RouterError{
		"tooBusy: The server is too busy to help you now.":              RouterErrorNodeRateLimited,
		"slowDown: You are placing too much load on the server.":        RouterErrorNodeRateLimited,
		"tefALREADY: The exact transaction was already in this ledger.": RouterErrorChainTxAlreadyKnown,
		"noPermission: You don't have permission for this command.":     RouterErrorNodeAccessDenied,
		"forbidden: Bad credentials.":                                   RouterErrorNodeAccessDenied,
	}
	spoiler := policyOf5597(t, `{"id":"xrp.spoiler","interface":"jsonrpc","regex":"^(tooBusy|slowDown|tefALREADY|noPermission|forbidden)\\b","error":"NODE_INTERNAL_ERROR"}`)
	for text, want := range first {
		t.Run(text, func(t *testing.T) {
			verdict := ClassifyInOrder(nil, ChainFamilyXRP, xrp(text), ReplyContext{Method: "submit", IsWrite: true})
			require.Equal(t, StageXRPFirst.String(), verdict.Stage.String())
			require.Equal(t, want.Name, verdict.RouterError.Name)
			verdict = ClassifyInOrder(spoiler, ChainFamilyXRP, xrp(text), ReplyContext{Method: "submit", IsWrite: true})
			require.Equal(t, StageXRPFirst.String(), verdict.Stage.String(), "ahead of a spec row that would read it")
			require.Equal(t, want.Name, verdict.RouterError.Name)
			require.Equal(t, want.Name, ClassifyError(nil, ChainFamilyXRP, TransportJsonRPC, 200, text).Name, "ClassifyError gives the same verdict")
			require.Equal(t, want.Name, ClassifyError(nil, ChainFamilyXRP, TransportJsonRPC, 503, "HTTP 503: "+text).Name, "with the direct sender's prefix too")
		})
	}
	t.Run("the first rows read the text on any interface XRP serves", func(t *testing.T) {
		record := ErrorRecord{Interface: spectypes.APIInterfaceRest, Status: 200, Text: "tooBusy: The server is too busy to help you now.", InSuccess: true}
		verdict := ClassifyInOrder(nil, ChainFamilyXRP, record, ReplyContext{})
		require.Equal(t, StageXRPFirst.String(), verdict.Stage.String())
	})
	t.Run("tefPAST_SEQ reaches the family table and the signature with no policy", func(t *testing.T) {
		text := "tefPAST_SEQ: This sequence number has already passed."
		verdict := ClassifyInOrder(nil, ChainFamilyXRP, xrp(text), ReplyContext{Method: "submit", IsWrite: true})
		require.Equal(t, StageRouterRows.String(), verdict.Stage.String())
		require.Equal(t, RouterErrorChainNonceTooLow.Name, verdict.RouterError.Name)
		require.True(t, IsSlotAlreadyUsed(ChainFamilyXRP, spectypes.APIInterfaceJsonRPC, 0, text))
	})
	t.Run("a spec row for tefPAST_SEQ decides before the family table; the catch-all after it", func(t *testing.T) {
		policy := policyOf5597(t, `{"id":"xrp.past-seq","interface":"jsonrpc","methods":["submit","submit_multisigned"],"regex":"^tefPAST_SEQ\\b","error":"CHAIN_NONCE_TOO_LOW"}`)
		verdict := ClassifyInOrder(policy, ChainFamilyXRP, xrp("tefPAST_SEQ: This sequence number has already passed."), ReplyContext{Method: "submit", IsWrite: true})
		require.Equal(t, StageSpecRow.String(), verdict.Stage.String())
		require.Equal(t, "xrp.past-seq", verdict.Row.ID)
		require.Equal(t, RouterErrorChainNonceTooLow.Name, verdict.RouterError.Name)
		verdict = ClassifyInOrder(policy, ChainFamilyXRP, xrp("temBAD_FEE: Invalid fee, negative or not XRP."), ReplyContext{Method: "submit", IsWrite: true})
		require.Equal(t, StageRouterRows.String(), verdict.Stage.String(), "the te[fmlr] catch-all runs after the spec rows")
		require.Equal(t, RouterErrorChainTxRejected.Name, verdict.RouterError.Name)
	})
}

// A gateway's HTTP status is the record's only where grpc-go writes it (Part III D12; MAG-5597): at
// the start of the status message, or of the router's JSON of it, which the results manager reads.
// A node's own message that quotes grpc-go's words further on keeps its gRPC status, so the prefix
// leaves it to the rows: the keep-today rule for a rate-limit phrase inside gRPC 3/5/10/11/12 holds
// for a quoted 429 too.
func TestClassificationOrderReadsAGatewayStatusWhereGRPCWritesIt(t *testing.T) {
	gateway := `unexpected HTTP status code received from server: 429 (Too Many Requests); transport: received unexpected content-type "text/html"`
	rendered, err := json.Marshal(struct {
		ErrorMessage string `json:"error_message"`
		ErrorCode    uint32 `json:"error_code"`
	}{gateway, 14})
	require.NoError(t, err)
	for name, text := range map[string]string{"the status message": gateway, "the router's JSON of it": string(rendered)} {
		_, records := ReadGRPCReply(14, text, nil, false)
		require.Equal(t, 429, records[0].Status, name)
		require.Equal(t, 14, records[0].GRPCCode, name)
		verdict, ok := ClassifyPrefix(nil, ChainFamilyUnknown, records[0], ReplyContext{})
		require.True(t, ok, name)
		require.Equal(t, StageStatus.String(), verdict.Stage.String(), name)
		require.Equal(t, RouterErrorNodeRateLimited.Name, verdict.RouterError.Name, name)
	}

	quoted := "unexpected HTTP status code received from server: 429"
	for _, code := range []int{2, 3, 5, 9, 10, 11, 12, 13} {
		for _, text := range []string{"invalid request: " + quoted, `{"error_message":"invalid request: ` + quoted + `","error_code":3}`} {
			_, records := ReadGRPCReply(code, text, nil, false)
			require.Equal(t, code, records[0].Status, "code %d, %q: words a node quotes are not a gateway's status", code, text)
			_, decided := ClassifyPrefix(nil, ChainFamilyUnknown, records[0], ReplyContext{})
			require.False(t, decided, "code %d, %q: left to the rows", code, text)
		}
	}
}

// A write copy's reply that reaches the order is an error, never an answer (PRD §3.1; MAG-5597). A
// success row without `at` reads a 2xx answer only (§2.2, §3.2 step 2), so it reads neither a
// well-formed error under a 200 nor an error a shape found inside a success (§8.2, Shapes: such an
// error never ends a write's fan-out as its success). A success row with `at` still reads the body,
// and decides by what its pointer finds there.
func TestClassificationOrderSuccessRowsReadAnswersOnly(t *testing.T) {
	ethAny := `{"id":"eth1.sent-any","interface":"jsonrpc","methods":["eth_sendRawTransaction"],"scope":"write","answer":true,"write":"took_it"}`
	cosmosAny := `{"id":"cosmos.sent-any","interface":"rest","methods":["POST /cosmos/tx/v1beta1/txs"],"scope":"write","answer":true,"write":"took_it"}`
	cosmosDuplicate := `{"id":"cosmos.in-mempool","interface":"rest","methods":["POST /cosmos/tx/v1beta1/txs"],"scope":"write","answer":true,"at":"/tx_response/code","code":19,"write":"took_it"}`
	policyOf := func(writeRows string, rows ...string) *spectypes.ErrorsSection {
		policy := &spectypes.ErrorsSection{SchemaVersion: 1, WriteRows: []string{writeRows}}
		for _, literal := range rows {
			policy.Rows = append(policy.Rows, rowOf(t, literal))
		}
		return policy
	}

	t.Run("a well-formed JSON-RPC error under a 200", func(t *testing.T) {
		record := ErrorRecord{Interface: spectypes.APIInterfaceJsonRPC, Status: 200, Code: json.Number("-32000"), Text: "insufficient funds for gas * price + value"}
		ctx := ReplyContext{Method: "eth_sendRawTransaction", IsWrite: true, Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"insufficient funds for gas * price + value"}}`)}
		verdict := ClassifyInOrder(policyOf("jsonrpc", ethAny), ChainFamilyEVM, record, ctx)
		require.False(t, verdict.TookIt, "a refused write is not took it")
		require.Equal(t, StageRouterRows.String(), verdict.Stage.String())
		require.Equal(t, ClassifyRecord(ChainFamilyEVM, record).Name, verdict.Classified().Name)
	})
	refused := ErrorRecord{Interface: spectypes.APIInterfaceRest, Status: 200, Text: "insufficient fee", InSuccess: true}
	refusedBody := []byte(`{"tx_response":{"code":13,"codespace":"sdk","txhash":"AB","raw_log":"insufficient fee"}}`)
	broadcast := ReplyContext{Method: "POST /cosmos/tx/v1beta1/txs", IsWrite: true, Body: refusedBody}
	t.Run("an error a shape found inside a 200", func(t *testing.T) {
		verdict := ClassifyInOrder(policyOf("rest", cosmosAny), ChainFamilyCosmosSDK, refused, broadcast)
		require.False(t, verdict.TookIt, "a refused broadcast is not took it")
		require.NotEqual(t, StageWriteRow.String(), verdict.Stage.String())
	})
	t.Run("a success row with at reads the body and decides by its pointer", func(t *testing.T) {
		verdict := ClassifyInOrder(policyOf("rest", cosmosDuplicate), ChainFamilyCosmosSDK, refused, broadcast)
		require.False(t, verdict.TookIt, "code 13 is not the row's 19")
		inMempool := ErrorRecord{Interface: spectypes.APIInterfaceRest, Status: 200, Text: "tx already in mempool", InSuccess: true}
		ctx := ReplyContext{Method: "POST /cosmos/tx/v1beta1/txs", IsWrite: true, Body: []byte(`{"tx_response":{"code":19,"codespace":"sdk","txhash":"AB","raw_log":"tx already in mempool"}}`)}
		verdict = ClassifyInOrder(policyOf("rest", cosmosDuplicate), ChainFamilyCosmosSDK, inMempool, ctx)
		require.True(t, verdict.TookIt)
		require.Equal(t, StageWriteRow.String(), verdict.Stage.String())
		require.Equal(t, "cosmos.in-mempool", verdict.Row.ID)
	})
}

// ClassifyPrefix is steps 1 to 3 alone, for the results manager, which still gives the router's
// rows today's inputs after it.
func TestClassifyPrefix(t *testing.T) {
	record := ErrorRecord{Interface: spectypes.APIInterfaceJsonRPC, Status: 429, Code: json.Number("-32603"), Text: "internal error"}
	verdict, ok := ClassifyPrefix(nil, ChainFamilyUnknown, record, ReplyContext{})
	require.True(t, ok)
	require.Equal(t, StageStatus.String(), verdict.Stage.String())
	require.Equal(t, RouterErrorNodeRateLimited.Name, verdict.RouterError.Name)

	verdict, ok = ClassifyPrefix(nil, ChainFamilyUnknown, record, ReplyContext{ConnectionError: RouterErrorConnectionTimeout})
	require.True(t, ok)
	require.Equal(t, StageNoReply.String(), verdict.Stage.String(), "no reply comes first")

	record.Status = 200
	_, ok = ClassifyPrefix(nil, ChainFamilyUnknown, record, ReplyContext{})
	require.False(t, ok, "a 200 is left to the rows")
	record.Status = 503
	_, ok = ClassifyPrefix(nil, ChainFamilyUnknown, record, ReplyContext{})
	require.False(t, ok, "a 503 is the rows' today (the status rows at the last stage)")
}

// A spec row's verdict: the registry's error when the row names one, the row alone with its class
// otherwise; Classified never returns nil.
func TestSpecRowVerdict(t *testing.T) {
	named := ClassifyInOrder(policyOf5597(t, `{"id":"a","interface":"rest","status":409,"error":"CHAIN_TX_ALREADY_KNOWN"}`), ChainFamilyUnknown, ErrorRecord{Interface: "rest", Status: 409, Text: "conflict"}, ReplyContext{})
	require.Equal(t, StageSpecRow.String(), named.Stage.String())
	require.Equal(t, RouterErrorChainTxAlreadyKnown.Name, named.Classified().Name)

	classed := ClassifyInOrder(policyOf5597(t, `{"id":"b","interface":"rest","status":409,"class":"C3","reason":"not_found"}`), ChainFamilyUnknown, ErrorRecord{Interface: "rest", Status: 409, Text: "conflict"}, ReplyContext{})
	require.Equal(t, StageSpecRow.String(), classed.Stage.String())
	require.Nil(t, classed.RouterError)
	require.Equal(t, "b", classed.Row.ID)
	require.Equal(t, RouterErrorUnknown.Name, classed.Classified().Name, "what the router does with a chain's own class is MAG-5606's")

	unknownName := ClassifyInOrder(policyOf5597(t, `{"id":"c","interface":"rest","status":409,"error":"NO_SUCH_ERROR"}`), ChainFamilyUnknown, ErrorRecord{Interface: "rest", Status: 409, Text: "conflict"}, ReplyContext{})
	require.Nil(t, unknownName.RouterError, "a name the registry lacks is the validator's to refuse (MAG-5604)")
	require.Equal(t, "c", unknownName.Row.ID)
}

func TestRouterErrorByName(t *testing.T) {
	routerError, ok := RouterErrorByName("NODE_RATE_LIMITED")
	require.True(t, ok)
	require.Same(t, RouterErrorNodeRateLimited, routerError)
	routerError, ok = RouterErrorByName(RouterErrorUnknown.Name)
	require.True(t, ok)
	require.Same(t, RouterErrorUnknown, routerError)
	_, ok = RouterErrorByName("node_rate_limited")
	require.False(t, ok, "names as the registry writes them")
}

func TestStageString(t *testing.T) {
	require.Equal(t, "none", StageNone.String())
	require.Equal(t, "no_reply", StageNoReply.String())
	require.Equal(t, "write_row", StageWriteRow.String())
	require.Equal(t, "status", StageStatus.String())
	require.Equal(t, "xrp_first", StageXRPFirst.String())
	require.Equal(t, "spec_row", StageSpecRow.String())
	require.Equal(t, "router_rows", StageRouterRows.String())
}

// policyOf5597 is a policy of error rows written as JSON, with no write rows.
func policyOf5597(t *testing.T, rows ...string) *spectypes.ErrorsSection {
	t.Helper()
	policy := &spectypes.ErrorsSection{SchemaVersion: 1}
	for _, literal := range rows {
		policy.Rows = append(policy.Rows, rowOf(t, literal))
	}
	return policy
}
