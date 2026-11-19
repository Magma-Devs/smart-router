package rpcsmartrouter

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-5339: the cache filed a reply as final from block numbers alone. In one production case a
// Ripple node that was behind the network answered a numbered ledger read with its own open copy
// of that ledger: HTTP 200, "status":"success", "validated":false. The ledger was five behind the
// router's tip, the spec's distance is four, so the reply was written as final and served from the
// cache for as long as final data lives.
//
// The rule these tests pin: a reply is final only when the block distance says so AND the reply's
// own finality field, where the chain's spec names one and the reply carries it, does not say
// otherwise. A reply that does not say (no field, no declaration, a body the router does not
// read) is filed by block distance, exactly as before.

const (
	rippleFinalityPath  = "result.validated"
	rippleFinalityValue = "true"

	// The ledger of the production case and a tip five ahead of it: final by the spec's distance
	// of four.
	rippleLedger = int64(108310846)
	rippleTip    = rippleLedger + 5

	rippleLedgerRead     = `{"jsonrpc":"2.0","id":1,"method":"ledger","params":[{"ledger_index":108310846,"transactions":true,"expand":true}]}`
	rippleNearTipRead    = `{"jsonrpc":"2.0","id":1,"method":"ledger","params":[{"ledger_index":108310849,"transactions":true,"expand":true}]}`
	rippleOpenLedger     = `{"result":{"ledger":{"closed":false,"ledger_index":"108310846","parent_hash":"AB"},"ledger_current_index":108310846,"status":"success","validated":false}}`
	rippleValidLedger    = `{"result":{"ledger":{"closed":true,"ledger_index":"108310846","ledger_hash":"CD","parent_hash":"AB"},"ledger_hash":"CD","ledger_index":108310846,"status":"success","validated":true}}`
	rippleNoFieldLedger  = `{"result":{"ledger":{"closed":true,"ledger_index":"108310846","ledger_hash":"CD","parent_hash":"AB"},"ledger_hash":"CD","ledger_index":108310846,"status":"success"}}`
	rippleValidNearTip   = `{"result":{"ledger":{"closed":true,"ledger_index":"108310849","ledger_hash":"EF"},"ledger_hash":"EF","ledger_index":108310849,"status":"success","validated":true}}`
	rippleOpenLedgerHead = `{"result":{"validated":false,"status":"success","ledger_current_index":108310846,"ledger":{"closed":false,"ledger_index":"108310846","parent_hash":"AB"`
)

// rippleFinalitySpec is the part of lava-specs' ripple.json the cache write reads: the block
// distance, the ledger method's block parser, and, when declared, the finality field.
func rippleFinalitySpec(declared bool) spectypes.Spec {
	spec := spectypes.Spec{
		Index:                         "XRP",
		Name:                          "Ripple Mainnet",
		Enabled:                       true,
		AverageBlockTime:              4000,
		AllowedBlockLagForQosSync:     3,
		BlockDistanceForFinalizedData: 4,
		BlocksInFinalizationProof:     3,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC, Type: http.MethodPost},
			Apis: []*spectypes.Api{{
				Name:         "ledger",
				Enabled:      true,
				ComputeUnits: 20,
				Category:     spectypes.SpecCategory{Deterministic: true},
				BlockParsing: spectypes.BlockParser{
					ParserArg:    []string{"0", "ledger_index"},
					ParserFunc:   spectypes.PARSER_FUNC_PARSE_CANONICAL,
					DefaultValue: "validated",
				},
			}},
		}},
	}
	if declared {
		spec.FinalityFieldPath = rippleFinalityPath
		spec.FinalityFieldFinalValue = rippleFinalityValue
	}
	return spec
}

func rippleFinalityParser(t *testing.T, declared bool) chainlib.ChainParser {
	t.Helper()
	chainParser, err := chainlib.NewJrpcChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(rippleFinalitySpec(declared))
	return chainParser
}

// rippleLedgerOfSize returns a ledger reply of exactly size bytes, shaped as rippled writes one:
// an expanded transaction list, then "validated" as the last member of the body.
func rippleLedgerOfSize(size int, validated string) string {
	const (
		head = `{"result":{"ledger":{"closed":false,"ledger_index":"108310846","transactions":[`
		tx   = `{"Account":"rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH","Fee":"12","Sequence":812,"TransactionType":"Payment","hash":"E3FE6EA3D48F0C2B639448020EA4F03D4F4F8FFDB243A852A0F59177921B4879","metaData":{"TransactionIndex":4,"TransactionResult":"tesSUCCESS"}},`
		last = `{"Memo":"`
		tail = `"}]},"ledger_current_index":108310846,"status":"success","validated":`
	)
	room := size - len(head) - len(last) - len(tail) - len(validated) - len(`}}`)
	if room < 0 {
		room = 0
	}
	return head + strings.Repeat(tx, room/len(tx)) + last + strings.Repeat("a", room%len(tx)) + tail + validated + `}}`
}

// finalityFieldParser declares a finality field on a parser whose bundled spec has none, so the
// by-hash path, which only the EVM by-hash methods take, can be driven with one.
type finalityFieldParser struct {
	chainlib.ChainParser
	path, finalValue string
}

func (p finalityFieldParser) FinalityField() (string, string) { return p.path, p.finalValue }

// writtenAsFinal sends body's reply through the cache write and returns how it was filed.
func writtenAsFinal(t *testing.T, chainID string, parser chainlib.ChainParser, tip int64, body, reply string) (requestedBlock int64, final bool) {
	t.Helper()
	backend := &recordingCacheBackend{}
	rpcss := &RPCSmartRouterServer{
		cache:          backend,
		chainParser:    parser,
		listenEndpoint: &routersession.RPCEndpoint{ChainID: chainID, ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
	withTip := chainstate.New(chainID, chainstate.DefaultConfig(12*time.Second))
	withTip.SetLatestBlock(tip)
	rpcss.chainState = withTip

	msg, err := rpcss.ParseRelay(context.Background(), "", body, http.MethodPost, "dapp", "1.2.3.4", nil)
	require.NoError(t, err)
	replyBytes := []byte(reply)
	rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: replyBytes, LatestBlock: tip},
		StatusCode: http.StatusOK,
	})
	require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 10*time.Millisecond, "the answer is written to the cache")
	set := backend.recorded()[0]
	require.Equal(t, reply, string(set.Response.Data), "the reply is written as the node gave it")
	require.Equal(t, reply, string(replyBytes), "and the reply the caller holds is untouched")
	return set.RequestedBlock, set.Finalized
}

// TestCacheWriteOfAReplyThatSaysNotFinalIsNeverFinal is the ticket's acceptance at the write
// site. The backend keeps what the router hands it, so the test reads the decision itself.
func TestCacheWriteOfAReplyThatSaysNotFinalIsNeverFinal(t *testing.T) {
	cases := []struct {
		name     string
		declared bool
		body     string
		reply    string
		block    int64
		final    bool
	}{
		{
			// The production case. Fails without the check: written as final.
			name:     "a ledger final by distance whose reply says validated false is written as non-final",
			declared: true,
			body:     rippleLedgerRead,
			reply:    rippleOpenLedger,
			block:    rippleLedger,
		},
		{
			name:     "the same ledger whose reply says validated true is written as final",
			declared: true,
			body:     rippleLedgerRead,
			reply:    rippleValidLedger,
			block:    rippleLedger,
			final:    true,
		},
		{
			// A missing field changes nothing: the block distance decides.
			name:     "the same ledger whose reply has no validated field is written as final",
			declared: true,
			body:     rippleLedgerRead,
			reply:    rippleNoFieldLedger,
			block:    rippleLedger,
			final:    true,
		},
		{
			// A chain whose spec names no field is filed as before, whatever its replies carry.
			name:  "with no field declared, a reply that says validated false is written as final",
			body:  rippleLedgerRead,
			reply: rippleOpenLedger,
			block: rippleLedger,
			final: true,
		},
		{
			// The field never makes a reply final: a validated ledger too close to the tip
			// still takes the short lifetime.
			name:     "a ledger not final by distance whose reply says validated true is written as non-final",
			declared: true,
			body:     rippleNearTipRead,
			reply:    rippleValidNearTip,
			block:    rippleLedger + 3,
		},
		{
			name:     "validated false as the last member of a large reply is read",
			declared: true,
			body:     rippleLedgerRead,
			reply:    rippleLedgerOfSize(400_000, "false"),
			block:    rippleLedger,
		},
		{
			// Accepted and deliberate (MAG-5339, decision 9): above
			// maxJSONRPCResponseSizeForBlockExtraction the router does not read a reply body,
			// so such a reply "does not say" and the block distance decides. This case is
			// here so that nobody "fixes" it without that decision being reopened.
			name:     "a reply above the read limit that says validated false is still written as final",
			declared: true,
			body:     rippleLedgerRead,
			reply:    rippleLedgerOfSize(maxJSONRPCResponseSizeForBlockExtraction+1, "false"),
			block:    rippleLedger,
			final:    true,
		},
		{
			name:     "a reply exactly at the read limit that says validated false is written as non-final",
			declared: true,
			body:     rippleLedgerRead,
			reply:    rippleLedgerOfSize(maxJSONRPCResponseSizeForBlockExtraction, "false"),
			block:    rippleLedger,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			block, final := writtenAsFinal(t, "XRP", rippleFinalityParser(t, tc.declared), rippleTip, tc.body, tc.reply)
			require.Equal(t, tc.block, block, "under the ledger the request asks for")
			require.Equal(t, tc.final, final, "written as final")
		})
	}
}

// A request keyed by its hash takes its finality from the block its answer names
// (byHashFinalized), a second place a "final" is decided. The reply's own word overrides that
// one too.
func TestCacheWriteOfAByHashReplyThatSaysNotFinalIsNeverFinal(t *testing.T) {
	const (
		tip     = int64(0xffffff)
		receipt = `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0x88df016429689c079f3b2f6ad39fa052532c56795b733da78a91ebe6a713944b"]}`
	)
	noop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	eth, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	require.NoError(t, err)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	declared := finalityFieldParser{ChainParser: eth, path: "result.settled", finalValue: "true"}

	for _, tc := range []struct {
		name, reply string
		final       bool
	}{
		{"a receipt in a final block that says not final is written as non-final", `{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x10","settled":false}}`, false},
		{"a receipt in a final block that says final is written as final", `{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x10","settled":true}}`, true},
		{"a receipt in a final block that does not say is written as final", `{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x10"}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, final := writtenAsFinal(t, "ETH1", declared, tip, receipt, tc.reply)
			require.Equal(t, identityKeyBlock, block, "a by-hash answer is keyed by the request, not by a block")
			require.Equal(t, tc.final, final, "written as final")
		})
	}
}

func TestReplyAllowsFinal(t *testing.T) {
	cases := []struct {
		name             string
		reply            string
		path, finalValue string
		final            bool
	}{
		{name: "false", reply: rippleOpenLedger, path: rippleFinalityPath, finalValue: rippleFinalityValue},
		{name: "true", reply: rippleValidLedger, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "field missing", reply: rippleNoFieldLedger, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "field first in the body", reply: rippleOpenLedgerHead + `}}}`, path: rippleFinalityPath, finalValue: rippleFinalityValue},
		{name: "field last in a large body", reply: rippleLedgerOfSize(400_000, "false"), path: rippleFinalityPath, finalValue: rippleFinalityValue},
		{name: "true last in a large body", reply: rippleLedgerOfSize(400_000, "true"), path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "leading whitespace and spaced members", reply: " \r\n\t{ \"result\" : { \"validated\" : false } }", path: rippleFinalityPath, finalValue: rippleFinalityValue},

		// The value is compared as raw JSON. Anything that is not the declared value has not
		// said "final", a quoted copy of it included.
		{name: `the string "false"`, reply: `{"result":{"validated":"false"}}`, path: rippleFinalityPath, finalValue: rippleFinalityValue},
		{name: `the string "true"`, reply: `{"result":{"validated":"true"}}`, path: rippleFinalityPath, finalValue: rippleFinalityValue},
		{name: "null", reply: `{"result":{"validated":null}}`, path: rippleFinalityPath, finalValue: rippleFinalityValue},
		{name: "a declared string value matches its quoted form", reply: `{"result":{"state":"final"}}`, path: "result.state", finalValue: `"final"`, final: true},
		{name: "a declared string value against another string", reply: `{"result":{"state":"open"}}`, path: "result.state", finalValue: `"final"`},

		// Only the declared place counts: the same name elsewhere, or the same text inside a
		// value, is not the field.
		{name: "the name one level deeper", reply: `{"result":{"ledger":{"validated":false}}}`, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "the name at the root", reply: `{"validated":false,"result":{}}`, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "the text inside a memo", reply: `{"result":{"memo":"\"validated\":false","validated":true}}`, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},

		// A reply that does not say.
		{name: "batch reply", reply: `[{"result":{"validated":false}},{"result":{"validated":false}}]`, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "empty body", reply: ``, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "not JSON", reply: `<html>validated false</html>`, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "result is not an object", reply: `{"result":"validated"}`, path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},
		{name: "above the read limit", reply: rippleLedgerOfSize(maxJSONRPCResponseSizeForBlockExtraction+1, "false"), path: rippleFinalityPath, finalValue: rippleFinalityValue, final: true},

		// No declaration, or half of one: the reply is not looked at.
		{name: "no declaration", reply: rippleOpenLedger, final: true},
		{name: "path without a value", reply: rippleOpenLedger, path: rippleFinalityPath, final: true},
		{name: "value without a path", reply: rippleOpenLedger, finalValue: rippleFinalityValue, final: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.final, replyAllowsFinal([]byte(tc.reply), tc.path, tc.finalValue))
		})
	}
}

// A chain with no declaration pays nothing: the reply is not read, so nothing is allocated,
// whatever its size.
func TestReplyAllowsFinalDoesNotReadAReplyWithNoDeclaration(t *testing.T) {
	reply := []byte(rippleLedgerOfSize(400_000, "false"))
	allocs := testing.AllocsPerRun(100, func() {
		if !replyAllowsFinal(reply, "", "") {
			t.Fatal("no declaration, yet the reply was read")
		}
	})
	require.Zero(t, allocs)
}

// BenchmarkReplyAllowsFinal measures the check on a ledger-sized reply with the field as its
// last member, the worst case and the one rippled produces: the whole body is walked.
func BenchmarkReplyAllowsFinal(b *testing.B) {
	for _, size := range []struct {
		name  string
		bytes int
	}{
		{"277KB", 277 * 1024},
		{"1MiB", maxJSONRPCResponseSizeForBlockExtraction},
	} {
		reply := []byte(rippleLedgerOfSize(size.bytes, "false"))
		b.Run(size.name, func(b *testing.B) {
			b.SetBytes(int64(len(reply)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if replyAllowsFinal(reply, rippleFinalityPath, rippleFinalityValue) {
					b.Fatal("the reply says not final")
				}
			}
		})
	}
}
