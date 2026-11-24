package rpcsmartrouter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// MAG-5232 end to end: a write goes through a real JSON-RPC or Tendermint RPC listener, the real
// router and the real direct sender to httptest upstreams, and the test asserts what the client
// reads: the HTTP status, the body, Router-Write-Outcome, Router-Tx-Hash and Router-Guid.

// evmRawTx is the signed transaction eth_sendRawTransaction sends here. Its hash is keccak-256 of
// its bytes, whatever they hold.
const evmRawTx = "0x02f86c0180843b9aca00825208940000000000000000000000000000000000000000880de0b6b3a764000080c0"

func evmRawTxKeccak(t *testing.T) string {
	t.Helper()
	raw, err := hexutil.Decode(evmRawTx)
	require.NoError(t, err)
	return hexutil.Encode(crypto.Keccak256(raw))
}

// writeIface is a write on chainID's apiInterface, whose window is short enough for a test that
// waits out a copy that never answers.
func writeIface(name, chainID, apiInterface, method, path, body string) failedReplyInterface {
	return failedReplyInterface{
		name: name, chainID: chainID, apiInterface: apiInterface, method: method, path: path, body: body, jsonRpcShape: true,
		parser: func(t *testing.T) chainlib.ChainParser {
			t.Helper()
			parser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), chainID, apiInterface,
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }), nil, "../../", nil)
			require.NoError(t, err)
			if closeServer != nil {
				t.Cleanup(closeServer)
			}
			shortenWriteWindow(t, parser, chainID)
			return parser
		},
	}
}

var (
	evmWrite = writeIface("jsonrpc", "ETH1", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/",
		`{"jsonrpc":"2.0","id":77,"method":"eth_sendRawTransaction","params":["`+evmRawTx+`"]}`)
	tendermintPostWrite = writeIface("tendermintrpc POST", "COSMOSHUB", spectypes.APIInterfaceTendermintRPC, http.MethodPost, "/",
		`{"jsonrpc":"2.0","id":77,"method":"broadcast_tx_sync","params":{"tx":"dHg="}}`)
	tendermintGetWrite = writeIface("tendermintrpc GET", "COSMOSHUB", spectypes.APIInterfaceTendermintRPC, http.MethodGet, "/broadcast_tx_sync?tx=0x1234", "")
	bitcoinWrite       = writeIface("bitcoin", "BTC", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/",
		`{"jsonrpc":"1.0","id":77,"method":"sendrawtransaction","params":["0200000001"]}`)
)

// routerWriteReply is the router's own reply to a write, as the client reads it.
type routerWriteReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   struct {
		Code    int               `json:"code"`
		Message string            `json:"message"`
		Data    map[string]string `json:"data"`
	} `json:"error"`
}

// requireRouterWriteReply asserts the router's own reply to a write (MAG-5232 Expected 4 to 6): HTTP
// 200, a JSON-RPC error object with code's code, the id, router_error, write_outcome, tx_hash when
// there is one, the guid Router-Guid carries, and the outcome and hash headers. It returns the reply.
func requireRouterWriteReply(t *testing.T, read failedReplyClientRead, code chainlib.RouterReplyCode, outcome, id, txHash string) routerWriteReply {
	t.Helper()
	require.Equal(t, http.StatusOK, read.status, "a router reply to a write is HTTP 200: a 5xx gets the write sent again. body=%s", read.body)
	var reply routerWriteReply
	require.NoError(t, json.Unmarshal([]byte(read.body), &reply), read.body)
	require.Equal(t, "2.0", reply.JSONRPC)
	require.JSONEq(t, id, string(reply.ID), "the caller's id")
	require.Equal(t, code.Code, reply.Error.Code, read.body)
	require.Equal(t, code.Name, reply.Error.Data["router_error"])
	require.Equal(t, outcome, reply.Error.Data["write_outcome"])
	require.NotEmpty(t, read.guid)
	require.Equal(t, read.guid, reply.Error.Data["guid"], "the body's guid is Router-Guid")
	require.Equal(t, outcome, read.header.Get(common.WRITE_OUTCOME_HEADER_NAME))
	require.Equal(t, txHash, read.header.Get(common.TX_HASH_HEADER_NAME))
	require.Equal(t, txHash, reply.Error.Data["tx_hash"])
	require.Empty(t, read.nodeError, "the router's own reply is not a node's error")
	for _, leak := range []string{"127.0.0.1", "upstream-", "503", "Service Unavailable"} {
		require.NotContains(t, reply.Error.Message, leak, "no provider and no transport in the sentence")
	}
	return reply
}

// requireUnknownWrite asserts the OUTCOME_UNKNOWN reply with Part I's sentence, naming txHash.
func requireUnknownWrite(t *testing.T, read failedReplyClientRead, id, txHash string) {
	t.Helper()
	reply := requireRouterWriteReply(t, read, chainlib.RouterReplyOutcomeUnknown, writeOutcomeUnknown, id, txHash)
	require.Equal(t, unknownWriteMessage(txHash), reply.Error.Message)
	require.True(t, strings.HasPrefix(reply.Error.Message, "Status unknown - this transaction may have been broadcast. "))
}

func shortWriteBudget(t *testing.T) {
	t.Helper()
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 300 * time.Millisecond // stateful processing budget: 6 x 300 ms
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })
}

// No copy was accepted and at least one may have run: the client gets the router's OUTCOME_UNKNOWN
// reply with the hash, not a node's 5xx (MAG-5232 Expected 1 and 4; tests 1 to 4 and 18). A 5xx
// often comes from a gateway that had already passed the transaction to a node; a client that reads
// it as "failed" signs a new transaction. A refusal beside a copy that may have run is still unknown.
func TestJSONRPCWrite_UnknownOutcomeIsTheRoutersReply(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	shortWriteBudget(t)
	hash := evmRawTxKeccak(t)
	for _, c := range []struct {
		name      string
		upstreams func(t *testing.T) []string
	}{
		{"every copy answered 503", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 503, `{"message":"no healthy upstream"}`, "application/json"), answeringUpstream(t, 503, `{"message":"no healthy upstream"}`, "application/json")}
		}},
		{"every copy answered 500", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 500, "internal error", "text/plain"), answeringUpstream(t, 500, "internal error", "text/plain")}
		}},
		{"every copy answered 501", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 501, "not implemented", "text/plain"), answeringUpstream(t, 501, "not implemented", "text/plain")}
		}},
		{"every copy answered 520", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 520, "", ""), answeringUpstream(t, 520, "", "")}
		}},
		{"one copy 503, one refused by 403", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 503, `{"message":"no healthy upstream"}`, "application/json"), answeringUpstream(t, 403, `{"message":"forbidden"}`, "application/json")}
		}},
		{"every copy slower than the write budget", func(t *testing.T) []string {
			return []string{hangingUpstream(t, nil), hangingUpstream(t, nil)}
		}},
		{"every copy answered 200 with an empty body", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 200, "", ""), answeringUpstream(t, 200, "", "")}
		}},
		{"every copy answered 200 with JSON that is not JSON-RPC", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 200, `{}`, "application/json"), answeringUpstream(t, 200, `{"error":"rate limited"}`, "application/json")}
		}},
		{"200 empty, 200 HTML, and a copy that never answered", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 200, "", ""), answeringUpstream(t, 200, "<html>bad gateway</html>", "text/html"), hangingUpstream(t, nil)}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			send := startFailedReplyRouter(t, evmWrite, c.upstreams(t)...)
			requireUnknownWrite(t, send(), `77`, hash)
		})
	}
}

// Masked mode keeps the unknown reply's sentence and fields (MAG-5061 item 5, MAG-5232 test 5):
// they are the advice the client acts on. Main replaced them with "Internal server error".
func TestJSONRPCWrite_MaskedModeKeepsTheUnknownReply(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	previous := metrics.ReturnMaskedErrors
	metrics.ReturnMaskedErrors = "true"
	t.Cleanup(func() { metrics.ReturnMaskedErrors = previous })
	send := startFailedReplyRouter(t, evmWrite,
		answeringUpstream(t, 503, `{"message":"down"}`, "application/json"), answeringUpstream(t, 503, `{"message":"down"}`, "application/json"))
	requireUnknownWrite(t, send(), `77`, evmRawTxKeccak(t))
}

// Every copy refused by status, 401, 402, 403 or 429: the client gets the best-ranked node reply as
// it came, with Router-Write-Outcome: rejected and the hash (MAG-5232 Expected 2, tests 6 and 7).
func TestJSONRPCWrite_RejectedByStatusIsTheNodesReply(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	const throttled = `{"code":-32007,"message":"50/second request limit reached"}`
	for _, c := range []struct {
		name       string
		statuses   [2]int
		wantStatus int
		wantBody   string
	}{
		{"every copy answered 401", [2]int{401, 401}, 401, `{"message":"refused"}`},
		{"every copy answered 403", [2]int{403, 403}, 403, `{"message":"refused"}`},
		{"every copy answered 429", [2]int{429, 429}, 429, throttled},
		{"a 403 and a 429: the 429", [2]int{403, 429}, 429, throttled},
	} {
		t.Run(c.name, func(t *testing.T) {
			upstreams := make([]string, 2)
			for i, status := range c.statuses {
				body := `{"message":"refused"}`
				if status == http.StatusTooManyRequests {
					body = throttled
				}
				upstreams[i] = answeringUpstream(t, status, body, "application/json")
			}
			read := startFailedReplyRouter(t, evmWrite, upstreams...)()
			require.Equal(t, c.wantStatus, read.status, read.body)
			require.JSONEq(t, c.wantBody, read.body, "the node's reply as it came")
			require.Equal(t, writeOutcomeRejected, read.header.Get(common.WRITE_OUTCOME_HEADER_NAME))
			require.Equal(t, evmRawTxKeccak(t), read.header.Get(common.TX_HASH_HEADER_NAME))
			require.NotEmpty(t, read.guid)
		})
	}
}

// Transport proof that no byte left the router: HTTP 200, NO_PROVIDER_ANSWER, write_outcome:
// not_sent (MAG-5232 Expected 5, test 8). Main answered with the 502 a read gets.
func TestJSONRPCWrite_NotSentWhenEveryPortRefused(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	read := startFailedReplyRouter(t, evmWrite, refusingUpstream(t), refusingUpstream(t))()
	reply := requireRouterWriteReply(t, read, chainlib.RouterReplyNoProviderAnswer, writeOutcomeNotSent, `77`, evmRawTxKeccak(t))
	require.Equal(t, "No provider answered - this transaction was not sent.", reply.Error.Message)
}

// The hash is computed with the chain's formula (MAG-5232 Expected 7, test 9): Solana's is the
// transaction's first signature. A chain with no formula here, Bitcoin, gets no Router-Tx-Hash and the
// sentence that tells the client how to find the hash.
func TestJSONRPCWrite_TxHashByChainFormula(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	t.Run("solana: the first signature", func(t *testing.T) {
		signature := make([]byte, solanaSignatureSize)
		for i := range signature {
			signature[i] = byte(i + 1)
		}
		// One signature, then a message the router does not read.
		raw := append(append([]byte{1}, signature...), []byte("message")...)
		for _, encoding := range []struct {
			name, param, config string
		}{
			{"base58, the default", base58Encode(raw), ""},
			{"base64", base64.StdEncoding.EncodeToString(raw), `,{"encoding":"base64"}`},
		} {
			t.Run(encoding.name, func(t *testing.T) {
				iface := writeIface("solana", "SOLANA", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/",
					`{"jsonrpc":"2.0","id":77,"method":"sendTransaction","params":["`+encoding.param+`"`+encoding.config+`]}`)
				read := startFailedReplyRouter(t, iface,
					answeringUpstream(t, 503, "down", "text/plain"), answeringUpstream(t, 503, "down", "text/plain"))()
				requireUnknownWrite(t, read, `77`, base58Encode(signature))
			})
		}
	})
	t.Run("evm: a blob transaction's hash leaves its sidecar out", func(t *testing.T) {
		blob := hexutil.MustDecode(rywGethVectors[5].raw)
		wrapped, err := rlp.EncodeToBytes([]interface{}{
			rlp.RawValue(blob[1:]), [][]byte{make([]byte, 64)}, [][]byte{make([]byte, 48)}, [][]byte{make([]byte, 48)},
		})
		require.NoError(t, err)
		iface := writeIface("evm blob", "ETH1", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/",
			`{"jsonrpc":"2.0","id":77,"method":"eth_sendRawTransaction","params":["`+hexutil.Encode(append([]byte{0x03}, wrapped...))+`"]}`)
		read := startFailedReplyRouter(t, iface,
			answeringUpstream(t, 503, "down", "text/plain"), answeringUpstream(t, 503, "down", "text/plain"))()
		requireUnknownWrite(t, read, `77`, rywGethVectors[5].hash)
	})
	t.Run("bitcoin: no formula, no hash", func(t *testing.T) {
		read := startFailedReplyRouter(t, bitcoinWrite,
			answeringUpstream(t, 503, "down", "text/plain"), answeringUpstream(t, 503, "down", "text/plain"))()
		requireUnknownWrite(t, read, `77`, "")
		require.Contains(t, read.body, "Look up this transaction by the hash of your signed bytes before signing anything new.")
	})
}

// Tendermint RPC gets the same replies (Part IV Appendix F), a GET included: a GET has no id, and
// its reply carries -1 (Part I R-C). Tendermint has no hash formula here.
func TestTendermintWrite_UnknownOutcomeIsTheRoutersReply(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	for _, c := range []struct {
		iface failedReplyInterface
		id    string
	}{
		{tendermintPostWrite, `77`},
		{tendermintGetWrite, `-1`},
	} {
		t.Run(c.iface.name, func(t *testing.T) {
			read := startFailedReplyRouter(t, c.iface,
				answeringUpstream(t, 503, "down", "text/plain"), answeringUpstream(t, 503, "down", "text/plain"))()
			requireUnknownWrite(t, read, c.id, "")
		})
	}
}

// What the ticket leaves as it is (MAG-5232 tests 10 and 11). A node's 2xx JSON-RPC reply is
// forwarded as today with no Router-Write-Outcome until the chain's write rows ship, and beside a 503
// main's handling stays: the client gets "nonce too low" as the node sent it. A plain success is the
// node's reply with no outcome header.
func TestJSONRPCWrite_ANodesJSONRPCReplyIsForwardedAsToday(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	const nonceTooLow = `{"jsonrpc":"2.0","id":77,"error":{"code":-32000,"message":"nonce too low"}}`
	const accepted = `{"jsonrpc":"2.0","id":77,"result":"0xabc"}`
	for _, c := range []struct {
		name       string
		upstreams  func(t *testing.T) []string
		wantStatus int
		wantBody   string
	}{
		{"nonce too low beside a 503", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 200, nonceTooLow, "application/json"), answeringUpstream(t, 503, "down", "text/plain")}
		}, http.StatusOK, nonceTooLow},
		{"a plain success", func(t *testing.T) []string {
			return []string{answeringUpstream(t, 200, accepted, "application/json"), answeringUpstream(t, 200, accepted, "application/json")}
		}, http.StatusOK, accepted},
	} {
		t.Run(c.name, func(t *testing.T) {
			read := startFailedReplyRouter(t, evmWrite, c.upstreams(t)...)()
			require.Equal(t, c.wantStatus, read.status)
			require.JSONEq(t, c.wantBody, read.body)
			require.Empty(t, read.header.Get(common.WRITE_OUTCOME_HEADER_NAME))
		})
	}
}
