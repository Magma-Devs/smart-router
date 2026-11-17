package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cachecore "github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MAG-4650: since PR #547, a call that names no block takes no part in a batch's block, so a
// Hyperliquid gas price batched with a read of a numbered block takes that block. On Hyperliquid
// every block at or below the tip is final (block_distance_for_finalized_data is 0), so the router
// wrote the batch to the cache as final data and answered the same batch from the cache for an
// hour, with the first gas price. Sent alone, eth_bigBlockGasPrice is never cached.
//
// The batch keeps its block, which routing and the archive rule read, but its message records the
// call the fold left out, and the cache never writes such a request as final. The entry lives the
// chain's short lifetime, as a block-less call beside a latest read does: on Hyperliquid 500 ms,
// the floor for an entry that is not final, since an eighth of its one-second block is less.
//
// With the record ignored at the cache write, as PR #547 left it, both batches fail here: they are
// written as final, and the same batch sent after the short lifetime is answered from the cache
// with the first gas price, without reaching the node. The two controls pass either way.

// hyperliquidTip is the chain tip the router knows. The batches read blocks just below it, inside
// the archive extension's 127 blocks, so the one provider, which serves no extension, answers them.
const hyperliquidTip = int64(0xffffff)

// The calls the cases batch, and the answers hyperliquidNode gives them.
const (
	hyperliquidGasPrice      = `{"jsonrpc":"2.0","id":1,"method":"eth_bigBlockGasPrice","params":[]}`
	hyperliquidBlockRead     = `{"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":["0xfffff0",false]}`
	hyperliquidNextBlockRead = `{"jsonrpc":"2.0","id":3,"method":"eth_getBlockByNumber","params":["0xfffff1",false]}`
	hyperliquidFirstPrice    = `{"jsonrpc":"2.0","id":1,"result":"0x3b9aca01"}`
	hyperliquidSecondPrice   = `{"jsonrpc":"2.0","id":1,"result":"0x3b9aca02"}`
	hyperliquidBlock         = `{"jsonrpc":"2.0","id":2,"result":{"number":"0xfffff0"}}`
	hyperliquidNextBlock     = `{"jsonrpc":"2.0","id":3,"result":{"number":"0xfffff1"}}`
)

func hyperliquidBatch(elements ...string) string { return "[" + strings.Join(elements, ",") + "]" }

// hyperliquidJsonRPCParser builds the router's parser from specs/hyperliquid.json, whose
// eth_bigBlockGasPrice (EMPTY block parser, deterministic false) and eth_getBlockByNumber match
// lava-specs main bda0597.
func hyperliquidJsonRPCParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "HYPERLIQUID", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	require.NoError(t, err)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	return chainParser
}

// hyperliquidNode answers as the fake node of the ticket's run does: eth_bigBlockGasPrice with a
// new gas price on every call, 0x3b9aca01 first, and eth_getBlockByNumber with the block it names.
// It counts the requests it gets.
func hyperliquidNode(requests *atomic.Int32) http.HandlerFunc {
	var gasPrices atomic.Int64
	type call struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		batch := bytes.HasPrefix(bytes.TrimSpace(body), []byte("["))
		var calls []call
		if batch {
			err = json.Unmarshal(body, &calls)
		} else {
			calls = make([]call, 1)
			err = json.Unmarshal(body, &calls[0])
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		answers := make([]string, 0, len(calls))
		for _, c := range calls {
			var result string
			switch {
			case c.Method == "eth_bigBlockGasPrice":
				result = fmt.Sprintf(`"0x%x"`, 1_000_000_000+gasPrices.Add(1))
			case c.Method == "eth_getBlockByNumber" && len(c.Params) > 0:
				result = `{"number":` + string(c.Params[0]) + `}`
			default:
				http.Error(w, "unexpected call "+c.Method, http.StatusBadRequest)
				return
			}
			answers = append(answers, `{"jsonrpc":"2.0","id":`+string(c.ID)+`,"result":`+result+`}`)
		}
		reply := answers[0]
		if batch {
			reply = "[" + strings.Join(answers, ",") + "]"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(reply))
	}
}

func TestJsonRpcGasPriceBesideAFinalBlockReadIsNotCachedAsFinal(t *testing.T) {
	rand.InitRandomSeed()
	cases := []struct {
		name string
		body string
		// requestedBlock is the block the request asks for, and blockless whether its message
		// records a call left out of that block. filedAt is where the cache holds the answer: the
		// batch's block, or the tip for a request with no block, as for a latest read.
		requestedBlock, filedAt int64
		blockless               bool
		// written is whether the answer reaches the cache at all, and final whether it is still
		// there, and answers the same request, once the short lifetime has passed.
		written, final bool
		// replies are what the client gets on the first send and on the second.
		replies [2]string
	}{
		{
			// Control: a batch whose calls all name a final block is cached as final.
			name:           "two final block reads are cached as final",
			body:           hyperliquidBatch(hyperliquidBlockRead, hyperliquidNextBlockRead),
			requestedBlock: 0xfffff1,
			filedAt:        0xfffff1,
			written:        true,
			final:          true,
			replies: [2]string{
				hyperliquidBatch(hyperliquidBlock, hyperliquidNextBlock),
				hyperliquidBatch(hyperliquidBlock, hyperliquidNextBlock),
			},
		},
		{
			// The ticket's batch.
			name:           "a gas price beside a final block read is cached as non-final",
			body:           hyperliquidBatch(hyperliquidGasPrice, hyperliquidBlockRead),
			requestedBlock: 0xfffff0,
			filedAt:        0xfffff0,
			blockless:      true,
			written:        true,
			replies: [2]string{
				hyperliquidBatch(hyperliquidFirstPrice, hyperliquidBlock),
				hyperliquidBatch(hyperliquidSecondPrice, hyperliquidBlock),
			},
		},
		{
			// The same batch with the gas price second: the fold takes a request's first element
			// apart from the others.
			name:           "a final block read beside a gas price is cached as non-final",
			body:           hyperliquidBatch(hyperliquidBlockRead, hyperliquidGasPrice),
			requestedBlock: 0xfffff0,
			filedAt:        0xfffff0,
			blockless:      true,
			written:        true,
			replies: [2]string{
				hyperliquidBatch(hyperliquidBlock, hyperliquidFirstPrice),
				hyperliquidBatch(hyperliquidBlock, hyperliquidSecondPrice),
			},
		},
		{
			// Control: the gas price alone names no block, so it is never cached.
			name:           "a gas price alone is never cached",
			body:           hyperliquidGasPrice,
			requestedBlock: spectypes.NOT_APPLICABLE,
			filedAt:        hyperliquidTip,
			blockless:      true,
			replies:        [2]string{hyperliquidFirstPrice, hyperliquidSecondPrice},
		},
	}
	chainParser := hyperliquidJsonRPCParser(t)
	// The lifetime the cache gives an entry that is not final, on this chain.
	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()
	shortLifetime := cachecore.DefaultPolicy().ForChain(averageBlockTime)
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary, rcs := startCacheServerWithNonFinalizedTTLForTest(t, cachecore.DefaultExpirationForNonFinalized)
			var upstreamCalls atomic.Int32
			server := newRelayEndServer(t, "HYPERLIQUID", spectypes.APIInterfaceJsonRPC, chainParser, relayEndNode{"hyperliquid-node", hyperliquidNode(&upstreamCalls)})
			server.cache = primary
			// A long staleness window keeps the tip fresh for the whole case. With the tip known,
			// a block at or below it is final, so a batch filed under its block can live an hour.
			withTip := chainstate.New("HYPERLIQUID", chainstate.DefaultConfig(12*time.Second))
			withTip.SetLatestBlock(hyperliquidTip)
			server.chainState = withTip

			// The key the router computes for this request: ParseRelay is the parse SendRelay runs.
			parsed, err := server.ParseRelay(context.Background(), "", tc.body, http.MethodPost, "dapp", "1.2.3.4", nil)
			require.NoError(t, err)
			// Not fatal: the cache checks below must run and fail on their own.
			requested, _ := parsed.RequestedBlock()
			assert.Equal(t, tc.requestedBlock, requested, "the block the request asks for")
			assert.Equal(t, tc.blockless, parsed.HasBlocklessElement(), "the message records a call left out of its block")
			key, _, err := parsed.HashCacheRequest("HYPERLIQUID")
			require.NoError(t, err)
			written := func() bool {
				return directGetOn(rcs, "HYPERLIQUID", key, tc.filedAt, hyperliquidTip).GetReply() != nil
			}

			send := func(guid uint64, want string) *common.RelayResult {
				t.Helper()
				result, err := sendRelayEnd(t, server, guid, relayEndShortWait, "", tc.body)
				require.NoError(t, err)
				require.NotNil(t, result.Reply)
				// Not fatal either: an old gas price is the ticket's symptom, and the checks after
				// it say where the answer came from.
				assert.JSONEq(t, want, string(result.Reply.Data))
				return result
			}
			guid := uint64(4_650_000 + 10*i)

			first := send(guid, tc.replies[0])
			require.Equal(t, common.CacheTierNone, first.CacheLookup.ServedTier, "nothing is cached before the first send")
			require.Equal(t, "hyperliquid-node", first.ProviderInfo.ProviderAddress)
			require.EqualValues(t, 1, upstreamCalls.Load())
			if tc.written {
				require.Eventually(t, written, 3*time.Second, 20*time.Millisecond, "the answer is written to the cache under its block")
			}

			// The ticket sends the batch again 10 seconds later. Past the short lifetime is enough:
			// an entry that is not final is gone by then, and a final one stays.
			time.Sleep(3 * shortLifetime)
			assert.Equal(t, tc.final, written(), "the answer is still cached once the short lifetime has passed")

			second := send(guid+1, tc.replies[1])
			if tc.final {
				require.Equal(t, common.CacheTierPrimary, second.CacheLookup.ServedTier, "the same batch sent again is answered from the cache")
				require.EqualValues(t, 1, upstreamCalls.Load(), "the second send does not reach the node")
			} else {
				require.Equal(t, common.CacheTierNone, second.CacheLookup.ServedTier, "the same request sent again is not answered from the cache")
				require.Equal(t, "hyperliquid-node", second.ProviderInfo.ProviderAddress)
				require.EqualValues(t, 2, upstreamCalls.Load(), "every send reaches the node")
			}
		})
	}
}

// TestCacheWriteOfARequestWithABlocklessElementIsNeverFinal is MAG-4650 at the write site: a
// request whose message records an element left out of its block is written as non-final, even
// when its block is final. The backend keeps what the router hands it, so the test reads the
// decision itself rather than a lifetime. Sui's chainIdentifier beside a numbered checkpoint read
// is the GraphQL shape of the ticket's batch.
func TestCacheWriteOfARequestWithABlocklessElementIsNeverFinal(t *testing.T) {
	const (
		suiCheckpoint      = int64(334_287_000)
		suiCheckpointRead  = `{"query":"{ checkpoint(sequenceNumber: 334287000) { digest } }"}`
		suiBesideChainID   = `{"query":"{ chainIdentifier checkpoint(sequenceNumber: 334287000) { digest } }"}`
		suiCheckpointReply = `{"data":{"checkpoint":{"digest":"4btiuTfgHRbVTjXdoUMfqc5hr4Tvf4jHjWYcMbdg6xkF"}}}`
		suiBesideReply     = `{"data":{"chainIdentifier":"35834a8a","checkpoint":{"digest":"4btiuTfgHRbVTjXdoUMfqc5hr4Tvf4jHjWYcMbdg6xkF"}}}`
	)
	hyperliquid := hyperliquidJsonRPCParser(t)
	sui := newSuiBlocklessCacheParser(t)
	type chain struct {
		id, apiInterface string
		parser           chainlib.ChainParser
		tip              int64
	}
	hyperliquidChain := chain{"HYPERLIQUID", spectypes.APIInterfaceJsonRPC, hyperliquid, hyperliquidTip}
	suiChain := chain{"SUI", spectypes.APIInterfaceGraphQL, sui, suiCacheTestTip}
	cases := []struct {
		name        string
		chain       chain
		body, reply string
		block       int64
		final       bool
	}{
		{
			name:  "two final block reads are written as final",
			chain: hyperliquidChain,
			body:  hyperliquidBatch(hyperliquidBlockRead, hyperliquidNextBlockRead),
			reply: hyperliquidBatch(hyperliquidBlock, hyperliquidNextBlock),
			block: 0xfffff1,
			final: true,
		},
		{
			name:  "a gas price beside a final block read is written as non-final",
			chain: hyperliquidChain,
			body:  hyperliquidBatch(hyperliquidGasPrice, hyperliquidBlockRead),
			reply: hyperliquidBatch(hyperliquidFirstPrice, hyperliquidBlock),
			block: 0xfffff0,
		},
		{
			name:  "a final block read beside a gas price is written as non-final",
			chain: hyperliquidChain,
			body:  hyperliquidBatch(hyperliquidBlockRead, hyperliquidGasPrice),
			reply: hyperliquidBatch(hyperliquidBlock, hyperliquidFirstPrice),
			block: 0xfffff0,
		},
		{
			name:  "a numbered checkpoint read is written as final",
			chain: suiChain,
			body:  suiCheckpointRead,
			reply: suiCheckpointReply,
			block: suiCheckpoint,
			final: true,
		},
		{
			name:  "chainIdentifier beside a numbered checkpoint read is written as non-final",
			chain: suiChain,
			body:  suiBesideChainID,
			reply: suiBesideReply,
			block: suiCheckpoint,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &recordingCacheBackend{}
			rpcss := &RPCSmartRouterServer{
				cache:          backend,
				chainParser:    tc.chain.parser,
				listenEndpoint: &routersession.RPCEndpoint{ChainID: tc.chain.id, ApiInterface: tc.chain.apiInterface},
			}
			withTip := chainstate.New(tc.chain.id, chainstate.DefaultConfig(12*time.Second))
			withTip.SetLatestBlock(tc.chain.tip)
			rpcss.chainState = withTip

			msg, err := rpcss.ParseRelay(context.Background(), "", tc.body, http.MethodPost, "dapp", "1.2.3.4", nil)
			require.NoError(t, err)
			rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: []byte(tc.reply), LatestBlock: tc.chain.tip},
				StatusCode: http.StatusOK,
			})
			require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 10*time.Millisecond, "the answer is written to the cache")
			set := backend.recorded()[0]
			require.Equal(t, tc.block, set.RequestedBlock, "under the block the request asks for")
			require.Equal(t, tc.final, set.Finalized, "written as final")
		})
	}
}
