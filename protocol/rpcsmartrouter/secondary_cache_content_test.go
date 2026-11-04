package rpcsmartrouter

import (
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// The REST harness above cannot exercise a content check, since a REST body carries no
// envelope; these tests build the same scene on Ethereum JSON-RPC, with the real spec, out
// of the shared ETH helpers in cache_test_helpers_test.go.
func newSecondaryEthTestServer(chainParser chainlib.ChainParser, primary *performance.Cache, secondary performance.CacheReader) *RPCSmartRouterServer {
	rpcss := ethCacheTestServer(chainParser, primary)
	rpcss.secondaryCache = secondary
	rpcss.secondaryCacheTimeout = 100 * time.Millisecond
	return rpcss
}

const secondaryEthRequest = `{"jsonrpc":"2.0","id":7,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`

// MAG-3597, symptom 1: the entry's contents decide its kind. A zone whose build predates
// the label writes an error body with no label and no placeholder; it must reach the
// caller as the node error it is, header and all, and never be backfilled as a success.
// The success control shows the same path serving a good entry, id restored.
func TestSecondaryEntryKindComesFromItsContents(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	protocolMessage := ethProtocolMessage(t, chainParser, secondaryEthRequest, 100)
	hashKey, _, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)

	t.Run("an error body with neither signal is served as a node error and never backfilled", func(t *testing.T) {
		fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}`), LatestBlock: 100},
			SeenBlock:  100,
			StatusCode: http.StatusOK,
		}}
		rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
		served, result, report := runSecondaryLookupOn(t, rpcss, "ETH1", protocolMessage, 100)
		require.True(t, served, "a well-formed error is served, exactly like a labelled one")
		require.True(t, result.IsNodeError, "the header comes from the contents when the label is missing")
		require.Equal(t, metrics.CacheOutcomeHit, report.SecondaryOutcome)
		require.Equal(t, `7`, gjson.GetBytes(result.Reply.Data, "id").Raw, "the caller's id is restored on the error too")

		time.Sleep(700 * time.Millisecond)
		require.Nil(t, directGetOn(rcs, "ETH1", hashKey, 100, 100).GetReply(), "an error is never written into the primary as a success")
	})

	t.Run("a success body is served as a success, id restored", func(t *testing.T) {
		fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`), LatestBlock: 100},
			SeenBlock:  100,
			StatusCode: http.StatusOK,
		}}
		rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
		served, result, _ := runSecondaryLookupOn(t, rpcss, "ETH1", protocolMessage, 100)
		require.True(t, served)
		require.False(t, result.IsNodeError)
		require.Equal(t, `7`, gjson.GetBytes(result.Reply.Data, "id").Raw)
		require.Equal(t, `"0x64"`, gjson.GetBytes(result.Reply.Data, "result").Raw)
	})
}

// MAG-3597, symptom 2 and the empty-entry case: an entry that is not a reply at all is
// never served, whatever label it carries. It is dropped as a secondary error, nothing
// is composed from its bytes, and nothing reaches the primary.
func TestSecondaryEntryThatIsNotAReplyIsNotServed(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	protocolMessage := ethProtocolMessage(t, chainParser, secondaryEthRequest, 100)
	hashKey, _, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)

	cases := []struct {
		name string
		data string
	}{
		{"a web page", `<html>502 Bad Gateway</html>`},
		{"a truncated envelope", `{"jsonrpc":"2.0","resu`},
		{"no bytes at all", ``},
		{"a bare value", `"0x64"`},
		{"an empty object", `{}`},
		{"an envelope with no payload", `{"jsonrpc":"2.0","id":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
				Reply:      &pairingtypes.RelayReply{Data: []byte(tc.data), LatestBlock: 100},
				SeenBlock:  100,
				StatusCode: http.StatusOK,
			}}
			rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
			served, _, report := runSecondaryLookupOn(t, rpcss, "ETH1", protocolMessage, 100)
			require.False(t, served, "not a reply, so not served as one")
			require.Equal(t, metrics.CacheOutcomeError, report.SecondaryOutcome, "recorded as an error, not a miss: the tier answered, this router refused it")

			time.Sleep(300 * time.Millisecond)
			require.Nil(t, directGetOn(rcs, "ETH1", hashKey, 100, 100).GetReply(), "nothing reaches the primary")
		})
	}
}

// A batch is judged element by element. One sibling that answers does not vouch for one
// that answers nothing: such a batch used to be served as a success and backfilled into
// the primary, because the batch classifier let the good element mask the empty one
// (Codex review of #412).
func TestSecondaryBatchWithAnElementThatAnswersNothingIsNotServed(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	protocolMessage := ethProtocolMessage(t, chainParser, `[{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]},{"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":["0x2222222222222222222222222222222222222222","0x64"]}]`, 100)
	hashKey, _, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)
	fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x64"},{"jsonrpc":"2.0","id":2}]`), LatestBlock: 100},
		SeenBlock:  100,
		StatusCode: http.StatusOK,
	}}
	rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
	served, _, report := runSecondaryLookupOn(t, rpcss, "ETH1", protocolMessage, 100)
	require.False(t, served, "an element with no payload makes the batch unusable")
	require.Equal(t, metrics.CacheOutcomeError, report.SecondaryOutcome)

	time.Sleep(300 * time.Millisecond)
	require.Nil(t, directGetOn(rcs, "ETH1", hashKey, 100, 100).GetReply(), "nothing reaches the primary")
}
