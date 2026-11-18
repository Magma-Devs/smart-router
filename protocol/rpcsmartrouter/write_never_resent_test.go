package rpcsmartrouter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// rateLimitedWrite is how a vendor refuses a write for rate: a 429 with its own body.
var rateLimitedWrite = writeUpstream{status: http.StatusTooManyRequests, body: `{"code":-32007,"message":"50/second request limit reached"}`}

// requireNoSendAfterTheBroadcast waits long enough for a re-send to land on a backup, and fails if
// one does.
func requireNoSendAfterTheBroadcast(t *testing.T, backupHits *int64) {
	t.Helper()
	require.Never(t, func() bool { return atomic.LoadInt64(backupHits) > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"the write was sent again, to the backup, after the broadcast")
}

// A write is sent once to each provider, all at once, and never again (MAG-5232 Expected 3, test 7;
// Part I R-L): not when every copy answered 429, on any interface. A 429 from a gateway does not
// prove the node did not run the transaction, and a second send can come back as a refusal of bytes
// that already ran. The re-send main allowed went to a provider not yet used, here the backup, which
// is the tier a write reaches when no primary can be selected and one the broadcast did not reach.
func TestWrite_NeverResentWhenEveryCopyIsRateLimited(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range writeInterfaceCases {
		for _, primaries := range []int{1, 2} {
			t.Run(tc.name+"/"+[]string{"", "one primary", "two primaries"}[primaries], func(t *testing.T) {
				chainParser := tc.parser(t)
				upstreams := make([]writeUpstream, primaries)
				for i := range upstreams {
					upstreams[i] = rateLimitedWrite
					upstreams[i].name = string(rune('a'+i)) + "-" + t.Name()
				}
				backup := writeUpstream{name: "backup-" + t.Name(), status: http.StatusOK, body: tc.success}
				hits := make([]int64, primaries+1)
				server := newWriteServer(t, tc, chainParser, upstreams, []writeUpstream{backup}, hits)

				_, err := sendWrite(t, tc, chainParser, server)
				require.Error(t, err, "no copy was accepted")
				for i := 0; i < primaries; i++ {
					require.Equal(t, int64(1), atomic.LoadInt64(&hits[i]), "each primary got the broadcast once")
				}
				requireNoSendAfterTheBroadcast(t, &hits[primaries])
			})
		}
	}
}

// jsonRPCBatchWithAWrite is a batch that carries a write: the batch is stateful as a whole.
func jsonRPCBatchWithAWrite(t *testing.T) writeInterfaceCase {
	t.Helper()
	tc := writeInterfaceCases[0]
	require.Equal(t, spectypes.APIInterfaceJsonRPC, tc.apiInterface, "setup: the first write case is JSON-RPC")
	tc.name = "jsonrpc batch"
	tc.body = `[{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1},{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x01"],"id":2}]`
	tc.success = `[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0xabc"}]`
	return tc
}

// A batch that holds a write is never sent again either (MAG-5232 Expected 3, test 17; MAG-5061
// item 4): not after every copy answered 429, and not after every provider's port refused the
// connection. On main the policy's rate-limit carve-out sent such a batch again when the whole batch
// was refused for rate at the transport.
func TestWrite_BatchHoldingAWriteIsNeverResent(t *testing.T) {
	rand.InitRandomSeed()
	for _, c := range []struct {
		name     string
		upstream writeUpstream
	}{
		{"every copy answered 429", rateLimitedWrite},
		{"every port refused the connection", writeUpstream{refuse: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tc := jsonRPCBatchWithAWrite(t)
			chainParser := tc.parser(t)
			a, b := c.upstream, c.upstream
			a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
			backup := writeUpstream{name: "backup-" + t.Name(), status: http.StatusOK, body: tc.success}
			hits := make([]int64, 3)
			server := newWriteServer(t, tc, chainParser, []writeUpstream{a, b}, []writeUpstream{backup}, hits)

			_, err := sendWrite(t, tc, chainParser, server)
			require.Error(t, err, "no copy was accepted")
			if !c.upstream.refuse {
				require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "each primary got the batch once")
				require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "each primary got the batch once")
			}
			requireNoSendAfterTheBroadcast(t, &hits[2])
		})
	}
}

// A write ignores lava-relay-timeout (MAG-5232 Expected 9, test 14; Part I): the write's window is
// its own, whatever the caller asks. A shorter value can only cut off a copy that may already have
// broadcast, turning a write that landed into an "unknown", and a longer one (under
// --max-caller-relay-timeout) held the request past the write's budget. On main the header set the
// window both ways, and Lava-Relay-Timeout-Applied reported the caller's value.
func TestWrite_IgnoresTheCallersRelayTimeout(t *testing.T) {
	rand.InitRandomSeed()
	tc := writeInterfaceCases[0]
	chainParser := tc.parser(t)
	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()
	parse := func(t *testing.T, override time.Duration) chainlib.ChainMessage {
		t.Helper()
		chainMessage, err := chainParser.ParseMsg(tc.path, []byte(tc.body), tc.method, nil, extensionslib.ExtensionInfo{})
		require.NoError(t, err)
		if override > 0 {
			chainMessage.TimeoutOverride(override)
		}
		return chainMessage
	}
	ownWindow := chainlib.GetRelayTimeout(parse(t, 0), averageBlockTime)

	origMax := common.MaxCallerRelayTimeout
	t.Cleanup(func() { common.MaxCallerRelayTimeout = origMax })
	common.MaxCallerRelayTimeout = 10 * time.Minute
	for _, override := range []time.Duration{time.Second, 5 * time.Minute} {
		require.Equal(t, ownWindow, chainlib.GetRelayTimeout(parse(t, override), averageBlockTime),
			"a write's window is its own with lava-relay-timeout: %s", override)
	}

	// End to end: a copy slower than the caller's 1 s is answered, and the reply names the window
	// the write used.
	const callerTimeout = time.Second
	delay := callerTimeout + 500*time.Millisecond
	slow := writeUpstream{name: "slow-" + t.Name(), status: http.StatusOK, body: tc.success, delay: delay}
	hits := make([]int64, 1)
	server := newWriteServer(t, tc, chainParser, []writeUpstream{slow}, nil, hits)

	result, err := sendWriteWithDirectives(t, tc, chainParser, server, map[string]string{common.RELAY_TIMEOUT_HEADER_NAME: callerTimeout.String()})
	require.NoError(t, err, "the copy answered inside the write's budget: its answer is the result")
	require.JSONEq(t, tc.success, string(result.Reply.Data))
	require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]))
	applied, _ := replyHeader(result.Reply.GetMetadata(), common.RELAY_TIMEOUT_APPLIED_HEADER_NAME)
	require.Equal(t, ownWindow.String(), applied, "the reply names the window the write used, not the caller's")
}

// The caller's lava-relay-timeout still sets a read's window.
func TestRead_KeepsTheCallersRelayTimeout(t *testing.T) {
	tc := writeInterfaceCases[0]
	chainParser := tc.parser(t)
	chainMessage, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	chainMessage.TimeoutOverride(time.Second)
	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()
	require.Equal(t, time.Second, chainlib.GetRelayTimeout(chainMessage, averageBlockTime))
}

// When every provider failed, the client gets the answer of the best kind, not the first to arrive
// (MAG-5232 Expected 8, test 13; MAG-5061 item 2): one provider answers 403 at once, the other 429
// after 300 ms, and the client gets the 429, on a write and on a read. A 403 is about the router's
// key; a rate limit passes on its own. On main the 403, a node error, beat the 429.
func TestEveryProviderFailed_A429BeatsA403(t *testing.T) {
	rand.InitRandomSeed()
	const refusal = `{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"message":"key disabled"}}`
	const throttle = `{"code":-32007,"message":"50/second request limit reached"}`
	tc := writeInterfaceCases[0]
	read := tc
	read.body = `{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`

	for _, c := range []struct {
		name string
		send func(t *testing.T, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error)
	}{
		{"write", func(t *testing.T, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
			return sendWrite(t, tc, chainParser, server)
		}},
		{"read", func(t *testing.T, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
			return sendRead(t, read, chainParser, server)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			refused := writeUpstream{name: "refused-" + t.Name(), status: http.StatusForbidden, body: refusal}
			throttled := writeUpstream{name: "throttled-" + t.Name(), status: http.StatusTooManyRequests, body: throttle, delay: 300 * time.Millisecond}
			hits := make([]int64, 2)
			server := newWriteServer(t, tc, chainParser, []writeUpstream{refused, throttled}, nil, hits)

			result, _ := c.send(t, chainParser, server)
			require.NotNil(t, result)
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "setup: both providers were asked")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "setup: both providers were asked")
			require.Equal(t, http.StatusTooManyRequests, result.StatusCode)
			cf := result.GetClientFacing()
			require.NotNil(t, cf, "the 429 is forwarded as the upstream sent it")
			require.JSONEq(t, throttle, string(cf.UpstreamBody))
		})
	}
}

// sendRead sends tc's request, a read, through SendParsedRelay.
func sendRead(t *testing.T, tc writeInterfaceCase, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
	t.Helper()
	ctx := context.Background()
	chainMessage, err := chainParser.ParseMsg(tc.path, []byte(tc.body), tc.method, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(ctx, tc.method, tc.path, []byte(tc.body), 0, spectypes.NOT_APPLICABLE, tc.apiInterface,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")
	require.Equal(t, uint32(common.NO_STATE), chainlib.GetStateful(protocolMessage), "setup: the request must be a read")
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.SendParsedRelay(callCtx, nil, protocolMessage)
}
