package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	zerolog "github.com/rs/zerolog"
)

// relayEndEventJSON is the event as the log line carries it.
func relayEndEventJSON(t *testing.T, end *relayEnd) map[string]any {
	t.Helper()
	var line bytes.Buffer
	logger := zerolog.New(&line)
	logger.Info().EmbedObject(end).Msg(relayEndEvent)
	var event map[string]any
	require.NoError(t, json.Unmarshal(line.Bytes(), &event), line.String())
	return event
}

// requested_block is a block the request names: never one of the spec's negative sentinels, and
// absent when the api names no block.
func TestRelayEnd_RequestedBlock(t *testing.T) {
	eth := ethJsonRPCParser(t)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth)
	for _, tc := range []struct {
		name, body string
		want       any // nil: absent
	}{
		{"a block by number", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x64",false]}`, float64(100)},
		{"the genesis block", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x0",false]}`, float64(0)},
		{"latest", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`, nil},
		{"earliest", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["earliest",false]}`, nil},
		{"pending", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["pending",false]}`, nil},
		{"safe", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["safe",false]}`, nil},
		{"finalized", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["finalized",false]}`, nil},
		{"a range", `{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x10","toBlock":"0x20"}]}`, float64(32)},
		{"a range up to a tag", `{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x10","toBlock":"latest"}]}`, nil},
		{"an api that names no block", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`, nil},
	} {
		end := server.newRelayEnd(context.Background(), ethProtocolMessage(t, eth, tc.body, 0), time.Now(), nil)
		require.Equal(t, tc.want, relayEndEventJSON(t, end)["requested_block"], tc.name)
	}
	unparsed := server.newRelayEnd(context.Background(), nil, time.Now(), errors.New("invalid character"))
	require.NotContains(t, relayEndEventJSON(t, unparsed), "requested_block", "a request that did not parse")
}

// request_bytes and reply_bytes measure the request the router relays and the reply the client gets;
// seen_block is the tip the router stamped on the request when it parsed it.
func TestRelayEnd_SizesAndSeenBlock(t *testing.T) {
	eth := ethJsonRPCParser(t)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth)
	build := func(seenBlock int64, result *common.RelayResult, err error) map[string]any {
		in := relayEndInput{protocolMessage: ethProtocolMessage(t, eth, relayEndRead, seenBlock), result: result, err: err, cause: err, start: time.Now()}
		return relayEndEventJSON(t, server.buildRelayEnd(utils.WithUniqueIdentifier(context.Background(), 1), in))
	}
	served := &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(relayEndBalance)}, StatusCode: http.StatusOK}

	event := build(19_000_000, served, nil)
	require.Equal(t, float64(len(relayEndRead)), event["request_bytes"])
	require.Equal(t, float64(len(relayEndBalance)), event["reply_bytes"])
	require.Equal(t, float64(19_000_000), event["seen_block"])

	event = build(0, nil, errors.New("failed relay, insufficient results"))
	require.NotContains(t, event, "reply_bytes", "the client got an error, not a reply")
	require.NotContains(t, event, "seen_block", "the router knew no tip")
	require.Equal(t, float64(len(relayEndRead)), event["request_bytes"], "relayed or not, the request had a body")

	empty := &common.RelayResult{Reply: &pairingtypes.RelayReply{}, StatusCode: http.StatusOK}
	require.Equal(t, float64(0), build(0, empty, nil)["reply_bytes"], "an empty reply is a reply")
}

// A node error that is not a JSON-RPC error has no code to name: REST's error body is the node's
// own, whatever fields it carries.
func TestRelayEnd_NodeCodeOnlyFromAJSONRPCError(t *testing.T) {
	rand.InitRandomSeed()
	read := captureRelayEnds(t)
	server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, relayEndSpecParser(t, "COSMOSHUB", spectypes.APIInterfaceRest),
		relayEndNode{"node-a", relayEndReply(http.StatusBadRequest, `{"code":3,"message":"decoding bech32 failed: invalid checksum","details":[]}`, 0)})
	guid := relayEndCaseGUID("rest_node_error")
	ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), guid), relayEndShortWait)
	defer cancel()
	_, _ = server.SendRelay(ctx, "/cosmos/bank/v1beta1/balances/cosmos1demoacme", "", http.MethodGet, relayEndNoProjectID, "", nil, nil)
	event := normalizeRelayEnd(t, onlyRelayEnd(t, read(), guid), false)
	require.Equal(t, relayOutcomeNodeError, event["outcome"], "the case under test: a node error")
	requireRelayEndGolden(t, "rest_node_error", event)
}
