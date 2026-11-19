package rpcsmartrouter

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BroadcastTx replies as cosmos-sdk v0.53.5's own types (types/tx.BroadcastTxResponse,
// types.TxResponse) marshal them, so the field numbers under test are the SDK's, not this package's.
const (
	// Code 5 in codespace sdk, with a raw_log, gas_wanted and gas_used.
	relayEndCosmosGRPCRefused = "0a9a0112403439423936453039343533453835313337453330393635373731453138414238373334394145463335364545363342413230304534343938353741374641303" +
		"81a0373646b200532477370656e6461626c652062616c616e63652031307561746f6d20697320736d616c6c6572207468616e20313030307561746f6d3a20696e73756666696369656e742066756e647348c09a0c50a29003"
	// No code on the wire, which is 0; raw_log "[]" and gas_wanted.
	relayEndCosmosGRPCAccepted = "0a4a1240343942393645303934353345383531333745333039363537373145313841423837333439414546333536454536334241323030453434393835374137464130383202" +
		"5b5d48c09a0c"
	relayEndConcordiumHash = "8e7a2d9b0a3c4f5e6d7c8b9a0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c"
)

func relayEndHexBytes(t *testing.T, text string) []byte {
	t.Helper()
	b, err := hex.DecodeString(text)
	require.NoError(t, err)
	return b
}

// relayEndConcordiumReply is a TransactionHash: value, field 1, the hash's bytes.
func relayEndConcordiumReply(t *testing.T, hash []byte) []byte {
	t.Helper()
	return append([]byte{0x0a, byte(len(hash))}, hash...)
}

func TestRelayEnd_GRPCSubmitReplies(t *testing.T) {
	t.Run("BroadcastTx", func(t *testing.T) {
		tx, found := rpcInterfaceMessages.ReadCosmosBroadcastTx(relayEndHexBytes(t, relayEndCosmosGRPCRefused))
		require.True(t, found)
		require.Equal(t, rpcInterfaceMessages.CosmosBroadcastTx{Hash: relayEndCosmosHash, Codespace: "sdk", Code: 5, RawLog: "spendable balance 10uatom is smaller than 1000uatom: insufficient funds"}, tx)

		tx, found = rpcInterfaceMessages.ReadCosmosBroadcastTx(relayEndHexBytes(t, relayEndCosmosGRPCAccepted))
		require.True(t, found, "a code of 0 is not on the wire")
		require.Equal(t, rpcInterfaceMessages.CosmosBroadcastTx{Hash: relayEndCosmosHash, RawLog: "[]"}, tx)

		tx, found = rpcInterfaceMessages.ReadCosmosBroadcastTx([]byte{0x0a, 0x00})
		require.True(t, found, "an empty tx_response still names code 0")
		require.Empty(t, tx.Hash)

		for name, reply := range map[string][]byte{
			"a node error, the router's JSON":  []byte(`{"error_message":"signature verification failed","error_code":3}`),
			"no tx_response":                   {0x10, 0x01},
			"cut short":                        relayEndHexBytes(t, relayEndCosmosGRPCRefused)[:40],
			"tx_response that does not decode": {0x0a, 0x02, 0x12, 0x40},
		} {
			_, found := rpcInterfaceMessages.ReadCosmosBroadcastTx(reply)
			require.False(t, found, name)
		}
	})
	t.Run("SendBlockItem", func(t *testing.T) {
		hash := relayEndHexBytes(t, relayEndConcordiumHash)
		require.Equal(t, relayEndConcordiumHash, concordiumTransactionHash(relayEndConcordiumReply(t, hash)))
		require.Empty(t, concordiumTransactionHash(relayEndConcordiumReply(t, hash[:31])), "not a hash's length")
		require.Empty(t, concordiumTransactionHash([]byte(`{"error_message":"duplicate","error_code":6}`)))
	})
}

// gRPC submits through the listener, to a node whose reflection service never answers: the reply is
// read with no descriptor. The node receives the client's request once, as the client sent it.
func TestRelayEnd_GRPCWrites(t *testing.T) {
	read := captureRelayEnds(t)
	var (
		mu       sync.Mutex
		answer   func() ([]byte, error)
		received [][]byte
	)
	node := startStalledReflectionNodeAnswering(t, func(_ string, body []byte) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, body)
		return answer()
	})
	api := func(name string, stateful int) string {
		return fmt.Sprintf(`{"name": %q, "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": %d}}`, name, stateful)
	}
	client := startRelayEndGRPCRouter(t, strings.Join([]string{
		api(cosmosGRPCBroadcastMethod, 1),
		api("cosmos.tx.v1beta1.Service/Simulate", 0),
		api(concordiumGRPCSendMethod, 1),
	}, ","), node)

	refused := relayEndHexBytes(t, relayEndCosmosGRPCRefused)
	for _, tc := range []struct {
		name, method string
		reply        []byte
		nodeErr      error
		wantCode     codes.Code
	}{
		{name: "grpc_broadcast_tx_refused", method: cosmosGRPCBroadcastMethod, reply: refused},
		{name: "grpc_broadcast_tx_invalid_argument", method: cosmosGRPCBroadcastMethod, nodeErr: status.Error(codes.InvalidArgument, "signature verification failed; please verify account number (5), sequence (12) and chain-id (cosmoshub-4): unauthorized"), wantCode: codes.InvalidArgument},
		{name: "grpc_send_block_item", method: concordiumGRPCSendMethod, reply: relayEndConcordiumReply(t, relayEndHexBytes(t, relayEndConcordiumHash))},
		// A read whose reply has the same shape is not read.
		{name: "grpc_simulate_not_a_write", method: "cosmos.tx.v1beta1.Service/Simulate", reply: refused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := []byte(tc.name)
			mu.Lock()
			answer, received = func() ([]byte, error) { return tc.reply, tc.nodeErr }, nil
			mu.Unlock()
			seen := len(read())

			callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer callCancel()
			var reply []byte
			err := client.Invoke(callCtx, "/"+tc.method, request, &reply, grpc.ForceCodec(stalledReflectionCodec{}))
			if tc.nodeErr != nil {
				require.Equal(t, tc.wantCode, status.Code(err), "the node's status reached the client")
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.reply, reply, "the node's reply reached the client as it sent it")
			}
			mu.Lock()
			require.Equal(t, [][]byte{request}, received, "the node got the request once, byte for byte")
			mu.Unlock()

			events := newRelayEndsSince(read, seen)
			require.Len(t, events, 1, "one relay_end per gRPC call")
			event := normalizeRelayEnd(t, events[0], false)
			event["GUID"] = "<minted by the listener>"
			requireRelayEndGolden(t, tc.name, event)
		})
	}
}
