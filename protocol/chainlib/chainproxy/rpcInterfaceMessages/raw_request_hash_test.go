package rpcInterfaceMessages

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// TestGetRawRequestHashGolden pins every message type's request hash, so that a change to how a
// type composes its identity, body and headers shows up here rather than as a cache that stops
// hitting.
func TestGetRawRequestHashGolden(t *testing.T) {
	base := chainproxy.BaseMessage{Headers: []pairingtypes.Metadata{{Name: "x-api-key", Value: "k"}}}
	jsonrpc := JsonrpcMessage{Method: "eth_getBalance", Params: []interface{}{"0xabc", "latest"}, BaseMessage: base}
	cases := []struct {
		name string
		msg  interface{ GetRawRequestHash() ([]byte, error) }
		want string
	}{
		{"jsonrpc", &jsonrpc, "99c79685384d11775080834208d6966e7f031607d9d1033992b849f51fd16d56"},
		{"tendermintrpc", &TendermintrpcMessage{JsonrpcMessage: jsonrpc, Path: "/status"}, "dbf0df9249a9d040f5f01195162ade9a68cb90433dd91c91af2715d6d52e2430"},
		{"rest", &RestMessage{Path: "/cosmos/bank/v1beta1/balances/x", Msg: []byte(`{"a":1}`), BaseMessage: base}, "893fd7e3ecbfa07f52e8eb1f33c3b4f25faaa4b4aaa9feb308ed3fe54f5c3421"},
		{"grpc", &GrpcMessage{Path: "cosmos.bank.v1beta1.Query/Balance", Msg: []byte(`{"address":"x"}`), BaseMessage: base}, "c5bb928310b81c80578695a97c454e5b887c7b379443b69a4ac5bc1865530dea"},
		{"graphql", &GraphQLMessage{Msg: []byte(`{"query":"{ block { number } }"}`), RootFields: []GraphQLRootField{{Name: "block"}}, BaseMessage: base}, "e433279d3a9f11486747ae70af55dd0585852eedc5d1e2dc9d7bbb8846b06bdd"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.msg.GetRawRequestHash()
			require.NoError(t, err)
			require.Equal(t, tc.want, hex.EncodeToString(got))
		})
	}
}
