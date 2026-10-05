package chainlib

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// MAG-3597: the formatter the subscription loop runs on every frame it pushes restores
// the caller's id on a reply and leaves a streamed notification alone. The loop used to
// stamp the subscribe request's id onto every notification; a JSON-RPC 2.0 notification
// carries none, and this pins the wire as it is now, in both directions.
func TestSubscriptionReplyFormatterLeavesNotificationsWithoutAnId(t *testing.T) {
	outputFormatter := subscriptionReplyFormatter(spectypes.APIInterfaceJsonRPC,
		[]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_subscribe","params":["newHeads"]}`))

	reply := outputFormatter([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xcd0c3e8af590364c09d0fa6a1210faf5"}`))
	require.JSONEq(t, `{"jsonrpc":"2.0","id":7,"result":"0xcd0c3e8af590364c09d0fa6a1210faf5"}`, string(reply),
		"the subscribe reply gets the caller's id back")

	notification := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xcd0c3e8af590364c09d0fa6a1210faf5","result":{"number":"0x1b4","hash":"0x1"}}}`
	pushed := outputFormatter([]byte(notification))
	require.Equal(t, notification, string(pushed), "a notification is pushed byte for byte")
	require.False(t, gjson.GetBytes(pushed, "id").Exists(), "no id is manufactured on a notification")
	require.Equal(t, `"0xcd0c3e8af590364c09d0fa6a1210faf5"`, gjson.GetBytes(pushed, "params.subscription").Raw,
		"params.subscription, what a client keys pushes on, is untouched")

	ack := outputFormatter([]byte(`{"jsonrpc":"2.0","id":1,"result":true}`))
	require.JSONEq(t, `{"jsonrpc":"2.0","id":7,"result":true}`, string(ack), "an acknowledgement is a reply and gets the id back")

	errorReply := outputFormatter([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`))
	require.JSONEq(t, `{"jsonrpc":"2.0","id":7,"error":{"code":-32601,"message":"method not found"}}`, string(errorReply),
		"an error reply is a reply and gets the id back")
}
