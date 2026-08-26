package rpcclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMessageRawWithID(t *testing.T) {
	c := &Client{}
	msg := c.newMessageRawWithID("eth_getBalance", json.RawMessage(`7`), json.RawMessage(`["0xabc", "latest"]`))
	require.Equal(t, `7`, string(msg.ID))
	require.Equal(t, "eth_getBalance", msg.Method)
	require.Equal(t, `["0xabc", "latest"]`, string(msg.Params))

	body, err := encodeRequestBody(msg)
	require.NoError(t, err)
	require.Equal(t, `{"jsonrpc":"2.0","id":7,"method":"eth_getBalance","params":["0xabc", "latest"]}`, string(body))

	// nil id gets a client id; empty params are omitted.
	msg = c.newMessageRawWithID("eth_chainId", nil, nil)
	require.NotEmpty(t, msg.ID)
	require.Nil(t, msg.Params)
}

func TestNewBatchElementWithId_AcceptsRawParams(t *testing.T) {
	_, err := NewBatchElementWithId("m", json.RawMessage(`[1]`), &json.RawMessage{}, json.RawMessage(`1`))
	require.NoError(t, err)
	_, err = NewBatchElementWithId("m", "a string", &json.RawMessage{}, json.RawMessage(`1`))
	require.Error(t, err)
}
