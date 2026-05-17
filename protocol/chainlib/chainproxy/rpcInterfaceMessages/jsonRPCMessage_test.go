package rpcInterfaceMessages

import (
	"encoding/json"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertJsonRPCMsg_Success(t *testing.T) {
	rpcMsg := &rpcclient.JsonrpcMessage{
		Version: "2.0",
		ID:      json.RawMessage(`"1"`),
		Method:  "test",
		Params:  json.RawMessage(`"test_params"`),
		Error:   nil,
		Result:  json.RawMessage(`"test_result"`),
	}

	msg, err := ConvertJsonRPCMsg(rpcMsg)
	assert.NoError(t, err)
	assert.Equal(t, "2.0", msg.Version)
	assert.Equal(t, json.RawMessage(`"1"`), msg.ID)
	assert.Equal(t, "test", msg.Method)
	assert.Equal(t, json.RawMessage(`"test_params"`), msg.Params)
	assert.Nil(t, msg.Error)
	assert.Equal(t, json.RawMessage(`"test_result"`), msg.Result)
}

func TestConvertJsonRPCMsg_Nil(t *testing.T) {
	msg, err := ConvertJsonRPCMsg(nil)
	assert.EqualError(t, err, ErrFailedToConvertMessage.Error())
	assert.Nil(t, msg)
}

func TestJsonrpcMessage_GetParams(t *testing.T) {
	cp := JsonrpcMessage{
		Params: "test_params",
	}

	assert.Equal(t, "test_params", cp.GetParams())
}

func TestJsonrpcMessage_GetResult(t *testing.T) {
	cp := JsonrpcMessage{
		Result: json.RawMessage(`"test_result"`),
	}

	assert.Equal(t, json.RawMessage(`"test_result"`), cp.GetResult())
}

func TestJsonrpcMessage_ParseBlock(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name     string
		input    string
		expected int64
	}{
		{
			name:     "Default block param",
			input:    "latest",
			expected: -2,
		},
		{
			name:     "String representation of int64",
			input:    "80",
			expected: 80,
		},
		{
			name:     "Hex representation of int64",
			input:    "0x26D",
			expected: 621,
		},
		{
			name:     "Ripple validated block param",
			input:    "validated",
			expected: -6,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			restMessage := JsonrpcMessage{}

			block, err := restMessage.ParseBlock(testCase.input)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if block != testCase.expected {
				t.Errorf("Expected %v, but got %v", testCase.expected, block)
			}
		})
	}
}

func TestParseJsonRPCMsg(t *testing.T) {
	// Test Case 1: Valid JSON input
	data := []byte(`{"jsonrpc": "2.0", "id": 1, "method": "getblock", "params": [], "result": {"block": "block data"}}`)
	msgs, err := ParseJsonRPCMsg(data)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	require.Len(t, msgs, 1)
	msg := msgs[0]
	if msg.Version != "2.0" {
		t.Errorf("Expected msg.Version to be 2.0, but got %s", msg.Version)
	}
	if msg.Method != "getblock" {
		t.Errorf("Expected msg.Method to be getblock, but got %s", msg.Method)
	}

	// Test Case 2: Invalid JSON input
	data = []byte(`{"jsonrpc": "2.0", "id": 1, "method": "getblock", "params": []`)
	_, err = ParseJsonRPCMsg(data)
	if err == nil {
		t.Errorf("Expected error, but got nil")
	}
}

func TestParseJsonRPCMissingId(t *testing.T) {
	// Test Case 1: Valid JSON input
	data := []byte(`{"jsonrpc": "2.0", "id": nil, "method": "getblock", "params": []}`)
	_, err := ParseJsonRPCMsg(data)
	require.Error(t, err, err)

	data = []byte(`{"jsonrpc": "2.0", "method": "getblock", "params": []}`)
	msg, err := ParseJsonRPCMsg(data)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage([]byte("null")), msg[0].ID)
}

func TestParseJsonRPCBatch(t *testing.T) {
	// Test Case 1: Valid JSON input
	data := []byte(`[{"method":"eth_chainId","params":[],"id":1,"jsonrpc":"2.0"},{"method":"eth_accounts","params":[],"id":2,"jsonrpc":"2.0"},{"method":"eth_blockNumber","params":[],"id":3,"jsonrpc":"2.0"}]`)
	msgs, err := ParseJsonRPCMsg(data)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	values := []string{"eth_chainId", "eth_accounts", "eth_blockNumber"}
	for idx, msg := range msgs {
		if msg.Version != "2.0" {
			t.Errorf("Expected msg.Version to be 2.0, but got %s", msg.Version)
		}
		require.Equal(t, values[idx], msg.Method, "Expected msg.Method to be %s, but got %s", values[idx], msg.Method)

		// Test Case 2: Invalid JSON input
		data = []byte(`{"jsonrpc": "2.0", "id": 1, "method": "getblock", "params": []`)
		_, err = ParseJsonRPCMsg(data)
		if err == nil {
			t.Errorf("Expected error, but got nil")
		}
	}
}

func TestParseJsonRPCMsgWithBatchFlag(t *testing.T) {
	t.Run("single_object_not_batch", func(t *testing.T) {
		data := []byte(`{"jsonrpc":"2.0","id":1,"method":"getblockhash","params":[100]}`)
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.False(t, isBatch, "single object should not be a batch")
		require.Len(t, msgs, 1)
		require.Equal(t, "getblockhash", msgs[0].Method)
		require.Equal(t, json.RawMessage("1"), msgs[0].ID)
	})

	t.Run("single_element_array_is_batch", func(t *testing.T) {
		// This is the critical case: a batch request with a single element
		// must be detected as a batch so the response is wrapped in an array
		data := []byte(`[{"jsonrpc":"2.0","id":"1773768178254-0","method":"getblockhash","params":[6127543]}]`)
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.True(t, isBatch, "single-element array must be detected as batch")
		require.Len(t, msgs, 1)
		require.Equal(t, "getblockhash", msgs[0].Method)
		require.Equal(t, json.RawMessage(`"1773768178254-0"`), msgs[0].ID)
	})

	t.Run("multi_element_array_is_batch", func(t *testing.T) {
		data := []byte(`[{"method":"eth_chainId","params":[],"id":1,"jsonrpc":"2.0"},{"method":"eth_blockNumber","params":[],"id":2,"jsonrpc":"2.0"}]`)
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.True(t, isBatch, "multi-element array should be a batch")
		require.Len(t, msgs, 2)
	})

	t.Run("empty_array_is_batch", func(t *testing.T) {
		data := []byte(`[]`)
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.True(t, isBatch, "empty array should be detected as batch")
		require.Len(t, msgs, 0)
	})

	t.Run("missing_id_gets_null", func(t *testing.T) {
		data := []byte(`{"jsonrpc":"2.0","method":"getblock","params":[]}`)
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.False(t, isBatch)
		require.Len(t, msgs, 1)
		require.Equal(t, json.RawMessage("null"), msgs[0].ID)
	})

	t.Run("invalid_json_returns_error", func(t *testing.T) {
		data := []byte(`not valid json`)
		_, _, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.Error(t, err)
	})

	t.Run("invalid_batch_json_returns_error", func(t *testing.T) {
		data := []byte(`[not valid json]`)
		_, _, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.Error(t, err)
	})

	t.Run("leading_whitespace_batch", func(t *testing.T) {
		data := []byte("\n  \t [{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getblockhash\",\"params\":[100]}]")
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.True(t, isBatch, "leading whitespace before [ must still be detected as batch")
		require.Len(t, msgs, 1)
		require.Equal(t, "getblockhash", msgs[0].Method)
	})

	t.Run("leading_whitespace_single", func(t *testing.T) {
		data := []byte("  \n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\"}")
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.False(t, isBatch, "leading whitespace before { must not be detected as batch")
		require.Len(t, msgs, 1)
	})

	t.Run("utf8_bom_batch", func(t *testing.T) {
		// UTF-8 BOM (0xEF 0xBB 0xBF) followed by a batch
		data := append([]byte{0xEF, 0xBB, 0xBF}, []byte(`[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}]`)...)
		msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag(data)
		require.NoError(t, err)
		require.True(t, isBatch, "UTF-8 BOM before [ must still be detected as batch")
		require.Len(t, msgs, 1)
	})
}

func TestCheckResponseErrorForJsonRpcBatch(t *testing.T) {
	t.Run("all_success_no_error", func(t *testing.T) {
		// All sub-requests succeeded
		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x123"}},
			{"jsonrpc":"2.0","id":2,"result":{"blockNumber":"0x124"}},
			{"jsonrpc":"2.0","id":3,"result":{"blockNumber":"0x125"}}
		]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "All success should not have error")
		require.Empty(t, errorMsg)
	})

	t.Run("all_errors_should_return_error", func(t *testing.T) {
		// All sub-requests failed
		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Slot was skipped"}},
			{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Slot was skipped"}},
			{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"Slot was skipped"}}
		]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.True(t, hasError, "All errors should return error")
		require.Contains(t, errorMsg, "Slot was skipped")
	})

	t.Run("partial_success_should_not_error", func(t *testing.T) {
		// Some sub-requests succeeded, some failed - should NOT be considered an error
		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x123"}},
			{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Slot was skipped"}},
			{"jsonrpc":"2.0","id":3,"result":{"blockNumber":"0x125"}}
		]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "Partial success should not be considered an error")
		require.Empty(t, errorMsg)
	})

	t.Run("single_success_among_errors_should_not_error", func(t *testing.T) {
		// Only one success among multiple errors - should NOT be considered an error
		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Slot was skipped"}},
			{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Slot was skipped"}},
			{"jsonrpc":"2.0","id":3,"result":{"blockNumber":"0x125"}}
		]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "Single success should prevent error classification")
		require.Empty(t, errorMsg)
	})

	t.Run("empty_batch_no_error", func(t *testing.T) {
		data := []byte(`[]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError)
		require.Empty(t, errorMsg)
	})

	t.Run("invalid_json_is_malformed", func(t *testing.T) {
		// Previously this asserted (false, "") under a "fail-open" rationale.
		// That silently propagated truncated upstream bodies to clients; the
		// router now classifies a malformed batch envelope as a wrong-data
		// verdict so the relay pipeline retries against another provider.
		data := []byte(`not valid json`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.True(t, hasError, "malformed batch envelope must be flagged for retry")
		require.Contains(t, errorMsg, "malformed JSON-RPC batch response")
	})

	t.Run("null_result_with_no_error_is_success", func(t *testing.T) {
		// null result without error is still a valid response (API returned successfully)
		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"result":null},
			{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Some error"}}
		]`)
		// null result is a valid JSON-RPC response - it means the method executed successfully
		// and returned null. This counts as a success.
		hasError, _ := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "null result is a valid success response")
	})

	t.Run("real_world_solana_batch_partial_error", func(t *testing.T) {
		// Real-world scenario: Solana batch request where some slots are skipped
		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"result":{"slot":123,"blockhash":"abc"}},
			{"jsonrpc":"2.0","id":2,"error":{"code":-32007,"message":"Slot 124 was skipped, or missing in long-term storage"}},
			{"jsonrpc":"2.0","id":3,"result":{"slot":125,"blockhash":"def"}}
		]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "Solana batch with partial skipped slots should not trigger retry")
		require.Empty(t, errorMsg)
	})
}

func TestCheckResponseErrorForJsonRpcBatch_StrictMode(t *testing.T) {
	// Save and restore original flag value
	originalValue := BatchNodeErrorOnAny
	defer func() { BatchNodeErrorOnAny = originalValue }()

	t.Run("strict_mode_partial_success_is_error", func(t *testing.T) {
		BatchNodeErrorOnAny = true // Enable strict mode

		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x123"}},
			{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Slot was skipped"}},
			{"jsonrpc":"2.0","id":3,"result":{"blockNumber":"0x125"}}
		]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.True(t, hasError, "Strict mode: partial success should be an error")
		require.Contains(t, errorMsg, "Slot was skipped")
	})

	t.Run("strict_mode_all_success_no_error", func(t *testing.T) {
		BatchNodeErrorOnAny = true // Enable strict mode

		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x123"}},
			{"jsonrpc":"2.0","id":2,"result":{"blockNumber":"0x124"}},
			{"jsonrpc":"2.0","id":3,"result":{"blockNumber":"0x125"}}
		]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "Strict mode: all success should not have error")
		require.Empty(t, errorMsg)
	})

	t.Run("default_mode_partial_success_no_error", func(t *testing.T) {
		BatchNodeErrorOnAny = false // Default mode

		data := []byte(`[
			{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x123"}},
			{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Slot was skipped"}},
			{"jsonrpc":"2.0","id":3,"result":{"blockNumber":"0x125"}}
		]`)
		hasError, _ := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "Default mode: partial success should not be an error")
	})
}

// TestJsonrpcMessage_CheckResponseError_MalformedShapes covers MAG-1718 Phase 1.6
// wrong-data cases at the application layer: a body that omits both result and
// error fields, and an unparseable body as a backstop. Truncated wire-level
// failures are caught at the transport layer in direct_rpc_relay.go via
// json.Valid; this is the in-function defense for malformed shapes.
func TestJsonrpcMessage_CheckResponseError_MalformedShapes(t *testing.T) {
	jm := JsonrpcMessage{}

	t.Run("missing_both_result_and_error", func(t *testing.T) {
		// Schema violation: spec requires exactly one of result or error.
		hasError, msg := jm.CheckResponseError([]byte(`{"jsonrpc":"2.0","id":1}`), 200)
		require.True(t, hasError, "missing both fields must be flagged")
		require.Contains(t, msg, "missing both 'result' and 'error'")
	})

	t.Run("truncated_body_is_flagged_as_backstop", func(t *testing.T) {
		// Direct callers should catch this earlier via json.Valid; flagged here as defense.
		hasError, msg := jm.CheckResponseError([]byte(`{"jsonrpc":"2.0","id":1,"resu`), 200)
		require.True(t, hasError)
		require.Contains(t, msg, "malformed JSON-RPC response")
	})

	t.Run("null_result_is_success", func(t *testing.T) {
		// result:null is a legal JSON-RPC response — eth_getTransactionByHash
		// for an unknown hash returns null, for example.
		hasError, msg := jm.CheckResponseError([]byte(`{"jsonrpc":"2.0","id":1,"result":null}`), 200)
		require.False(t, hasError)
		require.Empty(t, msg)
	})

	t.Run("normal_result_is_success", func(t *testing.T) {
		hasError, _ := jm.CheckResponseError([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x12a7b5c"}`), 200)
		require.False(t, hasError)
	})

	t.Run("error_with_message_returns_message", func(t *testing.T) {
		hasError, msg := jm.CheckResponseError(
			[]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`),
			200,
		)
		require.True(t, hasError)
		require.Equal(t, "method not found", msg)
	})

	t.Run("error_with_empty_message_is_not_flagged", func(t *testing.T) {
		// Preserves the previous behavior: an error object with an empty
		// message is not surfaced as an error.
		hasError, _ := jm.CheckResponseError(
			[]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":0,"message":""}}`),
			200,
		)
		require.False(t, hasError)
	})

	t.Run("top_level_scalar_body", func(t *testing.T) {
		hasError, msg := jm.CheckResponseError([]byte(`"just a string"`), 200)
		require.True(t, hasError)
		require.Contains(t, msg, "malformed JSON-RPC response")
	})

	t.Run("empty_body", func(t *testing.T) {
		hasError, _ := jm.CheckResponseError(nil, 200)
		require.True(t, hasError)
	})

	t.Run("error_null_and_no_result_is_malformed", func(t *testing.T) {
		// "error": null is semantically equivalent to no error key. Combined
		// with a missing result it's a schema violation — must be flagged so
		// the relay pipeline doesn't forward a useless body to the client.
		hasError, msg := jm.CheckResponseError([]byte(`{"jsonrpc":"2.0","id":1,"error":null}`), 200)
		require.True(t, hasError, "error:null + no result must be flagged as malformed")
		require.Contains(t, msg, "missing both 'result' and 'error'")
	})

	t.Run("error_null_with_result_is_success", func(t *testing.T) {
		// "error": null alongside a real result is a (mildly off-spec but)
		// healthy response. Some upstreams emit this shape.
		hasError, _ := jm.CheckResponseError(
			[]byte(`{"jsonrpc":"2.0","id":1,"result":"0x12a7b5c","error":null}`),
			200,
		)
		require.False(t, hasError, "error:null alongside a result must not be flagged")
	})
}

// TestCheckResponseErrorForJsonRpcBatch_MalformedElements covers the
// analogous cases at the batch level: a batch where every element is
// malformed must NOT slip through as "no errors" the way it did before.
func TestCheckResponseErrorForJsonRpcBatch_MalformedElements(t *testing.T) {
	t.Run("all_elements_missing_both_fields", func(t *testing.T) {
		// Previously this returned (false, "") because the aggregator only
		// collected sub-messages from elements with a non-nil error object.
		data := []byte(`[{"jsonrpc":"2.0","id":1},{"jsonrpc":"2.0","id":2}]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.True(t, hasError, "all-malformed batch must be flagged for retry")
		require.Contains(t, errorMsg, "missing both 'result' and 'error'")
	})

	t.Run("partial_success_with_malformed_sibling_default_mode", func(t *testing.T) {
		originalValue := BatchNodeErrorOnAny
		defer func() { BatchNodeErrorOnAny = originalValue }()
		BatchNodeErrorOnAny = false
		data := []byte(`[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":2}]`)
		hasError, _ := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.False(t, hasError, "a success masks a malformed sibling in default mode")
	})

	t.Run("partial_success_with_malformed_sibling_strict_mode", func(t *testing.T) {
		originalValue := BatchNodeErrorOnAny
		defer func() { BatchNodeErrorOnAny = originalValue }()
		BatchNodeErrorOnAny = true
		data := []byte(`[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":2}]`)
		hasError, errorMsg := CheckResponseErrorForJsonRpcBatch(data, 200)
		require.True(t, hasError, "strict mode flags any fault, including malformed elements")
		require.Contains(t, errorMsg, "missing both 'result' and 'error'")
	})
}
