package rpcInterfaceMessages

// FlattenXRPLWebSocketParams extracts rippled's one params object so its fields can be sent at
// the request's top level instead of nested under "params". rippled's WebSocket API reads a
// request's fields from the top level: it accepts "method" for "command" and echoes
// "jsonrpc"/"id", but it ignores "params" entirely, so a field nested there (e.g. GET_BLOCKNUM's
// ledger_index) never reaches the node over wss:// (MAG-5452).
//
// Every XRP Ledger RPC method takes exactly one params object, matching the HTTP JSON-RPC shape
// `{"method":"...","params":[{...}]}`. Any other shape — no params (a no-argument command), or
// something that is not a single-object array — is reported as not flattenable, so the caller
// falls back to sending it unchanged.
func FlattenXRPLWebSocketParams(params interface{}) (fields map[string]interface{}, ok bool) {
	arr, ok := params.([]interface{})
	if !ok || len(arr) != 1 {
		return nil, false
	}
	fields, ok = arr[0].(map[string]interface{})
	return fields, ok
}
