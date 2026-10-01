package rpcInterfaceMessages

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestMessage(t *testing.T) {
	// Test GetParams method
	restMessage := RestMessage{
		Path:     "blocks/latest",
		SpecPath: "blocks/latest",
	}

	// Test GetParams method
	params := restMessage.GetParams()
	require.Nil(t, params)

	// Test GetResult method
	result := restMessage.GetResult()
	if result != nil {
		t.Errorf("Expected nil, but got %v", result)
	}
}

// TestRestMessageGetParamsTrailingSlash covers path parameter extraction when the request
// and the spec name disagree about a trailing slash — reachable since the api matcher stopped
// letting the slash decide the match. GetParams zips the two paths by segment index, and the
// slash adds an empty segment on one side, so a misalignment here would hand the block parser
// the wrong path segment and silently change the requested block.
func TestRestMessageGetParamsTrailingSlash(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name     string
		specPath string
		path     string
		expected map[string]interface{}
	}{
		{
			name:     "request carries a slash the spec name omits",
			specPath: "/chains/main/blocks/{block_id}/header",
			path:     "/chains/main/blocks/9427283/header/",
			expected: map[string]interface{}{"block_id": "9427283"},
		},
		{
			name:     "trailing placeholder with a trailing slash",
			specPath: "/cosmos/base/tendermint/v1beta1/blocks/{height}",
			path:     "/cosmos/base/tendermint/v1beta1/blocks/244590/",
			expected: map[string]interface{}{"height": "244590"},
		},
		{
			// The STACKS direction: the spec name carries the slash, the request does not.
			name:     "spec name carries a slash after the placeholder",
			specPath: "/extended/v1/address/{principal}/balances/",
			path:     "/extended/v1/address/SP2J6ZY48GV1EZ5V2V5RB9MP66SW86PYKKNRV9EJ7/balances",
			expected: map[string]interface{}{"principal": "SP2J6ZY48GV1EZ5V2V5RB9MP66SW86PYKKNRV9EJ7"},
		},
		{
			name:     "several placeholders with a trailing slash",
			specPath: "/cosmos/gov/v1/proposals/{proposal_id}/votes/{voter}",
			path:     "/cosmos/gov/v1/proposals/7/votes/lava@1abc/",
			expected: map[string]interface{}{"proposal_id": "7", "voter": "lava@1abc"},
		},
		{
			name:     "a query string survives the trailing slash",
			specPath: "/cosmos/base/tendermint/v1beta1/blocks/{height}",
			path:     "/cosmos/base/tendermint/v1beta1/blocks/244590/?pretty=true",
			expected: map[string]interface{}{"height": "244590", "pretty": "true"},
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			restMessage := RestMessage{Path: testCase.path, SpecPath: testCase.specPath}
			require.Equal(t, testCase.expected, restMessage.GetParams())
		})
	}
}

func TestRestParseBlock(t *testing.T) {
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

			restMessage := RestMessage{}

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

// TestCheckResponseError_CosmosTransaction_Success tests successful Cosmos transaction (code=0)
func TestCheckResponseError_CosmosTransaction_Success(t *testing.T) {
	restMsg := RestMessage{}

	// Cosmos SDK transaction success response (HTTP 200 + code=0)
	successResponse := `{
		"tx_response": {
			"height": "12345",
			"txhash": "ABC123DEF456789",
			"codespace": "",
			"code": 0,
			"data": "0A1E0A1C2F636F736D6F732E62616E6B2E763162657461312E4D736753656E64",
			"raw_log": "[]",
			"logs": [{"msg_index": 0, "log": "", "events": []}],
			"info": "",
			"gas_wanted": "200000",
			"gas_used": "85123",
			"tx": {},
			"timestamp": "2025-01-15T10:30:00Z",
			"events": []
		}
	}`

	hasError, errorMessage := restMsg.CheckResponseError([]byte(successResponse), 200)

	require.False(t, hasError, "Should not detect error for code=0")
	require.Empty(t, errorMessage, "Error message should be empty for success")
}

// TestCheckResponseError_CosmosTransaction_SequenceMismatch tests sequence mismatch error (code=32)
func TestCheckResponseError_CosmosTransaction_SequenceMismatch(t *testing.T) {
	restMsg := RestMessage{}

	// Cosmos SDK transaction error (HTTP 200 + code=32)
	errorResponse := `{
		"tx_response": {
			"height": "0",
			"txhash": "CB584A80FE7EA54E4CCEFCA6BE89B4FABCF4B0087766733FB8E1741ACB61EFFE",
			"codespace": "sdk",
			"code": 32,
			"data": "",
			"raw_log": "account sequence mismatch, expected 379, got 392: incorrect account sequence",
			"logs": [],
			"info": "",
			"gas_wanted": "0",
			"gas_used": "0",
			"tx": null,
			"timestamp": "",
			"events": []
		}
	}`

	hasError, errorMessage := restMsg.CheckResponseError([]byte(errorResponse), 200)

	require.True(t, hasError, "Should detect error for code=32")
	require.Contains(t, errorMessage, "account sequence mismatch", "Should return raw_log as error message")
}

// TestCheckResponseError_CosmosTransaction_VariousErrorCodes tests various Cosmos transaction error codes
func TestCheckResponseError_CosmosTransaction_VariousErrorCodes(t *testing.T) {
	testCases := []struct {
		name        string
		code        int
		rawLog      string
		expectError bool
	}{
		{
			name:        "Success (code=0)",
			code:        0,
			rawLog:      "[]",
			expectError: false,
		},
		{
			name:        "Invalid Signature (code=4)",
			code:        4,
			rawLog:      "signature verification failed",
			expectError: true,
		},
		{
			name:        "Out of Gas (code=11)",
			code:        11,
			rawLog:      "out of gas",
			expectError: true,
		},
		{
			name:        "Insufficient Fee (code=13)",
			code:        13,
			rawLog:      "insufficient fees",
			expectError: true,
		},
		{
			name:        "Invalid Transaction (code=18)",
			code:        18,
			rawLog:      "invalid transaction: must contain at least one message",
			expectError: true,
		},
		{
			name:        "TX Already in Cache (code=19)",
			code:        19,
			rawLog:      "tx already exists in cache",
			expectError: true,
		},
		{
			name:        "Sequence Mismatch (code=32)",
			code:        32,
			rawLog:      "account sequence mismatch",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			restMsg := RestMessage{}

			response := fmt.Sprintf(`{
				"tx_response": {
					"height": "12345",
					"txhash": "ABC123DEF456",
					"code": %d,
					"raw_log": "%s",
					"logs": [],
					"gas_wanted": "200000",
					"gas_used": "150000"
				}
			}`, tc.code, tc.rawLog)

			hasError, errorMessage := restMsg.CheckResponseError([]byte(response), 200)

			require.Equal(t, tc.expectError, hasError, "Error detection mismatch for %s", tc.name)
			if tc.expectError {
				require.Equal(t, tc.rawLog, errorMessage, "Error message should match raw_log for %s", tc.name)
			}
		})
	}
}

// TestCheckResponseError_HTTPStatusCodeVariations tests different HTTP status codes
func TestCheckResponseError_HTTPStatusCodeVariations(t *testing.T) {
	testCases := []struct {
		name          string
		httpStatus    int
		response      string
		expectedError bool
		errorCheck    func(t *testing.T, hasError bool, errorMessage string)
	}{
		{
			name:       "HTTP 200 + code=0 (Success)",
			httpStatus: 200,
			response: `{
				"tx_response": {
					"code": 0,
					"raw_log": "[]"
				}
			}`,
			expectedError: false,
		},
		{
			name:       "HTTP 200 + code=32 (Error)",
			httpStatus: 200,
			response: `{
				"tx_response": {
					"code": 32,
					"raw_log": "sequence mismatch"
				}
			}`,
			expectedError: true,
			errorCheck: func(t *testing.T, hasError bool, errorMessage string) {
				require.Contains(t, errorMessage, "sequence mismatch")
			},
		},
		{
			name:       "HTTP 400 (Bad Request)",
			httpStatus: 400,
			response: `{
				"message": "bad request",
				"code": 400
			}`,
			expectedError: true, // any status outside 2xx is a node error; the body is returned to the caller as-is
		},
		{
			name:       "HTTP 500 (Server Error)",
			httpStatus: 500,
			response: `{
				"message": "internal server error",
				"code": 500
			}`,
			expectedError: true,
			errorCheck: func(t *testing.T, hasError bool, errorMessage string) {
				require.Contains(t, errorMessage, "internal server error")
			},
		},
		{
			name:       "HTTP 404 (Not Found)",
			httpStatus: 404,
			response: `{
				"message": "not found",
				"code": 404
			}`,
			expectedError: true, // any status outside 2xx is a node error; the body is returned to the caller as-is
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			restMsg := RestMessage{}

			hasError, errorMessage := restMsg.CheckResponseError([]byte(tc.response), tc.httpStatus)

			require.Equal(t, tc.expectedError, hasError, "Error detection mismatch for %s", tc.name)
			if tc.errorCheck != nil {
				tc.errorCheck(t, hasError, errorMessage)
			}
		})
	}
}

// TestCheckResponseError_NonTransactionCalls tests non-transaction REST calls (regression)
func TestCheckResponseError_NonTransactionCalls(t *testing.T) {
	testCases := []struct {
		name          string
		response      string
		httpStatus    int
		expectedError bool
	}{
		{
			name: "Query Balance - Success",
			response: `{
				"balances": [
					{"denom": "ulava", "amount": "1000000"}
				],
				"pagination": {}
			}`,
			httpStatus:    200,
			expectedError: false,
		},
		{
			name: "Query TX by Hash - Success",
			response: `{
				"tx": {},
				"tx_response": {
					"code": 0,
					"txhash": "ABC123"
				}
			}`,
			httpStatus:    200,
			expectedError: false,
		},
		{
			name: "Query TX by Hash - Not Found",
			response: `{
				"message": "tx not found",
				"code": 404
			}`,
			httpStatus:    404,
			expectedError: true, // any status outside 2xx is a node error; the body is returned to the caller as-is
		},
		{
			name: "Query Block - Success",
			response: `{
				"block": {
					"header": {
						"height": "12345"
					}
				}
			}`,
			httpStatus:    200,
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			restMsg := RestMessage{}

			hasError, _ := restMsg.CheckResponseError([]byte(tc.response), tc.httpStatus)

			require.Equal(t, tc.expectedError, hasError, "Error detection mismatch for %s", tc.name)
		})
	}
}

// TestCheckResponseError_EdgeCases tests edge cases and malformed data
func TestCheckResponseError_EdgeCases(t *testing.T) {
	testCases := []struct {
		name          string
		response      string
		httpStatus    int
		expectedError bool
	}{
		{
			name:          "Empty Response",
			response:      "",
			httpStatus:    200,
			expectedError: false,
		},
		{
			name:          "Invalid JSON",
			response:      "{invalid json",
			httpStatus:    200,
			expectedError: false,
		},
		{
			name: "Missing tx_response field",
			response: `{
				"height": "12345",
				"txhash": "ABC123"
			}`,
			httpStatus:    200,
			expectedError: false,
		},
		{
			name: "tx_response with missing code field",
			response: `{
				"tx_response": {
					"txhash": "ABC123",
					"raw_log": "some log"
				}
			}`,
			httpStatus:    200,
			expectedError: false, // code defaults to 0 (success)
		},
		{
			name: "Empty tx_response",
			response: `{
				"tx_response": {}
			}`,
			httpStatus:    200,
			expectedError: false, // code defaults to 0
		},
		{
			name: "tx_response with only code field",
			response: `{
				"tx_response": {
					"code": 19
				}
			}`,
			httpStatus:    200,
			expectedError: true, // code=19 is an error, even without raw_log
		},
		{
			name: "Nested JSON with tx_response",
			response: `{
				"data": {
					"tx_response": {
						"code": 0
					}
				}
			}`,
			httpStatus:    200,
			expectedError: false, // Nested structure won't be parsed as tx_response
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			restMsg := RestMessage{}

			hasError, _ := restMsg.CheckResponseError([]byte(tc.response), tc.httpStatus)

			require.Equal(t, tc.expectedError, hasError, "Error detection mismatch for %s", tc.name)
		})
	}
}

// TestCheckResponseError_RealWorldScenarios tests real-world response examples
func TestCheckResponseError_RealWorldScenarios(t *testing.T) {
	testCases := []struct {
		name          string
		response      string
		httpStatus    int
		expectedError bool
		errorContains string
	}{
		{
			name: "Real Success Response",
			response: `{
				"tx_response": {
					"height": "8123456",
					"txhash": "5F4A8B9C7D6E1F2A3B4C5D6E7F8A9B0C1D2E3F4A5B6C7D8E9F0A1B2C3D4E5F6A",
					"codespace": "",
					"code": 0,
					"data": "0A1E0A1C2F636F736D6F732E62616E6B2E763162657461312E4D736753656E64",
					"raw_log": "[{\"events\":[{\"type\":\"coin_spent\",\"attributes\":[{\"key\":\"spender\",\"value\":\"lava1...\"},{\"key\":\"amount\",\"value\":\"1000ulava\"}]}]}]",
					"logs": [{"msg_index": 0, "log": "", "events": [{"type": "coin_spent"}]}],
					"info": "",
					"gas_wanted": "200000",
					"gas_used": "85123",
					"timestamp": "2025-10-16T14:30:00Z"
				}
			}`,
			httpStatus:    200,
			expectedError: false,
		},
		{
			name: "Real Sequence Error",
			response: `{
				"tx_response": {
					"height": "0",
					"txhash": "CB584A80FE7EA54E4CCEFCA6BE89B4FABCF4B0087766733FB8E1741ACB61EFFE",
					"codespace": "sdk",
					"code": 32,
					"data": "",
					"raw_log": "account sequence mismatch, expected 379, got 392: incorrect account sequence",
					"logs": [],
					"info": "",
					"gas_wanted": "0",
					"gas_used": "0",
					"timestamp": ""
				}
			}`,
			httpStatus:    200,
			expectedError: true,
			errorContains: "account sequence mismatch",
		},
		{
			name: "Real TX in Mempool Cache Error",
			response: `{
				"tx_response": {
					"height": "0",
					"txhash": "A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6Q7R8S9T0U1V2W3X4Y5Z6",
					"codespace": "sdk",
					"code": 19,
					"data": "",
					"raw_log": "tx already exists in cache",
					"logs": [],
					"info": "",
					"gas_wanted": "0",
					"gas_used": "0",
					"timestamp": ""
				}
			}`,
			httpStatus:    200,
			expectedError: true,
			errorContains: "tx already exists in cache",
		},
		{
			name: "Real Invalid Transaction Error",
			response: `{
				"tx_response": {
					"height": "0",
					"txhash": "",
					"codespace": "sdk",
					"code": 18,
					"data": "",
					"raw_log": "invalid transaction: must contain at least one message",
					"logs": [],
					"info": "",
					"gas_wanted": "0",
					"gas_used": "0",
					"timestamp": ""
				}
			}`,
			httpStatus:    200,
			expectedError: true,
			errorContains: "invalid transaction",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			restMsg := RestMessage{}

			hasError, errorMessage := restMsg.CheckResponseError([]byte(tc.response), tc.httpStatus)

			require.Equal(t, tc.expectedError, hasError, "Error detection mismatch for %s", tc.name)
			if tc.errorContains != "" {
				require.Contains(t, errorMessage, tc.errorContains, "Error message should contain expected text")
			}
		})
	}
}

// TestCheckResponseError_ServerErrors tests 5xx and 429 handling (Phase 4 corrections)
func TestCheckResponseError_ServerErrors(t *testing.T) {
	testCases := []struct {
		name          string
		httpStatus    int
		response      string
		expectedError bool // Should be node error?
		errorCheck    func(t *testing.T, errorMessage string)
	}{
		{
			name:          "503 Service Unavailable with JSON error",
			httpStatus:    503,
			response:      `{"error":"service temporarily unavailable"}`,
			expectedError: true, // Node error - triggers retry
			errorCheck: func(t *testing.T, errorMessage string) {
				require.Contains(t, errorMessage, "service temporarily unavailable")
			},
		},
		{
			name:          "503 with plain text body",
			httpStatus:    503,
			response:      `Service Unavailable`,
			expectedError: true, // Node error even without JSON
			errorCheck: func(t *testing.T, errorMessage string) {
				require.Contains(t, errorMessage, "Service Unavailable")
			},
		},
		{
			name:          "500 Internal Server Error",
			httpStatus:    500,
			response:      `{"message":"internal server error"}`,
			expectedError: true, // Node error
			errorCheck: func(t *testing.T, errorMessage string) {
				require.Contains(t, errorMessage, "internal server error")
			},
		},
		{
			name:          "502 Bad Gateway",
			httpStatus:    502,
			response:      `Bad Gateway`,
			expectedError: true, // Node error
		},
		{
			name:          "429 Rate Limit Exceeded",
			httpStatus:    429,
			response:      `{"error":"rate limit exceeded"}`,
			expectedError: true, // Node error (triggers backoff/retry)
			errorCheck: func(t *testing.T, errorMessage string) {
				require.Contains(t, errorMessage, "rate limit exceeded")
			},
		},
		{
			name:          "429 with empty body",
			httpStatus:    429,
			response:      ``,
			expectedError: true, // Still node error
			errorCheck: func(t *testing.T, errorMessage string) {
				require.Equal(t, "HTTP 429", errorMessage) // Fallback to status
			},
		},
		{
			name:          "501 Not Implemented (treated as node error, not client error)",
			httpStatus:    501,
			response:      `{"error":"method not implemented on this node"}`,
			expectedError: true, // 5xx → node error → try another provider
			errorCheck: func(t *testing.T, errorMessage string) {
				require.Contains(t, errorMessage, "method not implemented")
			},
		},
		{
			name:          "404 Not Found",
			httpStatus:    404,
			response:      `{"code":5,"message":"block not found"}`,
			expectedError: true, // a node error, classified by the registry's 404 row (non-retryable, not the node's fault)
		},
		{
			name:          "400 Bad Request",
			httpStatus:    400,
			response:      `{"error":"invalid request"}`,
			expectedError: true, // a node error, classified by the registry's 400 row (non-retryable, not the node's fault)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			restMsg := RestMessage{}

			hasError, errorMessage := restMsg.CheckResponseError([]byte(tc.response), tc.httpStatus)

			require.Equal(t, tc.expectedError, hasError,
				"Expected node error=%v for HTTP %d", tc.expectedError, tc.httpStatus)

			if tc.errorCheck != nil {
				tc.errorCheck(t, errorMessage)
			}
		})
	}
}

// TestCheckResponseError_AnyNonSuccessIsANodeError pins the rule: every status outside 2xx is a
// node error whatever the body says, a 2xx is a success unless it carries a Cosmos transaction
// error, and a status of 0 is "not set" and reads as a success. The bodies are what real nodes
// and gateways answered with on 2026-09-27 (docs/ERROR-REGISTRY-DESIGN.md, "REST replies").
func TestCheckResponseError_AnyNonSuccessIsANodeError(t *testing.T) {
	testCases := []struct {
		name          string
		httpStatus    int
		response      string
		expectedError bool
		msgContains   string
	}{
		{"200 JSON", 200, `{"balances":[]}`, false, ""},
		{"200 non-JSON body is still a success (a raw Tezos balance)", 200, `"459159730"`, false, ""},
		{"201 Horizon async submit accepted", 201, `{"tx_status":"PENDING","hash":"1a3b"}`, false, ""},
		{"status 0 is unset and reads as a success", 0, `{"result":"x"}`, false, ""},
		{"Aptos unknown account is a 200 with a fresh account, not an error", 200, `{"sequence_number":"0","authentication_key":"0xdead"}`, false, ""},
		{"Cosmos broadcast rejected inside a 200", 200, `{"tx_response":{"code":2,"raw_log":"invalid length"}}`, true, "invalid length"},
		{"Horizon not_found problem document", 404, `{"type":"https://stellar.org/horizon-errors/not_found","title":"Resource Missing","status":404}`, true, ""},
		{"Horizon transaction_failed (a rejected write)", 400, `{"type":"https://stellar.org/horizon-errors/transaction_failed","status":400,"extras":{"result_codes":{"transaction":"tx_bad_seq"}}}`, true, ""},
		{"Horizon async submit ERROR", 400, `{"tx_status":"ERROR","error_result_xdr":"AAAA","hash":"0367"}`, true, ""},
		{"Horizon before_history (410)", 410, `{"type":"https://stellar.org/horizon-errors/before_history","status":410}`, true, ""},
		{"Aptos resource_not_found", 404, `{"message":"Resource not found","error_code":"resource_not_found","vm_error_code":null}`, true, "Resource not found"},
		{"Aptos version_pruned (410)", 410, `{"error_code":"version_pruned","message":"Ledger version(1) has been pruned"}`, true, "pruned"},
		{"Cosmos tx not found (404, code 5)", 404, `{"code":5,"message":"tx not found: 0000","details":[]}`, true, "tx not found"},
		{"Cosmos pruned height (500 with the gateway envelope)", 500, `{"code":2,"message":"height 1 is not available, lowest height is 25280088","details":[]}`, true, "not available"},
		{"Sidecar unknown block hash (500)", 500, `{"code":500,"message":"Unable to retrieve header and parent from supplied hash"}`, true, "Unable to retrieve"},
		{"nodeos rejected transaction (500)", 500, `{"code":500,"message":"Internal Service Error","error":{"code":3010010,"name":"packed_transaction_type_exception"}}`, true, ""},
		{"Tezos injection failure is a JSON array in a 500", 500, `[{"kind":"temporary","id":"failure","msg":"Can't parse the operation"}]`, true, ""},
		{"Beacon NOT_FOUND", 404, `{"code":404,"message":"NOT_FOUND: beacon block at slot 9","stacktraces":[]}`, true, "NOT_FOUND"},
		{"toncenter ok:false with a 422", 422, `{"ok":false,"error":"failed to parse ton_addr","code":422}`, true, "failed to parse"},
		{"tatum gateway empty 404 (the dfns incident)", 404, ``, true, "HTTP 404"},
		{"nginx HTML 404", 404, `<html><head><title>404 Not Found</title></head></html>`, true, "404 Not Found"},
		{"Horizon's own empty 405", 405, ``, true, "HTTP 405"},
		{"Tezos missing block is an empty 404", 404, ``, true, "HTTP 404"},
		{"Stacks rejects a submit with text/plain 400", 400, `Failed to decode: unrecognized auth flags 103`, true, "Failed to decode"},
		{"Cloudflare HTML 403", 403, `<!DOCTYPE html><html>blocked</html>`, true, ""},
		{"gateway 302 redirect page", 302, `<html><body>302 Found</body></html>`, true, ""},
		{"empty 500", 500, ``, true, "HTTP 500"},
		{"HTML 502", 502, `<html><body>502 Bad Gateway</body></html>`, true, ""},
		{"429 with a body", 429, `{"error":"rate limit exceeded"}`, true, "rate limit"},
		{"429 with an empty body", 429, ``, true, "HTTP 429"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasError, errorMessage := RestMessage{}.CheckResponseError([]byte(tc.response), tc.httpStatus)
			require.Equal(t, tc.expectedError, hasError)
			if tc.msgContains != "" {
				require.Contains(t, errorMessage, tc.msgContains)
			}
		})
	}
}

// TestServerErrorIsNodeReply pins which REST 5xx replies keep their body for the client.
func TestServerErrorIsNodeReply(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"Horizon 503 TRY_AGAIN_LATER", 503, `{"tx_status":"TRY_AGAIN_LATER","hash":"07"}`, true},
		{"Cosmos 500 with the gateway envelope", 500, `{"code":2,"message":"height 1 is not available"}`, true},
		{"sidecar 500", 500, `{"code":500,"message":"Unable to retrieve header and parent from supplied hash"}`, true},
		{"empty 500", 500, ``, false},
		{"HTML 502", 502, `<html><body>502 Bad Gateway</body></html>`, false},
		{"text 503", 503, `Service Unavailable`, false},
		{"JSON 502 — may have forwarded the request", 502, `{"message":"bad gateway"}`, false},
		{"JSON 504 — Horizon timeout, the tx may still apply", 504, `{"type":"https://stellar.org/horizon-errors/timeout","status":504}`, false},
		{"JSON 524 — Cloudflare timeout", 524, `{"message":"timeout"}`, false},
		{"not a 5xx", 404, `{"message":"not found"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ServerErrorIsNodeReply(tc.status, []byte(tc.body)))
		})
	}
}
