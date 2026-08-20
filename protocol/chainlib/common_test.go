package chainlib

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	websocket2 "github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchSpecApiByName(t *testing.T) {
	t.Parallel()
	connectionType := ""
	testTable := []struct {
		name        string
		apis        []*spectypes.Api
		inputName   string
		expectedApi spectypes.Api
		expectedOk  bool
	}{
		{
			name: "test1",
			apis: []*spectypes.Api{{
				Name: "/blocks/{height}",
				BlockParsing: spectypes.BlockParser{
					ParserArg:  []string{"0"},
					ParserFunc: spectypes.PARSER_FUNC_PARSE_BY_ARG,
				},
				ComputeUnits: 10,
				Enabled:      true,
				Category:     spectypes.SpecCategory{Deterministic: true},
			}},
			inputName:   "/blocks/10",
			expectedApi: spectypes.Api{Name: "/blocks/{height}"},
			expectedOk:  true,
		},
		{
			name: "test2",
			apis: []*spectypes.Api{{
				Name: "/cosmos/base/tendermint/v1beta1/blocks/{height}",
				BlockParsing: spectypes.BlockParser{
					ParserArg:  []string{"0"},
					ParserFunc: spectypes.PARSER_FUNC_PARSE_BY_ARG,
				},
				ComputeUnits: 10,
				Enabled:      true,
				Category:     spectypes.SpecCategory{Deterministic: true},
			}},
			inputName:   "/cosmos/base/tendermint/v1beta1/blocks/10",
			expectedApi: spectypes.Api{Name: "/cosmos/base/tendermint/v1beta1/blocks/{height}"},
			expectedOk:  true,
		},
		{
			name: "test3",
			apis: []*spectypes.Api{{
				Name: "/cosmos/base/tendermint/v1beta1/blocks/latest",
				BlockParsing: spectypes.BlockParser{
					ParserArg:  []string{"0"},
					ParserFunc: spectypes.PARSER_FUNC_DEFAULT,
				},
				ComputeUnits: 10,
				Enabled:      true,
				Category:     spectypes.SpecCategory{Deterministic: true},
			}},
			inputName:   "/cosmos/base/tendermint/v1beta1/blocks/latest",
			expectedApi: spectypes.Api{Name: "/cosmos/base/tendermint/v1beta1/blocks/latest"},
			expectedOk:  true,
		},
	}
	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			serverApis := restApiContainers(t, connectionType, testCase.apis...)
			api, ok := matchSpecApiByName(testCase.inputName, connectionType, serverApis)
			if ok != testCase.expectedOk {
				t.Fatalf("expected ok value %v, but got %v", testCase.expectedOk, ok)
			}
			if api.api.Name != testCase.expectedApi.Name {
				t.Fatalf("expected api %v, but got %v", testCase.expectedApi.Name, api.api.Name)
			}
		})
	}
}

func TestConvertToJsonError(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name     string
		errorMsg string
		expected string
	}{
		{
			name:     "valid json",
			errorMsg: "some error message",
			expected: `{"error":"some error message"}`,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result := convertToJsonError(testCase.errorMsg)
			if string(result) != testCase.expected {
				t.Errorf("Expected result to be %s, but got %s", testCase.expected, result)
			}
		})
	}
}

// TestConvertToJsonRpcError verifies the spec-compliant JSON-RPC 2.0 error
// envelope: `error` is an Object with code/message, not a stringified envelope.
// Regression coverage for MAG-1866.
func TestConvertToJsonRpcError(t *testing.T) {
	t.Parallel()

	rawErrMsg := `{"Error_GUID":"3789588031954078542","Error":"Selected provider not available {selectedProvider:quicknode1,validProviders:google1,GUID:3789588031954078542}"}`
	reqBody := []byte(`{"jsonrpc":"2.0","id":42,"method":"engine_getPayloadV3","params":[]}`)

	result := convertToJsonRpcError(rawErrMsg, reqBody)

	var parsed map[string]any
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v. Result: %s", err, result)
	}

	if v, _ := parsed["jsonrpc"].(string); v != "2.0" {
		t.Errorf("expected jsonrpc=\"2.0\", got %v", parsed["jsonrpc"])
	}
	// id must round-trip the request id as a JSON number.
	if v, _ := parsed["id"].(float64); v != 42 {
		t.Errorf("expected id=42, got %v (%T)", parsed["id"], parsed["id"])
	}

	errObj, ok := parsed["error"].(map[string]any)
	if !ok {
		t.Fatalf("error must be an Object per JSON-RPC 2.0 §5.1, got %T: %v", parsed["error"], parsed["error"])
	}
	if code, _ := errObj["code"].(float64); int(code) != -32000 {
		t.Errorf("expected error.code=-32000, got %v", errObj["code"])
	}
	msg, _ := errObj["message"].(string)
	if msg == "" {
		t.Errorf("error.message must be a non-empty string, got %v", errObj["message"])
	}
	if !strings.Contains(msg, "Selected provider not available") {
		t.Errorf("error.message should preserve inner error context, got %q", msg)
	}
	data, ok := errObj["data"].(map[string]any)
	if !ok {
		t.Fatalf("error.data must be an Object, got %T", errObj["data"])
	}
	if g, _ := data["guid"].(string); g != "3789588031954078542" {
		t.Errorf("expected data.guid to be preserved, got %v", data["guid"])
	}
}

// TestConvertToJsonRpcError_FallbackOnUnparseable verifies that when the raw
// error message is not the expected GetUniqueGuidResponseForError shape, the
// envelope still passes JSON-RPC 2.0 validation.
func TestConvertToJsonRpcError_FallbackOnUnparseable(t *testing.T) {
	t.Parallel()

	result := convertToJsonRpcError("plain text error, not JSON", []byte(`{}`))
	var parsed map[string]any
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	errObj, ok := parsed["error"].(map[string]any)
	if !ok {
		t.Fatalf("error must be an Object, got %T", parsed["error"])
	}
	if int(errObj["code"].(float64)) != -32000 {
		t.Errorf("expected error.code=-32000, got %v", errObj["code"])
	}
	if msg, _ := errObj["message"].(string); msg != "plain text error, not JSON" {
		t.Errorf("expected raw message to pass through, got %q", msg)
	}
	// id absent in request body → null in response (still spec-valid).
	if v, exists := parsed["id"]; !exists || v != nil {
		t.Errorf("expected id=null when request has no id, got %v", v)
	}
}

// TestConvertToJsonRpcError_MaskedModeOmitsErrorField covers the
// ReturnMaskedErrors=true path where GetUniqueGuidResponseForError elides
// the Error field via ,omitempty. Without the masked-mode branch in the
// helper, error.message would surface the raw `{"Error_GUID":"..."}`
// envelope — defeating the spec-compliance goal one layer in.
func TestConvertToJsonRpcError_MaskedModeOmitsErrorField(t *testing.T) {
	t.Parallel()

	// Masked-mode envelope: Error_GUID only, Error elided by ,omitempty.
	rawErrMsg := `{"Error_GUID":"3789588031954078542"}`
	reqBody := []byte(`{"jsonrpc":"2.0","id":42,"method":"engine_getPayloadV3","params":[]}`)

	result := convertToJsonRpcError(rawErrMsg, reqBody)

	var parsed map[string]any
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v. Result: %s", err, result)
	}

	errObj, ok := parsed["error"].(map[string]any)
	if !ok {
		t.Fatalf("error must be an Object, got %T", parsed["error"])
	}
	msg, _ := errObj["message"].(string)
	if msg == "" {
		t.Errorf("error.message must be non-empty")
	}
	if strings.Contains(msg, "Error_GUID") || strings.Contains(msg, "{") {
		t.Errorf("error.message must not leak the raw JSON envelope under masking; got %q", msg)
	}
	// data.guid is still useful debugging info even when message is generic.
	data, ok := errObj["data"].(map[string]any)
	if !ok {
		t.Fatalf("error.data must be an Object, got %T", errObj["data"])
	}
	if g, _ := data["guid"].(string); g != "3789588031954078542" {
		t.Errorf("expected data.guid to be preserved under masking, got %v", data["guid"])
	}
}

func TestAddAttributeToError(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name         string
		key          string
		value        string
		errorMessage string
		expected     string
	}{
		{
			name:         "Valid conversion",
			key:          "key1",
			value:        "value1",
			errorMessage: `"errorKey": "error_value"`,
			expected:     `"errorKey": "error_value", "key1": "value1"`,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := addAttributeToError(testCase.key, testCase.value, testCase.errorMessage)
			if result != testCase.expected {
				t.Errorf("addAttributeToError(%q, %q, %q) = %q; expected %q", testCase.key, testCase.value, testCase.errorMessage, result, testCase.expected)
			}
		})
	}
}

func TestExtractDappIDFromWebsocketConnection(t *testing.T) {
	testCases := []struct {
		name     string
		route    string
		headers  map[string][]string
		expected string
	}{
		{
			name:     "dappId exists in params",
			route:    "/ws",
			headers:  map[string][]string{"project-id": {"DappID123"}},
			expected: "DappID123",
		},
		{
			name:     "dappId does not exist in params",
			route:    "/ws",
			headers:  map[string][]string{},
			expected: "DefaultDappID",
		},
	}

	app := fiber.New()

	webSocketCallback := websocket.New(func(websockConn *websocket.Conn) {
		mt, _, _ := websockConn.ReadMessage()
		dappID, ok := websockConn.Locals("project-id").(string)
		if !ok {
			t.Fatalf("Unable to extract dappID")
		}
		websockConn.WriteMessage(mt, []byte(dappID))
	})

	app.Get("/ws", constructFiberCallbackWithHeaderAndParameterExtraction(webSocketCallback, false))

	// Bind before serving so the port is known and the bind error is not swallowed:
	// a hardcoded port silently loses the race to anything else holding it, and the
	// dial then hangs against the wrong server until the package timeout.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	defer func() {
		_ = app.Shutdown()
	}()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			url := "ws://" + ln.Addr().String() + testCase.route
			dialer := &websocket2.Dialer{}
			conn, _, err := dialer.Dial(url, testCase.headers)
			if err != nil {
				t.Fatalf("Error dialing websocket connection: %s", err)
			}
			defer conn.Close()

			err = conn.WriteMessage(websocket.TextMessage, []byte("test"))
			if err != nil {
				t.Fatalf("Error writing message to websocket connection: %s", err)
			}

			_, response, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("Error reading message from websocket connection: %s", err)
			}

			responseString := string(response)
			if responseString != testCase.expected {
				t.Errorf("Expected %s but got %s", testCase.expected, responseString)
			}
		})
	}
}

func TestExtractDappIDFromFiberContext(t *testing.T) {
	testCases := []struct {
		name     string
		headers  map[string]string
		expected string
	}{
		{
			name:     "dappId exists in headers",
			headers:  map[string]string{"project-id": "DappID123"},
			expected: "DappID123",
		},
		{
			name:     "dappId does not exist in headers",
			headers:  map[string]string{},
			expected: "DefaultDappID",
		},
	}

	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		dappID := extractDappIDFromFiberContext(c)
		return c.SendString(dappID)
	})

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			for key, value := range testCase.headers {
				req.Header.Set(key, value)
			}
			resp, _ := app.Test(req)
			body, _ := io.ReadAll(resp.Body)
			responseString := string(body)
			if responseString != testCase.expected {
				t.Errorf("Expected %s but got %s", testCase.expected, responseString)
			}
		})
	}
}

func TestParsedMessage_GetServiceApi(t *testing.T) {
	pm := baseChainMessageContainer{
		api: &spectypes.Api{},
	}
	assert.Equal(t, &spectypes.Api{}, pm.GetApi())
}

func TestParsedMessage_GetApiCollection(t *testing.T) {
	pm := baseChainMessageContainer{
		apiCollection: &spectypes.ApiCollection{},
	}
	assert.Equal(t, &spectypes.ApiCollection{}, pm.GetApiCollection())
}

func TestParsedMessage_RequestedBlock(t *testing.T) {
	pm := baseChainMessageContainer{
		latestRequestedBlock: 123,
	}
	requestedBlock, _ := pm.RequestedBlock()
	assert.Equal(t, int64(123), requestedBlock)
}

func TestParsedMessage_GetRPCMessage(t *testing.T) {
	rpcInput := &mockRPCInput{}

	pm := baseChainMessageContainer{
		msg: rpcInput,
	}
	assert.Equal(t, rpcInput, pm.GetRPCMessage())
}

type mockRPCInput struct {
	chainproxy.BaseMessage
}

func (m *mockRPCInput) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return ""
}

func (m *mockRPCInput) GetRawRequestHash() ([]byte, error) {
	return nil, fmt.Errorf("test")
}

func (m *mockRPCInput) GetParams() interface{} {
	return nil
}

func (m *mockRPCInput) GetResult() json.RawMessage {
	return nil
}

func (m *mockRPCInput) UpdateLatestBlockInMessage(uint64, bool) bool {
	return false
}

func (m *mockRPCInput) ParseBlock(block string) (int64, error) {
	return 0, nil
}

func TestGetServiceApis(t *testing.T) {
	spec := spectypes.Spec{
		Enabled: true,
		ApiCollections: []*spectypes.ApiCollection{
			{
				Enabled: true,
				CollectionData: spectypes.CollectionData{
					ApiInterface: spectypes.APIInterfaceRest,
				},
				Apis: []*spectypes.Api{
					{
						Enabled: true,
						Name:    "test-api",
					},
					{
						Enabled: true,
						Name:    "test-api-2",
					},
					{
						Enabled: false,
						Name:    "test-api-disabled",
					},
					{
						Enabled: true,
						Name:    "test-api-3",
					},
				},
			},
			{
				Enabled: true,
				CollectionData: spectypes.CollectionData{
					ApiInterface: spectypes.APIInterfaceGrpc,
				},
				Apis: []*spectypes.Api{
					{
						Enabled: true,
						Name:    "gtest-api",
					},
					{
						Enabled: true,
						Name:    "gtest-api-2",
					},
					{
						Enabled: false,
						Name:    "gtest-api-disabled",
					},
					{
						Enabled: true,
						Name:    "gtest-api-3",
					},
				},
			},
		},
	}

	rpcInterface := spectypes.APIInterfaceRest
	_, serverApis, _, _, _, _ := getServiceApis(spec, rpcInterface)

	// Test serverApis
	if len(serverApis) != 3 {
		t.Errorf("Expected serverApis length to be 3, but got %d", len(serverApis))
	}
}

func TestCheckUTXOResponseAndFixReply(t *testing.T) {
	t.Run("single_response_preserves_error_null", func(t *testing.T) {
		// BTC-family nodes return "error":null; the relay pipeline uses omitempty which strips it
		input := `{"jsonrpc":"2.0","id":"1","result":"abc","error":null}`
		result := checkUTXOResponseAndFixReply("DOGE", []byte(input))
		// Should preserve error:null and strip jsonrpc (BTC uses JSON-RPC 1.0)
		var parsed map[string]interface{}
		require.NoError(t, json.Unmarshal(result, &parsed))
		_, hasError := parsed["error"]
		require.True(t, hasError, "error field must be present (even when null)")
		_, hasJsonrpc := parsed["jsonrpc"]
		require.False(t, hasJsonrpc, "jsonrpc field should be stripped for BTC-family chains")
	})

	t.Run("batch_response_preserves_error_null", func(t *testing.T) {
		// Multi-element batch: relay pipeline reconstructs with jsonrpc:"2.0" and omitempty on error
		input := `[{"jsonrpc":"2.0","id":"1","result":"hash1"},{"jsonrpc":"2.0","id":"2","result":"hash2"}]`
		result := checkUTXOResponseAndFixReply("DOGE", []byte(input))
		var parsed []map[string]interface{}
		require.NoError(t, json.Unmarshal(result, &parsed))
		require.Len(t, parsed, 2)
		for i, elem := range parsed {
			_, hasError := elem["error"]
			require.True(t, hasError, "batch element %d must have error field", i)
			_, hasJsonrpc := elem["jsonrpc"]
			require.False(t, hasJsonrpc, "batch element %d should not have jsonrpc field", i)
		}
	})

	t.Run("single_element_batch_response", func(t *testing.T) {
		// Single-element batch must stay as array
		input := `[{"jsonrpc":"2.0","id":"1773768178254-0","result":"23699c7e"}]`
		result := checkUTXOResponseAndFixReply("DOGE", []byte(input))
		var parsed []map[string]interface{}
		require.NoError(t, json.Unmarshal(result, &parsed), "single-element batch must remain an array")
		require.Len(t, parsed, 1)
		require.Equal(t, "1773768178254-0", parsed[0]["id"])
		_, hasError := parsed[0]["error"]
		require.True(t, hasError, "error field must be present")
		_, hasJsonrpc := parsed[0]["jsonrpc"]
		require.False(t, hasJsonrpc, "jsonrpc field should be stripped")
	})

	t.Run("non_btc_chain_passthrough", func(t *testing.T) {
		input := `{"jsonrpc":"2.0","id":1,"result":"abc"}`
		replyData := []byte(input)
		result := checkUTXOResponseAndFixReply("ETH1", replyData)
		require.Equal(t, input, string(result), "non-BTC chains should pass through unchanged")
		// Zero-copy passthrough: non-UTXO chains must return the exact same backing slice
		// (same ptr + len), not a copy. This is the core guarantee of this function for the
		// hot path — regressing it would reintroduce the 4.5GB/12m alloc that motivated the fix.
		require.Same(t, &replyData[0], &result[0], "non-UTXO chains must return the input slice without copying")
	})

	t.Run("btc_with_actual_error", func(t *testing.T) {
		input := `{"id":"1","error":{"code":-8,"message":"Block height out of range"},"result":null}`
		result := checkUTXOResponseAndFixReply("BTC", []byte(input))
		var parsed map[string]interface{}
		require.NoError(t, json.Unmarshal(result, &parsed))
		errorField := parsed["error"]
		require.NotNil(t, errorField, "error field must be preserved when not null")
	})
}

func TestStripBrotliAcceptEncoding(t *testing.T) {
	cases := []struct {
		name, input, want string
		// wantPresent distinguishes "header absent" (fasthttp.Peek == nil) from
		// "header present with empty value" — the two cases are semantically
		// different for downstream content negotiation.
		wantPresent bool
	}{
		{"br_only_deletes_header", "br", "", false},
		{"br_with_gzip_and_deflate", "br, gzip, deflate", " gzip, deflate", true},
		{"br_with_qvalues", "br;q=1.0, gzip;q=0.8", " gzip;q=0.8", true},
		{"gzip_only_no_op", "gzip, deflate", "gzip, deflate", true},
		{"uppercase_BR_stripped", "BR, gzip", " gzip", true},
		{"br_not_a_token_preserved", "gzip, xbr, deflate", "gzip, xbr, deflate", true},
		{"empty_header_no_op", "", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			var seen string
			var present bool
			app.Use(stripBrotliAcceptEncoding)
			app.Get("/", func(c *fiber.Ctx) error {
				seen = c.Get(fiber.HeaderAcceptEncoding)
				present = c.Request().Header.Peek(fiber.HeaderAcceptEncoding) != nil
				return c.SendStatus(fiber.StatusOK)
			})

			req := httptest.NewRequest(fiber.MethodGet, "/", nil)
			if tc.input != "" {
				req.Header.Set(fiber.HeaderAcceptEncoding, tc.input)
			}
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.want, seen)
			require.Equal(t, tc.wantPresent, present, "header presence mismatch (absent vs empty-value)")
		})
	}
}

func TestApplyResponseCompression(t *testing.T) {
	// Payload large enough to exceed fasthttp's built-in minimum compression threshold.
	payload := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":"%s"}`, strings.Repeat("a", 4096)))

	cases := []struct {
		name             string
		mode             string
		acceptEncoding   string
		wantEncoding     string // "" means no Content-Encoding header
		wantBodyPassThru bool   // true means response body should equal payload byte-for-byte
	}{
		{"off_mode_no_compression", common.ResponseCompressionOff, "br, gzip, deflate", "", true},
		{"brotli_mode_encodes_br_when_advertised", common.ResponseCompressionBrotli, "br, gzip", "br", false},
		{"brotli_mode_falls_back_to_gzip_when_br_absent", common.ResponseCompressionBrotli, "gzip, deflate", "gzip", false},
		{"gzip_mode_strips_br_and_falls_back_to_gzip", common.ResponseCompressionGzip, "br, gzip, deflate", "gzip", false},
		{"gzip_mode_with_no_client_br_still_uses_gzip", common.ResponseCompressionGzip, "gzip", "gzip", false},
		{"unknown_mode_defaults_to_gzip", "something-unknown", "br, gzip", "gzip", false},
		{"empty_mode_defaults_to_gzip", "", "br, gzip", "gzip", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			applyResponseCompression(app, tc.mode)
			app.Get("/", func(c *fiber.Ctx) error {
				c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
				return c.Send(payload)
			})

			req := httptest.NewRequest(fiber.MethodGet, "/", nil)
			req.Header.Set(fiber.HeaderAcceptEncoding, tc.acceptEncoding)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, tc.wantEncoding, resp.Header.Get(fiber.HeaderContentEncoding),
				"unexpected Content-Encoding")

			if tc.wantBodyPassThru {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, payload, body, "off mode must return raw bytes")
			}
		})
	}
}

func TestAddHeadersAndSendBytes(t *testing.T) {
	t.Run("writes_body_bytes_and_metadata_headers", func(t *testing.T) {
		payload := []byte(`{"jsonrpc":"2.0","id":1,"result":"0xdeadbeef"}`)
		meta := []pairingtypes.Metadata{
			{Name: "X-Test-Trace-Id", Value: "abc-123"},
			{Name: "Content-Type", Value: "application/json"},
		}

		app := fiber.New()
		app.Get("/", func(c *fiber.Ctx) error {
			return addHeadersAndSendBytes(c, meta, payload)
		})

		req := httptest.NewRequest(fiber.MethodGet, "/", nil)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, fiber.StatusOK, resp.StatusCode)
		require.Equal(t, payload, body, "response body must match the []byte payload exactly")
		require.Equal(t, "abc-123", resp.Header.Get("X-Test-Trace-Id"))
		require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	})

	t.Run("empty_body_is_allowed", func(t *testing.T) {
		app := fiber.New()
		app.Get("/", func(c *fiber.Ctx) error {
			return addHeadersAndSendBytes(c, nil, nil)
		})

		req := httptest.NewRequest(fiber.MethodGet, "/", nil)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, fiber.StatusOK, resp.StatusCode)
		require.Empty(t, body)
	})

	t.Run("preserves_binary_payload_bytes", func(t *testing.T) {
		// The whole point of the []byte signature is that non-UTF8 / binary content
		// isn't mangled by a []byte → string → []byte round-trip. Verify with a
		// payload that includes every byte value, including embedded NULs.
		payload := make([]byte, 256)
		for i := range payload {
			payload[i] = byte(i)
		}

		app := fiber.New()
		app.Get("/", func(c *fiber.Ctx) error {
			return addHeadersAndSendBytes(c, nil, payload)
		})

		req := httptest.NewRequest(fiber.MethodGet, "/", nil)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, payload, body, "binary payload must round-trip byte-for-byte")
	})
}

func TestCompareRequestedBlockInBatch(t *testing.T) {
	playbook := []struct {
		latest           int64
		earliest         int64
		parsed           int64
		expectedLatest   int64
		expectedEarliest int64
	}{
		{
			latest:           spectypes.LATEST_BLOCK,
			earliest:         spectypes.LATEST_BLOCK,
			parsed:           spectypes.LATEST_BLOCK,
			expectedLatest:   spectypes.LATEST_BLOCK,
			expectedEarliest: spectypes.LATEST_BLOCK,
		},
		{
			latest:           10,
			earliest:         5,
			parsed:           7,
			expectedLatest:   10,
			expectedEarliest: 5,
		},
		{
			latest:           10,
			earliest:         5,
			parsed:           2,
			expectedLatest:   10,
			expectedEarliest: 2,
		},
		{
			latest:           10,
			earliest:         5,
			parsed:           12,
			expectedLatest:   12,
			expectedEarliest: 5,
		},
		{
			latest:           spectypes.LATEST_BLOCK,
			earliest:         5,
			parsed:           10,
			expectedLatest:   spectypes.LATEST_BLOCK,
			expectedEarliest: 5,
		},
		{
			latest:           10,
			earliest:         5,
			parsed:           spectypes.LATEST_BLOCK,
			expectedLatest:   spectypes.LATEST_BLOCK,
			expectedEarliest: 5,
		},
		{
			latest:           10,
			earliest:         5,
			parsed:           spectypes.LATEST_BLOCK,
			expectedLatest:   spectypes.LATEST_BLOCK,
			expectedEarliest: 5,
		},
		{
			latest:           10,
			earliest:         spectypes.EARLIEST_BLOCK,
			parsed:           2,
			expectedLatest:   10,
			expectedEarliest: spectypes.EARLIEST_BLOCK,
		},
		{
			latest:           10,
			earliest:         5,
			parsed:           spectypes.EARLIEST_BLOCK,
			expectedLatest:   10,
			expectedEarliest: spectypes.EARLIEST_BLOCK,
		},
		{
			latest:           spectypes.LATEST_BLOCK,
			earliest:         spectypes.EARLIEST_BLOCK,
			parsed:           5,
			expectedLatest:   spectypes.LATEST_BLOCK,
			expectedEarliest: spectypes.EARLIEST_BLOCK,
		},
		{
			latest:           spectypes.EARLIEST_BLOCK,
			earliest:         spectypes.LATEST_BLOCK,
			parsed:           5,
			expectedLatest:   5,
			expectedEarliest: 5,
		},
		{
			latest:           spectypes.LATEST_BLOCK,
			earliest:         spectypes.EARLIEST_BLOCK,
			parsed:           spectypes.NOT_APPLICABLE,
			expectedLatest:   spectypes.NOT_APPLICABLE,
			expectedEarliest: spectypes.EARLIEST_BLOCK,
		},
		{
			latest:           4,
			earliest:         spectypes.EARLIEST_BLOCK,
			parsed:           spectypes.NOT_APPLICABLE,
			expectedLatest:   spectypes.NOT_APPLICABLE,
			expectedEarliest: spectypes.EARLIEST_BLOCK,
		},
		{
			latest:           4,
			earliest:         2,
			parsed:           spectypes.NOT_APPLICABLE,
			expectedLatest:   spectypes.NOT_APPLICABLE,
			expectedEarliest: spectypes.NOT_APPLICABLE,
		},
	}

	for _, test := range playbook {
		testName := fmt.Sprintf("latest=%d_earliest=%d_parsed=%d", test.latest, test.earliest, test.parsed)
		t.Run(testName, func(t *testing.T) {
			latest, earliest := CompareRequestedBlockInBatch(test.latest, test.earliest, test.parsed)
			require.Equal(t, test.expectedLatest, latest, "latest")
			require.Equal(t, test.expectedEarliest, earliest, "earliest")
		})
	}
}

// alwaysHealthyReporter is a HealthReporter that always reports healthy.
// Used by the buffer-size tests below to satisfy createAndSetupBaseAppListener's signature.
type alwaysHealthyReporter struct{}

func (alwaysHealthyReporter) IsHealthy() bool { return true }

// TestCreateAndSetupBaseAppListener_HandlesLargeHeaders pins fasthttp's request-buffer
// ceiling. Fiber's default ReadBufferSize is 4 KiB, which caps the combined size of the
// HTTP request line + all headers; above that fasthttp returns 431 before any handler
// runs. mTLS deployments forward the client cert via X-Forwarded-Client-Cert (XFCC), and
// a PEM-encoded cert plus chain routinely exceeds 4 KiB, so the default broke every
// mTLS-forwarded relay.
//
// Test goes over a real TCP socket — Fiber's in-process `app.Test` bypasses the network
// path and won't exercise ReadBufferSize.
func TestCreateAndSetupBaseAppListener_HandlesLargeHeaders(t *testing.T) {
	cmdFlags := common.ConsumerCmdFlags{
		HeadersFlag:      "*",
		CredentialsFlag:  "true",
		OriginFlag:       "*",
		MethodsFlag:      "GET,POST,OPTIONS",
		CDNCacheDuration: "86400",
	}

	app := createAndSetupBaseAppListener(cmdFlags, "/health", alwaysHealthyReporter{})
	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	serverErrCh := make(chan error, 1)
	go func() { serverErrCh <- app.Listener(ln) }()
	defer func() {
		_ = app.Shutdown()
		<-serverErrCh
	}()

	// Poll until the listener actually accepts connections — app.Listener is async.
	addr := ln.Addr().String()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond, "fiber listener never came up")

	// Build a realistic XFCC header. Real mTLS proxies emit a few fixed fields plus
	// `Cert="<URL-encoded PEM>"`; here we just need the total header bytes to clear the
	// 4 KiB default — 8 KiB of base64-ish padding is plenty without being slow to write.
	const padLen = 8 * 1024
	xfccPayload := "By=spiffe://cluster.local/ns/smart-router-system/sa/default" +
		";Hash=" + strings.Repeat("a", 64) +
		";Subject=\"CN=fake\"" +
		";URI=spiffe://cluster.local/ns/test/sa/test" +
		";Cert=\"-----BEGIN CERTIFICATE-----%0A" + strings.Repeat("A", padLen) + "%0A-----END CERTIFICATE-----\""

	tests := []struct {
		name       string
		xfcc       string
		wantStatus int
	}{
		{
			name:       "small_headers_succeed",
			xfcc:       "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "large_xfcc_header_succeeds_after_buffer_bump",
			xfcc:       xfccPayload,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			require.NoError(t, err)
			defer conn.Close()

			require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))

			var req strings.Builder
			req.WriteString("GET / HTTP/1.1\r\n")
			req.WriteString("Host: " + addr + "\r\n")
			if tc.xfcc != "" {
				req.WriteString("X-Forwarded-Client-Cert: " + tc.xfcc + "\r\n")
				// Sanity-check the request actually exceeds Fiber's old 4 KiB default,
				// otherwise the test is silently a tautology.
				require.Greater(t, len(tc.xfcc), 4096, "test header must exceed the old 4 KiB default")
			}
			req.WriteString("Connection: close\r\n\r\n")

			_, err = conn.Write([]byte(req.String()))
			require.NoError(t, err)

			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, tc.wantStatus, resp.StatusCode,
				"expected %d, got %d (header size = %d B). 431 means fasthttp's ReadBufferSize is back at the 4 KiB default.",
				tc.wantStatus, resp.StatusCode, len(tc.xfcc))
		})
	}
}

// TestConstructFiberCallback_StashesOriginInLocals exercises the end-to-end
// flow that AddMetricForWebSocket relies on: the HTTP-upgrade handler
// extracts Origin from the fasthttp request headers, clones it, and stashes
// it in fiber Locals under metrics.OriginHeaderKey so the websocket handler
// can read it after the upgrade. Catches regressions where the Locals
// storage is moved out of the metric-enabled branch or the key is changed.
func TestConstructFiberCallback_StashesOriginInLocals(t *testing.T) {
	app := fiber.New()
	captured := make(chan string, 1)

	webSocketCallback := websocket.New(func(c *websocket.Conn) {
		origin, _ := c.Locals(metrics.OriginHeaderKey).(string)
		captured <- origin
		c.Close()
	})

	app.Get("/ws", constructFiberCallbackWithHeaderAndParameterExtraction(webSocketCallback, true))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	defer func() { _ = app.Shutdown() }()

	dialer := &websocket2.Dialer{}
	conn, _, err := dialer.Dial("ws://"+ln.Addr().String()+"/ws", http.Header{"Origin": {"https://test.example"}})
	require.NoError(t, err)
	defer conn.Close()

	select {
	case got := <-captured:
		require.Equal(t, "https://test.example", got)
	case <-time.After(2 * time.Second):
		t.Fatal("Origin never reached websocket handler via Locals")
	}
}

// TestConstructFiberCallback_NoOriginWhenMetricsDisabled covers the negative
// branch: when isMetricEnabled=false the Origin Locals is intentionally
// absent, so AddMetricForWebSocket reads back an empty string. Ensures the
// flag still gates the extraction work.
func TestConstructFiberCallback_NoOriginWhenMetricsDisabled(t *testing.T) {
	app := fiber.New()
	captured := make(chan string, 1)

	webSocketCallback := websocket.New(func(c *websocket.Conn) {
		origin, _ := c.Locals(metrics.OriginHeaderKey).(string)
		captured <- origin
		c.Close()
	})

	app.Get("/ws", constructFiberCallbackWithHeaderAndParameterExtraction(webSocketCallback, false))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	defer func() { _ = app.Shutdown() }()

	dialer := &websocket2.Dialer{}
	conn, _, err := dialer.Dial("ws://"+ln.Addr().String()+"/ws", http.Header{"Origin": {"https://test.example"}})
	require.NoError(t, err)
	defer conn.Close()

	select {
	case got := <-captured:
		require.Empty(t, got)
	case <-time.After(2 * time.Second):
		t.Fatal("websocket handler never ran")
	}
}

// restApiContainers builds a serverApis map through the same constructor the spec loader
// uses, so these cases exercise the real compiled pattern and ranking rather than a
// hand-copied one — and so every container carries the precompiled matcher the lookup
// requires. Building one by hand would key an api the lookup then drops.
func restApiContainers(tb testing.TB, connectionType string, apis ...*spectypes.Api) map[ApiKey]ApiContainer {
	tb.Helper()
	serverApis := map[ApiKey]ApiContainer{}
	for _, api := range apis {
		apiKey, apiContainer, err := newRestApiContainer(api, CollectionKey{ConnectionType: connectionType})
		require.NoError(tb, err)
		serverApis[apiKey] = apiContainer
	}
	return serverApis
}

// restApis is restApiContainers for the cases where only the api name matters.
func restApis(tb testing.TB, connectionType string, apiNames ...string) map[ApiKey]ApiContainer {
	tb.Helper()
	apis := make([]*spectypes.Api, 0, len(apiNames))
	for _, apiName := range apiNames {
		apis = append(apis, &spectypes.Api{Name: apiName, Enabled: true, ComputeUnits: 10})
	}
	return restApiContainers(tb, connectionType, apis...)
}

// TestMatchSpecApiByNameTrailingSlash covers the slash-insensitive fallback: specs name
// apis both with and without a trailing slash and clients send either form, but the
// compiled name is anchored so the slash alone used to decide the match. A miss is not
// only a metrics problem — it falls through to defaultApiContainer, which bills a flat 20
// compute units and pins block parsing to latest.
func TestMatchSpecApiByNameTrailingSlash(t *testing.T) {
	t.Parallel()
	connectionType := "GET"

	testTable := []struct {
		name         string
		apiNames     []string
		inputName    string
		expectedName string
		expectedOk   bool
	}{
		{
			// The production shape: TEZOS names the api without a trailing slash, a client
			// polls with one, and every concrete block number became its own Default- api.
			name:         "spec omits the slash, request carries it",
			apiNames:     []string{"/chains/main/blocks/{block_id}/header"},
			inputName:    "/chains/main/blocks/9427283/header/",
			expectedName: "/chains/main/blocks/{block_id}/header",
			expectedOk:   true,
		},
		{
			name:         "a named block_id is matched the same way",
			apiNames:     []string{"/chains/main/blocks/{block_id}/header"},
			inputName:    "/chains/main/blocks/head/header/",
			expectedName: "/chains/main/blocks/{block_id}/header",
			expectedOk:   true,
		},
		{
			// The reverse direction: STACKS names 11 apis WITH a trailing slash.
			name:         "spec carries the slash, request omits it",
			apiNames:     []string{"/extended/v1/tx/"},
			inputName:    "/extended/v1/tx",
			expectedName: "/extended/v1/tx/",
			expectedOk:   true,
		},
		{
			name:         "path with no placeholder still matches with a slash",
			apiNames:     []string{"/chains/main/chain_id"},
			inputName:    "/chains/main/chain_id/",
			expectedName: "/chains/main/chain_id",
			expectedOk:   true,
		},
		{
			// ARWEAVE, APT1 and XLM each name a real api exactly "/" — trimming must not
			// reduce it to the empty string and match everything.
			name:         "the root api keeps matching",
			apiNames:     []string{"/"},
			inputName:    "/",
			expectedName: "/",
			expectedOk:   true,
		},
		{
			// The fallback must never re-route a request that matches an api as sent:
			// both apis are present and the exactly-matching one has to win regardless of
			// map iteration order.
			name:         "an exact match wins over the slash-insensitive fallback",
			apiNames:     []string{"/extended/v1/tx/", "/extended/v1/tx"},
			inputName:    "/extended/v1/tx",
			expectedName: "/extended/v1/tx",
			expectedOk:   true,
		},
		{
			name:         "an exact match wins in the other direction too",
			apiNames:     []string{"/extended/v1/tx/", "/extended/v1/tx"},
			inputName:    "/extended/v1/tx/",
			expectedName: "/extended/v1/tx/",
			expectedOk:   true,
		},
		{
			// STACKS names apis with a trailing slash AND a placeholder before it, the one
			// shape where the two patterns differ in more than their last character.
			name:         "spec carries the slash after a placeholder",
			apiNames:     []string{"/extended/v1/address/{principal}/balances/"},
			inputName:    "/extended/v1/address/SP2J6ZY48GV1EZ5V2V5RB9MP66SW86PYKKNRV9EJ7/balances",
			expectedName: "/extended/v1/address/{principal}/balances/",
			expectedOk:   true,
		},
		{
			// Relaxing the slash must not relax anything else: a genuinely unspecced path
			// still misses, so the Default- fallthrough keeps reporting real spec gaps.
			name:       "an unspecced path still misses",
			apiNames:   []string{"/chains/main/blocks/{block_id}/header"},
			inputName:  "/chains/main/blocks/9427283/header/shell/",
			expectedOk: false,
		},
		{
			name:       "a sibling path is not swallowed by the relaxed match",
			apiNames:   []string{"/chains/main/blocks/{block_id}/header"},
			inputName:  "/chains/main/blocks/9427283/metadata/",
			expectedOk: false,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// Repeated to catch an order-dependent answer: serverApis is a map, so a case
			// with two candidates would otherwise pass or fail at random.
			for i := 0; i < 32; i++ {
				api, ok := matchSpecApiByName(testCase.inputName, connectionType, restApis(t, connectionType, testCase.apiNames...))
				require.Equal(t, testCase.expectedOk, ok)
				if testCase.expectedOk {
					require.Equal(t, testCase.expectedName, api.api.Name)
				}
			}
		})
	}
}

// TestMatchSpecApiByNameRanksMostSpecific pins the answer when more than one api covers
// the requested path. Specs pair a literal with the {placeholder} sibling that swallows it
// all over the catalog — ARWEAVE /info next to /{id}, CARDANO /blocks/latest next to
// /blocks/{hash_or_number} — and serverApis is a map, so before ranking the winner was Go
// iteration order: a coin flip per call between apis that carry different compute units
// (ARWEAVE /block_index is 1000, /{id} is 20) and different block parsing.
//
// Every case runs 32× for that reason: a two-candidate case decided by map order passes at
// random, and a single run proves nothing.
func TestMatchSpecApiByNameRanksMostSpecific(t *testing.T) {
	t.Parallel()
	connectionType := "GET"

	testTable := []struct {
		name         string
		apiNames     []string
		inputName    string
		expectedName string
	}{
		{
			// ARWEAVE names /{id} at the root, so it covers every one-segment path in the
			// spec — including the api named exactly "/".
			name:         "the root api beats the catch-all placeholder",
			apiNames:     []string{"/", "/{id}"},
			inputName:    "/",
			expectedName: "/",
		},
		{
			name:         "a literal beats the catch-all placeholder",
			apiNames:     []string{"/info", "/{id}"},
			inputName:    "/info",
			expectedName: "/info",
		},
		{
			name:         "a literal beats the catch-all placeholder with a trailing slash too",
			apiNames:     []string{"/info", "/{id}"},
			inputName:    "/info/",
			expectedName: "/info",
		},
		{
			// The pair this repo's own specs carry (cosmossdk.json), reached through the
			// slash-insensitive fallback rather than an exact match.
			name: "a literal beats its templated sibling",
			apiNames: []string{
				"/cosmos/base/tendermint/v1beta1/blocks/latest",
				"/cosmos/base/tendermint/v1beta1/blocks/{height}",
			},
			inputName:    "/cosmos/base/tendermint/v1beta1/blocks/latest/",
			expectedName: "/cosmos/base/tendermint/v1beta1/blocks/latest",
		},
		{
			// Same pair without the slash: both patterns match the path as sent, so this
			// one was already order-dependent before the fallback existed.
			name: "a literal beats its templated sibling on the exact path too",
			apiNames: []string{
				"/cosmos/base/tendermint/v1beta1/blocks/latest",
				"/cosmos/base/tendermint/v1beta1/blocks/{height}",
			},
			inputName:    "/cosmos/base/tendermint/v1beta1/blocks/latest",
			expectedName: "/cosmos/base/tendermint/v1beta1/blocks/latest",
		},
		{
			name:         "a templated block id still resolves when no literal covers it",
			apiNames:     []string{"/blocks/latest", "/blocks/{hash_or_number}"},
			inputName:    "/blocks/9427283/",
			expectedName: "/blocks/{hash_or_number}",
		},
		{
			// MORALIS: fewer placeholders wins even when neither name is fully literal.
			name:         "fewer placeholders wins",
			apiNames:     []string{"/nft/{address}/metadata", "/nft/{address}/{token_id}"},
			inputName:    "/nft/0x1234/metadata/",
			expectedName: "/nft/{address}/metadata",
		},
		{
			name:         "a literal beats its templated sibling on a slash-named spec",
			apiNames:     []string{"/v3/tenures/info", "/v3/tenures/{block_id}"},
			inputName:    "/v3/tenures/info/",
			expectedName: "/v3/tenures/info",
		},
		{
			// KNOWN LIMITATION, pinned deliberately: a trailing placeholder compiles to a
			// pattern that also matches empty, so the list path with a slash is an EXACT
			// match for the item api and never reaches the fallback that would prefer the
			// list. Changing it would re-route requests that resolve this way today, which
			// is out of scope here.
			name:         "a list path with a slash still resolves to its item api",
			apiNames:     []string{"/cosmos/gov/v1/proposals", "/cosmos/gov/v1/proposals/{proposal_id}"},
			inputName:    "/cosmos/gov/v1/proposals/",
			expectedName: "/cosmos/gov/v1/proposals/{proposal_id}",
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			for i := 0; i < 32; i++ {
				api, ok := matchSpecApiByName(testCase.inputName, connectionType, restApis(t, connectionType, testCase.apiNames...))
				require.True(t, ok)
				require.Equal(t, testCase.expectedName, api.api.Name)
			}
		})
	}
}

// TestRestApiKeyCollapsesNamesWithTheSameShape documents what happens when a spec names
// one path twice — COSMOSSDK has /cosmos/auth/v1beta1/bech32/{address_bytes} next to
// {address_string}, CANTO and ELYS do the same. Placeholder identifiers are erased by the
// name→pattern transform, so both names key the same ApiKey and the later one replaces the
// earlier at load. The lookup is therefore never ambiguous between them; whichever survives
// is a property of the spec file, not of map iteration order.
func TestRestApiKeyCollapsesNamesWithTheSameShape(t *testing.T) {
	t.Parallel()

	serverApis := restApis(t, "GET",
		"/cosmos/auth/v1beta1/bech32/{address_string}",
		"/cosmos/auth/v1beta1/bech32/{address_bytes}",
	)
	require.Len(t, serverApis, 1)

	for i := 0; i < 32; i++ {
		api, ok := matchSpecApiByName("/cosmos/auth/v1beta1/bech32/lava@1abc/", "GET", serverApis)
		require.True(t, ok)
		require.Equal(t, "/cosmos/auth/v1beta1/bech32/{address_bytes}", api.api.Name)
	}
}

// TestRestApiMatcherOrdering exercises moreSpecificThan directly, including the lexicographic
// last resort. No shape in the catalog reaches that step today, but it is not unreachable the
// way collapsing names like {address_bytes}/{address_string} are (see the test above): two
// names can rank equal and still compile to different patterns, so they key different
// ApiKeys and both reach the lookup. It is what keeps the ordering total — without it two
// candidates could each claim to outrank the other and the winner would depend on map
// iteration order again.
func TestRestApiMatcherOrdering(t *testing.T) {
	t.Parallel()

	matcherFor := func(apiName string) *restApiMatcher {
		matcher, err := buildRestApiMatcher(apiName, restApiNameToRegex(apiName))
		require.NoError(t, err)
		return matcher
	}

	literal := matcherFor("/blocks/latest")
	templated := matcherFor("/blocks/{height}")
	twoPlaceholders := matcherFor("/blocks/{height}/{index}")

	// A match on the path as sent outranks a slash-insensitive one, whatever the names are.
	require.True(t, templated.moreSpecificThan(true, literal, false))
	require.False(t, literal.moreSpecificThan(false, templated, true))

	// Within a tier, fewer placeholders wins, then more literal characters.
	require.True(t, literal.moreSpecificThan(true, templated, true))
	require.False(t, templated.moreSpecificThan(true, literal, true))
	require.True(t, templated.moreSpecificThan(false, twoPlaceholders, false))

	// Equal rank falls back to the api name, which is stable across restarts. Asserted in
	// both directions: exactly one of the two must win.
	first := matcherFor("/a/{x}")
	second := matcherFor("/b/{x}")
	require.True(t, first.moreSpecificThan(true, second, true))
	require.False(t, second.moreSpecificThan(true, first, true))

	// The last resort is reachable by two names that DON'T collapse into one ApiKey, so it
	// is not dead code: these compile to /a/[^\/\s]+/c and /a/b/[^\/\s]*, both cover /a/b/c,
	// and both carry one placeholder and five literal characters.
	crossA := matcherFor("/a/{x}/c")
	crossB := matcherFor("/a/b/{y}")
	require.NotEqual(t, restApiNameToRegex("/a/{x}/c"), restApiNameToRegex("/a/b/{y}"))
	require.True(t, crossA.pattern.MatchString("/a/b/c"))
	require.True(t, crossB.pattern.MatchString("/a/b/c"))
	require.Equal(t, crossA.placeholders, crossB.placeholders)
	require.Equal(t, crossA.literalLen, crossB.literalLen)

	// '{' sorts above every character a path segment holds, so the name comparison resolves
	// it the way an HTTP router would: literal beats placeholder at the first differing
	// segment, here /a/b/{y} over /a/{x}/c.
	require.True(t, crossB.moreSpecificThan(true, crossA, true))
	require.False(t, crossA.moreSpecificThan(true, crossB, true))

	// And end to end: the winner is the same on every one of 32 map iteration orders.
	for i := 0; i < 32; i++ {
		api, ok := matchSpecApiByName("/a/b/c", "GET", restApis(t, "GET", "/a/{x}/c", "/a/b/{y}"))
		require.True(t, ok)
		require.Equal(t, "/a/b/{y}", api.api.Name)
	}
}

// TestMatchSpecApiByNameTrailingSlashConnectionType guards the fallback against widening
// the match across connection types — a GET api must not answer a POST.
func TestMatchSpecApiByNameTrailingSlashConnectionType(t *testing.T) {
	t.Parallel()

	serverApis := restApis(t, "GET", "/chains/main/blocks/{block_id}/header")
	_, ok := matchSpecApiByName("/chains/main/blocks/9427283/header/", "POST", serverApis)
	require.False(t, ok)
}

// BenchmarkMatchSpecApiByName sizes the lookup against a spec the size of a real one (TEZOS
// carries 219 apis). Every candidate is now ranked instead of returning on the first hit,
// which is only affordable because the patterns are compiled at spec load rather than per
// lookup.
func BenchmarkMatchSpecApiByName(b *testing.B) {
	apiNames := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		apiNames = append(apiNames, fmt.Sprintf("/chains/main/blocks/{block_id}/pad%d/header", i))
	}
	serverApis := restApis(b, "GET", apiNames...)

	for _, benchCase := range []struct {
		name string
		path string
	}{
		{name: "hit", path: "/chains/main/blocks/9427283/pad180/header"},
		{name: "hit_trailing_slash", path: "/chains/main/blocks/9427283/pad180/header/"},
		{name: "miss", path: "/chains/main/blocks/9427283/nothing/header"},
	} {
		b.Run(benchCase.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				matchSpecApiByName(benchCase.path, "GET", serverApis)
			}
		})
	}
}
