package rpcInterfaceMessages

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/parser"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// selfDescribedCBORTag is the tag the IC wraps every body in (RFC 8949 "self-described CBOR").
const selfDescribedCBORTag = 55799

// mustCBOR encodes v as CBOR or fails the test.
func mustCBOR(t *testing.T, v interface{}) []byte {
	t.Helper()
	encoded, err := cbor.Marshal(v)
	require.NoError(t, err)
	return encoded
}

// icStatusBody builds a body shaped exactly like a real GET /api/v2/status reply:
// tag 55799 wrapping a 2-entry map of {root_key: <blob>, replica_health_status: "healthy"}.
// The live capture against icp-api.io showed precisely these two fields.
func icStatusBody(t *testing.T, rootKey []byte) []byte {
	t.Helper()
	return mustCBOR(t, cbor.Tag{
		Number: selfDescribedCBORTag,
		Content: map[interface{}]interface{}{
			"root_key":              rootKey,
			"replica_health_status": "healthy",
		},
	})
}

func TestCBORToJSON(t *testing.T) {
	rootKey := []byte{0x30, 0x81, 0x82, 0x30, 0x1d, 0x06, 0x0d, 0x2b}

	tests := []struct {
		name     string
		input    []byte
		wantJSON map[string]interface{}
		wantErr  string
	}{
		{
			name:  "IC status body: tag unwrapped, blob to hex, text preserved",
			input: icStatusBody(t, rootKey),
			wantJSON: map[string]interface{}{
				"root_key":              hex.EncodeToString(rootKey),
				"replica_health_status": "healthy",
			},
		},
		{
			name:  "plain map with no tag",
			input: mustCBOR(t, map[interface{}]interface{}{"a": "b"}),
			wantJSON: map[string]interface{}{
				"a": "b",
			},
		},
		{
			name: "nested map and array are transcoded recursively",
			input: mustCBOR(t, map[interface{}]interface{}{
				"outer": map[interface{}]interface{}{
					"blob": []byte{0xde, 0xad, 0xbe, 0xef},
				},
				"list": []interface{}{"x", []byte{0x01, 0x02}},
			}),
			wantJSON: map[string]interface{}{
				"outer": map[string]interface{}{"blob": "deadbeef"},
				"list":  []interface{}{"x", "0102"},
			},
		},
		{
			name: "integer map keys are coerced to strings",
			input: mustCBOR(t, map[interface{}]interface{}{
				1: "one",
			}),
			wantJSON: map[string]interface{}{
				"1": "one",
			},
		},
		{
			// Built from raw CBOR because Go maps cannot have []byte keys:
			//   a1          map(1)
			//   42 ab cd    byte string(2) 0xabcd   <- the key
			//   61 76       text(1) "v"
			name:  "byte-string map keys become hex",
			input: []byte{0xa1, 0x42, 0xab, 0xcd, 0x61, 0x76},
			wantJSON: map[string]interface{}{
				"abcd": "v",
			},
		},
		{
			name: "scalars survive: bool, null, number",
			input: mustCBOR(t, map[interface{}]interface{}{
				"b": true,
				"n": nil,
				"i": uint64(42),
			}),
			wantJSON: map[string]interface{}{
				"b": true,
				"n": nil,
				"i": float64(42), // JSON numbers round-trip as float64
			},
		},
		{
			name:    "malformed CBOR is rejected, not silently passed through",
			input:   []byte{0xff, 0xff, 0xff, 0xff},
			wantErr: "failed decoding CBOR body",
		},
		{
			name:    "empty body is rejected",
			input:   []byte{},
			wantErr: "cannot decode an empty CBOR body",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cborToJSON(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)

			var decoded map[string]interface{}
			require.NoError(t, json.Unmarshal(got, &decoded), "transcoder must emit valid JSON")
			require.Equal(t, tt.wantJSON, decoded)
		})
	}
}

// A blob must render as hex, never base64. Getting this wrong is the failure mode
// that decodes perfectly and then fails every expected_value comparison written in
// hex — the response looks fine and the verification still fails.
func TestCBORBlobsRenderAsHexNotBase64(t *testing.T) {
	raw := []byte{0xde, 0xad, 0xbe, 0xef}
	got, err := cborToJSON(mustCBOR(t, map[interface{}]interface{}{"k": raw}))
	require.NoError(t, err)

	require.Contains(t, string(got), `"deadbeef"`)
	require.NotContains(t, string(got), "3q2+7w==", "blob must not be base64-encoded")
}

// Composite map keys have no unambiguous JSON representation and must never be
// flattened into something misleading. In practice the CBOR decoder rejects them
// before our normalizer is reached ("invalid map key type"); cborMapKeyToString
// keeps its own guard as defence in depth for values that arrive by other paths.
// What matters is that the body is refused rather than silently mangled.
func TestCBORCompositeMapKeyIsRejected(t *testing.T) {
	//   a1                 map(1)
	//   82 01 02           array(2) [1, 2]   <- composite key
	//   61 76              text(1) "v"
	_, err := cborToJSON([]byte{0xa1, 0x82, 0x01, 0x02, 0x61, 0x76})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid map key type")
}

// Direct coverage of the normalizer's own composite-key guard, which the decoder
// shields in the end-to-end path above.
func TestCBORMapKeyToStringRejectsComposites(t *testing.T) {
	_, err := cborMapKeyToString([]interface{}{1, 2})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no JSON object-key representation")

	_, err = cborMapKeyToString(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "null")
}

func TestRestMessageNewParsableRPCInput(t *testing.T) {
	rootKey := []byte{0x01, 0x02, 0x03}

	t.Run("CBOR collection transcodes the body", func(t *testing.T) {
		rm := RestMessage{Path: "/api/v2/status", Encoding: spectypes.CollectionEncodingCBOR}
		require.True(t, rm.IsCBOR())

		input, err := rm.NewParsableRPCInput(icStatusBody(t, rootKey))
		require.NoError(t, err)

		var decoded map[string]interface{}
		require.NoError(t, json.Unmarshal(input.GetResult(), &decoded))
		require.Equal(t, hex.EncodeToString(rootKey), decoded["root_key"])
	})

	// Declaring NewParsableRPCInput makes every RestMessage satisfy
	// CustomParsingMessage, so JSON collections now flow through it too. They must
	// come out byte-identical to what DefaultParsableRPCInput would have produced.
	t.Run("JSON collection passes through untouched", func(t *testing.T) {
		rm := RestMessage{Path: "/cosmos/base/tendermint/v1beta1/blocks/latest"}
		require.False(t, rm.IsCBOR())

		body := []byte(`{"block":{"header":{"height":"123"}}}`)
		input, err := rm.NewParsableRPCInput(body)
		require.NoError(t, err)
		require.JSONEq(t, string(body), string(input.GetResult()))
	})

	t.Run("malformed CBOR surfaces an error instead of a bad parse", func(t *testing.T) {
		rm := RestMessage{Path: "/api/v2/status", Encoding: spectypes.CollectionEncodingCBOR}
		_, err := rm.NewParsableRPCInput([]byte{0xff, 0xff})
		require.Error(t, err)
	})
}

// End-to-end proof of the L1 goal: a CBOR /api/v2/status body must flow through the
// transcode seam into the ordinary spec parser and yield the value the chain-id
// verification compares against. The parser is untouched — it only ever sees JSON.
func TestCBORStatusFeedsChainIDVerification(t *testing.T) {
	rootKey := []byte{0x30, 0x81, 0x82, 0x30, 0x1d, 0x06, 0x0d, 0x2b, 0x06, 0x01}
	expectedValue := hex.EncodeToString(rootKey)

	rm := RestMessage{Path: "/api/v2/status", Encoding: spectypes.CollectionEncodingCBOR}
	parserInput, err := rm.NewParsableRPCInput(icStatusBody(t, rootKey))
	require.NoError(t, err)

	// Exactly the directive the drafted origyn.json uses for its chain-id check.
	blockParser := spectypes.BlockParser{
		ParserArg:  []string{"0", "root_key"},
		ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL,
	}

	parsed := parser.ParseBlockFromReply(parserInput, blockParser, nil)
	require.NotNil(t, parsed)
	require.Equal(t, expectedValue, parsed.GetRawParsedData(),
		"PARSE_CANONICAL must extract root_key as hex so it matches the spec's expected_value")
	require.True(t, strings.HasPrefix(parsed.GetRawParsedData(), "308182"),
		"root_key must keep its DER prefix")
}
