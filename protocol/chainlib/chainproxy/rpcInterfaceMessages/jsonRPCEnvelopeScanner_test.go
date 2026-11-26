package rpcInterfaceMessages

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanJsonrpcEnvelope_ValidWithResult(t *testing.T) {
	data := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x12a7b5c"}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.False(t, res.hasError)
	require.Equal(t, `"0x12a7b5c"`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_ValidWithError(t *testing.T) {
	data := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.False(t, res.hasResult)
	require.True(t, res.hasError)
	require.Equal(t, `{"code":-32601,"message":"method not found"}`, string(res.errorBytes))
}

func TestScanJsonrpcEnvelope_NullResult(t *testing.T) {
	// {"result": null} is a valid JSON-RPC response — null is a legal result value.
	data := []byte(`{"jsonrpc":"2.0","id":1,"result":null}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult, "result:null must register as present")
	require.Equal(t, `null`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_MissingBoth(t *testing.T) {
	// The second shape from the ticket.
	data := []byte(`{"jsonrpc":"2.0","id":1}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err, "valid JSON should parse")
	require.False(t, res.hasResult)
	require.False(t, res.hasError)
}

func TestScanJsonrpcEnvelope_BothPresent(t *testing.T) {
	// Spec says a response must have exactly one of result/error, but we
	// shouldn't crash on both. Record both presences.
	data := []byte(`{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":0,"message":"x"}}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.True(t, res.hasError)
}

func TestScanJsonrpcEnvelope_EmptyObject(t *testing.T) {
	data := []byte(`{}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.False(t, res.hasResult)
	require.False(t, res.hasError)
}

func TestScanJsonrpcEnvelope_TruncatedMidValue(t *testing.T) {
	// First shape from the ticket: TCP cut mid-write.
	data := []byte(`{"jsonrpc":"2.0","id":1,"resu`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_TruncatedMidString(t *testing.T) {
	data := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x12a7b5`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_TruncatedMidEscape(t *testing.T) {
	data := []byte(`{"result":"abc\`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_TruncatedMidUnicodeEscape(t *testing.T) {
	data := []byte(`{"result":"\u003`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_TruncatedAtClose(t *testing.T) {
	data := []byte(`{"jsonrpc":"2.0","id":1`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_EscapedQuoteInString(t *testing.T) {
	// Inner quote inside the error message must not terminate the string.
	data := []byte(`{"error":{"code":1,"message":"got \"x\" instead of \"y\""}}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasError)
	require.Equal(t, `{"code":1,"message":"got \"x\" instead of \"y\""}`, string(res.errorBytes))
}

func TestScanJsonrpcEnvelope_EscapedBackslashInString(t *testing.T) {
	// A lone backslash before " must not be mistaken for an escape.
	data := []byte(`{"result":"path\\"}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.Equal(t, `"path\\"`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_BraceInsideString(t *testing.T) {
	// `}` inside a string value must not pop the depth counter.
	data := []byte(`{"result":"a}b{c"}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.Equal(t, `"a}b{c"`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_NestedObjectResult(t *testing.T) {
	data := []byte(`{"result":{"number":"0x1","parentHash":"0xabc"}}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.Equal(t, `{"number":"0x1","parentHash":"0xabc"}`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_NestedArrayResult(t *testing.T) {
	data := []byte(`{"result":[{"blockNumber":"0x1"},{"blockNumber":"0x2"}]}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.Equal(t, `[{"blockNumber":"0x1"},{"blockNumber":"0x2"}]`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_DeeplyNestedResult(t *testing.T) {
	data := []byte(`{"result":{"a":{"b":{"c":[1,[2,[3,{"d":"e"}]]]}}}}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
}

func TestScanJsonrpcEnvelope_TopLevelArray(t *testing.T) {
	// scanJsonrpcEnvelope must reject arrays — that's the batch path.
	data := []byte(`[{"jsonrpc":"2.0","id":1,"result":1}]`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_TopLevelScalar(t *testing.T) {
	data := []byte(`"just a string"`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_LeadingWhitespace(t *testing.T) {
	data := []byte(" \n\t\r {\"result\":1}")
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
}

func TestScanJsonrpcEnvelope_WhitespaceBetweenTokens(t *testing.T) {
	data := []byte(`{  "jsonrpc" : "2.0" , "id" : 1 , "result" : { "x" : 1 } }`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
}

func TestScanJsonrpcEnvelope_EmptyInput(t *testing.T) {
	_, err := scanJsonrpcEnvelope(nil)
	require.Error(t, err)
	_, err = scanJsonrpcEnvelope([]byte{})
	require.Error(t, err)
	_, err = scanJsonrpcEnvelope([]byte("   "))
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_TrailingCommaRejected(t *testing.T) {
	// Strict JSON: trailing commas are invalid.
	data := []byte(`{"result":1,}`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_MismatchedBracket(t *testing.T) {
	data := []byte(`{"result":[1,2}`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_NumberResult(t *testing.T) {
	data := []byte(`{"result":-1.5e10}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.Equal(t, `-1.5e10`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_BoolResult(t *testing.T) {
	data := []byte(`{"result":true,"id":1}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.True(t, res.hasResult)
	require.Equal(t, `true`, string(res.resultBytes))
}

func TestScanJsonrpcEnvelope_NonStringKey(t *testing.T) {
	// JSON requires string keys — bare identifiers are invalid.
	data := []byte(`{result:1}`)
	_, err := scanJsonrpcEnvelope(data)
	require.Error(t, err)
}

func TestScanJsonrpcEnvelope_KeysWithSimilarNames(t *testing.T) {
	// "results" and "errors" must NOT match — only exact "result" and "error".
	data := []byte(`{"results":[1,2],"errors":[]}`)
	res, err := scanJsonrpcEnvelope(data)
	require.NoError(t, err)
	require.False(t, res.hasResult)
	require.False(t, res.hasError)
}

func TestScanJsonrpcBatchEnvelopes_Multi(t *testing.T) {
	data := []byte(`[{"id":1,"result":1},{"id":2,"result":2},{"id":3,"result":3}]`)
	var collected []string
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool {
		collected = append(collected, string(b))
		return true
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		`{"id":1,"result":1}`,
		`{"id":2,"result":2}`,
		`{"id":3,"result":3}`,
	}, collected)
}

func TestScanJsonrpcBatchEnvelopes_Empty(t *testing.T) {
	data := []byte(`[]`)
	count := 0
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestScanJsonrpcBatchEnvelopes_Single(t *testing.T) {
	data := []byte(`[{"id":1,"result":1}]`)
	var collected []string
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool {
		collected = append(collected, string(b))
		return true
	})
	require.NoError(t, err)
	require.Equal(t, []string{`{"id":1,"result":1}`}, collected)
}

func TestScanJsonrpcBatchEnvelopes_Truncated(t *testing.T) {
	data := []byte(`[{"id":1,"result":1},{"id":2,"result"`)
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool { return true })
	require.Error(t, err)
}

func TestScanJsonrpcBatchEnvelopes_TopLevelObject(t *testing.T) {
	data := []byte(`{"id":1,"result":1}`)
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool { return true })
	require.Error(t, err)
}

func TestScanJsonrpcBatchEnvelopes_EarlyExit(t *testing.T) {
	data := []byte(`[{"id":1,"result":1},{"id":2,"result":2},{"id":3,"result":3}]`)
	count := 0
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool {
		count++
		return count < 2
	})
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestScanJsonrpcBatchEnvelopes_LeadingWhitespace(t *testing.T) {
	data := []byte("\n\t [{\"id\":1,\"result\":1}]")
	count := 0
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestScanJsonrpcBatchEnvelopes_HeterogeneousElements(t *testing.T) {
	// Scanner doesn't validate elements — they may be any JSON value.
	// Schema validation is the caller's responsibility.
	data := []byte(`[{"id":1,"result":1},42,"string","array",null,true]`)
	count := 0
	err := scanJsonrpcBatchEnvelopes(data, func(_ jsonrpcScanResult, _ error, b []byte) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 6, count)
}

// A reply is one JSON value. Anything but whitespace after the object is refused, and the error
// names the first byte of it and where it is (MAG-5819).
func TestScanJsonrpcEnvelope_BytesAfterTheObjectAreRefused(t *testing.T) {
	for _, object := range []string{`{"jsonrpc":"2.0","id":1,"result":"0x1"}`, `{}`, ` { "error" : null } `} {
		for _, tail := range []string{`<html>`, `x`, `{}`, `{"id":2,"result":2}`, `]`, `}`, `,`, `null`, "\x00"} {
			for _, gap := range []string{``, ` `, "\r\n"} {
				_, err := scanJsonrpcEnvelope([]byte(object + gap + tail))
				require.EqualError(t, err,
					fmt.Sprintf("expected end of input at position %d, got %q", len(object)+len(gap), tail[0]),
					"object %q gap %q tail %q", object, gap, tail)
			}
		}
	}
}

// Whitespace after the object is part of no value: a reply that ends in a newline, as many nodes
// write it, is read as the reply alone.
func TestScanJsonrpcEnvelope_WhitespaceAfterTheObjectIsAccepted(t *testing.T) {
	object := `{"jsonrpc":"2.0","id":1,"result":"0x1"}`
	for _, tail := range []string{``, "\n", "\r\n", ` `, "\t", " \r\n\t \n"} {
		res, err := scanJsonrpcEnvelope([]byte(object + tail))
		require.NoError(t, err, "tail %q", tail)
		require.True(t, res.hasResult, "tail %q", tail)
		require.Equal(t, `"0x1"`, string(res.resultBytes), "tail %q", tail)
	}
	_, err := scanJsonrpcEnvelope([]byte("{}\n"))
	require.NoError(t, err)
}

// Anything but whitespace after the batch's closing bracket is refused, for an empty batch too,
// and every element is handed out before the refusal.
func TestScanJsonrpcBatchEnvelopes_BytesAfterTheBatchAreRefused(t *testing.T) {
	for batch, elements := range map[string]int{`[{"id":1,"result":1},{"id":2,"result":2}]`: 2, `[]`: 0, `[ ]`: 0, ` [42] `: 1} {
		for _, tail := range []string{`trailing`, `]`, `[]`, `{"id":3,"result":3}`, `,`, "\x00"} {
			for _, gap := range []string{``, ` `, "\n"} {
				handed := 0
				err := scanJsonrpcBatchEnvelopes([]byte(batch+gap+tail), func(jsonrpcScanResult, error, []byte) bool {
					handed++
					return true
				})
				require.EqualError(t, err,
					fmt.Sprintf("expected end of input at position %d, got %q", len(batch)+len(gap), tail[0]),
					"batch %q gap %q tail %q", batch, gap, tail)
				require.Equal(t, elements, handed, "batch %q gap %q tail %q", batch, gap, tail)
			}
		}
	}
}

// Whitespace after the batch is accepted, as it is after a single reply.
func TestScanJsonrpcBatchEnvelopes_WhitespaceAfterTheBatchIsAccepted(t *testing.T) {
	for batch, elements := range map[string]int{`[{"id":1,"result":1},{"id":2,"result":2}]`: 2, `[]`: 0} {
		for _, tail := range []string{``, "\n", "\r\n", ` `, "\t", " \r\n\t \n"} {
			handed := 0
			err := scanJsonrpcBatchEnvelopes([]byte(batch+tail), func(jsonrpcScanResult, error, []byte) bool {
				handed++
				return true
			})
			require.NoError(t, err, "batch %q tail %q", batch, tail)
			require.Equal(t, elements, handed, "batch %q tail %q", batch, tail)
		}
	}
}
