package spec

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// enumCase pins one enum's JSON contract: a known value marshals to its name, an
// unregistered value to "<TYPE>(<n>)", and an unknown name decodes to 0 without error.
type enumCase struct {
	name         string
	known        json.Marshaler
	knownJSON    string
	unregistered json.Marshaler
	unregJSON    string
	// decode unmarshals data into a fresh value and returns it as int32.
	decode func(data []byte) (int32, error)
}

func decodeEnum[T ~int32, P interface {
	*T
	json.Unmarshaler
}](data []byte) (int32, error) {
	var v T
	err := P(&v).UnmarshalJSON(data)
	return int32(v), err
}

func TestEnumJSONContract(t *testing.T) {
	cases := []enumCase{
		{"FUNCTION_TAG", FUNCTION_TAG_GET_BLOCK_BY_NUM, `"GET_BLOCK_BY_NUM"`, FUNCTION_TAG(4), `"FUNCTION_TAG(4)"`, decodeEnum[FUNCTION_TAG]},
		{"PARSER_TYPE", PARSER_TYPE_BLOCK_HASH, `"BLOCK_HASH"`, PARSER_TYPE(4), `"PARSER_TYPE(4)"`, decodeEnum[PARSER_TYPE]},
		{"PARSER_FUNC", PARSER_FUNC_PARSE_DICTIONARY_OR_ORDERED, `"PARSE_DICTIONARY_OR_ORDERED"`, PARSER_FUNC(5), `"PARSER_FUNC(5)"`, decodeEnum[PARSER_FUNC]},
		{"Header_HeaderType", Header_pass_override, `"pass_override"`, Header_HeaderType(9), `"Header_HeaderType(9)"`, decodeEnum[Header_HeaderType]},
		{"ParseValue_VerificationSeverity", ParseValue_Warning, `"Warning"`, ParseValue_VerificationSeverity(-1), `"ParseValue_VerificationSeverity(-1)"`, decodeEnum[ParseValue_VerificationSeverity]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.known)
			require.NoError(t, err)
			require.Equal(t, tc.knownJSON, string(got))

			got, err = json.Marshal(tc.unregistered)
			require.NoError(t, err)
			require.Equal(t, tc.unregJSON, string(got))

			v, err := tc.decode([]byte(tc.knownJSON))
			require.NoError(t, err)
			require.NotZero(t, v, "a known name round-trips to its value")
			back, err := json.Marshal(tc.known)
			require.NoError(t, err)
			require.Equal(t, tc.knownJSON, string(back))

			// An unknown name decodes to the zero value without an error.
			v, err = tc.decode([]byte(`"NOT_A_NAME"`))
			require.NoError(t, err)
			require.Zero(t, v)

			_, err = tc.decode([]byte(`7`))
			require.Error(t, err, "a non-string is rejected")
		})
	}
}

func TestEnumRoundTripsEveryName(t *testing.T) {
	for n := range FUNCTION_TAG_value {
		var v FUNCTION_TAG
		require.NoError(t, json.Unmarshal([]byte(`"`+n+`"`), &v))
		require.Equal(t, n, v.String())
	}
	for n := range PARSER_TYPE_value {
		var v PARSER_TYPE
		require.NoError(t, json.Unmarshal([]byte(`"`+n+`"`), &v))
		require.Equal(t, n, v.String())
	}
	for n := range PARSER_FUNC_value {
		var v PARSER_FUNC
		require.NoError(t, json.Unmarshal([]byte(`"`+n+`"`), &v))
		require.Equal(t, n, v.String())
	}
	for n := range Header_HeaderType_value {
		var v Header_HeaderType
		require.NoError(t, json.Unmarshal([]byte(`"`+n+`"`), &v))
		require.Equal(t, n, v.String())
	}
	for n := range ParseValue_VerificationSeverity_value {
		var v ParseValue_VerificationSeverity
		require.NoError(t, json.Unmarshal([]byte(`"`+n+`"`), &v))
		require.Equal(t, n, v.String())
	}
}

func TestCombinableEqualPrologue(t *testing.T) {
	a := &Api{Name: "eth_call", Enabled: true, ComputeUnits: 10}
	var nilAPI *Api

	require.True(t, a.Equal(&Api{Name: "eth_call", Enabled: true, ComputeUnits: 10}))
	require.True(t, a.Equal(Api{Name: "eth_call", Enabled: true, ComputeUnits: 10}), "a value is compared like a pointer")
	require.False(t, a.Equal(&Api{Name: "eth_call", Enabled: true, ComputeUnits: 11}))
	require.False(t, a.Equal(nil))
	require.False(t, a.Equal(nilAPI))
	require.False(t, a.Equal(&Header{Name: "eth_call"}), "another type is never equal")
	require.True(t, nilAPI.Equal(nil))
	var otherNil *Api
	require.True(t, nilAPI.Equal(otherNil))
	require.False(t, nilAPI.Equal(a))

	require.True(t, (&Header{Name: "h", Kind: Header_pass_both}).Equal(Header{Name: "h", Kind: Header_pass_both}))
	require.False(t, (&Header{Name: "h", Kind: Header_pass_both}).Equal(&Header{Name: "h"}))
	require.True(t, (&ParseDirective{ApiName: "x", FunctionTag: FUNCTION_TAG_GET_BLOCKNUM}).Equal(&ParseDirective{ApiName: "x", FunctionTag: FUNCTION_TAG_GET_BLOCKNUM}))
	require.False(t, (&ParseDirective{ApiName: "x"}).Equal(&ParseDirective{ApiName: "y"}))
	require.True(t, (&Extension{Name: "archive", CuMultiplier: 5}).Equal(Extension{Name: "archive", CuMultiplier: 5}))
	require.False(t, (&Extension{Name: "archive", CuMultiplier: 5}).Equal(&Extension{Name: "archive"}))
	require.True(t, (&Verification{Name: "chain-id"}).Equal(&Verification{Name: "chain-id"}))
	require.False(t, (&Verification{Name: "chain-id"}).Equal(&Verification{Name: "pruning"}))
}
