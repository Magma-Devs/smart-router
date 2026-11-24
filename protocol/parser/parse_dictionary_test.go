package parser

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// parseDictionary and parseDictionaryOrOrdered look a property up the same way. Only
// parseDictionaryOrOrdered falls back to a position: an array's element at the index, or a map's
// entry keyed by the index's text. Each keeps its own argument-count error.
func TestParseDictionary_AndOrOrdered(t *testing.T) {
	type outcome struct {
		values []interface{}
		err    string // "" for none; ValueNotSetError is matched with errors.Is
	}
	notSet := outcome{err: ValueNotSetError.Error()}
	value := func(v string) outcome { return outcome{values: []interface{}{v}} }

	for _, tc := range []struct {
		name       string
		input      *MockRPCInput
		dataSource int
		dictionary outcome // parseDictionary(prop, sep)
		ordered    outcome // parseDictionaryOrOrdered(prop, sep, index)
		index      string
	}{
		{
			name:  "array element prop=value",
			input: &MockRPCInput{params: []interface{}{"other=1", "key=0x5"}}, dataSource: PARSE_PARAMS, index: "0",
			dictionary: value("0x5"), ordered: value("0x5"),
		},
		{
			name:  "array element map",
			input: &MockRPCInput{params: []interface{}{map[string]interface{}{"key": "0x6"}}}, dataSource: PARSE_PARAMS, index: "0",
			dictionary: value("0x6"), ordered: value("0x6"),
		},
		{
			name:  "array miss, index in range",
			input: &MockRPCInput{params: []interface{}{"a", "b", "0x20"}}, dataSource: PARSE_PARAMS, index: "2",
			dictionary: notSet, ordered: value("0x20"),
		},
		{
			name:  "array miss, index out of range",
			input: &MockRPCInput{params: []interface{}{"a"}}, dataSource: PARSE_PARAMS, index: "3",
			dictionary: notSet, ordered: notSet,
		},
		{
			name:  "array miss, element at index unset",
			input: &MockRPCInput{params: []interface{}{"a", "0"}}, dataSource: PARSE_PARAMS, index: "1",
			dictionary: notSet, ordered: notSet,
		},
		{
			name:  "map hit",
			input: &MockRPCInput{params: map[string]interface{}{"key": "0x10", "1": "0x30"}}, dataSource: PARSE_PARAMS, index: "1",
			dictionary: value("0x10"), ordered: value("0x10"),
		},
		{
			name:  "map hit, value unset",
			input: &MockRPCInput{params: map[string]interface{}{"key": "0"}}, dataSource: PARSE_PARAMS, index: "1",
			dictionary: notSet, ordered: notSet,
		},
		{
			name:  "map miss, index key present",
			input: &MockRPCInput{params: map[string]interface{}{"other": "0x1", "1": "0x30"}}, dataSource: PARSE_PARAMS, index: "1",
			dictionary: notSet, ordered: value("0x30"),
		},
		{
			name:  "map miss, no index key",
			input: &MockRPCInput{params: map[string]interface{}{"other": "0x1"}}, dataSource: PARSE_PARAMS, index: "1",
			dictionary: notSet, ordered: notSet,
		},
		{
			name:  "string",
			input: &MockRPCInput{params: "0x40"}, dataSource: PARSE_PARAMS, index: "0",
			dictionary: value("0x40"), ordered: value("0x40"),
		},
		{
			name:  "nil",
			input: &MockRPCInput{params: nil}, dataSource: PARSE_PARAMS, index: "0",
			dictionary: notSet, ordered: notSet,
		},
		{
			name:  "unsupported type",
			input: &MockRPCInput{params: float64(42)}, dataSource: PARSE_PARAMS, index: "0",
			dictionary: outcome{err: "not Supported ParseDictionary with other types: float64"},
			ordered:    outcome{err: "not Supported ParseDictionary with other types: float64"},
		},
		{
			name:  "result object",
			input: &MockRPCInput{result: json.RawMessage(`{"key":"0x50"}`)}, dataSource: PARSE_RESULT, index: "0",
			dictionary: value("0x50"), ordered: value("0x50"),
		},
		{
			name:  "no result",
			input: &MockRPCInput{}, dataSource: PARSE_RESULT, index: "0",
			dictionary: outcome{err: "invalid input format, data is not json: %!s(<nil>), error: GetDataToParse failure Get.Result is empty"},
			ordered:    outcome{err: "invalid input format, data is not json: %!s(<nil>), error: GetDataToParse failure Get.Result is empty"},
		},
		{
			name:  "index not a number",
			input: &MockRPCInput{params: []interface{}{"a"}}, dataSource: PARSE_PARAMS, index: "x",
			dictionary: notSet,
			ordered:    outcome{err: `invalid input format, input isn't an unsigned index: x, error: strconv.ParseUint: parsing "x": invalid syntax`},
		},
		{
			// The data is read before the index, so a request failing both reports the data.
			name:  "no result and index not a number",
			input: &MockRPCInput{}, dataSource: PARSE_RESULT, index: "x",
			dictionary: outcome{err: "invalid input format, data is not json: %!s(<nil>), error: GetDataToParse failure Get.Result is empty"},
			ordered:    outcome{err: "invalid input format, data is not json: %!s(<nil>), error: GetDataToParse failure Get.Result is empty"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := func(label string, want outcome, got []interface{}, err error) {
				if want.err == "" {
					require.NoError(t, err, label)
					require.Equal(t, want.values, got, label)
					return
				}
				require.Error(t, err, label)
				require.Nil(t, got, label)
				if want.err == ValueNotSetError.Error() {
					require.True(t, errors.Is(err, ValueNotSetError), "%s: %v", label, err)
					return
				}
				require.Equal(t, want.err, err.Error(), label)
			}
			got, err := parseDictionary(tc.input, []string{"key", "="}, tc.dataSource)
			check("parseDictionary", tc.dictionary, got, err)
			got, err = parseDictionaryOrOrdered(tc.input, []string{"key", "=", tc.index}, tc.dataSource)
			check("parseDictionaryOrOrdered", tc.ordered, got, err)
		})
	}

	// Each names its own argument count.
	_, err := parseDictionary(&MockRPCInput{}, []string{"key"}, PARSE_PARAMS)
	require.EqualError(t, err, "invalid input format, input length: 1 and needs to be 2")
	_, err = parseDictionaryOrOrdered(&MockRPCInput{}, []string{"key", "="}, PARSE_PARAMS)
	require.EqualError(t, err, "ParseDictionaryOrOrdered: invalid input format, input length: 2 and needs to be 3: key,=")
}
