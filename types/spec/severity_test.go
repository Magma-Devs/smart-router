package spec

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// A value whose spec sets no severity decodes as Default, the zero value, and stays
// absent when a tool writes the spec back. An explicit severity survives the round
// trip: an explicit "Fail" used to decode as the zero value and was dropped on the
// way out (MAG-3950).
func TestParseValueSeverity_RoundTrip(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want ParseValue_VerificationSeverity
		out  string
	}{
		{`{"expected_value":"0x1"}`, ParseValue_Default, `{"expected_value":"0x1"}`},
		{`{"expected_value":"0x1","severity":"Fail"}`, ParseValue_Fail, `{"expected_value":"0x1","severity":"Fail"}`},
		{`{"expected_value":"0x1","severity":"PartialFail"}`, ParseValue_PartialFail, `{"expected_value":"0x1","severity":"PartialFail"}`},
		{`{"expected_value":"0x1","severity":"Warning"}`, ParseValue_Warning, `{"expected_value":"0x1","severity":"Warning"}`},
	} {
		var value ParseValue
		require.NoError(t, json.Unmarshal([]byte(tc.in), &value), tc.in)
		require.Equal(t, tc.want, value.Severity, tc.in)
		out, err := json.Marshal(value)
		require.NoError(t, err)
		require.JSONEq(t, tc.out, string(out), tc.in)
	}
}

// A severity this router does not know reads as Default, as the spec setting none
// does, and is handed out once so the parser that loads the spec can say so.
func TestParseValueSeverity_AnUnknownNameReadsAsDefaultAndIsReported(t *testing.T) {
	TakeUnknownSeverities()
	var value ParseValue
	require.NoError(t, json.Unmarshal([]byte(`{"severity":"Stop"}`), &value))
	require.Equal(t, ParseValue_Default, value.Severity)
	require.Equal(t, []string{"Stop"}, TakeUnknownSeverities())
	require.Empty(t, TakeUnknownSeverities(), "each is handed out once")
}
