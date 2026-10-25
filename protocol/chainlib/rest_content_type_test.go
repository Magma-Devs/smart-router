package chainlib

import (
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

func TestNormalizeGenericContentType(t *testing.T) {
	const (
		jsonObject = `{"function":"0x1::chain_id::get","type_arguments":[],"arguments":[]}`
		jsonArray  = "  \n[1, 2, 3]\n"
	)
	for _, tc := range []struct {
		name        string
		contentType string // empty: the client sent no Content-Type
		body        string
		want        string // the Content-Type left in the metadata; empty: none
	}{
		{"curl -d default with a JSON object", "application/x-www-form-urlencoded", jsonObject, jsonContentType},
		{"fetch default with a JSON array", "text/plain;charset=UTF-8", jsonArray, jsonContentType},
		{"media type matched case-insensitively", "Text/Plain", jsonObject, jsonContentType},
		{"a real form body keeps its type", "application/x-www-form-urlencoded", "tx=AAAA", "application/x-www-form-urlencoded"},
		{"a JSON string goes as JSON (Tezos /injection/operation)", "application/x-www-form-urlencoded", `"6c0a1f"`, jsonContentType},
		{"a text payload that is a JSON number keeps its type", "text/plain", "42", "text/plain"},
		{"a text payload that is a JSON literal keeps its type", "text/plain", "true", "text/plain"},
		{"a quoted text that is not a JSON string keeps its type", "text/plain", `"unterminated`, "text/plain"},
		{"a body that only looks like JSON keeps its type", "text/plain", "{not json", "text/plain"},
		{"an empty body keeps its type", "application/x-www-form-urlencoded", "", "application/x-www-form-urlencoded"},
		{"a specific type is never rewritten", "application/x.aptos.view_function+bcs", jsonObject, "application/x.aptos.view_function+bcs"},
		{"application/json is left as sent", "application/json; charset=utf-8", jsonObject, "application/json; charset=utf-8"},
		{"an unparseable value is left as sent", ";;", jsonObject, ";;"},
		{"no Content-Type stays absent", "", jsonObject, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := []pairingtypes.Metadata{{Name: "x-other", Value: "kept"}}
			if tc.contentType != "" {
				metadata = append(metadata, pairingtypes.Metadata{Name: "Content-Type", Value: tc.contentType})
			}
			original := append([]pairingtypes.Metadata(nil), metadata...)

			got := normalizeGenericContentType(metadata, []byte(tc.body))

			require.Equal(t, original, metadata, "the caller's metadata must not be modified")
			require.Contains(t, got, pairingtypes.Metadata{Name: "x-other", Value: "kept"}, "other headers pass through")
			var contentType string
			for _, entry := range got {
				if entry.Name == "Content-Type" {
					contentType = entry.Value
				}
			}
			require.Equal(t, tc.want, contentType)
			require.Len(t, got, len(metadata), "the header is rewritten in place, never added or removed")
		})
	}
}
