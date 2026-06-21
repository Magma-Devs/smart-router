package chainlib

import (
	"net/http"
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// valueFor returns the logged value for header name in a redacted metadata
// slice. Fails the test if the header is absent.
func valueFor(t *testing.T, md []pairingtypes.Metadata, name string) string {
	t.Helper()
	for _, m := range md {
		if m.Name == name {
			return m.Value
		}
	}
	t.Fatalf("header %q not present in redacted output", name)
	return ""
}

func TestRedactSensitiveMetadata(t *testing.T) {
	in := []pairingtypes.Metadata{
		{Name: "Authorization", Value: "Bearer secret-token"},
		{Name: "authorization", Value: "Bearer lower-case"}, // case-insensitive match
		{Name: "Cookie", Value: "session=abc123"},
		{Name: "X-Api-Key", Value: "live_key_should_not_leak"},
		{Name: "Token", Value: "raw-token"},
		{Name: "Content-Type", Value: "application/json"}, // safe, preserved
		{Name: "X-Lava-Dapp-Id", Value: "dapp-42"},        // safe, preserved
	}

	out := RedactSensitiveMetadata(in)

	require.Equal(t, redactedHeaderValue, valueFor(t, out, "Authorization"))
	require.Equal(t, redactedHeaderValue, valueFor(t, out, "authorization"))
	require.Equal(t, redactedHeaderValue, valueFor(t, out, "Cookie"))
	require.Equal(t, redactedHeaderValue, valueFor(t, out, "X-Api-Key"))
	require.Equal(t, redactedHeaderValue, valueFor(t, out, "Token"))
	require.Equal(t, "application/json", valueFor(t, out, "Content-Type"))
	require.Equal(t, "dapp-42", valueFor(t, out, "X-Lava-Dapp-Id"))

	// Input must be untouched — redaction must never alter the headers forwarded
	// upstream, only the logged copy.
	require.Equal(t, "Bearer secret-token", in[0].Value, "input slice must not be mutated")
}

func TestRedactSensitiveHeaderMap(t *testing.T) {
	in := http.Header{
		"Authorization": {"Bearer secret-token"},
		"Set-Cookie":    {"session=abc123"},
		"X-API-Key":     {"live_key"},
		"Accept":        {"application/json"},
	}

	out := RedactSensitiveHeaderMap(in)

	require.Equal(t, []string{redactedHeaderValue}, out["Authorization"])
	require.Equal(t, []string{redactedHeaderValue}, out["Set-Cookie"])
	require.Equal(t, []string{redactedHeaderValue}, out["X-API-Key"])
	require.Equal(t, []string{"application/json"}, out["Accept"])

	// Input untouched.
	require.Equal(t, []string{"Bearer secret-token"}, in["Authorization"], "input map must not be mutated")
}

func TestRedactSensitiveStringMap(t *testing.T) {
	in := map[string]string{
		"authorization": "Bearer secret-token",
		"apikey":        "live_key",
		"x-access-token": "access-secret",
		"user-agent":     "smart-router-test",
	}

	out := RedactSensitiveStringMap(in)

	require.Equal(t, redactedHeaderValue, out["authorization"])
	require.Equal(t, redactedHeaderValue, out["apikey"])
	require.Equal(t, redactedHeaderValue, out["x-access-token"])
	require.Equal(t, "smart-router-test", out["user-agent"])

	require.Equal(t, "Bearer secret-token", in["authorization"], "input map must not be mutated")
}
