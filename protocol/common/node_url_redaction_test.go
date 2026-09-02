package common

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// MAG-3330: UrlStr is the display form of a node-url — it feeds logs, error text
// and the health command. Vendors put the api key in the path or the query, so
// stripping the userinfo alone is not enough.
func TestNodeUrl_UrlStr_DropsCredentials(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"key in path", "https://eth-mainnet.example.com/v2/AbC123SecretKey", "https://eth-mainnet.example.com/[redacted]"},
		{"key in query", "https://node.example.com/rpc?apikey=AbC123SecretKey", "https://node.example.com/[redacted]"},
		{"key in userinfo", "https://user:s3cr3t@node.example.com/rpc", "https://node.example.com/[redacted]"},
		{"nothing to drop", "https://node.example.com", "https://node.example.com"},
		{"host and port kept", "grpc://node.example.com:9090", "grpc://node.example.com:9090"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nurl := NodeUrl{Url: tt.url}
			assert.Equal(t, tt.want, nurl.UrlStr())
			// String() builds on UrlStr, so it must not reintroduce the credential.
			assert.NotContains(t, nurl.String(), "AbC123SecretKey")
			assert.NotContains(t, nurl.String(), "s3cr3t")
		})
	}
}

// The dialing address is untouched — only the display form is redacted.
func TestNodeUrl_UrlStr_DoesNotMutateUrl(t *testing.T) {
	raw := "https://eth-mainnet.example.com/v2/AbC123SecretKey"
	nurl := NodeUrl{Url: raw}
	_ = nurl.UrlStr()
	assert.Equal(t, raw, nurl.Url)
}

// Relay metadata carries the same credentials as an http.Header — the caller's
// own key inbound, the configured auth-headers outbound.
func TestRedactMetadata(t *testing.T) {
	md := []pairingtypes.Metadata{
		{Name: "x-api-key", Value: "AbC123SecretKey"},
		{Name: "content-type", Value: "application/json"},
		{Name: "authorization", Value: "Bearer AbC123SecretKey"},
	}

	got := RedactMetadata(md)

	assert.NotContains(t, got, "AbC123SecretKey")
	assert.Contains(t, got, "content-type:application/json")
	// Sorted, so the rendering is stable across runs.
	assert.Equal(t, "{authorization:[redacted], content-type:application/json, x-api-key:[redacted]}", got)
}

// The input slice must not be reordered — it is live relay data, not a copy.
func TestRedactMetadata_DoesNotMutateInput(t *testing.T) {
	md := []pairingtypes.Metadata{
		{Name: "zeta", Value: "1"},
		{Name: "alpha", Value: "2"},
	}
	_ = RedactMetadata(md)
	assert.Equal(t, "zeta", md[0].Name)
	assert.Equal(t, "alpha", md[1].Name)
}

func TestRedactMetadata_Empty(t *testing.T) {
	assert.Equal(t, "{}", RedactMetadata(nil))
}
