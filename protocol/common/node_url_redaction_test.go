package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
