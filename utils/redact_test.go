package utils

import (
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// The shapes upstream vendors actually use for api keys.
		{"key in path", "https://eth-mainnet.example.com/v2/AbC123SecretKey", "https://eth-mainnet.example.com/[redacted]"},
		{"key as whole path", "https://node.example.com/AbC123SecretKey", "https://node.example.com/[redacted]"},
		{"key in query", "https://node.example.com/rpc?apikey=AbC123SecretKey", "https://node.example.com/[redacted]"},
		{"key in userinfo", "https://user:s3cr3t@node.example.com/rpc", "https://node.example.com/[redacted]"},
		{"userinfo only", "https://user:s3cr3t@node.example.com", "https://node.example.com"},
		{"key in fragment", "https://node.example.com#AbC123SecretKey", "https://node.example.com/[redacted]"},
		{"scheme-less with key", "node.example.com/v2/AbC123SecretKey", "node.example.com/[redacted]"},

		// Nothing secret to remove — these must survive intact so logs stay useful.
		{"host only", "https://node.example.com", "https://node.example.com"},
		{"host and port", "grpc://node.example.com:9090", "grpc://node.example.com:9090"},
		{"trailing slash", "https://node.example.com/", "https://node.example.com/"},
		{"ws host and port", "wss://node.example.com:443", "wss://node.example.com:443"},
		{"scheme-less host and port", "node.example.com:9090", "node.example.com:9090"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RedactURL(tt.in))
		})
	}
}

func TestRedactSecrets_LeavesNonURLTextAlone(t *testing.T) {
	// Anchoring on "://" is what keeps prose, listen addresses and dial errors
	// readable. A regression here makes every log line worse.
	for _, s := range []string{
		"failed relay, insufficient results",
		"dial tcp 10.0.0.7:443: connect: connection refused",
		"prometheus endpoint listening 0.0.0.0:7779",
		"provider eth-primary-1 blocked",
		"eth-primary-1",
		"lava@harvestGenProvider",
		"lava@1abcdefghijklmnop",
		"ep:8545",
		"",
	} {
		assert.Equal(t, s, RedactSecrets(s))
	}
}

func TestRedactSecrets_RedactsEmbeddedURLs(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			// The exact shape net/http produces, quotes included.
			name: "url.Error message",
			in:   `Post "https://eth-mainnet.example.com/v2/SecretKey123": read tcp 10.0.0.1:53422->1.2.3.4:443: connection reset by peer`,
			want: `Post "https://eth-mainnet.example.com/[redacted]": read tcp 10.0.0.1:53422->1.2.3.4:443: connection reset by peer`,
		},
		{
			name: "two urls in one message",
			in:   "failed https://a.example.com/v2/KEY1 then https://b.example.com/v3/KEY2",
			want: "failed https://a.example.com/[redacted] then https://b.example.com/[redacted]",
		},
		{
			name: "url in attribute-style text",
			in:   "{url:https://node.example.com/v2/SecretKey123,method:eth_call}",
			want: "{url:https://node.example.com/[redacted],method:eth_call}",
		},
		{
			name: "scheme-less url with key in path",
			in:   "failed node.example.com/v2/SecretKey123",
			want: "failed node.example.com/[redacted]",
		},
		{
			name: "scheme-less url with key in query",
			in:   `Post "node.example.com/rpc?apikey=SecretKey123": connection refused`,
			want: `Post "node.example.com/[redacted]": connection refused`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RedactSecrets(tt.in))
		})
	}
}

// The leak this fix exists for: Go masks only the userinfo password, so the key
// in the path survives into the error message.
func TestRedactSecrets_RealURLError(t *testing.T) {
	const secret = "AbC123SecretKey"
	raw := &url.Error{
		Op:  "Post",
		URL: "https://eth-mainnet.example.com/v2/" + secret,
		Err: errors.New("connection reset by peer"),
	}
	wrapped := fmt.Errorf("http request failed: %w", raw)

	require.Contains(t, wrapped.Error(), secret, "precondition: the raw error carries the key")
	assert.NotContains(t, RedactSecrets(wrapped.Error()), secret)
	assert.Contains(t, RedactSecrets(wrapped.Error()), "eth-mainnet.example.com")
}

func TestRedactSecretsErr_PreservesCause(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := fmt.Errorf(`Post "https://node.example.com/v2/SecretKey123": %w`, sentinel)

	redacted := RedactSecretsErr(err)
	assert.NotContains(t, redacted.Error(), "SecretKey123")
	assert.True(t, errors.Is(redacted, sentinel), "errors.Is must still reach the cause")

	// No url means no allocation and no rewrap — the original error comes back.
	plain := errors.New("no url here")
	assert.Same(t, plain, RedactSecretsErr(plain))
	assert.Nil(t, RedactSecretsErr(nil))
}

// Redaction runs at both the log layer and the client-error boundary, so an
// already-redacted string passes through it a second time. It must be stable.
func TestRedactSecrets_Idempotent(t *testing.T) {
	for _, in := range []string{
		"https://node.example.com/v2/AbC123SecretKey",
		"https://node.example.com:9090/rpc?apikey=AbC123SecretKey",
		`Post "https://node.example.com/v2/AbC123SecretKey": connection reset`,
	} {
		once := RedactSecrets(in)
		assert.Equal(t, once, RedactSecrets(once), "second pass changed %q", once)
		assert.NotContains(t, RedactSecrets(once), "AbC123SecretKey")
	}
}

// The metrics fallback is fed either a node-url or an already-resolved provider
// identifier. Mangling the latter silently renames a Prometheus series, so the
// identifier cases matter as much as the url ones.
func TestRedactIfURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// Not urls — must survive byte-for-byte. A lava address carries an "@",
		// which RedactURL would otherwise read as userinfo and strip.
		{"lava provider address", "lava@harvestGenProvider", "lava@harvestGenProvider"},
		{"lava bech32 address", "lava@1abcdefghijklmnop", "lava@1abcdefghijklmnop"},
		{"provider name", "eth-primary-1", "eth-primary-1"},
		{"host and port", "ep:8545", "ep:8545"},
		{"empty", "", ""},

		// Urls — redacted as usual.
		{"url with scheme", "http://ep:8545", "http://ep:8545"},
		{"url with key in path", "https://node.example.com/v2/AbC123SecretKey", "https://node.example.com/[redacted]"},
		{"scheme-less url with key", "node.example.com/v2/AbC123SecretKey", "node.example.com/[redacted]"},
		{"url with key in query", "https://node.example.com?apikey=AbC123SecretKey", "https://node.example.com/[redacted]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RedactIfURL(tt.in))
		})
	}
}
