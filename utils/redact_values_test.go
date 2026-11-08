package utils

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const testKey = "KEY123SECRET"

// withConfiguredSecrets runs a test against an empty registry and restores the process's own after.
func withConfiguredSecrets(t *testing.T) {
	t.Helper()
	saved := configuredSecrets.Load()
	configuredSecrets.Store(nil)
	t.Cleanup(func() { configuredSecrets.Store(saved) })
}

// MAG-4863: the shapes RedactSecrets let through. Each must lose the key, and keep the host where
// there is one.
func TestRedactSecrets_ShapesTheURLPassMissed(t *testing.T) {
	withConfiguredSecrets(t)
	cases := []struct{ name, in, want string }{
		{
			"a query with ;",
			`Get "https://eth.example.com/rpc?a=1;apikey=KEY123SECRET": EOF`,
			`Get "https://eth.example.com/[redacted]": EOF`,
		},
		{
			"a scheme-less query with ;",
			`dial eth.example.com/rpc?a=1;apikey=KEY123SECRET failed`,
			`dial eth.example.com/[redacted] failed`,
		},
		{
			"escaped slashes, as JSON allows",
			`{"message":"bad url https:\/\/eth.example.com\/v2\/KEY123SECRET"}`,
			`{"message":"bad url https:\/\/eth.example.com\/[redacted]"}`,
		},
		{
			"an escaped url before an escaped quote keeps the quote",
			`{"m":"see \"https:\/\/eth.example.com\/v2\/KEY123SECRET\" now"}`,
			`{"m":"see \"https:\/\/eth.example.com\/[redacted]\" now"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RedactSecrets(tc.in))
		})
	}
}

// A ";" stays a boundary outside a query, so a url in a log attribute list still does not swallow
// what follows it.
func TestRedactSecrets_SemicolonOutsideAQueryStillEndsTheURL(t *testing.T) {
	withConfiguredSecrets(t)
	require.Equal(t, "{url:https://host.example.com/[redacted];method:eth_call}",
		RedactSecrets("{url:https://host.example.com/v2/key;method:eth_call}"))
}

// The shapes no url pattern can see. A registered key is withheld wherever it appears.
func TestRedactSecrets_ARegisteredKeyIsWithheldInAnyShape(t *testing.T) {
	withConfiguredSecrets(t)
	RegisterSecrets(testKey)
	for _, in := range []string{
		"upstream said: invalid api key KEY123SECRET",
		"dial node-host/v2/KEY123SECRET failed",
		"Cannot POST /v2/KEY123SECRET",
		"<pre>Cannot POST /v2/KEY123SECRET</pre>",
		`{"error":{"code":-32000,"message":"key KEY123SECRET is over its limit"}}`,
	} {
		out := RedactSecrets(in)
		require.NotContains(t, out, testKey, in)
		require.Contains(t, out, RedactedMark, in)
	}
}

// A registered key inside a url leaves one mark after the host, as an unregistered one does: the
// url pass stops at the mark the value pass left, and must not add its own beside it.
func TestRedactSecrets_ARegisteredKeyInAURLLeavesOneMark(t *testing.T) {
	withConfiguredSecrets(t)
	RegisterSecrets(testKey)
	for in, want := range map[string]string{
		`Post "https://eth.example.com/v2/KEY123SECRET": EOF`:        `Post "https://eth.example.com/[redacted]": EOF`,
		`Get "https://eth.example.com/rpc?apikey=KEY123SECRET": EOF`: `Get "https://eth.example.com/[redacted]": EOF`,
		`dial eth.example.com/v2/KEY123SECRET failed`:                `dial eth.example.com/[redacted] failed`,
		`{"m":"https:\/\/eth.example.com\/v2\/KEY123SECRET"}`:        `{"m":"https:\/\/eth.example.com\/[redacted]"}`,
	} {
		require.Equal(t, want, RedactSecrets(in), in)
	}
}

// A secret is withheld in the forms it takes in a url and in a JSON string, whatever it contains.
func TestRedactSecrets_ARegisteredKeyIsWithheldInEveryEncoding(t *testing.T) {
	withConfiguredSecrets(t)
	const secret = "abc/def+ghi=jk;l,m)n%1"
	RegisterSecrets(secret)
	quoted, err := json.Marshal(secret)
	require.NoError(t, err)
	jsonEscaped := string(quoted[1 : len(quoted)-1])
	for _, form := range []string{
		secret,
		url.QueryEscape(secret),
		url.PathEscape(secret),
		jsonEscaped,
		strings.ReplaceAll(jsonEscaped, "/", `\/`),
	} {
		require.Equal(t, "token "+RedactedMark+" rejected", RedactSecrets("token "+form+" rejected"), form)
	}
}

// Path words and short values are not secrets; replacing them would mangle every log line.
func TestRegisterSecrets_KeepsWordsOut(t *testing.T) {
	withConfiguredSecrets(t)
	RegisterSecretCandidates("v2", "rpc", "ext", "ethereum-mainnet", "mainnet")
	RegisterSecrets("Bearer", "short")
	require.Nil(t, configuredSecrets.Load(), "nothing here looks like a key")

	RegisterSecretCandidates(testKey, "abcdefghijklmnopqrstuvwxyz")
	RegisterSecrets("lettersonly")
	text := "KEY123SECRET abcdefghijklmnopqrstuvwxyz lettersonly ethereum-mainnet"
	require.Equal(t, "[redacted] [redacted] [redacted] ethereum-mainnet", RedactSecrets(text))
}

// A secret that contains another is replaced whole, not around the shorter one.
func TestRegisterSecrets_TheLongerSecretWins(t *testing.T) {
	withConfiguredSecrets(t)
	RegisterSecrets("KEY123SECRET", "KEY123SECRETEXTENDED")
	require.Equal(t, "a [redacted] b", RedactSecrets("a KEY123SECRETEXTENDED b"))
}

// Text without a registered secret comes back as it was, and a body without one is not copied.
func TestRedactSecretsBytes_LeavesACleanBodyAlone(t *testing.T) {
	withConfiguredSecrets(t)
	RegisterSecrets(testKey)
	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}`)
	out := RedactSecretsBytes(body)
	require.Equal(t, body, out)
	require.Same(t, &body[0], &out[0], "the same slice, not a copy")
}
