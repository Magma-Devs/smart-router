package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// relayNodeError runs one relay through relayInnerDirect against an upstream that answers HTTP 200
// with body, classified as a node error, and returns the reply the caller would get.
func relayNodeError(t *testing.T, body string) []byte {
	t.Helper()
	h := newDirectRelayHarness(t, http.StatusOK, body)
	msg := &mockChainMessage{
		requestData:        []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xa1","latest"]}`),
		checkResponseError: func([]byte, int) (bool, string) { return true, "rejected" },
	}
	relayResult := &common.RelayResult{}
	_, err := h.rpcss.relayInnerDirect(
		context.Background(), h.session, relayResult, 5*time.Second, 30*time.Second,
		msg, msg.requestData, func() bool { return false }, nil,
	)
	require.NoError(t, err)
	require.True(t, relayResult.IsNodeError, "the upstream's answer is a node error")
	require.NotNil(t, relayResult.Reply)
	return relayResult.Reply.Data
}

// MAG-4863, case A: a node's own JSON-RPC error that repeats the url it was called on reached the
// caller unchanged, with HTTP 200 and the key. It is redacted like a router-built error: the host
// stays, the rest of the url goes, and the reply is still the node's JSON-RPC error.
func TestRelayInnerDirect_NodeErrorReplyLosesTheKeyInAURL(t *testing.T) {
	reply := relayNodeError(t,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rejected by https://node.example.com/v2/KEY123SECRET"}}`)

	require.NotContains(t, string(reply), "KEY123SECRET")
	require.True(t, json.Valid(reply), "still JSON: %s", reply)
	var parsed struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(reply, &parsed))
	require.Equal(t, -32000, parsed.Error.Code, "the node's error is still the reply")
	require.Equal(t, "rejected by https://node.example.com/[redacted]", parsed.Error.Message)
}

// A node that echoes a configured key on its own, with no url around it, is caught by value.
func TestRelayInnerDirect_NodeErrorReplyLosesARegisteredKeyOnItsOwn(t *testing.T) {
	const key = "MAG4863EchoedKey0123456789"
	utils.RegisterSecrets(key)
	reply := relayNodeError(t,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"invalid api key `+key+`"}}`)

	require.NotContains(t, string(reply), key)
	require.Contains(t, string(reply), "invalid api key [redacted]")
	require.True(t, json.Valid(reply))
}

// A node error that holds no secret is the node's reply byte for byte.
func TestRelayInnerDirect_NodeErrorReplyWithoutASecretIsUnchanged(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted","data":"0x08c379a0"}}`
	reply := relayNodeError(t, body)
	require.JSONEq(t, body, string(reply))
}

// Parsing the provider config registers the secrets of every node-url in it, before anything can
// log or relay through them.
func TestParseStaticProviderEndpoints_RegistersTheNodeUrlSecrets(t *testing.T) {
	const (
		pathKey   = "MAG4863PathKey0123456789"
		headerKey = "MAG4863HeaderKey0123456789"
		queryKey  = "MAG4863QueryKey0123456789"
	)
	yamlBody := "" +
		common.DirectRPCConfigName + ":\n" +
		"  - name: eth-rpc-1\n" +
		"    chain-id: ETH1\n" +
		"    api-interface: jsonrpc\n" +
		"    node-urls:\n" +
		"      - url: node-host/v2/" + pathKey + "\n" +
		"        auth-config:\n" +
		"          auth-query: apikey=" + queryKey + "\n" +
		"          auth-headers:\n" +
		"            x-api-key: " + headerKey + "\n"
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

	_, err := ParseStaticProviderEndpoints(v, common.DirectRPCConfigName)
	require.NoError(t, err)

	for _, key := range []string{pathKey, headerKey, queryKey} {
		text := "dial node-host/v2/" + key + " failed: upstream said " + key
		require.NotContains(t, utils.RedactSecrets(text), key)
	}
}

// A node's error status goes back as is (MAG-4481), minus the secrets: end to end through a real
// listener and router, on every HTTP interface, the configured key is withheld both inside a url
// and on its own, and the rest of the node's body is left as the node wrote it.
func TestFailedRelayReply_EndToEnd_NodeErrorBodyWithholdsTheKey(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 300 * time.Millisecond
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	const key = "MAG4863FailedReplyKey0123456789"
	body := `{"jsonrpc":"2.0","id":77,"error":{"code":-32000,"message":"rejected by https://node.example.com/v2/` + key + `","data":"Cannot POST /v2/` + key + `"}}`
	want := `{"jsonrpc":"2.0","id":77,"error":{"code":-32000,"message":"rejected by https://node.example.com/[redacted]","data":"Cannot POST /v2/[redacted]"}}`
	for _, iface := range failedReplyInterfaces() {
		t.Run(iface.name, func(t *testing.T) {
			nodeURL := answeringUpstream(t, http.StatusInternalServerError, body, "application/json") + "/v2/" + key
			// What ParseStaticProviderEndpoints does for every configured node-url.
			(&common.NodeUrl{Url: nodeURL}).RegisterSecrets()

			read := startFailedReplyRouter(t, iface, nodeURL)()
			require.Equal(t, http.StatusInternalServerError, read.status)
			require.NotContains(t, read.body, key)
			require.Equal(t, want, read.body, "the node's body, as the node wrote it but for the key")
			require.Equal(t, "application/json", read.contentType)
		})
	}
}
