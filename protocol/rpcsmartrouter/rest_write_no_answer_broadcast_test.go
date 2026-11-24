package rpcsmartrouter

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/utils/rand"
)

// restWriteCase is the REST write of writeInterfaceCases: POST /cosmos/tx/v1beta1/txs on LAVA.
func restWriteCase(t *testing.T) writeInterfaceCase {
	t.Helper()
	for _, tc := range writeInterfaceCases {
		if tc.name == "rest" {
			return tc
		}
	}
	t.Fatal("setup: writeInterfaceCases has no rest case")
	return writeInterfaceCase{}
}

// TestRESTWriteBroadcast_HTML200DoesNotEndTheBroadcast is MAG-3971 end to end, through the
// broadcast. One provider answers the write at once with an HTML page under a 200 (a CDN or WAF in
// front of the node). The other answers the real result 150 ms later. The client must get the real
// result, and each provider must receive the write once. Before MAG-3971 the instant HTML page won
// the broadcast and the real result was cut off.
func TestRESTWriteBroadcast_HTML200DoesNotEndTheBroadcast(t *testing.T) {
	rand.InitRandomSeed()
	tc := restWriteCase(t)
	for name, page := range map[string]string{
		"cloudflare challenge": "<html><body>Please enable cookies. Cloudflare Ray ID: 123</body></html>",
		"doctype after space":  "\n <!DOCTYPE html><title>Maintenance</title>",
	} {
		t.Run(name, func(t *testing.T) {
			chainParser := tc.parser(t)
			gateway := writeUpstream{name: "gateway-" + t.Name(), status: http.StatusOK, body: page}
			node := writeUpstream{name: "node-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 150 * time.Millisecond}
			hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{gateway, node})

			result, err := sendWrite(t, tc, chainParser, server)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, node.name, result.ProviderInfo.ProviderAddress,
				"the instant HTML page ended the broadcast: the client never saw the node's real result")
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.JSONEq(t, tc.success, string(result.Reply.Data))
			require.False(t, result.IsNodeError)
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "the gateway received the write once")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "the node received the write once")
		})
	}
}

// TestRESTWriteBroadcast_EmptyAndPlainText200AreAnswers: an empty 200 is the normal success of
// Ethereum Beacon's writes and of Algorand's async submit, and a plain-text 200 that of Esplora and
// Arweave. Either still wins the broadcast when it arrives first: making an empty 200 a node error
// made those writes wait for every sibling, and turned them into an "unclear" reply beside a
// sibling that hung.
func TestRESTWriteBroadcast_EmptyAndPlainText200AreAnswers(t *testing.T) {
	rand.InitRandomSeed()
	tc := restWriteCase(t)
	for name, body := range map[string]string{
		"empty": "",
		"text":  "OK",
	} {
		t.Run(name, func(t *testing.T) {
			chainParser := tc.parser(t)
			first := writeUpstream{name: "first-" + t.Name(), status: http.StatusOK, body: body}
			slow := writeUpstream{name: "slow-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 2 * time.Second}
			_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{first, slow})

			started := time.Now()
			result, err := sendWrite(t, tc, chainParser, server)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, first.name, result.ProviderInfo.ProviderAddress)
			require.Equal(t, body, string(result.Reply.Data))
			require.False(t, result.IsNodeError)
			require.Less(t, time.Since(started), time.Second, "the first answer must not wait for the slow sibling")
		})
	}
}

// TestRESTWriteBroadcast_EveryProviderAnswersHTML: when every provider answers with an HTML page,
// the client gets a provider's reply unchanged: its status and its body, marked as a node error.
// The router makes up no status of its own.
func TestRESTWriteBroadcast_EveryProviderAnswersHTML(t *testing.T) {
	rand.InitRandomSeed()
	tc := restWriteCase(t)
	chainParser := tc.parser(t)
	page := "<html><body>Just a moment...</body></html>"
	first := writeUpstream{name: "first-" + t.Name(), status: http.StatusOK, body: page}
	second := writeUpstream{name: "second-" + t.Name(), status: http.StatusOK, body: page, delay: 50 * time.Millisecond}
	hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{first, second})

	result, _ := sendWrite(t, tc, chainParser, server)
	require.NotNil(t, result)
	require.Equal(t, http.StatusOK, result.StatusCode, "the node's own status, not a router-made one")
	require.Equal(t, page, string(result.Reply.Data), "the node's own body, unchanged")
	require.True(t, result.IsNodeError)
	require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]))
	require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]))
}
