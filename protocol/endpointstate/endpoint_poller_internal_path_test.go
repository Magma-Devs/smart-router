package endpointstate

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// newTONTPoller builds a poller over a real REST ChainParser from the trimmed
// TON/TONT spec the chainlib package keeps for exactly this shape: two internal
// paths, each with its own head and block-hash directives, no add-ons.
func newTONTPoller(t *testing.T, conn *recordingConnection, internalPath string) *EndpointPoller {
	t.Helper()
	return NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: conn.url, InternalPath: internalPath, Enabled: true},
		conn,
		newRealChainParser(t, "TONT", spectypes.APIInterfaceRest, "../chainlib/testdata"),
		"TONT",
		spectypes.APIInterfaceRest,
	)
}

// TestEndpointPoller_InternalPathUrlPollsItsOwnCollection drives the real poll
// path for a url configured with `internal-path` and no `addons` — the MAG-4105
// shape — and pins the request that leaves the poller.
//
// In MAG-4105 (2026-10-03) the /api/v3 poller sent GET /api/v3/getMasterchainInfo (the /v2
// collection's directive, the first in the file), both vendors answered 500 or
// 404, and the per-url ChainTracker never started. The /v3 case here fails on
// that build with exactly that url.
func TestEndpointPoller_InternalPathUrlPollsItsOwnCollection(t *testing.T) {
	t.Run("v3 polls /masterchainInfo and parses last.seqno", func(t *testing.T) {
		conn := &recordingConnection{
			url:      "https://vendor.example/api/v3",
			respBody: []byte(`{"last":{"workchain":-1,"seqno":88713185,"root_hash":"xfHBPEGmJ6HXvCkYgqBxUdn+MGi9ijSkoh/8pKoVnqY="}}`),
		}
		poller := newTONTPoller(t, conn, "/v3")

		block, err := poller.FetchLatestBlockNum(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(88713185), block)

		require.Len(t, conn.httpCalls, 1)
		require.Equal(t, "GET", conn.httpCalls[0].Method)
		require.Equal(t, "https://vendor.example/api/v3/masterchainInfo", conn.httpCalls[0].URL,
			"a /v3 url must be asked for the /v3 collection's head, not /v2's /getMasterchainInfo")
	})

	t.Run("v2 polls /getMasterchainInfo and parses result.last.seqno", func(t *testing.T) {
		conn := &recordingConnection{
			url:      "https://vendor.example/api/v2",
			respBody: []byte(`{"ok":true,"result":{"last":{"workchain":-1,"seqno":88713190}}}`),
		}
		poller := newTONTPoller(t, conn, "/v2")

		block, err := poller.FetchLatestBlockNum(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(88713190), block)

		require.Len(t, conn.httpCalls, 1)
		require.Equal(t, "https://vendor.example/api/v2/getMasterchainInfo", conn.httpCalls[0].URL)
	})

	t.Run("v3 fetches a block hash from /blocks, not /v2's /lookupBlock", func(t *testing.T) {
		conn := &recordingConnection{
			url:      "https://vendor.example/api/v3",
			respBody: []byte(`{"blocks":[{"workchain":-1,"seqno":42,"root_hash":"xfHBPEGmJ6HXvCkYgqBxUdn+MGi9ijSkoh/8pKoVnqY="}]}`),
		}
		poller := newTONTPoller(t, conn, "/v3")

		hash, err := poller.FetchBlockHashByNum(context.Background(), 42)
		require.NoError(t, err)
		require.Equal(t, "xfHBPEGmJ6HXvCkYgqBxUdn+MGi9ijSkoh/8pKoVnqY=", hash)

		require.Len(t, conn.httpCalls, 1)
		require.Equal(t, "GET", conn.httpCalls[0].Method)
		require.Equal(t, "https://vendor.example/api/v3/blocks?workchain=-1&seqno=42", conn.httpCalls[0].URL)
	})
}
