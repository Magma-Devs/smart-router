package rpcsmartrouter

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// The slot-already-used replies of writeInterfaceCases' chains (PRD §5.2, MAG-5593): the EVM node's
// "nonce too low" on ETH1 (and, as a text row, on Sui's GraphQL), the Cosmos SDK's sequence mismatch
// on LAVA. Each is what a node answers the second copy of a transaction it already holds.
var slotAlreadyUsedBodies = map[string]string{
	"jsonrpc": `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}`,
	"rest":    `{"code":32,"message":"account sequence mismatch, expected 5, got 3: incorrect account sequence","details":[]}`,
	"graphql": `{"errors":[{"message":"nonce too low"}]}`,
}

// hasWriteOutcomeHeader reports whether the reply carries Router-Write-Outcome.
func hasWriteOutcomeHeader(result *common.RelayResult) bool {
	for _, md := range result.Reply.GetMetadata() {
		if md.Name == common.WRITE_OUTCOME_HEADER_NAME {
			return true
		}
	}
	return false
}

// A write whose every copy was refused for rate is the one write main sends again, to a provider not
// yet used (a backup). When one of those refusals carries a slot-already-used reply, this very
// transaction may be what used the slot, and the write is not sent again: the second send is the
// double-spend the rule exists to prevent. The client still gets the node's own 429, as before.
func TestStatefulBroadcastIsNotSentAgainAfterASlotAlreadyUsedReply(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range writeInterfaceCases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			slot := writeUpstream{name: "slot-" + t.Name(), status: http.StatusTooManyRequests, body: slotAlreadyUsedBodies[tc.name]}
			limited := writeUpstream{name: "limited-" + t.Name(), status: http.StatusTooManyRequests, body: `{"code":-32007,"message":"50/second request limit reached"}`}
			backup := writeUpstream{name: "backup-" + t.Name(), status: http.StatusOK, body: tc.success}
			hits := make([]int64, 3)
			server := newWriteServer(t, tc, chainParser, []writeUpstream{slot, limited}, []writeUpstream{backup}, hits)

			result, err := sendWrite(t, tc, chainParser, server)
			require.Error(t, err, "no provider executed the write")
			require.NotNil(t, result)
			require.Equal(t, http.StatusTooManyRequests, result.StatusCode, "the client gets the node's own 429, as before")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "the slot-already-used provider received the write once")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "the rate-limited provider received the write once")
			// A re-send would have landed before the reply, as a retry round; the wait only catches one
			// dispatched and not yet delivered.
			time.Sleep(100 * time.Millisecond)
			require.Equal(t, int64(0), atomic.LoadInt64(&hits[2]),
				"the write was sent again, to the backup: a copy said the slot is used, so the transaction may already be on chain")
		})
	}
}

// A copy answered under a 5xx with a slot-already-used reply may have run, however loudly the other
// copies failed: the outcome is unknown and the client is told so, in place of the node's 5xx, which
// reads as "failed" and gets a new transaction signed. Not the case on main, where every node error
// settles its copy and the client got the 503.
func TestStatefulWriteIsUnknownWhenA5xxSaysTheSlotIsUsed(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range writeInterfaceCases {
		if tc.apiInterface == spectypes.APIInterfaceGraphQL {
			continue // GraphQL states its own outcomes (MAG-5122); its 5xx is MAG-4909's
		}
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			slot := writeUpstream{name: "slot-" + t.Name(), status: http.StatusServiceUnavailable, body: slotAlreadyUsedBodies[tc.name]}
			down := writeUpstream{name: "down-" + t.Name(), status: http.StatusServiceUnavailable, body: `{"message":"no healthy upstream"}`}
			hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{slot, down})

			result, err := sendWrite(t, tc, chainParser, server)
			require.ErrorIs(t, err, errUnclearWriteSlotAlreadyUsed, "the client is told the outcome is unknown and why")
			require.NotNil(t, result)
			require.Equal(t, http.StatusInternalServerError, result.StatusCode, "an unclear write is the router's 500, never the node's 503")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]))
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]))
		})
	}
}

// A 2xx that says the slot is used is the node's own answer and goes back as it came, with no
// Router-Write-Outcome, while Part IV decision 3 parks the rule (MAG-4748): the signature changes
// nothing about the reply, and the write is still not sent again.
func TestStatefulWrite2xxSlotAlreadyUsedReplyIsForwardedAsIs(t *testing.T) {
	rand.InitRandomSeed()
	bodies := map[string]string{
		"jsonrpc": slotAlreadyUsedBodies["jsonrpc"],
		"rest":    `{"tx_response":{"code":32,"raw_log":"account sequence mismatch, expected 5, got 3: incorrect account sequence","txhash":"ABC"}}`,
	}
	for _, tc := range writeInterfaceCases {
		body, ok := bodies[tc.name]
		if !ok {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			first := writeUpstream{name: "slot-a-" + t.Name(), status: http.StatusOK, body: body}
			second := writeUpstream{name: "slot-b-" + t.Name(), status: http.StatusOK, body: body}
			hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{first, second})

			result, err := sendWrite(t, tc, chainParser, server)
			require.NoError(t, err, "a node's 2xx answer is forwarded, not failed")
			require.NotNil(t, result)
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.JSONEq(t, body, string(result.Reply.Data), "the node's reply, as it came")
			require.False(t, hasWriteOutcomeHeader(result), "no Router-Write-Outcome while the rule is parked")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]))
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]))
		})
	}
}

// An accepted sibling is the result whatever another copy said about the slot: the node that holds
// the transaction answered, and the one that saw it arrive by gossip said "nonce too low".
func TestStatefulWriteAcceptedSiblingWinsOverASlotAlreadyUsedCopy(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range writeInterfaceCases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			slot := writeUpstream{name: "slot-" + t.Name(), status: http.StatusServiceUnavailable, body: slotAlreadyUsedBodies[tc.name]}
			accepted := writeUpstream{name: "accepted-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 100 * time.Millisecond}
			_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{slot, accepted})

			result, err := sendWrite(t, tc, chainParser, server)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, accepted.name, result.ProviderInfo.ProviderAddress, "the accepted copy is the result")
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.JSONEq(t, tc.success, string(result.Reply.Data))
		})
	}
}
