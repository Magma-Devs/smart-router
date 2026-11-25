package rpcsmartrouter

import (
	"cmp"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// TestRelayEnd_SuiGraphQLWrites (MAG-4909): relay_end reads a Sui GraphQL write the way the router
// does. Each answered copy carries the write_outcome Sui's write rows give it. A write that ends
// unknown because a node answered with an error that may have run names that error's code, not "".
// tx_hash is the digest the reply names, else the one computed from the request: the digest the
// router's unknown reply hands the client.
func TestRelayEnd_SuiGraphQLWrites(t *testing.T) {
	rand.InitRandomSeed()
	read := captureRelayEnds(t)
	const (
		ran        = `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"}}}}`
		ranDigest  = `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS","digest":"7Lq3VbHbiAnZZzfbEcmCnQd3ys8KaWnGQwcFpoW3DUvk"}}}}`
		networkNo  = `{"data":null,"errors":[{"message":"Invalid argument: Transaction is rejected as invalid by more than 1/3 of validators by stake (non-retriable).","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`
		internal   = `{"data":null,"errors":[{"message":"Failed to execute transaction","path":["executeTransaction"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`
		refusedBcs = `{"data":null,"errors":[{"message":"Invalid BCS-encoded TransactionData: unexpected end of input","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`
	)
	for i, c := range []struct {
		name             string
		a, b             string
		statusA, statusB int           // 200 when 0
		delayA, delayB   time.Duration // a copy that ran ends the relay, so a later one is cancelled
		outcome          string
		errorCode        string
		txHash           string
		writeOutcomes    [2]any // a's and b's; nil for a copy with no reply
	}{
		{
			name: "a copy that ran is the answer", a: ran, b: networkNo, delayA: 150 * time.Millisecond,
			outcome: relayOutcomeOK, txHash: suiSnapshotDigest, writeOutcomes: [2]any{"accepted", "may_have_run"},
		},
		{
			name: "the reply's digest wins over the request's", a: ranDigest, b: ranDigest, delayB: 2 * time.Second,
			outcome: relayOutcomeOK, txHash: "7Lq3VbHbiAnZZzfbEcmCnQd3ys8KaWnGQwcFpoW3DUvk", writeOutcomes: [2]any{"accepted", nil},
		},
		{
			name: "every copy may have run: the code of the node's error", a: networkNo, b: networkNo,
			outcome: relayOutcomeWriteUnknown, errorCode: common.RouterErrorUserInvalidRequest.Name, txHash: suiSnapshotDigest,
			writeOutcomes: [2]any{"may_have_run", "may_have_run"},
		},
		{
			name: "a refusal beside a copy that may have run: the code of the one that may have run", a: refusedBcs, b: internal,
			outcome: relayOutcomeWriteUnknown, errorCode: common.RouterErrorNodeInternalError.Name, txHash: suiSnapshotDigest,
			writeOutcomes: [2]any{"rejected", "may_have_run"},
		},
		{
			// A gateway's 503 comes back as a transport error, with no node reply; its body is still
			// read by the write rows, as unclearWriteOutcomeFor reads it (W11, MAG-5122 T13).
			name: "every copy a gateway's 503: may have run, the status's code", a: "upstream down", statusA: http.StatusServiceUnavailable, b: "upstream down", statusB: http.StatusServiceUnavailable,
			outcome: relayOutcomeWriteUnknown, errorCode: common.RouterErrorNodeServiceUnavailable.Name, txHash: suiSnapshotDigest,
			writeOutcomes: [2]any{"may_have_run", "may_have_run"},
		},
		{
			name: "every copy refused: the node's error", a: refusedBcs, b: refusedBcs,
			outcome: relayOutcomeNodeError, errorCode: common.RouterErrorUserInvalidRequest.Name, txHash: suiSnapshotDigest,
			writeOutcomes: [2]any{"rejected", "rejected"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			tc := suiWriteOf(t, suiEffectsQuery)
			chainParser := tc.parser(t)
			a := writeUpstream{name: "a-" + t.Name(), status: cmp.Or(c.statusA, http.StatusOK), body: c.a, delay: c.delayA}
			b := writeUpstream{name: "b-" + t.Name(), status: cmp.Or(c.statusB, http.StatusOK), body: c.b, delay: c.delayB}
			_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

			guid := uint64(4909_000 + i)
			_, _ = sendWriteAs(t, guid, tc, chainParser, server)

			event := relayEndOfGUID(t, read, guid)
			require.Equal(t, c.outcome, event["outcome"])
			require.Equal(t, c.errorCode, event["error_code"])
			require.Equal(t, c.txHash, event["tx_hash"])
			attempts, ok := event["attempts"].([]any)
			require.True(t, ok, "a write lists its attempts")
			got := map[string]any{}
			for _, attempt := range attempts {
				fields, ok := attempt.(map[string]any)
				require.True(t, ok, "an attempt is an object")
				upstream, ok := fields["upstream"].(string)
				require.True(t, ok, "an attempt names its upstream")
				got[upstream] = fields["write_outcome"]
			}
			require.Equal(t, map[string]any{a.name: c.writeOutcomes[0], b.name: c.writeOutcomes[1]}, got)
		})
	}
}

// relayEndOfGUID is the one relay_end the request with guid wrote, decoded.
func relayEndOfGUID(t *testing.T, read func() []string, guid uint64) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range read() {
		var event map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &event))
		if event["GUID"] == strconv.FormatUint(guid, 10) {
			found = append(found, event)
		}
	}
	require.Len(t, found, 1, "one relay_end for the request")
	return found[0]
}
