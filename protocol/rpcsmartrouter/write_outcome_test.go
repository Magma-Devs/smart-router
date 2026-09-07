package rpcsmartrouter

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A write that reached an upstream which never answered used to be reported as failed. That is the
// worst of the three answers: it invites a resubmit of a transaction that may already be on chain.
//
// The rule is silence. A node's own reply passes through untouched; a connect-phase failure proves
// nothing was sent; anything else, including no evidence at all, leaves the outcome unknown.
func TestUnknownWriteOutcome(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		successes, answered, dispatched int
		ranOutOfRoad, cutOff            bool
		want                            bool
		why                             string
	}{
		{
			name: "one endpoint asked, it hung", answered: 0, dispatched: 1, ranOutOfRoad: true, want: true,
			why: "nothing recorded because the attempt was still in flight when the budget expired",
		},
		{
			name: "one answered with an error, one still silent", answered: 1, dispatched: 2, ranOutOfRoad: true, want: true,
			why: "a sibling's fast error must not decide this while another endpoint may hold the transaction",
		},
		{
			name: "one refused, one still silent", answered: 1, dispatched: 2, ranOutOfRoad: true, want: true,
			why: "a proven non-delivery on one endpoint says nothing about the other",
		},
		{
			name: "every endpoint answered, all with node errors", answered: 3, dispatched: 3, want: false,
			why: "every node replied, so the outcome is known and the reply passes through untouched",
		},
		{
			name: "every endpoint refused", answered: 2, dispatched: 2, want: false,
			why: "connect-phase failures prove nothing was sent — this write really did not happen",
		},
		{
			name:     "single endpoint, connection died after the request went out",
			answered: 1, dispatched: 1, cutOff: true, want: true,
			why: "a reset or EOF mid-delivery settles nothing; the node may already have the transaction",
		},
		{
			name:     "every endpoint answered, one was cut off mid-delivery",
			answered: 3, dispatched: 3, cutOff: true, want: true,
			why: "nobody is silent, but one of them never actually answered",
		},
		{
			name: "one succeeded", successes: 1, answered: 2, dispatched: 3, cutOff: true, want: false,
			why: "an endpoint served the write; that answer is the result whatever happened elsewhere",
		},
		{
			name: "nothing recorded and nothing dispatched", answered: 0, dispatched: 0, ranOutOfRoad: false, want: false,
			why: "no pairings — claiming the transaction may be on chain is the same lie, pointing the other way",
		},
		{
			name: "nothing recorded but the budget was spent", answered: 0, dispatched: 0, ranOutOfRoad: true, want: true,
			why: "only a request that used its whole budget can have had an attempt in flight",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want,
				unknownWriteOutcome(tc.successes, tc.answered, tc.dispatched, tc.ranOutOfRoad, tc.cutOff), tc.why)
		})
	}
}

// The classification the rule reads for the cut-off-mid-delivery case. Getting one backwards turns
// an honest "unclear" into a false "failed", or the reverse, on the requests that matter most.
func TestMayHaveReachedNodeClassification(t *testing.T) {
	for _, tc := range []struct {
		err  *common.LavaError
		want bool
	}{
		{common.LavaErrorConnectionTimeout, true},
		{common.LavaErrorConnectionReset, true},
		{common.LavaErrorConnectionClosed, true},
		{common.LavaErrorContextDeadline, true},

		{common.LavaErrorConnectionRefused, false},
		{common.LavaErrorDNSFailure, false},
		{common.LavaErrorTLSMismatch, false},
		{common.LavaErrorNetworkUnreachable, false},
		{common.LavaErrorContextCanceled, false},
	} {
		t.Run(tc.err.Name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.err.MayHaveReachedNode)
		})
	}
}

// What the customer actually receives. The message travels to the client body verbatim — SendRelay
// returns SendParsedRelay's error unwrapped, and the REST envelope carries err.Error() through
// GetUniqueGuidResponseForError — so this pins the text end to end rather than trusting that chain.
//
// It also guards the redaction step: that function runs every client-facing error through
// RedactSecrets, and a message mangled there would reach the customer mangled.
func TestUnknownWriteOutcomeReachesTheClientVerbatim(t *testing.T) {
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	envelope := logger.GetUniqueGuidResponseForError(errUnknownWriteOutcome, "1234567890")

	var body struct {
		ErrorGUID string `json:"Error_GUID"`
		Error     string `json:"Error"`
	}
	require.NoError(t, json.Unmarshal([]byte(envelope), &body), "the envelope must be valid JSON")
	assert.Equal(t, "1234567890", body.ErrorGUID)
	assert.Equal(t, errUnknownWriteOutcome.Error(), body.Error,
		"the message must reach the client exactly as written, redaction included")

	// The three things the customer has to be able to read out of it.
	assert.Contains(t, body.Error, "status unclear")
	assert.Contains(t, body.Error, "may already have been submitted")
	assert.Contains(t, body.Error, "verify on-chain before resubmitting")

	// And the one thing it must never say.
	assert.False(t, strings.Contains(strings.ToLower(body.Error), "failed"),
		"a write we cannot prove undelivered must not be reported as failed")
}
