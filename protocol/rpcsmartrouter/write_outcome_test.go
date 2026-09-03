package rpcsmartrouter

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Both GK8 write incidents ended the same way: the transaction reached the upstream, the upstream
// never answered, our deadline fired, and the customer was told the write had FAILED. It had not —
// it was broadcast. "Failed" is the worst of the three possible answers there, because it invites a
// resubmit of a transaction that may already be on chain.
//
// The rule is about what we can prove. A node's own reply is passed through untouched. A
// connect-phase failure proves nothing was sent. Everything else, including no evidence at all,
// leaves the outcome genuinely unknown, and that is what the client is told.

func relayErrorWith(le *common.LavaError) relaycore.RelayError {
	return relaycore.RelayError{LavaError: le}
}

func TestUnknownWriteOutcome(t *testing.T) {
	for _, tc := range []struct {
		name           string
		successResults []common.RelayResult
		nodeErrors     []common.RelayResult
		protocolErrors []relaycore.RelayError
		want           bool
		why            string
	}{
		{
			name: "no evidence at all",
			want: true,
			why:  "the ordinary hung write since attempts stopped being killed at their window: the goroutine was still in flight when the budget expired, so nothing was ever recorded",
		},
		{
			name:           "deadline exceeded",
			protocolErrors: []relaycore.RelayError{relayErrorWith(common.LavaErrorContextDeadline)},
			want:           true,
			why:            "the failure mode behind both incidents — the upstream had the request and never replied",
		},
		{
			name:           "connection reset mid-flight",
			protocolErrors: []relaycore.RelayError{relayErrorWith(common.LavaErrorConnectionReset)},
			want:           true,
			why:            "a reset arrives on an established connection, so the request may already have been read",
		},
		{
			name:           "connection closed (EOF)",
			protocolErrors: []relaycore.RelayError{relayErrorWith(common.LavaErrorConnectionClosed)},
			want:           true,
			why:            "an EOF likewise means we got as far as an established connection",
		},
		{
			name:           "connection refused",
			protocolErrors: []relaycore.RelayError{relayErrorWith(common.LavaErrorConnectionRefused)},
			want:           false,
			why:            "the connect phase failed, so no bytes were written — this one really did not happen and must not be softened",
		},
		{
			name:           "DNS failure",
			protocolErrors: []relaycore.RelayError{relayErrorWith(common.LavaErrorDNSFailure)},
			want:           false,
			why:            "we never resolved a host, let alone sent to one",
		},
		{
			name: "every endpoint refused",
			protocolErrors: []relaycore.RelayError{
				relayErrorWith(common.LavaErrorConnectionRefused),
				relayErrorWith(common.LavaErrorNetworkUnreachable),
				relayErrorWith(common.LavaErrorTLSMismatch),
			},
			want: false,
			why:  "a fan-out where every attempt failed to connect proves the write did not happen",
		},
		{
			name: "one endpoint refused, another timed out",
			protocolErrors: []relaycore.RelayError{
				relayErrorWith(common.LavaErrorConnectionRefused),
				relayErrorWith(common.LavaErrorContextDeadline),
			},
			want: true,
			why:  "on a fan-out a single endpoint that may have been reached is enough to make the outcome unknown",
		},
		{
			name:           "unclassified protocol error",
			protocolErrors: []relaycore.RelayError{relayErrorWith(nil)},
			want:           true,
			why:            "an error we could not classify proves nothing either, and for a write the safe direction is unknown",
		},
		{
			name:       "the node answered with an error",
			nodeErrors: []common.RelayResult{{StatusCode: 400}},
			want:       false,
			why:        "the node replied, so that reply is the answer — we never overwrite what a node said",
		},
		{
			name:           "a node answered successfully",
			successResults: []common.RelayResult{{StatusCode: 200}},
			want:           false,
			why:            "not a failure at all",
		},
		{
			name:           "a node answered and another timed out",
			successResults: []common.RelayResult{{StatusCode: 200}},
			protocolErrors: []relaycore.RelayError{relayErrorWith(common.LavaErrorContextDeadline)},
			want:           false,
			why:            "a real answer outranks a hung sibling on a fan-out",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want,
				unknownWriteOutcome(tc.successResults, tc.nodeErrors, tc.protocolErrors), tc.why)
		})
	}
}

// The classification the rule depends on, asserted at the registration site rather than trusted.
// Getting one of these backwards silently converts an honest "unclear" into a false "failed", or
// the reverse, on exactly the requests that matter most.
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
