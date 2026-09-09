package rpcsmartrouter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	relaycoretest "github.com/magma-Devs/smart-router/protocol/relaycoretest"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// unknownWriteOutcome is covered directly, but only the wrapper reads the live processor: the write
// gate, the nil guard, the counters it derives from the results snapshot, and the error scan that
// spots a connection cut off mid-delivery. Those readings are where a wrong answer would actually
// come from in production, and nothing exercised them.

// The results manager reads the protocol message when it files a response, so the state machine
// must carry the real one — with it nil, a node reply cannot be classified and is filed as a
// protocol error instead, which changes the very counters under test.
func newWriteOutcomeProcessor(t *testing.T, msg chainlib.ProtocolMessage, dispatched int) *relaycore.RelayProcessor {
	t.Helper()
	usedProviders := lavasession.NewUsedProviders(nil)
	if dispatched > 0 {
		sessions := lavasession.ConsumerSessionsMap{}
		for i := 0; i < dispatched; i++ {
			sessions["lava@"+string(rune('a'+i))] = &lavasession.SessionInfo{}
		}
		usedProviders.AddUsed(sessions, nil)
	}
	sm := &budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: msg}
	return relaycore.NewRelayProcessor(context.Background(), nil, cvGuardMetrics{}, cvGuardMetrics{},
		lavaprotocol.NewRelayRetriesManager(), sm)
}

// SetResponse only queues; the results manager files nothing until the responses are drained. An
// undrained processor reports zero of everything, which reads exactly like "nobody answered".
func drain(t *testing.T, p *relaycore.RelayProcessor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.WaitForResults(ctx)
}

func writeMessage(stateful uint32) *MockProtocolMessage {
	return &MockProtocolMessage{
		api: &spectypes.Api{
			Name:     "eth_sendRawTransaction",
			Category: spectypes.SpecCategory{Stateful: stateful},
		},
	}
}

func TestWriteOutcomeIsUnknown_Wrapper(t *testing.T) {
	t.Run("a read is never given the write message", func(t *testing.T) {
		msg := writeMessage(common.NO_STATE)
		processor := newWriteOutcomeProcessor(t, msg, 1)
		processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
		require.False(t, writeOutcomeIsUnknown(msg, processor),
			"the gate must reject a read before anything else is read off the processor")
	})

	t.Run("no processor means nothing was ever dispatched", func(t *testing.T) {
		require.False(t, writeOutcomeIsUnknown(writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS), nil),
			"the request failed before a processor existed, so no transaction can be in flight")
	})

	t.Run("hung write: dispatched, nothing recorded, budget spent", func(t *testing.T) {
		msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
		processor := newWriteOutcomeProcessor(t, msg, 1)
		processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
		require.True(t, writeOutcomeIsUnknown(msg, processor),
			"an attempt still in flight when the budget expired may be holding the transaction")
	})

	t.Run("nothing dispatched and the budget was not spent", func(t *testing.T) {
		msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
		processor := newWriteOutcomeProcessor(t, msg, 0)
		processor.SetStopReason("AllProvidersExhausted")
		require.False(t, writeOutcomeIsUnknown(msg, processor),
			"no pairings: claiming the transaction may be on chain is the same lie, pointing the other way")
	})

	t.Run("an endpoint served the write", func(t *testing.T) {
		msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
		processor := newWriteOutcomeProcessor(t, msg, 1)
		processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
		relaycoretest.SendNodeError(processor, "lava@a", 0)
		drain(t, processor)
		require.False(t, writeOutcomeIsUnknown(msg, processor),
			"an endpoint served the write, so that answer is the result and nothing is unknown")
	})

	t.Run("one failed at the connect phase, one still silent", func(t *testing.T) {
		msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
		processor := newWriteOutcomeProcessor(t, msg, 2)
		processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
		// Refused proves nothing was sent to THAT endpoint, so this isolates the silence clause:
		// the answer must come from the second endpoint never reporting, not from the first.
		relaycoretest.SendProtocolError(processor, "lava@a", 0, errors.New("connection refused"))
		drain(t, processor)
		require.True(t, writeOutcomeIsUnknown(msg, processor),
			"a sibling's proven non-delivery must not settle this while another endpoint is silent")
	})

	// The gap the review named: covered for a connect-phase failure and for a mid-delivery cut-off
	// beside a silent sibling, but not for a NODE error beside one. That is the shape that reached a
	// customer as "success".
	t.Run("one answered with a node error, one still silent", func(t *testing.T) {
		msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
		msg.repliesAreNodeErrors = true
		processor := newWriteOutcomeProcessor(t, msg, 2)
		processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
		relaycoretest.SendNodeError(processor, "lava@a", 0)
		drain(t, processor)
		require.True(t, writeOutcomeIsUnknown(msg, processor),
			"one endpoint replying with an error settles only itself; the silent sibling may have broadcast")
	})

	// And the reason that verdict never used to be consulted for the case above: the result path
	// hands back a node error with a NIL error, so a caller gated on err != nil skips the check
	// entirely. This pins the shape rather than the old gate, so it keeps its meaning after the fix.
	t.Run("a node error is returned with no error, so the verdict cannot be gated on err", func(t *testing.T) {
		msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
		msg.repliesAreNodeErrors = true
		processor := newWriteOutcomeProcessor(t, msg, 2)
		processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
		relaycoretest.SendNodeError(processor, "lava@a", 0)
		drain(t, processor)

		result, err := processor.ProcessingResult()
		require.NoError(t, err,
			"a node error with no successes comes back as a result, not an error — which is why gating the write verdict on err != nil skipped it")
		require.NotNil(t, result)
		require.True(t, writeOutcomeIsUnknown(msg, processor),
			"the verdict itself was always right; only the caller's gate was wrong")
	})

	t.Run("everyone came back, one was cut off mid-delivery", func(t *testing.T) {
		msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
		processor := newWriteOutcomeProcessor(t, msg, 1)
		processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
		// EOF classifies as PROTOCOL_CONNECTION_CLOSED, which carries MayHaveReachedNode: an
		// established connection died after the request was already on the wire.
		relaycoretest.SendProtocolError(processor, "lava@a", 0, errors.New("EOF"))
		drain(t, processor)
		require.True(t, writeOutcomeIsUnknown(msg, processor),
			"nobody is silent, but a connection that died mid-delivery settles nothing")
	})
}

// An unclear write must never go out with a status that invites a retry or reads as a success.
//
// Two statuses could otherwise reach the client. buildFailureResult stamps 503 when every recorded
// failure was a rate limit, and 503 is the one clients treat as "safe to retry" — the wrong advice
// for a transaction that may already be broadcast, and it also blames the router rather than an
// upstream. And a node error carries the NODE's status, typically 200, so once the verdict is
// consulted on that path an unclear write could go out looking like it worked.
func TestWithUnclearWriteStatus(t *testing.T) {
	t.Run("503 from the all-rate-limited path becomes 500", func(t *testing.T) {
		require.Equal(t, 500, withUnclearWriteStatus(&common.RelayResult{StatusCode: 503}).StatusCode)
	})

	t.Run("a node error's 200 becomes 500", func(t *testing.T) {
		require.Equal(t, 500, withUnclearWriteStatus(&common.RelayResult{StatusCode: 200}).StatusCode)
	})

	t.Run("an unset status becomes 500", func(t *testing.T) {
		require.Equal(t, 500, withUnclearWriteStatus(&common.RelayResult{}).StatusCode)
	})

	t.Run("nil stays nil", func(t *testing.T) {
		require.Nil(t, withUnclearWriteStatus(nil))
	})

	// returnedResult aliases a RelayResult the results manager owns, and the reply path is not its
	// owner, so the stamp must not be applied in place.
	t.Run("the stored result is not mutated", func(t *testing.T) {
		stored := &common.RelayResult{StatusCode: 503}
		stamped := withUnclearWriteStatus(stored)
		require.Equal(t, 503, stored.StatusCode, "the caller's result must be left alone")
		require.Equal(t, 500, stamped.StatusCode)
	})
}
