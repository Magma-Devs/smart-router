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
