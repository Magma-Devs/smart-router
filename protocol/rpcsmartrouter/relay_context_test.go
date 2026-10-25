package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// A write's delivery must outlive the request that returned the first acceptance, but only for the
// linger (MAG-4032); a straggler under cross-validation keeps its own bound; a hedged read must not
// outlive the batch, or the loser keeps a session for nothing.
func TestRelayContext(t *testing.T) {
	const linger = 40 * time.Millisecond

	t.Run("stateless dies with the batch", func(t *testing.T) {
		batch, endBatch := context.WithCancel(context.Background())
		relayCtx, cancel := relayContext(batch, relaycore.Stateless, linger)
		defer cancel()
		endBatch()
		require.ErrorIs(t, relayCtx.Err(), context.Canceled)
	})

	t.Run("cross-validation outlives the batch", func(t *testing.T) {
		batch, endBatch := context.WithCancel(context.Background())
		relayCtx, cancel := relayContext(batch, relaycore.CrossValidation, linger)
		defer cancel()
		endBatch()
		time.Sleep(3 * linger)
		require.NoError(t, relayCtx.Err(), "a straggler is bounded by its attempt, not by the linger")
	})

	t.Run("stateful outlives the batch by the linger, then is cut off", func(t *testing.T) {
		batch, endBatch := context.WithCancel(context.Background())
		relayCtx, cancel := relayContext(batch, relaycore.Stateful, linger)
		defer cancel()
		endBatch()
		require.NoError(t, relayCtx.Err(), "the delivery keeps going after the answer")
		select {
		case <-relayCtx.Done():
			require.ErrorIs(t, relayCtx.Err(), context.Canceled)
		case <-time.After(20 * linger):
			t.Fatal("the delivery was never cut off after the linger")
		}
	})

	t.Run("stateful that finishes first is not touched by the linger", func(t *testing.T) {
		batch, endBatch := context.WithCancel(context.Background())
		defer endBatch()
		relayCtx, cancel := relayContext(batch, relaycore.Stateful, linger)
		cancel() // the delivery ended on its own, before the batch
		require.ErrorIs(t, relayCtx.Err(), context.Canceled)
		endBatch()
		time.Sleep(3 * linger) // the linger must not have been armed, and nothing must panic
	})

	t.Run("values survive the detachment", func(t *testing.T) {
		type key struct{}
		batch, endBatch := context.WithCancel(context.WithValue(context.Background(), key{}, "guid"))
		defer endBatch()
		for _, selection := range []relaycore.Selection{relaycore.Stateless, relaycore.Stateful, relaycore.CrossValidation} {
			relayCtx, cancel := relayContext(batch, selection, linger)
			require.Equal(t, "guid", relayCtx.Value(key{}), "selection %d", selection)
			cancel()
		}
	})
}

func TestLostBroadcastRejection(t *testing.T) {
	cases := []struct {
		name                  string
		selection             relaycore.Selection
		answeredWithNodeError bool
		acceptedElsewhere     int
		lost                  bool
	}{
		{"a write's rejection after another upstream accepted", relaycore.Stateful, true, 1, true},
		{"a write's rejection with nothing accepted yet", relaycore.Stateful, true, 0, false},
		{"a write that failed at the transport", relaycore.Stateful, false, 1, false},
		{"a read's node error", relaycore.Stateless, true, 1, false},
		{"a cross-validated node error", relaycore.CrossValidation, true, 1, false},
	}
	for tcIndex, tc := range cases {
		require.Equal(t, tc.lost, lostBroadcastRejection(tc.selection, tc.answeredWithNodeError, tc.acceptedElsewhere), "%s, tc #%d", tc.name, tcIndex)
	}
}
