package chainstate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDebugSnapshot_RawUngatedFields verifies DebugSnapshot reports the raw ChainState fields —
// including a tip that the production GetLatestBlock getter would hide as TTL-expired — so the
// MAG-2202 /debug/chain-state black-box tests can observe TTL expiry and baseline establishment
// directly rather than seeing a collapsed (0, false).
func TestDebugSnapshot_RawUngatedFields(t *testing.T) {
	cs, clk := newTestState(t)

	// Cold start: nothing observed yet — every field is zero/false.
	snap := cs.DebugSnapshot()
	require.False(t, snap.Initialized)
	require.Equal(t, int64(0), snap.ObservedTip)
	require.False(t, snap.HasBaseline)
	require.True(t, snap.LastObservedAt.IsZero())
	require.True(t, snap.BaselineSince.IsZero())

	// Observe a tip and establish a 3-endpoint strict-majority baseline.
	cs.SetLatestBlock(1000)
	observedAt := clk.now()
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
		{URL: "c", Block: 1000, ObservedAt: clk.now()},
	})
	baselineSince := clk.now()

	snap = cs.DebugSnapshot()
	require.True(t, snap.Initialized)
	require.Equal(t, int64(1000), snap.ObservedTip)
	require.True(t, snap.HasBaseline)
	require.Equal(t, int64(1000), snap.ConsensusBaseline)
	require.Equal(t, observedAt, snap.LastObservedAt)
	require.Equal(t, baselineSince, snap.BaselineSince)

	// Age past the 10s TTL: GetLatestBlock now gates the tip to (0,false), but the RAW snapshot
	// still exposes the underlying block + its (now stale) timestamp so a test can assert that the
	// expiry actually happened rather than seeing a value that silently disappeared.
	clk.advance(11 * time.Second)
	_, fresh := cs.GetLatestBlock()
	require.False(t, fresh, "GetLatestBlock applies the TTL gate")
	snap = cs.DebugSnapshot()
	require.Equal(t, int64(1000), snap.ObservedTip, "DebugSnapshot is NOT TTL-gated")
	require.True(t, snap.Initialized, "initialized is sticky across TTL expiry")
}
