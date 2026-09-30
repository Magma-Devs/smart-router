package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// The write's broadcast must outlive the request that returned the first acceptance (MAG-4032);
// a hedged read must not, or the loser keeps a session for nothing.
func TestRelayParentContext(t *testing.T) {
	cases := []struct {
		selection        relaycore.Selection
		outlivesTheBatch bool
	}{
		{relaycore.Stateless, false},
		{relaycore.Stateful, true},
		{relaycore.CrossValidation, true},
	}
	for tcIndex, tc := range cases {
		batch, cancel := context.WithCancel(context.Background())
		relayCtx := relayParentContext(batch, tc.selection)
		cancel()
		if tc.outlivesTheBatch {
			require.NoError(t, relayCtx.Err(), "tc #%d", tcIndex)
		} else {
			require.ErrorIs(t, relayCtx.Err(), context.Canceled, "tc #%d", tcIndex)
		}
	}
}
