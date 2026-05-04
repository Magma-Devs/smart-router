package chainlib

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Compile-time assertions that all concrete listeners satisfy ChainListener.
// If a listener forgets to implement Shutdown, this file fails to compile.
var (
	_ ChainListener = (*EmptyChainListener)(nil)
	_ ChainListener = (*JsonRPCChainListener)(nil)
	_ ChainListener = (*TendermintRpcChainListener)(nil)
	_ ChainListener = (*RestChainListener)(nil)
	_ ChainListener = (*GrpcChainListener)(nil)
)

func TestEmptyChainListener_Shutdown_ReturnsNil(t *testing.T) {
	listener := NewEmptyChainListener()
	require.NoError(t, listener.Shutdown(context.Background()))
}
