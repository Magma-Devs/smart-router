package chainlib

// Compile-time assertions that all concrete listeners satisfy ChainListener.
// If a listener forgets to implement Shutdown, this file fails to compile.
var (
	_ ChainListener = (*JsonRPCChainListener)(nil)
	_ ChainListener = (*TendermintRpcChainListener)(nil)
	_ ChainListener = (*RestChainListener)(nil)
	_ ChainListener = (*GrpcChainListener)(nil)
)
