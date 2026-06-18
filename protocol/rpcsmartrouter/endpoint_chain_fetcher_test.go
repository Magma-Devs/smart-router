package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// TestEndpointChainFetcher_CustomMessage_POSTDelegatesToConnection verifies the
// Solana path: SVMChainTracker calls CustomMessage with the getLatestBlockhash
// JSON-RPC body. The previous implementation returned a hard error, so on every
// Solana-family chain the per-endpoint ChainTracker silently failed to start —
// no OnNewBlock callback, no per-endpoint metrics, backup rows stuck at N/A.
// This test asserts that CustomMessage now delegates to the direct RPC connection
// and returns the real response payload.
func TestEndpointChainFetcher_CustomMessage_POSTDelegatesToConnection(t *testing.T) {
	const (
		url        = "https://solana.lava.build:443/"
		svmRequest = `{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"finalized"}]}`
		svmResp    = `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":100},"value":{"blockhash":"abc","lastValidBlockHeight":42}}}`
	)

	conn := &mockDirectRPCConnection{
		url: url,
		responses: map[string][]byte{
			svmRequest: []byte(svmResp),
		},
	}
	fetcher := NewEndpointChainFetcher(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		conn,
		nil, // chainParser unused by the POST path
		"SOLANA",
		"jsonrpc",
	)

	got, err := fetcher.CustomMessage(context.Background(), "", []byte(svmRequest), "POST", "getLatestBlockhash")
	require.NoError(t, err,
		"CustomMessage must not return a stub error — SVMChainTracker depends on it for getLatestBlockhash")
	require.Equal(t, svmResp, string(got),
		"CustomMessage must return the actual upstream response body")
}

// TestEndpointChainTrackerManager_ForcesBlocksToSave1ForSolana guards the blocksToSave
// override that sidesteps SVMChainTracker's slot-cache-only-for-latest-block limitation.
// When blocksToSave > 1 the ChainTracker init loop fetches hashes for historical blocks,
// and on every Solana-family chain those fetches fail with "slot not found in cache",
// killing the tracker before OnNewBlock can fire.
func TestEndpointChainTrackerManager_ForcesBlocksToSave1ForSolana(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range []struct {
		chainID  string
		expected uint64
		reason   string
	}{
		{"SOLANA", 1, "Solana mainnet must force blocksToSave=1 to avoid SVMChainTracker slot-cache misses"},
		{"SOLANAT", 1, "Solana testnet uses the same SVMChainTracker"},
		{"KOII", 1, "KOII is a Solana fork — same chain tracker family"},
		{"ETH", 10, "EVM chains keep the caller-requested blocksToSave"},
		{"LAVA", 10, "non-SVM chains keep the caller-requested blocksToSave"},
	} {
		t.Run(tc.chainID, func(t *testing.T) {
			m := NewEndpointChainTrackerManager(ctx, EndpointChainTrackerConfig{
				ChainID:      tc.chainID,
				ApiInterface: "jsonrpc",
				BlocksToSave: 10,
			})
			require.NotNil(t, m)
			defer m.Stop()
			require.Equal(t, tc.expected, m.blocksToSave, tc.reason)
		})
	}
}

// TestEndpointChainFetcher_CustomMessage_PropagatesMissingConnection guards the
// remaining nil-connection check in sendRawRequest: with no direct connection the
// fetcher must surface an error rather than treat an empty body as a successful
// fetch. (The old per-socket health gate is gone — a live-but-failing socket now
// surfaces its failure through SendRequest itself, which the relay path turns into
// a QoS penalty, instead of being pre-empted by a latched health bit.)
func TestEndpointChainFetcher_CustomMessage_PropagatesMissingConnection(t *testing.T) {
	fetcher := NewEndpointChainFetcher(
		&lavasession.Endpoint{NetworkAddress: "https://solana.lava.build:443/", Enabled: true},
		nil, // no direct connection
		nil,
		"SOLANA",
		"jsonrpc",
	)

	_, err := fetcher.CustomMessage(context.Background(), "", []byte(`{}`), "POST", "getLatestBlockhash")
	require.Error(t, err, "CustomMessage must fail when there is no direct connection")
}
