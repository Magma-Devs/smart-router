package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// The failed-provider retry stores the pairing by the same rule as the epoch refresh
// and /debug/reset-pairing: a backup tier it hands the session manager empty is not
// kept as a chain entry. Before the three paths shared pushPairingLocked the retry was
// the one that skipped the delete, so a chain entry holding an empty backup map
// survived the readmit. No production path writes such an entry today (boot and the
// epoch refresh never store an empty backup map), so this pins the shared rule rather
// than a reachable fault.
func TestReadmitRecoveredProviders_StoresExactlyWhatTheSessionManagerIsHanded(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm

	recovered := bootTestProviders("primary1")
	rpsr.failedStaticProviders[chainKey] = recovered
	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{}

	rpsr.readmitRecoveredProviders(chainKey, rpcEndpoint,
		func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
			out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
			for i, p := range providers {
				out[uint64(i)] = createTestProviderSession(p.Name, 0)
			}
			return out
		},
		recovered, nil, nil, nil)

	require.Len(t, rpsr.providerSessions[chainKey], 1, "primary re-admitted")
	require.NotContains(t, rpsr.backupProviderSessions, chainKey,
		"an empty backup tier is not kept as a chain entry, as on the epoch refresh and reset-pairing")
}

// collectEndpoints picks a transport's urls the way the two collectors it replaced did:
// WebSocket takes the ws:// and wss:// urls of any provider, gRPC takes every url of a
// gRPC provider and nothing from the others.
func TestCollectEndpoints_EachTransportTakesItsOwnUrls(t *testing.T) {
	jsonrpc := &lavasession.RPCStaticProviderEndpoint{
		Name: "evm", ChainID: "ETH1", ApiInterface: "jsonrpc",
		NodeUrls: []common.NodeUrl{{Url: "https://evm"}, {Url: "WSS://evm/ws"}, {Url: "ws://evm/plain"}},
	}
	grpc := &lavasession.RPCStaticProviderEndpoint{
		Name: "cosmos", ChainID: "LAV1", ApiInterface: "grpc",
		NodeUrls: []common.NodeUrl{{Url: "cosmos:9090"}, {Url: "grpcs://cosmos:443"}},
	}
	providers := []*lavasession.RPCStaticProviderEndpoint{jsonrpc, grpc}

	urls := func(endpoints []*common.NodeUrl) []string {
		out := make([]string, 0, len(endpoints))
		for _, e := range endpoints {
			out = append(out, e.Url)
		}
		return out
	}
	require.Equal(t, []string{"WSS://evm/ws", "ws://evm/plain"}, urls(collectEndpoints(wsSubscriptions, providers, nil, "")))
	require.Equal(t, []string{"cosmos:9090", "grpcs://cosmos:443"}, urls(collectEndpoints(grpcSubscriptions, providers, nil, "primary")))
}
