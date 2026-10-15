package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

func directRPCProviderForAcquisition(t *testing.T) (*ConsumerSessionsWithProvider, *Endpoint) {
	t.Helper()
	rand.InitRandomSeed()
	nodeUrl := common.NodeUrl{Url: "https://rpc.example/v1"}
	directConn, err := NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	endpoint := &Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []DirectRPCConnection{directConn}}
	cswp := &ConsumerSessionsWithProvider{
		Sessions:          make(map[int64]*SingleConsumerSession),
		PairingEpoch:      100,
		StaticProvider:    true,
		PublicLavaAddress: "direct-rpc-provider",
		Endpoints:         []*Endpoint{endpoint},
	}
	return cswp, endpoint
}

// TestAcquisition_LeavesRecoveryEvidenceAlone is MAG-4242 item 11. Every session acquisition
// used to write Enabled = true and clear the recovery tracking on the endpoint it had just read
// as enabled: a wasted exclusive write per relay that also erased the failing relay recorded for
// the prober to replay (MAG-2550), and — if MarkUnhealthy landed between the pre-read and that
// write — re-enabled a just-benched endpoint with its refusals still at the threshold. A plain
// acquisition must change nothing about the endpoint.
func TestAcquisition_LeavesRecoveryEvidenceAlone(t *testing.T) {
	cswp, endpoint := directRPCProviderForAcquisition(t)
	endpoint.RecordFailingRelay("eth_getBalance", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0","latest"]}`), time.Second)
	require.Equal(t, "eth_getBalance", endpoint.HealthSnapshot().RelayProbeMethod)

	connected, endpoints, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickSkipDisabled, false, "", nil, nil)
	require.NoError(t, err)
	require.True(t, connected)
	require.Len(t, endpoints, 1)

	snapshot := endpoint.HealthSnapshot()
	require.True(t, snapshot.Enabled)
	require.Equal(t, "eth_getBalance", snapshot.RelayProbeMethod, "an acquisition must not erase the episode's recorded failing relay")
}

// TestReconnectRoutine_StillReenablesADisabledEndpoint guards the other caller: the reconnect
// routine asks with endpointPickReconnectDisabled and must still put a disabled endpoint it reached
// back into rotation, with the old episode's tracking cleared.
func TestReconnectRoutine_StillReenablesADisabledEndpoint(t *testing.T) {
	cswp, endpoint := directRPCProviderForAcquisition(t)
	endpoint.RecordFailingRelay("eth_getBalance", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0","latest"]}`), time.Second)
	for i := uint64(0); i < MaxConsecutiveConnectionAttempts; i++ {
		endpoint.MarkUnhealthy(EndpointDisableUnreachable)
	}
	require.False(t, endpoint.HealthSnapshot().Enabled)

	connected, endpoints, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickReconnectDisabled, false, "", nil, nil)
	require.NoError(t, err)
	require.True(t, connected)
	require.Len(t, endpoints, 1)

	snapshot := endpoint.HealthSnapshot()
	require.True(t, snapshot.Enabled, "the reconnect routine re-enables what it reached")
	require.Empty(t, snapshot.DisableReason)
	require.True(t, snapshot.DisabledAt.IsZero())
	require.Empty(t, snapshot.RelayProbeMethod, "the old episode's evidence goes with the re-enable")
}
