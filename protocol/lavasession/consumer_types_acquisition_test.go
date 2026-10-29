package lavasession

import (
	"context"
	"sync"
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

	connected, endpoints, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickSkipDisabled, false, "", nil, nil, nil)
	require.NoError(t, err)
	require.True(t, connected)
	require.Len(t, endpoints, 1)

	snapshot := endpoint.HealthSnapshot()
	require.True(t, snapshot.Enabled)
	require.Equal(t, "eth_getBalance", snapshot.RelayProbeMethod, "an acquisition must not erase the episode's recorded failing relay")
}

// benchMidPickConn is the endpoint's real direct connection with one hook: GetURL, which
// connectEndpoint reads after the enabled pre-read and before the post-connect write, runs bench
// once. That lands a MarkUnhealthy inside the window deterministically.
type benchMidPickConn struct {
	DirectRPCConnection
	once  sync.Once
	bench func()
}

func (c *benchMidPickConn) GetURL() string {
	c.once.Do(c.bench)
	return c.DirectRPCConnection.GetURL()
}

// TestAcquisition_NeverReenablesAnEndpointBenchedMidPick is the race MAG-4242 item 11 is named
// for: the endpoint reads as enabled, MarkUnhealthy benches it before the acquisition finishes,
// and the acquisition used to write Enabled = true over the disable — putting a just-benched
// endpoint back into rotation with its refusals at the threshold and its episode (disabledAt,
// reason, recorded relay) erased. In no pick mode may the acquisition change anything about the
// benched endpoint. With main's consumer_types.go the skip and reconnect modes fail; lend was
// already guarded by MAG-4232.
func TestAcquisition_NeverReenablesAnEndpointBenchedMidPick(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode endpointPickMode
	}{
		{"skip", endpointPickSkipDisabled},
		{"reconnect", endpointPickReconnectDisabled},
		{"lend", endpointPickLendDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cswp, endpoint := directRPCProviderForAcquisition(t)
			fired := false
			var benched EndpointHealthSnapshot
			endpoint.DirectConnections[0] = &benchMidPickConn{DirectRPCConnection: endpoint.DirectConnections[0], bench: func() {
				fired = true
				endpoint.RecordFailingRelay("eth_getBalance", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0","latest"]}`), time.Second)
				for i := uint64(0); i < MaxConsecutiveConnectionAttempts; i++ {
					endpoint.MarkUnhealthy(EndpointDisableNodeError)
				}
				benched = endpoint.HealthSnapshot()
			}}

			connected, endpoints, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), tc.mode, false, "", nil, nil, nil)
			require.NoError(t, err)
			require.True(t, connected, "the pick still returns the endpoint it read as enabled")
			require.Len(t, endpoints, 1)
			require.True(t, fired, "the bench must land between the pre-read and the end of the acquisition")
			require.False(t, benched.Enabled)
			require.Equal(t, benched, endpoint.HealthSnapshot(), "the acquisition must leave the benched endpoint exactly as MarkUnhealthy left it")
		})
	}
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

	connected, endpoints, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), endpointPickReconnectDisabled, false, "", nil, nil, nil)
	require.NoError(t, err)
	require.True(t, connected)
	require.Len(t, endpoints, 1)

	snapshot := endpoint.HealthSnapshot()
	require.True(t, snapshot.Enabled, "the reconnect routine re-enables what it reached")
	require.Empty(t, snapshot.DisableReason)
	require.True(t, snapshot.DisabledAt.IsZero())
	require.Empty(t, snapshot.RelayProbeMethod, "the old episode's evidence goes with the re-enable")
}
