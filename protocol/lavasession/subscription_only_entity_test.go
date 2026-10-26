package lavasession

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// directEndpoint builds the endpoint convertProvidersToSessions builds for one node url: a
// direct connection whose protocol follows the url's scheme. Nothing is dialed here.
func directEndpoint(t *testing.T, url string) *Endpoint {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: url}, 5, "jsonrpc")
	require.NoError(t, err)
	return &Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []DirectRPCConnection{conn}, Addons: map[string]struct{}{}, Extensions: map[string]struct{}{}}
}

func entity(name, parent string, endpoints ...*Endpoint) *ConsumerSessionsWithProvider {
	p := NewConsumerSessionWithProvider(name, endpoints, 200, firstEpochHeight, 10)
	p.StaticProvider = true
	p.Parent = parent
	p.GroupLabel = "tier-1"
	return p
}

// MAG-4219. A vendor's ws url beside its https url is a provider entity of its own. It cannot
// answer a relay (MAG-3477), so it must never be a relay candidate — before this rule the
// optimizer picked it on every other selection and the pick fell through — while it keeps the
// place in the pairing that gives it a chain tracker, probes, a health gauge and its series.
func TestSubscriptionOnlyEntity_TrackedButNeverARelayCandidate(t *testing.T) {
	csm := CreateConsumerSessionManager()
	primaries := map[uint64]*ConsumerSessionsWithProvider{
		0: entity("up-a/0", "up-a", directEndpoint(t, "http://127.0.0.1:1")),
		1: entity("up-a/1", "up-a", directEndpoint(t, "wss://127.0.0.1:1/ws")),
	}
	backups := map[uint64]*ConsumerSessionsWithProvider{
		0: entity("bk/0", "bk", directEndpoint(t, "ws://127.0.0.1:2")),
		1: entity("bk/1", "bk", directEndpoint(t, "http://127.0.0.1:2")),
	}
	require.True(t, primaries[1].servesOnlySubscriptions())
	require.False(t, primaries[0].servesOnlySubscriptions())
	require.False(t, (&ConsumerSessionsWithProvider{}).servesOnlySubscriptions(), "no endpoints is not this rule's business")

	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, primaries, backups))
	time.Sleep(5 * time.Millisecond)

	csm.lock.RLock()
	_, inPairing := csm.pairing["up-a/1"]
	_, inBackupPairing := csm.backupProviders["bk/0"]
	valid := slices.Clone(csm.validAddresses)
	length := csm.pairingAddressesLength
	eligibleBackups := csm.eligibleBackupsForStateful(context.Background(), map[string]struct{}{}, "", nil, nil, true)
	csm.lock.RUnlock()

	// In the pairing: trackers, probes and metrics find it.
	require.True(t, inPairing, "the ws entity is in the pairing map")
	require.True(t, inBackupPairing)
	tracked := make([]string, 0)
	for _, ep := range csm.GetAllDirectRPCEndpoints() {
		tracked = append(tracked, ep.ProviderAddress)
	}
	require.ElementsMatch(t, []string{"up-a/0", "up-a/1", "bk/0", "bk/1"}, tracked, "every entity has a direct endpoint for the trackers and the probes")

	// Not a candidate: never picked, never counted.
	require.Equal(t, []string{"up-a/0"}, valid)
	require.Equal(t, uint64(1), length)
	require.Equal(t, []string{"up-a/0"}, csm.getValidAddresses("", nil, context.Background()))
	require.Equal(t, map[string][]string{"tier-1": {"up-a/0"}}, csm.ProviderGroupAssignments())
	require.Equal(t, []string{"bk/1"}, eligibleBackups, "the ws backup is not a write-broadcast candidate")

	for range 20 {
		got, err := csm.getValidProviderAddresses(context.Background(), 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"up-a/0"}, got)
	}

	// The pin header names the configured provider: only the relay-capable url answers.
	got, err := csm.getValidProviderAddresses(context.Background(), 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "up-a", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"up-a/0"}, got)
}

// A provider whose every url is a websocket has no relay entity at all: relays find no
// pairing, exactly what main did after the first pick failed, and subscriptions are served by
// the subscription manager from its own list.
func TestSubscriptionOnlyEntity_WebsocketOnlyProviderLeavesNoRelayCandidate(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, map[uint64]*ConsumerSessionsWithProvider{
		0: entity("wsonly/0", "wsonly", directEndpoint(t, "wss://127.0.0.1:1/ws")),
	}, nil))
	_, err := csm.getValidProviderAddresses(context.Background(), 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "", nil)
	require.Error(t, err)
	require.Len(t, csm.GetAllDirectRPCEndpoints(), 1, "still tracked and probed")
}
