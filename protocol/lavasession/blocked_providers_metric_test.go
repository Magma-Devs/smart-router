package lavasession

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
)

// The blocked-providers gauge published len(previousEpochBlockedProviders) — the cross-epoch
// carry-over set, which is populated at an epoch boundary and cleared moments later by the
// re-block pass. So it read 0 while providers were blocked and serving no traffic, and an
// all-providers-down outage looked identical to a healthy router on every dashboard.
//
// It now publishes len(currentlyBlockedProviderAddresses), the standing block. These tests pin
// both halves so the two can never be confused again.

// blockOneProvider blocks a provider the way the product does, rather than by writing the field.
func blockOneProvider(t *testing.T, csm *ConsumerSessionManager, address string) {
	t.Helper()
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonExplicitSignal, false, csm.atomicReadCurrentEpoch(), 0, 0, nil))
}

// A blocked provider must be visible in the gauge. This is the test whose absence let the bug ship.
func TestBlockedProvidersGauge_ReflectsARealBlock(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	// Capture both up front. blockProvider removes the address from validAddresses, so reading
	// validAddresses[0] twice would silently mean two different providers.
	first, second := csm.validAddresses[0], csm.validAddresses[1]

	csm.publishStateSizes()
	require.Equal(t, 0, rec.blockedProvidersCount(), "nothing blocked yet")

	blockOneProvider(t, csm, first)
	csm.publishStateSizes()
	require.Equal(t, 1, rec.blockedProvidersCount(), "a blocked provider must show in the gauge")

	blockOneProvider(t, csm, second)
	csm.publishStateSizes()
	require.Equal(t, 2, rec.blockedProvidersCount(), "the gauge tracks the size of the blocked list")

	// The previous-epoch carry-over set is a different thing and must not move with it.
	require.Equal(t, 0, rec.prevEpochBlockedCount(), "blocking does not populate the cross-epoch set")
}

// ResetBlockedProviders is what /debug/reset-all uses to clear the standing block, so it must
// drive the gauge back to zero. ResetTransientFailureState deliberately does not.
func TestBlockedProvidersGauge_ZeroedByResetBlockedProviders(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	blockOneProvider(t, csm, csm.validAddresses[0])
	csm.publishStateSizes()
	require.Equal(t, 1, rec.blockedProvidersCount())

	csm.ResetTransientFailureState()
	require.Equal(t, 1, rec.blockedProvidersCount(),
		"ResetTransientFailureState preserves the standing block by design, so the gauge must hold")

	csm.ResetBlockedProviders()
	require.Equal(t, 0, rec.blockedProvidersCount(),
		"ResetBlockedProviders clears the standing block, so the gauge must follow")
}

// The per-provider gauge must survive a wholesale drain of the blocked list.
//
// setValidAddressesToDefaultValue empties currentlyBlockedProviderAddresses in one move and
// publishes nothing per provider. It runs on the pool-empty release — the last resort of the
// failover cascade, i.e. exactly the all-providers-down case this metric exists for — and on
// every epoch transition. Publishing only on the block/unblock edges therefore left every
// series it emptied stuck at 1 for a provider that was back in rotation and serving fine, so
// the aggregate count and the per-provider gauge disagreed on the dashboard.
func TestPerProviderGauge_ClearedByThePoolEmptyRelease(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	// An all-providers-down outage: block every provider in the pairing.
	for len(csm.validAddresses) > 0 {
		blockOneProvider(t, csm, csm.validAddresses[0])
	}
	csm.publishStateSizes()

	duringOutage := rec.providerBlockedSnapshot()
	require.NotEmpty(t, duringOutage, "every provider should have been published")
	for address, isBlocked := range duringOutage {
		require.True(t, isBlocked, "%s must read blocked during the outage", address)
	}

	// The last resort of the failover cascade: release the whole blocked list so the pool has
	// something left to serve from.
	csm.resetValidAddresses("", nil)
	csm.publishStateSizes()

	require.Equal(t, 0, rec.blockedProvidersCount(), "the release drained the standing block")
	for address, isBlocked := range rec.providerBlockedSnapshot() {
		require.False(t, isBlocked,
			"%s is back in rotation and serving, so its per-provider gauge must not still read blocked", address)
	}
}

// A backup is blocked in its own store (blockedBackupProviders), which the per-provider gauge never
// read: a blocked backup moved the smartrouter_csm_blocked_backup_providers count and nothing else,
// so which backup was out was on no series (MAG-4735). The gauge now covers both pools.
func TestPerProviderGauge_CoversTheBackupPool(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createNamedPairingList("regular-a"), createNamedPairingList("backup-b")))

	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"regular-a": false, "backup-b": false}, rec.providerBlockedSnapshot(),
		"both pools are published, nothing blocked")

	blockOneProvider(t, csm, "backup-b")
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"regular-a": false, "backup-b": true}, rec.providerBlockedSnapshot(),
		"a blocked backup must read blocked")
	require.Equal(t, 0, rec.blockedProvidersCount(), "the regular count does not move for a backup")

	// The recovery probe's release restores a provider in both pools.
	csm.RestoreRecoveredProvider("backup-b")
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"regular-a": false, "backup-b": false}, rec.providerBlockedSnapshot(),
		"a released backup must read serving again")
}

// A provider joins a block list only when a request picks it and finds no usable url. A backup on a
// healthy chain is never picked, so with the lists alone a backup whose only url was head-stalled
// read serving for as long as the stall lasted (MAG-4735). The gauge reads the urls too.
func TestPerProviderGauge_ABackupWithNoUsableURLReadsBlockedWithoutARequest(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	backups := createNamedPairingList("backup-b")
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createNamedPairingList("regular-a"), backups))
	stalled := backups[0].Endpoints[0]

	require.True(t, stalled.AddStateReason(EndpointDisableHeadStalled, time.Now(), 1000))
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"regular-a": false, "backup-b": true}, rec.providerBlockedSnapshot(),
		"a backup whose only url is head-stalled is out of rotation, picked by a request or not")
	csm.lock.RLock()
	_, listed := csm.blockedBackupProviders["backup-b"]
	csm.lock.RUnlock()
	require.False(t, listed, "nothing blocked it: the gauge reads the url, not a block list")

	stalled.ClearStateReasons()
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"regular-a": false, "backup-b": false}, rec.providerBlockedSnapshot(),
		"the url moves again, so the backup reads serving on the next tick")
}

// A regular provider is out of rotation when every url is disabled or head-stalled, before any
// request blocks it; one url left usable keeps it serving.
func TestPerProviderGauge_AProviderIsOutOnlyWhenNoURLIsUsable(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	pairing := createNamedPairingList("two-urls")
	first := pairing[0].Endpoints[0]
	second := &Endpoint{Connections: []*EndpointConnection{}, NetworkAddress: grpcListener + "-2", Enabled: true}
	pairing[0].Endpoints = append(pairing[0].Endpoints, second)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairing, nil))

	first.mu.Lock()
	first.Enabled = false // disabled after too many straight failures
	first.mu.Unlock()
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"two-urls": false}, rec.providerBlockedSnapshot(), "one url left in rotation")

	require.True(t, second.AddStateReason(EndpointDisableHeadStalled, time.Now(), 1000))
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"two-urls": true}, rec.providerBlockedSnapshot(), "no url left in rotation")
}

// blockedSeedRecorder keeps the provider set each epoch seeds the per-provider gauge with.
type blockedSeedRecorder struct {
	metrics.NoOpConsumerMetrics
	mu    sync.Mutex
	seeds []map[string]string
}

func (r *blockedSeedRecorder) ResetBlockedProvidersMetrics(_, _ string, providers map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seed := make(map[string]string, len(providers))
	for address, endpoint := range providers {
		seed[address] = endpoint
	}
	r.seeds = append(r.seeds, seed)
}

func (r *blockedSeedRecorder) lastSeed() (map[string]string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seeds) == 0 {
		return nil, false
	}
	return r.seeds[len(r.seeds)-1], true
}

// The epoch reset drops every series of the chain and seeds 0 for the providers that remain. A
// backup left out of the seed had its series deleted at each epoch and recreated by the next tick,
// a one-second hole per epoch on a provider that never changed.
func TestPerProviderGauge_EpochSeedCoversTheBackupPool(t *testing.T) {
	rec := &blockedSeedRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createNamedPairingList("regular-a"), createNamedPairingList("backup-b")))

	require.Eventually(t, func() bool {
		seed, ok := rec.lastSeed()
		if !ok {
			return false
		}
		_, regular := seed["regular-a"]
		_, backup := seed["backup-b"]
		return regular && backup
	}, time.Second, 10*time.Millisecond, "the epoch seed must name the backup as well as the regular provider")
}

// wsOnlyConnection is a websocket DirectRPCConnection that never dials: enough for
// Endpoint.ServesDirectRelays to say the url takes no relay.
type wsOnlyConnection struct{}

func (wsOnlyConnection) SendRequest(context.Context, []byte, map[string]string) (*DirectRPCResponse, error) {
	return nil, errors.New("a websocket-only test connection takes no relay")
}
func (wsOnlyConnection) GetProtocol() DirectRPCProtocol { return DirectRPCProtocolWSS }
func (wsOnlyConnection) Close() error                   { return nil }
func (wsOnlyConnection) GetURL() string                 { return "wss://ws-only.test" }
func (wsOnlyConnection) GetNodeUrl() *common.NodeUrl {
	return &common.NodeUrl{Url: "wss://ws-only.test"}
}

func wsOnlyEndpoint() *Endpoint {
	return &Endpoint{DirectConnections: []DirectRPCConnection{wsOnlyConnection{}}, NetworkAddress: "wss://ws-only.test", Enabled: true}
}

// A websocket url never takes a relay, so it keeps no provider in rotation while the https url is
// out. A provider with no https url at all is never picked for a relay and never blocked for
// lacking one, so it is judged by the urls it has: serving while one is usable, out when none is.
func TestPerProviderGauge_AWebsocketURLCountsOnlyForAProviderWithNothingElse(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	pairing := createNamedPairingList("https-and-ws", "ws-only")
	https := pairing[0].Endpoints[0]
	pairing[0].Endpoints = append(pairing[0].Endpoints, wsOnlyEndpoint())
	wsOnly := wsOnlyEndpoint()
	pairing[1].Endpoints = []*Endpoint{wsOnly}
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairing, nil))

	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"https-and-ws": false, "ws-only": false}, rec.providerBlockedSnapshot(),
		"a websocket-only provider is never picked for a relay and never blocked for it, so it reads serving")

	https.mu.Lock()
	https.Enabled = false
	https.mu.Unlock()
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"https-and-ws": true, "ws-only": false}, rec.providerBlockedSnapshot(),
		"the websocket url does not keep the provider in rotation once its https url is out")

	wsOnly.mu.Lock()
	wsOnly.Enabled = false
	wsOnly.mu.Unlock()
	csm.publishStateSizes()
	require.Equal(t, map[string]bool{"https-and-ws": true, "ws-only": true}, rec.providerBlockedSnapshot(),
		"a websocket-only provider is out when none of its urls is usable")
}
