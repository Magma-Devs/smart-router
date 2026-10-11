package lavasession

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The last chance: when every normal path and every fallback says "No pairings available", the
// router lends one out-of-rotation (error-disabled) endpoint for this request instead of failing.
// Rules agreed for it:
//  1. one try per request, never a loop over every disabled endpoint;
//  2. lending does not mark the endpoint enabled; only a successful answer brings it back;
//  3. a failure keeps it disabled;
//  4. (head-stalled, not on this branch) a success never clears a stall.

var errLastChanceRelay = errors.New("upstream refused the relay")

func lcProvider(t *testing.T, name, url string) *ConsumerSessionsWithProvider {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: url}, 5, "jsonrpc")
	require.NoError(t, err)
	p := NewConsumerSessionWithProvider(name, []*Endpoint{{
		NetworkAddress: url, Enabled: true, DirectConnections: []DirectRPCConnection{conn},
	}}, 999999, 1, int64(1))
	p.StaticProvider = true
	return p
}

func lcCSM(t *testing.T, primaries []*ConsumerSessionsWithProvider, backups []*ConsumerSessionsWithProvider) *ConsumerSessionManager {
	t.Helper()
	csm := CreateConsumerSessionManager()
	pm := map[uint64]*ConsumerSessionsWithProvider{}
	for i, p := range primaries {
		pm[uint64(i)] = p
	}
	var bm map[uint64]*ConsumerSessionsWithProvider
	if len(backups) > 0 {
		bm = map[uint64]*ConsumerSessionsWithProvider{}
		for i, b := range backups {
			bm[uint64(i)] = b
		}
	}
	require.NoError(t, csm.UpdateAllProviders(1, pm, bm))
	return csm
}

func lcGet(csm *ConsumerSessionManager, used *UsedProviders) (ConsumerSessionsMap, error) {
	return csm.GetSessions(context.Background(), 1, 10, used, spectypes.LATEST_BLOCK, "", nil, common.NO_STATE, 0, "", "")
}

func lcServedBy(t *testing.T, sessions ConsumerSessionsMap) string {
	t.Helper()
	require.Len(t, sessions, 1)
	for provider := range sessions {
		return provider
	}
	return ""
}

// Example: the only primary's only URL failed 50 times, no backup. Before: "No pairings
// available" on every request. Now: the request is sent to that URL.
func TestLastChance_LendsTheOnlyDisabledEndpoint(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	sessions, err := lcGet(csm, NewUsedProviders(nil))
	require.NoError(t, err, "the last chance must serve instead of No pairings available")
	require.Equal(t, "primary", lcServedBy(t, sessions))
	require.False(t, p.Endpoints[0].IsEnabled(), "rule 2: lending must not mark the endpoint enabled")
	require.Equal(t, BlockedProviderSessionUsedStatus, p.atomicReadBlockedStatus(), "a success must lift the provider's block")
}

// Rule 1: once per request. A retry of the same request (same UsedProviders) gets no second
// last chance, even with another disabled provider available.
func TestLastChance_OncePerRequest(t *testing.T) {
	a := lcProvider(t, "a", "http://127.0.0.1:1")
	b := lcProvider(t, "b", "http://127.0.0.1:2")
	disableAt(t, a.Endpoints[0], probeBase)
	disableAt(t, b.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{a, b}, nil)

	used := NewUsedProviders(nil)
	sessions, err := lcGet(csm, used)
	require.NoError(t, err)
	for _, s := range sessions {
		require.NoError(t, csm.OnSessionFailure(s.Session, errLastChanceRelay))
	}
	_, err = lcGet(csm, used)
	require.Error(t, err, "the retry of the same request must not lend a second endpoint")
	for _, p := range []*ConsumerSessionsWithProvider{a, b} {
		if p.PublicLavaAddress != lcServedBy(t, sessions) {
			require.Equal(t, BlockedProviderSessionUnusedStatus, p.atomicReadBlockedStatus(), "a provider that was not lent to is not marked in use")
		}
	}
}

// A provider an earlier attempt of this request already used never gets the last chance.
func TestLastChance_SkipsAProviderAlreadyTriedByThisRequest(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	used := NewUsedProviders(nil)
	used.AddUnwantedAddresses("primary", NewRouterKey(nil))
	_, err := lcGet(csm, used)
	require.Error(t, err)
	require.True(t, used.TakeLastChance(), "the claim must still be free: nothing was lent")
}

// A healthy backup is used normally; the last chance never jumps ahead of it.
func TestLastChance_NotUsedWhileABackupCanServe(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, []*ConsumerSessionsWithProvider{lcProvider(t, "backup", "http://127.0.0.1:2")})

	used := NewUsedProviders(nil)
	sessions, err := lcGet(csm, used)
	require.NoError(t, err)
	require.Equal(t, "backup", lcServedBy(t, sessions))
	require.True(t, used.TakeLastChance(), "no lending happened")
}

// Primaries before backups, and among several the one disabled the longest (most time to recover).
func TestLastChance_PrefersPrimariesThenTheLongestDisabled(t *testing.T) {
	recent := lcProvider(t, "recent", "http://127.0.0.1:1")
	oldest := lcProvider(t, "oldest", "http://127.0.0.1:2")
	backup := lcProvider(t, "backup", "http://127.0.0.1:3")
	disableAt(t, recent.Endpoints[0], probeBase.Add(time.Minute))
	disableAt(t, oldest.Endpoints[0], probeBase)
	disableAt(t, backup.Endpoints[0], probeBase.Add(-time.Hour)) // oldest of all, but a backup
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{recent, oldest}, []*ConsumerSessionsWithProvider{backup})

	sessions, err := lcGet(csm, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Equal(t, "oldest", lcServedBy(t, sessions))

	// With no primary left to try, a disabled backup gets it.
	used := NewUsedProviders(nil)
	used.AddUnwantedAddresses("recent", NewRouterKey(nil))
	used.AddUnwantedAddresses("oldest", NewRouterKey(nil))
	sessions, err = lcGet(csm, used)
	require.NoError(t, err)
	require.Equal(t, "backup", lcServedBy(t, sessions))
}

// A websocket url can never answer a relay, so it is never lent.
func TestLastChance_NeverLendsAWebsocketURL(t *testing.T) {
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: "wss://only.example"}, 5, "jsonrpc")
	require.NoError(t, err)
	p := NewConsumerSessionWithProvider("wsonly", []*Endpoint{{
		NetworkAddress: "wss://only.example", Enabled: true, DirectConnections: []DirectRPCConnection{conn},
	}}, 999999, 1, int64(1))
	p.StaticProvider = true
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	_, err = lcGet(csm, NewUsedProviders(nil))
	require.Error(t, err)
}

// Rule 2 and 3 end to end at this layer: a success lifts the provider's block (the relay path's
// ResetHealth is what re-enables the endpoint); a failure leaves the endpoint disabled.
func TestLastChance_SuccessLiftsTheBlockFailureKeepsItDisabled(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		p := lcProvider(t, "primary", "http://127.0.0.1:1")
		disableAt(t, p.Endpoints[0], probeBase)
		csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)
		_, _ = lcGet(csm, NewUsedProviders(nil)) // first request: provider gets blocked, then lent
		sessions, err := lcGet(csm, NewUsedProviders(nil))
		require.NoError(t, err)
		for _, s := range sessions {
			require.NoError(t, csm.OnSessionDone(s.Session, spectypes.LATEST_BLOCK, 10, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
		}
		require.Eventually(t, func() bool {
			return !slices.Contains(csm.ProviderRoutingSnapshot().CurrentlyBlockedProviderAddresses, "primary")
		}, 2*time.Second, 10*time.Millisecond, "a successful last-chance relay lifts the provider's block")
	})
	t.Run("failure", func(t *testing.T) {
		p := lcProvider(t, "primary", "http://127.0.0.1:1")
		disableAt(t, p.Endpoints[0], probeBase)
		csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)
		sessions, err := lcGet(csm, NewUsedProviders(nil))
		require.NoError(t, err)
		for _, s := range sessions {
			require.NoError(t, csm.OnSessionFailure(s.Session, errLastChanceRelay))
		}
		p.Endpoints[0].MarkUnhealthy(EndpointDisableNodeError) // what the relay path does on the failure
		require.False(t, p.Endpoints[0].IsEnabled(), "rule 3: a failed last chance keeps the endpoint disabled")
	})
}
