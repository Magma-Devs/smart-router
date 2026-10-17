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

// The last chance (MAG-4232): when every primary and every backup is out, the blocked-list fallback
// hands this request one blocked provider — a primary first, else a backup — and its disabled endpoint is lent for the request instead
// of skipped — no new step in the selection flow. Rules agreed for it:
//  1. one try per request, never a loop over every disabled endpoint;
//  2. lending does not mark the endpoint enabled; only a successful answer brings it back;
//  3. a failure keeps it disabled;
//  4. a head-stalled URL (MAG-3986) is never lent: a stalled node answers, so it is not what the
//     last chance is for, and a success could only clear the event slot, never the stall.

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

// lcBackupWithDebug is a backup named "backup" whose only URL also serves the debug add-on.
func lcBackupWithDebug(t *testing.T) *ConsumerSessionsWithProvider {
	t.Helper()
	b := lcProvider(t, "backup", "http://127.0.0.1:2")
	b.Endpoints[0].Addons = map[string]struct{}{"debug": {}}
	return b
}

func lcGetDebug(csm *ConsumerSessionManager, used *UsedProviders) (ConsumerSessionsMap, error) {
	return csm.GetSessions(context.Background(), 1, 10, used, spectypes.LATEST_BLOCK, "debug", nil, common.NO_STATE, 0, "", "")
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
		require.False(t, p.Endpoints[0].IsEnabled(), "the failed lend and the one never lent both stay disabled")
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

// A provider already blocked when the request arrives is not picked by normal selection at all. The
// pool-empty release re-picks it, its endpoint is still disabled, and the blocked-list fallback then
// lends it — this request must not end in "No pairings available" either.
func TestLastChance_LendsAProviderBlockedBeforeTheRequestArrived(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	sessions, err := lcGet(csm, NewUsedProviders(nil)) // first request: blocks it, then lends it
	require.NoError(t, err)
	for _, s := range sessions {
		require.NoError(t, csm.OnSessionFailure(s.Session, errLastChanceRelay))
	}
	require.True(t, slices.Contains(csm.ProviderRoutingSnapshot().CurrentlyBlockedProviderAddresses, "primary"))

	sessions, err = lcGet(csm, NewUsedProviders(nil)) // a new request, the provider already blocked
	require.NoError(t, err)
	require.Equal(t, "primary", lcServedBy(t, sessions))
	require.False(t, p.Endpoints[0].IsEnabled())
}

// With every primary and every backup disabled, the blocked-list fallback walks the blocked
// primaries before the blocked backups, so a primary is the one lent.
func TestLastChance_LendsAPrimaryWhenBothTiersAreDisabled(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	b := lcProvider(t, "backup", "http://127.0.0.1:2")
	disableAt(t, p.Endpoints[0], probeBase)
	disableAt(t, b.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, []*ConsumerSessionsWithProvider{b})

	sessions, err := lcGet(csm, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Equal(t, "primary", lcServedBy(t, sessions))
}

// A stale block — the provider is still blocked but its endpoint already works again (the epoch's
// re-enable runs before the block is lifted) — is served through that working endpoint, as on main.
func TestLastChance_StaleBlockIsServedByTheWorkingEndpoint(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)
	csm.removeAddressFromValidAddresses("primary", BlockRecord{Reason: BlockReasonAllEndpointsDisabled})

	used := NewUsedProviders(nil)
	sessions, err := lcGet(csm, used)
	require.NoError(t, err)
	require.Equal(t, "primary", lcServedBy(t, sessions))
	require.True(t, p.Endpoints[0].IsEnabled())
}

// Only the backup has the debug add-on, and both URLs are disabled. The blocked primaries cannot
// serve the request, so the blocked backup is lent. Before backups were walked: "No pairings
// available".
func TestLastChance_LendsABackupWhenOnlyItHasTheAddon(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	b := lcBackupWithDebug(t)
	disableAt(t, p.Endpoints[0], probeBase)
	disableAt(t, b.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, []*ConsumerSessionsWithProvider{b})

	sessions, err := lcGetDebug(csm, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Equal(t, "backup", lcServedBy(t, sessions))
	require.False(t, b.Endpoints[0].IsEnabled(), "rule 2: lending must not mark the endpoint enabled")
}

// An earlier attempt of this request already used the primary. The backup is disabled, so it is
// the one lent.
func TestLastChance_LendsABackupWhenThePrimaryWasAlreadyTried(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	b := lcProvider(t, "backup", "http://127.0.0.1:2")
	disableAt(t, b.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, []*ConsumerSessionsWithProvider{b})

	used := NewUsedProviders(nil)
	used.AddUnwantedAddresses("primary", NewRouterKey(nil))
	sessions, err := lcGet(csm, used)
	require.NoError(t, err)
	require.Equal(t, "backup", lcServedBy(t, sessions))
}

// A lent backup that answers leaves the blocked backups at once, as a lent primary leaves the
// blocked primaries.
func TestLastChance_BackupSuccessLiftsItsBlock(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	b := lcBackupWithDebug(t)
	disableAt(t, b.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, []*ConsumerSessionsWithProvider{b})

	sessions, err := lcGetDebug(csm, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Equal(t, "backup", lcServedBy(t, sessions))
	require.True(t, slices.Contains(csm.ProviderRoutingSnapshot().BlockedBackupProviders, "backup"))
	for _, s := range sessions {
		require.NoError(t, csm.OnSessionDone(s.Session, spectypes.LATEST_BLOCK, 10, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
	}
	require.Eventually(t, func() bool {
		return !slices.Contains(csm.ProviderRoutingSnapshot().BlockedBackupProviders, "backup")
	}, 2*time.Second, 10*time.Millisecond, "a successful last-chance relay lifts the backup's block")
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

// Rule 4: a URL that is both disabled and head-stalled is never lent. The request gets No pairings
// available, the claim stays free, and the stall is still in place.
func TestLastChance_NeverLendsAHeadStalledURL(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	require.True(t, p.Endpoints[0].AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	used := NewUsedProviders(nil)
	_, err := lcGet(csm, used)
	require.Error(t, err, "a stalled URL must not be lent")
	require.True(t, used.TakeLastChance(), "the claim must still be free: nothing was lent")
	_, stalled := p.Endpoints[0].StateReason(EndpointDisableHeadStalled)
	require.True(t, stalled, "the last chance must not clear the stall")
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
