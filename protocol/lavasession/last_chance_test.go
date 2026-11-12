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
//  4. a head-stalled URL (MAG-3986) is lent like a disabled one — a node behind the chain beats no
//     answer — and stays head-stalled whatever it answers: only the head-stall step clears it.

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
	b.Endpoints[0].ResetHealth() // what the relay path does on the success, before OnSessionDone
	for _, s := range sessions {
		require.NoError(t, csm.OnSessionDone(s.Session, spectypes.LATEST_BLOCK, 10, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
	}
	require.Eventually(t, func() bool {
		return !slices.Contains(csm.ProviderRoutingSnapshot().BlockedBackupProviders, "backup")
	}, 2*time.Second, 10*time.Millisecond, "a successful last-chance relay lifts the backup's block")
}

// MAG-4737: a lent url that is head-stalled can answer, and the answer switches it back on but
// leaves the stall. The provider has nothing usable, so lifting its block would only hand the next
// request a provider to block again — a block and a release on every request for as long as the
// stall lasts (365 pairs in 100 s in a local run). It stays blocked; the head-stall step lifts the
// block when the stall clears. Checked for both tiers: they keep separate blocked lists.
func TestLastChance_StalledSuccessKeepsTheBlock(t *testing.T) {
	stall := func(t *testing.T, e *Endpoint) {
		disableAt(t, e, probeBase)
		require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	}
	succeed := func(t *testing.T, csm *ConsumerSessionManager, e *Endpoint, sessions ConsumerSessionsMap) {
		e.ResetHealth() // what the relay path does on the success, before OnSessionDone
		require.True(t, e.IsEnabled())
		require.False(t, e.IsUsable(), "precondition: the stall still holds the url out")
		for _, s := range sessions {
			require.NoError(t, csm.OnSessionDone(s.Session, spectypes.LATEST_BLOCK, 10, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
		}
	}

	t.Run("backup", func(t *testing.T) {
		p := lcProvider(t, "primary", "http://127.0.0.1:1")
		b := lcBackupWithDebug(t)
		stall(t, b.Endpoints[0])
		csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, []*ConsumerSessionsWithProvider{b})
		sessions, err := lcGetDebug(csm, NewUsedProviders(nil))
		require.NoError(t, err)
		require.Equal(t, "backup", lcServedBy(t, sessions))
		succeed(t, csm, b.Endpoints[0], sessions)
		require.Never(t, func() bool {
			return !slices.Contains(csm.ProviderRoutingSnapshot().BlockedBackupProviders, "backup")
		}, 300*time.Millisecond, 10*time.Millisecond, "a stalled backup stays blocked after a lent success")
	})
	t.Run("primary", func(t *testing.T) {
		p := lcProvider(t, "primary", "http://127.0.0.1:1")
		stall(t, p.Endpoints[0])
		csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)
		_, _ = lcGet(csm, NewUsedProviders(nil)) // first request: provider gets blocked, then lent
		sessions, err := lcGet(csm, NewUsedProviders(nil))
		require.NoError(t, err)
		require.Equal(t, "primary", lcServedBy(t, sessions))
		succeed(t, csm, p.Endpoints[0], sessions)
		require.Never(t, func() bool {
			return !slices.Contains(csm.ProviderRoutingSnapshot().CurrentlyBlockedProviderAddresses, "primary")
		}, 300*time.Millisecond, 10*time.Millisecond, "a stalled primary stays blocked after a lent success")
	})
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

// Rule 4: a URL that is both disabled and head-stalled is lent when nothing else can serve. The
// claim is taken, and the stall is still in place after a successful answer.
func TestLastChance_LendsAHeadStalledURL(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	require.True(t, p.Endpoints[0].AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	used := NewUsedProviders(nil)
	sessions, err := lcGet(csm, used)
	require.NoError(t, err, "a stalled URL is lent when nothing else can serve")
	require.Equal(t, "primary", lcServedBy(t, sessions))
	require.False(t, used.TakeLastChance(), "lending takes the request's one claim")
	for _, s := range sessions {
		require.NoError(t, csm.OnSessionDone(s.Session, spectypes.LATEST_BLOCK, 10, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
	}
	_, stalled := p.Endpoints[0].StateReason(EndpointDisableHeadStalled)
	require.True(t, stalled, "a successful answer must not clear the stall")
}

// gk8 FTM250, 2026-10-22: the only primary serves the debug add-on, its URL is head-stalled but
// enabled (one block behind), and the backup does not serve debug. Before, every debug call got
// "No Providers For Addon" until the stall cleared. Now the debug call is lent the stalled URL,
// while a plain call still goes to the backup first.
func TestLastChance_HeadStalledOnlyAddonProvider(t *testing.T) {
	newCSM := func(t *testing.T) (*ConsumerSessionsWithProvider, *ConsumerSessionManager) {
		p := lcProvider(t, "primary", "http://127.0.0.1:1")
		p.Endpoints[0].Addons = map[string]struct{}{"debug": {}}
		require.True(t, p.Endpoints[0].AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
		backup := lcProvider(t, "backup", "http://127.0.0.1:2")
		return p, lcCSM(t, []*ConsumerSessionsWithProvider{p}, []*ConsumerSessionsWithProvider{backup})
	}

	t.Run("debug call is lent the stalled primary", func(t *testing.T) {
		p, csm := newCSM(t)
		sessions, err := lcGetDebug(csm, NewUsedProviders(nil))
		require.NoError(t, err)
		require.Equal(t, "primary", lcServedBy(t, sessions))
		require.True(t, p.Endpoints[0].IsEnabled(), "lending leaves the event slot as it was")
		_, stalled := p.Endpoints[0].StateReason(EndpointDisableHeadStalled)
		require.True(t, stalled)
	})
	t.Run("plain call still goes to the backup", func(t *testing.T) {
		_, csm := newCSM(t)
		used := NewUsedProviders(nil)
		sessions, err := lcGet(csm, used)
		require.NoError(t, err)
		require.Equal(t, "backup", lcServedBy(t, sessions))
		require.True(t, used.TakeLastChance(), "the backup served it: no last chance was used")
	})
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
		p.Endpoints[0].ResetHealth() // what the relay path does on the success, before OnSessionDone
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

// MAG-4829, the interleaving that skipped the lend. This request passed over the provider because
// every endpoint of it is off, and blocked it. Another request's pool-empty release then emptied the
// blocked list before this request reached the fallback. The provider is back in rotation for every
// other request but still ignored by this one, so normal selection cannot hand it over either. The
// fallback must still lend it: nothing else can serve.
func TestLastChance_LendsAProviderReleasedByAnotherRequestMidRequest(t *testing.T) {
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)
	require.Empty(t, csm.ProviderRoutingSnapshot().CurrentlyBlockedProviderAddresses, "the other request's release left the blocked list empty")

	ignored := &ignoredProviders{
		providers:          map[string]struct{}{"primary": {}}, // passed over by this attempt
		triedByThisRequest: map[string]struct{}{},
		currentEpoch:       csm.atomicReadCurrentEpoch(),
	}
	got, err := csm.tryGetConsumerSessionWithProviderFromBlockedProviderList(context.Background(), 1, ignored, 10, spectypes.LATEST_BLOCK, "", nil, common.NO_STATE, 0, NewUsedProviders(nil))
	require.NoError(t, err, "the last chance must not end in No pairings available")
	require.Contains(t, got, "primary")
	require.True(t, got["primary"].lastChance, "the provider must be handed out for a lend, not for normal selection")
}

// MAG-4829 end to end: many requests at once with the only URL off. Each one lends that URL once
// and none ends in "No pairings available", however the releases of the blocked list by the other
// requests interleave with it.
func TestLastChance_EveryConcurrentRequestIsLent(t *testing.T) {
	const rounds, concurrent = 20, 50
	p := lcProvider(t, "primary", "http://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	for round := 0; round < rounds; round++ {
		errs := make(chan error, concurrent)
		start := make(chan struct{})
		for i := 0; i < concurrent; i++ {
			go func() {
				<-start
				sessions, err := lcGet(csm, NewUsedProviders(nil))
				for _, s := range sessions {
					// The node's error, as in the ticket: the URL stays off.
					_ = csm.OnSessionFailure(s.Session, errLastChanceRelay)
				}
				errs <- err
			}()
		}
		close(start)
		for i := 0; i < concurrent; i++ {
			require.NoError(t, <-errs, "round %d: a request missed the last chance", round)
		}
		require.False(t, p.Endpoints[0].IsEnabled())
	}
}

// lcProviderWithWS is a provider listing an https url and a wss url — the documented ETH1
// jsonrpc shape — with the https url first, as the chart renders it.
func lcProviderWithWS(t *testing.T, name, httpURL, wsURL string) *ConsumerSessionsWithProvider {
	t.Helper()
	p := lcProvider(t, name, httpURL)
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: wsURL}, 5, "jsonrpc")
	require.NoError(t, err)
	p.Endpoints = append(p.Endpoints, &Endpoint{NetworkAddress: wsURL, Enabled: true, DirectConnections: []DirectRPCConnection{conn}})
	return p
}

// lcLentEndpoint is the endpoint a session was handed.
func lcLentEndpoint(t *testing.T, info *SessionInfo) *Endpoint {
	t.Helper()
	drsc, ok := info.Session.Connection.(*DirectRPCSessionConnection)
	require.True(t, ok, "a direct-RPC provider's session wraps a DirectRPCSessionConnection")
	return drsc.Endpoint
}

// MAG-4285: a wss url is never sent a relay, so it never fails and stays enabled. It used to
// keep its provider out of the blocked list once the https url was off, so the last chance
// never reached the provider and the request ended in "No pairings available". Now the https
// url is lent, exactly as for a provider that lists only https.
func TestLastChance_LendsTheHttpsURLOfAProviderThatAlsoListsAWss(t *testing.T) {
	p := lcProviderWithWS(t, "primary", "http://127.0.0.1:1", "ws://127.0.0.1:1")
	disableAt(t, p.Endpoints[0], probeBase)
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	sessions, err := lcGet(csm, NewUsedProviders(nil))
	require.NoError(t, err, "the last chance must serve instead of No pairings available")
	require.Equal(t, "primary", lcServedBy(t, sessions))
	require.Same(t, p.Endpoints[0], lcLentEndpoint(t, sessions["primary"]), "the https url is lent, never the wss one")
	require.False(t, p.Endpoints[0].IsEnabled(), "rule 2: lending must not mark the endpoint enabled")
	require.True(t, p.Endpoints[1].IsEnabled(), "the wss url is left as it was")
}

// MAG-4285, the mix the ticket left open: one https-only provider and two https + wss
// providers, every https url off. The https-only one was already tried by this request, so only
// an https + wss provider can take the last chance — before the fix neither was ever blocked,
// and the request ended in "No pairings available".
func TestLastChance_ServesAMixOfHttpsOnlyAndHttpsPlusWssProviders(t *testing.T) {
	plain := lcProvider(t, "plain", "http://127.0.0.1:1")
	withWS1 := lcProviderWithWS(t, "ws1", "http://127.0.0.1:2", "ws://127.0.0.1:2")
	withWS2 := lcProviderWithWS(t, "ws2", "http://127.0.0.1:3", "ws://127.0.0.1:3")
	for _, p := range []*ConsumerSessionsWithProvider{plain, withWS1, withWS2} {
		disableAt(t, p.Endpoints[0], probeBase)
	}
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{plain, withWS1, withWS2}, nil)

	used := NewUsedProviders(nil)
	used.AddUnwantedAddresses("plain", NewRouterKey(nil))
	sessions, err := lcGet(csm, used)
	require.NoError(t, err, "an https + wss provider must take the last chance")
	served := lcServedBy(t, sessions)
	require.Contains(t, []string{"ws1", "ws2"}, served)
	require.True(t, lcLentEndpoint(t, sessions[served]).ServesDirectRelays(), "its https url is lent, never its wss one")
}

// MAG-4285: the provider blocked for having no relay-capable url left must not be released by
// the reconnect or epoch probe on the strength of its wss url, which a relay can never use.
// A provider whose https url is back is released as before.
func TestProbeDirectRPCEndpoints_AWssURLDoesNotReleaseAProviderWhoseHttpsIsOff(t *testing.T) {
	p := lcProviderWithWS(t, "primary", "http://127.0.0.1:1", "ws://127.0.0.1:1")
	csm := lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)

	disableAt(t, p.Endpoints[0], probeBase)
	_, err := csm.probeDirectRPCEndpoints(p, "primary")
	require.Error(t, err, "only the wss url is usable, and no relay can use it")

	p.Endpoints[0].ResetHealth()
	_, err = csm.probeDirectRPCEndpoints(p, "primary")
	require.NoError(t, err, "control: the https url is back, so the provider is released")
}

// lcTendermintProviderWithWS is a provider on tendermintrpc listing an http url and a ws url, both
// built with the interface the router passes for a Tendermint provider (rpcsmartrouter builds one
// Endpoint per url, in config order).
func lcTendermintProviderWithWS(t *testing.T, name, httpURL, wsURL string) *ConsumerSessionsWithProvider {
	t.Helper()
	var endpoints []*Endpoint
	for _, url := range []string{httpURL, wsURL} {
		conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: url}, 5, "tendermintrpc")
		require.NoError(t, err)
		endpoints = append(endpoints, &Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []DirectRPCConnection{conn}})
	}
	ws, ok := endpoints[1].DirectConnections[0].(*WebSocketDirectRPCConnection)
	require.True(t, ok, "the ws url is a websocket connection")
	require.False(t, ws.isJsonRPC, "the ws url was built for tendermintrpc, not jsonrpc")
	p := NewConsumerSessionWithProvider(name, endpoints, 999999, 1, int64(1))
	p.StaticProvider = true
	return p
}

// MAG-4821: the MAG-4285 fix on the interface tendermintrpc, whose providers also list a ws url and
// which the automation suite cannot cover (no Tendermint simulator router lists one). Each subtest
// builds its own provider, so each assertion fails on its own.
func TestLastChance_TendermintRPCProviderWithAWsURL(t *testing.T) {
	// The http url is off and the ws url, which no relay uses, is on.
	setup := func(t *testing.T) (*ConsumerSessionsWithProvider, *ConsumerSessionManager) {
		p := lcTendermintProviderWithWS(t, "primary", "http://127.0.0.1:1", "ws://127.0.0.1:1")
		disableAt(t, p.Endpoints[0], probeBase)
		return p, lcCSM(t, []*ConsumerSessionsWithProvider{p}, nil)
	}

	t.Run("the provider is blocked and the last chance serves the request", func(t *testing.T) {
		_, csm := setup(t)
		sessions, err := lcGet(csm, NewUsedProviders(nil))
		require.NoError(t, err, "the last chance must serve instead of No pairings available")
		require.Equal(t, "primary", lcServedBy(t, sessions))
	})

	t.Run("the http url is lent, never the ws url", func(t *testing.T) {
		p, csm := setup(t)
		sessions, err := lcGet(csm, NewUsedProviders(nil))
		require.NoError(t, err)
		lent := lcLentEndpoint(t, sessions["primary"])
		require.NotSame(t, p.Endpoints[1], lent, "the ws url must never receive a relay")
		require.Same(t, p.Endpoints[0], lent)
	})

	t.Run("the probe does not release the provider while its http url is off", func(t *testing.T) {
		p, csm := setup(t)
		_, err := csm.probeDirectRPCEndpoints(p, "primary")
		require.Error(t, err, "only the ws url is usable, and no relay can use it")

		p.Endpoints[0].ResetHealth()
		_, err = csm.probeDirectRPCEndpoints(p, "primary")
		require.NoError(t, err, "control: the http url is back, so the provider is released")
	})
}
