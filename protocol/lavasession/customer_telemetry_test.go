package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

func TestCustomerRerouteActualSelectionPreservesProviderState(t *testing.T) {
	for _, tc := range []struct {
		name                                                 string
		allBlocked, oneBlocked, preferBackup, sticky, pinned bool
		emptyPrimary, configuredPrimary                      bool
		wantRecovery, wantProvider                           string
		wantError                                            bool
	}{
		{name: "verified primary pool empty at boot", emptyPrimary: true, configuredPrimary: true, wantRecovery: "reroute", wantProvider: "lava@backup0"},
		{name: "intentionally backup-only", emptyPrimary: true, wantRecovery: "none", wantProvider: "lava@backup0"},
		{name: "all blocked go to backup", allBlocked: true, wantRecovery: "reroute", wantProvider: "lava@backup0"},
		{name: "healthy reserve hedge is not a recovery", preferBackup: true, wantRecovery: "none", wantProvider: "lava@backup0"},
		{name: "unrelated blocked provider", oneBlocked: true, wantRecovery: "none", wantProvider: "lava@primary1"},
		{name: "blocked sticky intent", oneBlocked: true, sticky: true, wantRecovery: "reroute", wantProvider: "lava@primary1"},
		{name: "explicit blocked pin must still fail", oneBlocked: true, pinned: true, wantRecovery: "none", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			csm := setupBenchTestCSM(t, true)
			csm.SetCustomerPrimaryBaseConfigured(tc.configuredPrimary)
			if tc.emptyPrimary {
				csm.lock.Lock()
				csm.pairing = map[string]*ConsumerSessionsWithProvider{}
				csm.pairingAddresses = nil
				csm.validAddresses = nil
				csm.addonAddresses = nil
				csm.lock.Unlock()
			}
			if tc.allBlocked {
				blockEveryPrimary(csm)
			}
			if tc.oneBlocked {
				csm.lock.Lock()
				csm.currentlyBlockedProviderAddresses = []string{"lava@primary0"}
				csm.validAddresses = []string{"lava@primary1"}
				csm.addonAddresses = nil
				csm.lock.Unlock()
			}
			stickiness, pin := "", ""
			if tc.sticky {
				stickiness = "customer-session"
				csm.stickySessions.Set(stickiness, &StickySession{Provider: "lava@primary0", Epoch: firstEpochHeight})
			}
			if tc.pinned {
				pin = "lava@primary0"
			}
			logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			data := metrics.NewRelayAnalytics("tenant", "ETH1", "jsonrpc")
			ctx := logger.BeginCustomerRequest(context.Background(), data)
			sessions, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, stickiness, pin, GetSessionsOptions{PreferBackup: tc.preferBackup})
			if tc.wantError {
				require.ErrorIs(t, err, SelectedProviderUnavailableError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantProvider, onlyProvider(t, sessions))
				data.CaptureCustomerResult(&common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: onlyProvider(t, sessions)}, Reply: &pairingtypes.RelayReply{Data: []byte(`{"result":1}`)}}, "", nil, nil, false)
			}
			logger.AddMetricForWebSocket(data, err)
			final := data.Customer.Snapshot()
			require.Equal(t, tc.wantRecovery, final.Recovery)
			require.Zero(t, csm.numberOfResets, "instrumentation must never release blocked providers")
			if tc.allBlocked {
				require.Empty(t, csm.validAddresses)
				require.Len(t, csm.currentlyBlockedProviderAddresses, 2)
			}
			if tc.oneBlocked {
				require.Contains(t, csm.currentlyBlockedProviderAddresses, "lava@primary0")
			}
		})
	}
}
