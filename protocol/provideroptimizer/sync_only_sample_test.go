package provideroptimizer

import (
	"context"
	"testing"
	"time"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/score"
	"github.com/stretchr/testify/require"
)

// storeState is a score store's full stored state, so "untouched" can be checked exactly.
type storeState struct {
	num, denom float64
	at         time.Time
}

// dimensions reads a provider's stored scores. Writes go through the async ristretto cache, so it
// first waits for the provider's sync record to satisfy settled.
func dimensions(t *testing.T, po *ProviderOptimizer, provider string, settled func(sync storeState) bool) (availability, latency, sync storeState) {
	t.Helper()
	var data ProviderData
	state := func(s score.ScoreStorer) storeState {
		return storeState{num: s.GetNum(), denom: s.GetDenom(), at: s.GetLastUpdateTime()}
	}
	require.Eventually(t, func() bool {
		d, found := po.getProviderData(provider)
		if !found || !settled(state(d.Sync)) {
			return false
		}
		data = d
		return true
	}, 2*time.Second, time.Millisecond, "the provider's scores never settled in the cache")
	return state(data.Availability), state(data.Latency), state(data.Sync)
}

// A "not found" answer says nothing about availability or latency, so AppendSyncData must move the
// sync dimension alone, and only when there is a tip and a reference to measure it against.
func TestAppendSyncData_OnlyMovesSync(t *testing.T) {
	const tip = uint64(100000)
	po := NewProviderOptimizer(StrategyBalanced, 450*time.Millisecond, 1, nil, "BSC")
	ref := SyncReference{ConsensusConfigured: true, Fresh: true, Block: tip, Time: po.now()}
	po.AppendRelayDataConsensus("p1", 80*time.Millisecond, 10, tip, ref)
	availability, latency, sync := dimensions(t, po, "p1", func(storeState) bool { return true })

	po.AppendSyncData("p1", 0, ref)
	po.AppendSyncData("p1", tip-300, SyncReference{ConsensusConfigured: true, Fresh: false})
	time.Sleep(20 * time.Millisecond) // time for a stray write to land
	a, l, s := dimensions(t, po, "p1", func(storeState) bool { return true })
	require.Equal(t, sync, s, "no tip, or no fresh consensus: nothing to measure")
	require.Equal(t, availability, a)
	require.Equal(t, latency, l)

	// The chain moves on 300 blocks while p1 stays where it was (a provider's block never goes back).
	ahead := SyncReference{ConsensusConfigured: true, Fresh: true, Block: tip + 300, Time: po.now()}
	po.AppendSyncData("p1", tip, ahead)
	a, l, s = dimensions(t, po, "p1", func(got storeState) bool { return got != sync })
	require.Greater(t, s.num/s.denom, sync.num/sync.denom, "300 blocks behind the tip must show as sync lag")
	require.Equal(t, availability, a, "availability untouched")
	require.Equal(t, latency, l, "latency untouched")
}

// Why a "not found" answer records a sync sample rather than nothing. A lagging node that only ever
// answers "not found" must keep losing on sync; recording nothing hands it back its traffic share.
// Production selector configuration (adaptive normalisation), fixed seeds.
func TestAppendSyncData_LaggingNodeLosesShare(t *testing.T) {
	const (
		relays = 400
		tip    = uint64(100000)
		behind = tip - 300
	)
	providers := []string{"lagging", "healthy-a", "healthy-b"}

	run := func(recordSync bool) (lagging, healthy int) {
		for _, seed := range []int64{1, 7, 42, 1337, 20260817} {
			po := NewProviderOptimizer(StrategyBalanced, 450*time.Millisecond, 1, nil, "BSC")
			po.ConfigureUpstreamSelector(DefaultUpstreamSelectorConfig())
			po.SetDeterministicSeed(seed)
			for i := 0; i < relays; i++ {
				ref := SyncReference{ConsensusConfigured: true, Fresh: true, Block: tip, Time: po.now()}
				first := po.ChooseUpstream(context.Background(), providers, nil, 10, spectypes.LATEST_BLOCK)[0]
				if first != "lagging" {
					healthy++
					po.AppendRelayDataConsensus(first, 150*time.Millisecond, 10, tip, ref)
					continue
				}
				lagging++
				if recordSync {
					po.AppendSyncData("lagging", behind, ref)
				}
			}
		}
		return lagging, healthy
	}

	lagging, healthy := run(true)
	withoutSync, _ := run(false)
	t.Logf("lagging picks: %d with sync samples, %d without; healthy peers: %d", lagging, withoutSync, healthy)
	require.Less(t, 2*lagging, healthy, "a lagging node must be picked less than each healthy peer")
	require.Less(t, lagging, withoutSync, "recording nothing would hand the lagging node back its share")
}
