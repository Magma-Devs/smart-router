package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUpdatePairingListStake_PrunesOldEpochs is the regression for MAG-3722: the stake map
// kept one entry per provider per epoch for the life of the process. Reads only ever ask
// for the epoch being scored, so the current and previous epoch are all that is kept.
func TestUpdatePairingListStake_PrunesOldEpochs(t *testing.T) {
	coqc := NewConsumerOptimizerQoSClient("addr", NoopUsageSink{})
	const chain, provider = "ETH1", "provider-a"

	for epoch := uint64(1); epoch <= 10; epoch++ {
		coqc.UpdatePairingListStake(map[string]int64{provider: int64(epoch) * 100}, chain, epoch)
	}

	coqc.lock.RLock()
	defer coqc.lock.RUnlock()
	epochs := coqc.chainIdToProviderToEpochToStake[chain][provider]
	require.Len(t, epochs, 2, "only the current and previous epoch may remain")
	require.Equal(t, int64(1000), coqc.getProviderChainStake(chain, provider, 10))
	require.Equal(t, int64(900), coqc.getProviderChainStake(chain, provider, 9))
	require.Equal(t, int64(0), coqc.getProviderChainStake(chain, provider, 8), "an epoch two behind is gone")
}
