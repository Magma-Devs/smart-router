package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/stretchr/testify/require"
)

// MAG-2786 at the selection layer: GetSessionsOptions.RequiredBlock keeps a request off every
// provider, primary or backup, that the optimizer knows to be below the block, and leaves one it has
// never observed eligible.

const (
	// requiredBlock is the block the requests below require. A provider "behind" is one block short.
	requiredBlock = servicedBlockNumber
	behindBlock   = requiredBlock - 1
)

// knownProvider is a provider and the block the optimizer has observed of it, 0 for never.
type knownProvider struct {
	address string
	block   int64
}

func requiredBlockCSM(t *testing.T, primaries, backups []knownProvider) *ConsumerSessionManager {
	t.Helper()
	csm := CreateConsumerSessionManager()
	pool := func(providers []knownProvider) map[uint64]*ConsumerSessionsWithProvider {
		if len(providers) == 0 {
			return nil
		}
		built := make(map[uint64]*ConsumerSessionsWithProvider, len(providers))
		for i, provider := range providers {
			built[uint64(i)] = mkBlockListProvider(provider.address)
		}
		return built
	}
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pool(primaries), pool(backups)))
	for _, provider := range append(append([]knownProvider{}, primaries...), backups...) {
		if provider.block > 0 {
			csm.providerOptimizer.AppendSyncData(provider.address, uint64(provider.block), provideroptimizer.SyncReference{})
		}
	}
	return csm
}

func requiredBlockSessions(csm *ConsumerSessionManager, wanted int, opts GetSessionsOptions) (ConsumerSessionsMap, error) {
	return csm.GetSessions(context.Background(), wanted, cuForFirstRequest, NewUsedProviders(nil), requiredBlock, "", nil, common.NO_STATE, 0, "", "", opts)
}

func TestGetSessions_RequiredBlockSetsAsideProvidersBelowIt(t *testing.T) {
	primaries := []knownProvider{{"behind", behindBlock}, {"holds", requiredBlock}, {"never-observed", 0}}

	t.Run("a primary below the block is set aside, an unobserved one is not", func(t *testing.T) {
		sessions, err := requiredBlockSessions(requiredBlockCSM(t, primaries, nil), len(primaries), GetSessionsOptions{RequiredBlock: requiredBlock})
		require.NoError(t, err)
		require.Len(t, sessions, 2)
		require.Contains(t, sessions, "holds")
		require.Contains(t, sessions, "never-observed")
	})

	t.Run("control: without a required block the request block alone sets nothing aside", func(t *testing.T) {
		sessions, err := requiredBlockSessions(requiredBlockCSM(t, primaries, nil), len(primaries), GetSessionsOptions{})
		require.NoError(t, err)
		require.Contains(t, sessions, "behind")
	})

	t.Run("a backup below the block is set aside too", func(t *testing.T) {
		csm := requiredBlockCSM(t, []knownProvider{{"behind", behindBlock}}, []knownProvider{{"backup-behind", behindBlock}, {"backup-holds", requiredBlock}})
		sessions, err := requiredBlockSessions(csm, 1, GetSessionsOptions{RequiredBlock: requiredBlock})
		require.NoError(t, err)
		require.Len(t, sessions, 1)
		require.Contains(t, sessions, "backup-holds")
	})

	t.Run("nothing holds the block: selection runs out rather than settle", func(t *testing.T) {
		csm := requiredBlockCSM(t, []knownProvider{{"behind-1", behindBlock}, {"behind-2", behindBlock}}, nil)
		sessions, err := requiredBlockSessions(csm, 2, GetSessionsOptions{RequiredBlock: requiredBlock, CrossValidation: true})
		require.ErrorIs(t, err, PairingListEmptyError)
		require.Empty(t, sessions)
	})
}

// The backup tier sets aside for itself too. Within a request the primary pass has already set aside
// every backup below the block, but the backup pass starts by forgetting every provider set aside when
// the pairing's epoch has moved on since the request began, so it must set them aside again. One
// backup, below the block, and a request whose set-aside list dates from the previous epoch: the
// backup tier must refuse the request, not hand that backup out.
func TestBackupTier_RequiredBlockSetsAsideAgainAfterItsEpochReset(t *testing.T) {
	csm := requiredBlockCSM(t, []knownProvider{{"behind", behindBlock}}, []knownProvider{{"backup-behind", behindBlock}})
	ignored := &ignoredProviders{
		providers:          map[string]struct{}{},
		triedByThisRequest: map[string]struct{}{},
		currentEpoch:       csm.atomicReadCurrentEpoch() - 1,
		requiredBlock:      requiredBlock,
	}
	sessions, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(context.Background(), ignored, cuForFirstRequest, requiredBlock, "", nil, 0, NewUsedProviders(nil))
	require.Error(t, err, "a backup below the block was handed out after the epoch reset")
	require.Empty(t, sessions)
	require.Contains(t, ignored.providers, "backup-behind")
}
