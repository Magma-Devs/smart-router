package lavasession

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHasEligibleBackup_MAG3978 pins the question the relay policy asks before it fails a request
// on the error tolerance against what the backup fallback would do on the next GetSessions. The
// two read one list, so each case asks both.
func TestHasEligibleBackup_MAG3978(t *testing.T) {
	ctx := context.Background()
	nobodyTried := map[string]struct{}{}
	primaries := []string{"primary-a", "primary-b", "primary-c"}

	// fallbackFinds runs the backup fallback with the given tried set and reports whether it
	// handed out a provider.
	fallbackFinds := func(t *testing.T, csm *ConsumerSessionManager, tried map[string]struct{}, addon string) bool {
		t.Helper()
		ignored := &ignoredProviders{providers: tried, triedByThisRequest: tried, currentEpoch: csm.atomicReadCurrentEpoch()}
		sessions, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(ctx, ignored, 1, 0, addon, nil, 0, NewUsedProviders(nil))
		if err != nil {
			return false
		}
		return len(sessions) > 0
	}

	t.Run("no backup tier: nothing to wait for", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, nil)
		require.False(t, csm.HasEligibleBackup(ctx, nobodyTried, "", nil))
		require.False(t, fallbackFinds(t, csm, nobodyTried, ""))
	})

	t.Run("a backup nobody asked: yes", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, []string{"backup-1"})
		require.True(t, csm.HasEligibleBackup(ctx, nobodyTried, "", nil))
		require.True(t, fallbackFinds(t, csm, map[string]struct{}{}, ""))
	})

	t.Run("the only backup already tried by this request: no", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, []string{"backup-1"})
		tried := map[string]struct{}{"primary-a": {}, "primary-b": {}, "primary-c": {}, "backup-1": {}}
		require.False(t, csm.HasEligibleBackup(ctx, tried, "", nil))
		require.False(t, fallbackFinds(t, csm, tried, ""))
	})

	t.Run("one of two backups tried: the other still counts", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, []string{"backup-1", "backup-2"})
		tried := map[string]struct{}{"backup-1": {}}
		require.True(t, csm.HasEligibleBackup(ctx, tried, "", nil))
		require.True(t, fallbackFinds(t, csm, map[string]struct{}{"backup-1": {}}, ""))
	})

	t.Run("a backup blocked this epoch: no", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, []string{"backup-1"})
		csm.lock.Lock()
		csm.blockedBackupProviders["backup-1"] = struct{}{}
		csm.lock.Unlock()
		require.False(t, csm.HasEligibleBackup(ctx, nobodyTried, "", nil))
		require.False(t, fallbackFinds(t, csm, map[string]struct{}{}, ""))
	})

	t.Run("a backup that does not serve the request's addon: no", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, []string{"backup-1"})
		require.False(t, csm.HasEligibleBackup(ctx, nobodyTried, "debug", nil))
		require.False(t, fallbackFinds(t, csm, map[string]struct{}{}, "debug"))
		require.True(t, csm.HasEligibleBackup(ctx, nobodyTried, "", nil), "the same backup serves the base collection")
	})

	t.Run("tried primaries do not hide the backup", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, []string{"backup-1"})
		tried := map[string]struct{}{"primary-a": {}, "primary-b": {}, "primary-c": {}}
		require.True(t, csm.HasEligibleBackup(ctx, tried, "", nil))
	})
}
