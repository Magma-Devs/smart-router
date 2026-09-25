package lavasession

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestUsedProviders(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		usedProviders := NewUsedProviders(nil)
		canUse := usedProviders.tryLockSelection()
		require.True(t, canUse)
		canUseAgain := usedProviders.tryLockSelection()
		require.False(t, canUseAgain)
		require.Zero(t, usedProviders.CurrentlyUsed())
		require.Zero(t, usedProviders.SessionsLatestBatch())
		unwanted := usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Len(t, unwanted, 0)
		consumerSessionsMap := ConsumerSessionsMap{"test": &SessionInfo{}, "test2": &SessionInfo{}}
		usedProviders.AddUsed(consumerSessionsMap, nil)
		canUseAgain = usedProviders.tryLockSelection()
		require.True(t, canUseAgain)
		unwanted = usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Len(t, unwanted, 2)
		require.Equal(t, 2, usedProviders.CurrentlyUsed())
		canUseAgain = usedProviders.tryLockSelection()
		require.False(t, canUseAgain)
		consumerSessionsMap = ConsumerSessionsMap{"test3": &SessionInfo{}, "test4": &SessionInfo{}}
		usedProviders.AddUsed(consumerSessionsMap, nil)
		unwanted = usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Len(t, unwanted, 4)
		require.Equal(t, 4, usedProviders.CurrentlyUsed())
		// one provider gives a retry
		usedProviders.RemoveUsed("test", NewRouterKey(nil), status.Error(codes.Code(SessionOutOfSyncGRPCCode), ""))
		require.Equal(t, 3, usedProviders.CurrentlyUsed())
		unwanted = usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Len(t, unwanted, 3)
		// one provider gives a result
		usedProviders.RemoveUsed("test2", NewRouterKey(nil), nil)
		unwanted = usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Len(t, unwanted, 3)
		require.Equal(t, 2, usedProviders.CurrentlyUsed())
		// one provider gives an error
		usedProviders.RemoveUsed("test3", NewRouterKey(nil), fmt.Errorf("bad"))
		unwanted = usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Len(t, unwanted, 3)
		require.Equal(t, 1, usedProviders.CurrentlyUsed())
		canUseAgain = usedProviders.tryLockSelection()
		require.True(t, canUseAgain)
	})
}

func TestReleaseFromLatestBatch(t *testing.T) {
	t.Run("decrements batch counter and is idempotent", func(t *testing.T) {
		usedProviders := NewUsedProviders(nil)
		consumerSessionsMap := ConsumerSessionsMap{
			"p1": &SessionInfo{},
			"p2": &SessionInfo{},
			"p3": &SessionInfo{},
		}
		usedProviders.AddUsed(consumerSessionsMap, nil)
		require.Equal(t, 3, usedProviders.SessionsLatestBatch())
		require.Equal(t, 3, usedProviders.CurrentlyUsed())

		// Release p1: counter drops, currentlyUsed drops, p1 marked unwanted.
		usedProviders.ReleaseFromLatestBatch("p1", NewRouterKey(nil), fmt.Errorf("filtered"))
		require.Equal(t, 2, usedProviders.SessionsLatestBatch())
		require.Equal(t, 2, usedProviders.CurrentlyUsed())
		unwanted := usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Contains(t, unwanted, "p1")

		// Idempotent: second call for p1 must not double-decrement.
		usedProviders.ReleaseFromLatestBatch("p1", NewRouterKey(nil), fmt.Errorf("filtered"))
		require.Equal(t, 2, usedProviders.SessionsLatestBatch())
		require.Equal(t, 2, usedProviders.CurrentlyUsed())

		// Release the rest: counter floors at 0, no underflow.
		usedProviders.ReleaseFromLatestBatch("p2", NewRouterKey(nil), nil)
		usedProviders.ReleaseFromLatestBatch("p3", NewRouterKey(nil), nil)
		require.Zero(t, usedProviders.SessionsLatestBatch())
		require.Zero(t, usedProviders.CurrentlyUsed())

		// Releasing an unknown provider is a no-op (no panic, no underflow).
		usedProviders.ReleaseFromLatestBatch("never-added", NewRouterKey(nil), nil)
		require.Zero(t, usedProviders.SessionsLatestBatch())
	})

	t.Run("response-path RemoveUsed leaves counter alone, ReleaseFromLatestBatch decrements", func(t *testing.T) {
		usedProviders := NewUsedProviders(nil)
		consumerSessionsMap := ConsumerSessionsMap{
			"p1": &SessionInfo{},
			"p2": &SessionInfo{},
		}
		usedProviders.AddUsed(consumerSessionsMap, nil)
		require.Equal(t, 2, usedProviders.SessionsLatestBatch())

		// Simulate a normal response cycle for p1: RemoveUsed must NOT decrement
		// the batch counter (responsesCount on the RelayProcessor side handles it).
		usedProviders.RemoveUsed("p1", NewRouterKey(nil), nil)
		require.Equal(t, 2, usedProviders.SessionsLatestBatch())

		// Simulate a pre-dispatch filter dropping p2: counter must drop.
		usedProviders.ReleaseFromLatestBatch("p2", NewRouterKey(nil), fmt.Errorf("filtered"))
		require.Equal(t, 1, usedProviders.SessionsLatestBatch())
	})
}

func TestRemoveUnwantedAddresses(t *testing.T) {
	usedProviders := NewUsedProviders(nil)
	emptyKey := NewRouterKey(nil)
	archiveKey := NewRouterKey([]string{"archive"})

	for _, key := range []RouterKey{emptyKey, archiveKey} {
		usedProviders.AddUnwantedAddresses("stale-1", key)
		usedProviders.AddUnwantedAddresses("stale-2", key)
		usedProviders.AddUnwantedAddresses("transport-failure", key)
	}

	usedProviders.RemoveUnwantedAddresses([]string{"stale-1", "stale-2"})

	for _, key := range []RouterKey{emptyKey, archiveKey} {
		unwanted := usedProviders.GetUnwantedProvidersToSend(key)
		require.NotContains(t, unwanted, "stale-1")
		require.NotContains(t, unwanted, "stale-2")
		require.Contains(t, unwanted, "transport-failure",
			"the consistency fallback must preserve providers excluded for other reasons")
	}
}

func TestUsedProvidersAsync(t *testing.T) {
	t.Run("concurrency", func(t *testing.T) {
		usedProviders := NewUsedProviders(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.Nil(t, canUse)
		require.Zero(t, usedProviders.CurrentlyUsed())
		require.Zero(t, usedProviders.SessionsLatestBatch())
		go func() {
			time.Sleep(time.Millisecond * 10)
			consumerSessionsMap := ConsumerSessionsMap{"test": &SessionInfo{}, "test2": &SessionInfo{}}
			usedProviders.AddUsed(consumerSessionsMap, nil)
		}()
		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		canUseAgain := usedProviders.TryLockSelection(ctx)
		require.Nil(t, canUseAgain)
		unwanted := usedProviders.GetUnwantedProvidersToSend(NewRouterKey(nil))
		require.Len(t, unwanted, 2)
		require.Equal(t, 2, usedProviders.CurrentlyUsed())
	})
}

func TestUsedProvidersAsyncFail(t *testing.T) {
	t.Run("concurrency", func(t *testing.T) {
		usedProviders := NewUsedProviders(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.Nil(t, canUse)
		require.Zero(t, usedProviders.CurrentlyUsed())
		require.Zero(t, usedProviders.SessionsLatestBatch())
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*15)
		defer cancel()
		canUseAgain := usedProviders.TryLockSelection(ctx)
		require.Error(t, canUseAgain)
	})
}

func TestUsedProviderContextTimeout(t *testing.T) {
	t.Run("concurrency", func(t *testing.T) {
		usedProviders := NewUsedProviders(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.Nil(t, canUse)
		require.Zero(t, usedProviders.CurrentlyUsed())
		require.Zero(t, usedProviders.SessionsLatestBatch())
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		canUseAgain := usedProviders.TryLockSelection(ctx)
		require.Error(t, canUseAgain)
		require.True(t, errors.Is(canUseAgain, ContextDoneNoNeedToLockSelectionError))
	})
}

// TestDecideEligibility verifies the eligibility logic used by RemoveUsed.
func TestDecideEligibility(t *testing.T) {
	t.Run("unsupported method marks unwanted", func(t *testing.T) {
		unsupportedErrors := []error{
			fmt.Errorf("method not found"),
			fmt.Errorf("endpoint not found"),
			fmt.Errorf("method not supported"),
		}

		for _, err := range unsupportedErrors {
			isUnsupported := common.IsUnsupportedMethodError("", 0, err.Error())
			isSyncLoss := IsSessionSyncLoss(err)
			result := common.DecideEligibility(isUnsupported, isSyncLoss, !isSyncLoss)
			require.Equal(t, common.EligibilityMarkUnwanted, result.Action,
				"Should mark unwanted for unsupported method: %s", err.Error())
		}
	})

	t.Run("first sync loss allows retry", func(t *testing.T) {
		err := status.Error(codes.Code(SessionOutOfSyncGRPCCode), "session out of sync")
		isUnsupported := common.IsUnsupportedMethodError("", 0, err.Error())
		isSyncLoss := IsSessionSyncLoss(err)
		result := common.DecideEligibility(isUnsupported, isSyncLoss, true)
		require.Equal(t, common.EligibilityAllowRetry, result.Action,
			"Should allow retry on first sync loss")
	})

	t.Run("second sync loss marks unwanted", func(t *testing.T) {
		err := status.Error(codes.Code(SessionOutOfSyncGRPCCode), "session out of sync")
		isUnsupported := common.IsUnsupportedMethodError("", 0, err.Error())
		isSyncLoss := IsSessionSyncLoss(err)
		result := common.DecideEligibility(isUnsupported, isSyncLoss, false)
		require.Equal(t, common.EligibilityMarkUnwanted, result.Action,
			"Should mark unwanted on second sync loss")
	})

	t.Run("normal errors mark unwanted", func(t *testing.T) {
		normalErrors := []error{
			fmt.Errorf("execution reverted: some error"),
			fmt.Errorf("internal server error"),
			fmt.Errorf("timeout"),
		}

		for _, err := range normalErrors {
			isUnsupported := common.IsUnsupportedMethodError("", 0, err.Error())
			isSyncLoss := IsSessionSyncLoss(err)
			result := common.DecideEligibility(isUnsupported, isSyncLoss, true)
			require.Equal(t, common.EligibilityMarkUnwanted, result.Action,
				"Should mark unwanted for normal error: %s", err.Error())
		}
	})
}

// SessionsLatestBatch answers "how many did THIS batch launch"; SessionsDispatched answers "how
// many has this request asked in total". They are the same number until a second batch runs, which
// is exactly when a caller that picked the wrong one starts getting a wrong answer with no symptom.
func TestUsedProviders_SessionsDispatchedIsCumulativeAcrossBatches(t *testing.T) {
	usedProviders := NewUsedProviders(nil)

	usedProviders.AddUsed(ConsumerSessionsMap{
		"lava@a": &SessionInfo{},
		"lava@b": &SessionInfo{},
	}, nil)
	require.Equal(t, 2, usedProviders.SessionsLatestBatch())
	require.Equal(t, 2, usedProviders.SessionsDispatched())

	// A retry. SessionsLatestBatch resets to describe the new batch alone.
	usedProviders.AddUsed(ConsumerSessionsMap{"lava@c": &SessionInfo{}}, nil)
	require.Equal(t, 1, usedProviders.SessionsLatestBatch(),
		"the per-batch count describes the latest batch only — that is its job")
	require.Equal(t, 3, usedProviders.SessionsDispatched(),
		"three endpoints were asked; a cumulative comparison must see all three")

	// A provider dropped by a pre-dispatch filter was never asked, so it leaves both counts.
	usedProviders.ReleaseFromLatestBatch("lava@c", NewRouterKey(nil), nil)
	require.Equal(t, 0, usedProviders.SessionsLatestBatch())
	require.Equal(t, 2, usedProviders.SessionsDispatched(),
		"released before dispatch means never asked, in the cumulative count too")

	// A provider that ANSWERED goes through RemoveUsed, which must leave both alone — it did
	// dispatch, and the caller compares it against the responses it produced.
	usedProviders.RemoveUsed("lava@a", NewRouterKey(nil), nil)
	require.Equal(t, 2, usedProviders.SessionsDispatched(),
		"a provider that answered still counts as one we asked")
}

// The reply's attempt headers name every attempt the request sent, including one that never
// reported back (MAG-3762), so the names have to survive exactly as the count does: in the order
// the batches went out, one per session, gone only when released before dispatch.
func TestUsedProviders_DispatchedProvidersNamesEveryAttempt(t *testing.T) {
	usedProviders := NewUsedProviders(nil)
	require.Empty(t, usedProviders.DispatchedProviders())

	usedProviders.AddUsed(ConsumerSessionsMap{"lava@a": &SessionInfo{}}, nil)
	usedProviders.AddUsed(ConsumerSessionsMap{"lava@b": &SessionInfo{}}, nil)
	require.Equal(t, []string{"lava@a", "lava@b"}, usedProviders.DispatchedProviders(),
		"a hedge is a second batch, so the order the batches went out is the order of the attempts")

	// A retry path can ask the same provider again; that is a second attempt, not a duplicate.
	usedProviders.RemoveUsed("lava@a", NewRouterKey(nil), nil)
	usedProviders.AddUsed(ConsumerSessionsMap{"lava@a": &SessionInfo{}}, nil)
	require.Equal(t, []string{"lava@a", "lava@b", "lava@a"}, usedProviders.DispatchedProviders())
	require.Equal(t, 3, usedProviders.SessionsDispatched(), "the count is the length of the list")

	// Released before dispatch: never asked. Only the latest batch's entry goes, not the first one.
	usedProviders.ReleaseFromLatestBatch("lava@a", NewRouterKey(nil), nil)
	require.Equal(t, []string{"lava@a", "lava@b"}, usedProviders.DispatchedProviders())
	require.Equal(t, 2, usedProviders.SessionsDispatched())

	// Answering does not undo a dispatch.
	usedProviders.RemoveUsed("lava@b", NewRouterKey(nil), nil)
	require.Equal(t, []string{"lava@a", "lava@b"}, usedProviders.DispatchedProviders())

	// The caller gets a copy; the header path must not be able to rewrite the request's history.
	names := usedProviders.DispatchedProviders()
	names[0] = "lava@z"
	require.Equal(t, []string{"lava@a", "lava@b"}, usedProviders.DispatchedProviders())
}
