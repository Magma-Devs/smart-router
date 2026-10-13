package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSafeChannelSender_TrySend pins the non-waiting send the subscription fan-out uses
// (MAG-4242): a push lands when there is room, is refused when the receiver is behind, and is
// refused after the sender closed or its context ended — and none of it ever blocks.
func TestSafeChannelSender_TrySend(t *testing.T) {
	t.Run("lands while there is room, refused once full", func(t *testing.T) {
		ch := make(chan int, 2)
		sender := NewSafeChannelSender(context.Background(), ch)
		require.True(t, sender.TrySend(1))
		require.True(t, sender.TrySend(2))
		require.False(t, sender.TrySend(3), "a full receiver is reported, not waited for")
		require.Equal(t, 1, <-ch)
		require.True(t, sender.TrySend(4), "room again once the receiver caught up")
	})
	t.Run("refused after close", func(t *testing.T) {
		ch := make(chan int, 1)
		sender := NewSafeChannelSender(context.Background(), ch)
		sender.Close()
		require.False(t, sender.TrySend(1))
	})
	t.Run("refused after its context ended", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan int, 1)
		sender := NewSafeChannelSender(ctx, ch)
		cancel()
		require.False(t, sender.TrySend(1))
	})
}
