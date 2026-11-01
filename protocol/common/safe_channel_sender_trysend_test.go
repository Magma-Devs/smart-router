package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSafeChannelSender_TrySend pins the non-waiting send the subscription fan-out uses
// (MAG-4242): a push lands when there is room, is refused as full when the receiver is behind,
// and is refused as closed after the sender closed or its context ended, so a caller that drops
// slow receivers can tell one that left from one that fell behind. None of it ever blocks.
func TestSafeChannelSender_TrySend(t *testing.T) {
	t.Run("lands while there is room, full once the receiver is behind", func(t *testing.T) {
		ch := make(chan int, 2)
		sender := NewSafeChannelSender(context.Background(), ch)
		require.Equal(t, TrySendDelivered, sender.TrySend(1))
		require.Equal(t, TrySendDelivered, sender.TrySend(2))
		require.Equal(t, TrySendFull, sender.TrySend(3), "a full receiver is reported, not waited for")
		require.Equal(t, 1, <-ch)
		require.Equal(t, TrySendDelivered, sender.TrySend(4), "room again once the receiver caught up")
	})
	t.Run("closed after close", func(t *testing.T) {
		ch := make(chan int, 1)
		sender := NewSafeChannelSender(context.Background(), ch)
		sender.Close()
		require.Equal(t, TrySendClosed, sender.TrySend(1))
	})
	t.Run("closed after its context ended, even with room", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan int, 1)
		sender := NewSafeChannelSender(ctx, ch)
		cancel()
		require.Equal(t, TrySendClosed, sender.TrySend(1))
	})
	t.Run("closed after its context ended, with the receiver behind", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan int, 1)
		sender := NewSafeChannelSender(ctx, ch)
		require.Equal(t, TrySendDelivered, sender.TrySend(1))
		cancel()
		require.Equal(t, TrySendClosed, sender.TrySend(2), "a receiver that left is not a slow one")
	})
}
