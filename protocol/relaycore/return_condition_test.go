package relaycore

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSignalReturnCondition_NeverBlocks is the regression for MAG-3722: the main loop reads
// returnCondition at most once, so every validateReturnCondition goroutine past the second
// used to park forever on the buffer-1 channel, pinning the state machine and the request.
func TestSignalReturnCondition_NeverBlocks(t *testing.T) {
	returnCondition := make(chan error, 1)
	first := errors.New("first")

	done := make(chan struct{})
	go func() {
		defer close(done)
		signalReturnCondition(returnCondition, first)
		signalReturnCondition(returnCondition, errors.New("second"))
		signalReturnCondition(returnCondition, errors.New("third"))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("signalling a return condition nobody will read must not block")
	}

	require.Equal(t, first, <-returnCondition, "the first reason is the one the loop acts on")
	select {
	case extra := <-returnCondition:
		t.Fatalf("no further reason may be queued, got %v", extra)
	default:
	}
}
