package chainlib

import (
	"context"
	"errors"
	"testing"
	"time"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// testing hash cache request doesn't change the fields it resets to calculate hash.
func TestHashCacheRequest(t *testing.T) {
	var initSeenBlock int64 = 99
	var initRequestBlock int64 = 100
	initData := []byte{1, 2, 3, 4}
	initSalt := []byte{1, 3, 4, 5}

	relayRequest := &pairingtypes.RelayPrivateData{Data: initData, Salt: initSalt, RequestBlock: initRequestBlock, SeenBlock: initSeenBlock}
	_, _, err := HashCacheRequest(relayRequest, "Best-Chain-AKA-Test-Network")
	require.NoError(t, err)
	require.Equal(t, relayRequest.SeenBlock, initSeenBlock)
	require.Equal(t, relayRequest.RequestBlock, initRequestBlock)
	require.Equal(t, relayRequest.Data, initData)
	require.Equal(t, relayRequest.Salt, initSalt)
}

// deadlineBehindTheClock is a context whose deadline has passed while its timer has not run yet, so
// Err() is still nil: the moment a dial that ran into the deadline reports its failure.
type deadlineBehindTheClock struct {
	context.Context
	deadline time.Time
}

func (c deadlineBehindTheClock) Deadline() (time.Time, bool) { return c.deadline, true }

// A dial that runs into the deadline fails at the deadline's own time, and can be read before the
// runtime runs the context's timer. It was cut off by the deadline all the same, so the row must say
// so. TestHealth_ACutOffDialNamesTheDeadline read a bare "dial tcp ...: i/o timeout" in that gap
// (MAG-4919).
func TestDeadlineContext_ADeadlineBehindTheClockNamesItself(t *testing.T) {
	dialErr := errors.New("dial tcp 127.0.0.1:1: i/o timeout")
	passed := deadlineBehindTheClock{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond)}
	require.NoError(t, passed.Err(), "setup: the context's timer has not run")

	err := deadlineContext(passed, dialErr)
	require.ErrorContains(t, err, "verification deadline exceeded")
	require.ErrorIs(t, err, dialErr)
	require.NoError(t, deadlineContext(passed, nil))

	open, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	require.Equal(t, dialErr, deadlineContext(open, dialErr), "a failure inside the deadline keeps its own words")
	require.Equal(t, dialErr, deadlineContext(context.Background(), dialErr), "with no deadline there is none to name")
}
