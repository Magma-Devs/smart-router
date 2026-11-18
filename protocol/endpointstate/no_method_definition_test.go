package endpointstate

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// A poll that fails only because no provider of the chain has given a definition of the gRPC
// method it calls is flagged on the observation, so the probe reads the endpoint's height as
// unknown, not the endpoint as down (MAG-5134). Any other outcome clears the flag.
func TestPollObservation_NoMethodDefinitionIsFlagged(t *testing.T) {
	const url = "grpc://no-definition:9090"
	m := newPollBlockMonitor(t, &pollBlockSink{})
	gen := m.registerGenForTest(url)
	now := time.Now()
	at := func(ms int) time.Time { return now.Add(time.Duration(ms) * time.Millisecond) }
	flagged := func() bool {
		t.Helper()
		obs, ok := m.GetObservation(url)
		require.True(t, ok)
		return obs.LastPollNoDefinition
	}

	m.recordPollObservation(url, gen, 0, 0, fmt.Errorf("poll: %w", routersession.ErrNoMethodDefinition), at(0))
	require.True(t, flagged())

	m.recordPollObservation(url, gen, 0, 0, errors.New("503"), at(1))
	require.False(t, flagged(), "a failure of any other kind is the endpoint's again")

	m.recordPollObservation(url, gen, 0, 0, fmt.Errorf("poll: %w", routersession.ErrNoMethodDefinition), at(2))
	m.recordPollObservation(url, gen, 100, time.Millisecond, nil, at(3))
	require.False(t, flagged(), "an answered poll clears it")
}
