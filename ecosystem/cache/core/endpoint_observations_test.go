package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEngineEndpointObservation_ClampsTTL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		asked    time.Duration
		expected time.Duration
	}{
		{"zero is floored", 0, MinEndpointObservationTTL},
		{"negative is floored", -time.Second, MinEndpointObservationTTL},
		{"in range is kept", 3 * time.Second, 3 * time.Second},
		{"excessive is capped", time.Hour, MaxEndpointObservationTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			engine := &Engine{Store: store}
			applied, err := engine.PublishEndpointObservation(context.Background(), "ETH1", "jsonrpc", "ep1", "pod-a", 100, tc.asked)
			require.NoError(t, err)
			require.True(t, applied)
			require.Equal(t, tc.expected, store.observationTTLs[EndpointObservationKey("ETH1", "jsonrpc", "ep1")])
		})
	}
}

// A publish that names no endpoint or no positive block is a writer bug and is refused before
// it reaches the store, on either backend.
func TestEngineEndpointObservation_RejectsInvalidPublish(t *testing.T) {
	store := newFakeStore()
	engine := &Engine{Store: store}
	ctx := context.Background()

	for _, tc := range []struct {
		name            string
		chain, endpoint string
		block           int64
	}{
		{"missing chain id", "", "ep1", 1},
		{"missing endpoint id", "ETH1", "", 1},
		{"zero block", "ETH1", "ep1", 0},
		{"negative block", "ETH1", "ep1", -5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applied, err := engine.PublishEndpointObservation(ctx, tc.chain, "jsonrpc", tc.endpoint, "pod-a", tc.block, time.Second)
			require.False(t, applied)
			require.True(t, errors.Is(err, ErrInvalidEndpointObservation))
		})
	}
	require.Empty(t, store.observations, "rejected writes store nothing")

	_, _, found, err := engine.GetEndpointObservation(ctx, "", "jsonrpc", "ep1")
	require.NoError(t, err)
	require.False(t, found, "an unaddressable read is a miss, not an error")
}

// The key isolates chain, interface and endpoint from each other, so one endpoint serving two
// interfaces is polled — and borrowed — per interface.
func TestEngineEndpointObservation_KeyIsolation(t *testing.T) {
	store := newFakeStore()
	engine := &Engine{Store: store}
	ctx := context.Background()

	_, err := engine.PublishEndpointObservation(ctx, "ETH1", "jsonrpc", "ep1", "pod-a", 500, time.Second)
	require.NoError(t, err)

	obs, _, found, err := engine.GetEndpointObservation(ctx, "ETH1", "jsonrpc", "ep1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, EndpointObservation{Block: 500, PodID: "pod-a"}, obs)

	for _, other := range [][3]string{{"ETH1", "jsonrpc", "ep2"}, {"ETH1", "rest", "ep1"}, {"SEP1", "jsonrpc", "ep1"}} {
		_, _, found, err := engine.GetEndpointObservation(ctx, other[0], other[1], other[2])
		require.NoError(t, err)
		require.False(t, found, "%v must not see ep1's observation", other)
	}
}

// A store failure is surfaced, never reported as a miss: the caller polls locally either way,
// but counts an error on rpc_endpoint_tracker_gate_errors_total, which is the one signal that
// tells an unreachable backend apart from a fleet with nothing to share.
func TestEngineEndpointObservation_StoreErrorIsSurfaced(t *testing.T) {
	store := newFakeStore()
	store.observationErr = errors.New("backend unreachable")
	engine := &Engine{Store: store}
	ctx := context.Background()

	_, err := engine.PublishEndpointObservation(ctx, "ETH1", "jsonrpc", "ep1", "pod-a", 1, time.Second)
	require.ErrorIs(t, err, store.observationErr)
	_, _, found, err := engine.GetEndpointObservation(ctx, "ETH1", "jsonrpc", "ep1")
	require.ErrorIs(t, err, store.observationErr)
	require.False(t, found)
}
