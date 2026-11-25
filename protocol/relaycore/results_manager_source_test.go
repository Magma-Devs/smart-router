package relaycore

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// A relay's attempts are counted with the source the relay processor's ctx carries: the router's
// own crafted relays tag theirs boot or probe (MAG-5598). NewResultsManager, for a client's relay,
// counts as a client's.
func TestResultsManagerCountsTheRelaysSource(t *testing.T) {
	var sources []common.ErrorSource
	common.SetErrorMetricsCallback(func(_ uint32, _, _ string, _ bool, _, _ string, source common.ErrorSource) {
		sources = append(sources, source)
	})
	t.Cleanup(func() { common.SetErrorMetricsCallback(nil) })

	require.NoError(t, newResultsManager(1, "ETH1", common.ErrorSourceOf(common.WithErrorSource(context.Background(), common.ErrorSourceProbe))).SetResponse(&RelayResponse{Err: errors.New("connection refused")}, nil))
	require.NoError(t, NewResultsManager(2, "ETH1").SetResponse(&RelayResponse{Err: errors.New("connection refused")}, nil))
	require.Equal(t, []common.ErrorSource{common.ErrorSourceProbe, common.ErrorSourceClient}, sources)
}
