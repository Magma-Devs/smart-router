package endpointstate

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// TestEndpointMonitor_RealPoll_EveryChain_RecordsPollFailure is the MAG-2221 canary. A
// fetcher wrapper must record each latest-block poll one of two ways: through
// EndpointPoller.FetchLatestBlockNum (the default wrapper), or through the
// chaintracker.PollObserver hook (the SVM wrapper, which polls via CustomMessage). Before
// PR #143 the SVM wrapper did neither, and its endpoints silently recorded nothing.
//
// The test starts from chain IDs, not from wrappers. For every enabled spec in specs/ and
// every API interface it serves, it builds a real ChainTracker, so the ChainTracker itself
// picks the wrapper by chain ID, and points it at a connection that fails every request.
// A failed poll must still record a failure. A new wrapper picked for any of these chains
// that records through neither path leaves the observation empty, and the test fails
// without anyone having to list the new wrapper here.
//
// It only sees chains whose spec is in specs/. A wrapper picked for a chain that has no
// spec there is not covered.
func TestEndpointMonitor_RealPoll_EveryChain_RecordsPollFailure(t *testing.T) {
	ensureRandSeeded()

	specs, err := specutils.GetAllSpecsFromLocalDir("../../specs/")
	require.NoError(t, err)

	chainIDs := make([]string, 0, len(specs))
	for id, spec := range specs {
		if spec.Enabled {
			chainIDs = append(chainIDs, id)
		}
	}
	sort.Strings(chainIDs)
	// Guard against the loop silently testing nothing, and make sure both wrappers are in it.
	require.Contains(t, chainIDs, "ETH1")
	require.Contains(t, chainIDs, "SOLANA")

	for _, chainID := range chainIDs {
		spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, chainID)
		require.NoError(t, err, chainID)

		apiInterfaces := map[string]struct{}{}
		for _, collection := range spec.ApiCollections {
			if collection.Enabled {
				apiInterfaces[collection.CollectionData.ApiInterface] = struct{}{}
			}
		}
		require.NotEmpty(t, apiInterfaces, "%s serves no API interface", chainID)

		for apiInterface := range apiInterfaces {
			t.Run(chainID+"/"+apiInterface, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				chainParser, err := chainlib.NewChainParser(apiInterface)
				require.NoError(t, err)
				chainParser.SetSpec(spec)

				m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
					ChainParser:      chainParser,
					ChainID:          chainID,
					ApiInterface:     apiInterface,
					AverageBlockTime: 100 * time.Millisecond,
					BlocksToSave:     1,
				})
				require.NotNil(t, m)
				defer m.Stop()

				url := "http://" + chainID + "-down:1/"
				conn := &mockDirectRPCConnection{url: url, sendErr: errors.New("connection refused")}
				_, err = m.GetOrCreateTracker("", &lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					o, ok := m.GetObservation(url)
					return ok && o.ConsecutivePollFailures > 0
				}, 5*time.Second, 20*time.Millisecond,
					"a failed latest-block poll on %s/%s recorded no observation: its fetcher wrapper "+
						"records through neither EndpointPoller.FetchLatestBlockNum nor the PollObserver hook",
					chainID, apiInterface)

				o, _ := m.GetObservation(url)
				require.NotEmpty(t, o.LastPollError, "the poll error is recorded")
			})
		}
	}
}
