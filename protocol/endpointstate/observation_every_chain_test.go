package endpointstate

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// TestEndpointMonitor_RealPoll_EveryChain_RecordsPollFailure is the MAG-2221 canary. A
// fetcher wrapper must record each latest-block poll one of two ways: through
// EndpointPoller.FetchLatestBlockNum (the default wrapper), or through the
// chaintracker.PollObserver hook (the SVM wrapper, which polls via CustomMessage). Before
// PR #143 the SVM wrapper did neither, and its endpoints silently recorded nothing.
//
// The test starts from chain IDs, not from wrappers, and takes them from two places: every
// enabled spec in specs/ with every API interface it serves, and every chain in
// chaintracker's fetcher wrapper table (CustomFetcherWrapperChainIDs), which is what
// newCustomChainTracker picks the wrapper from. For each it builds a real ChainTracker, so
// the ChainTracker itself picks the wrapper by chain ID, and points it at a connection that
// fails every request. A failed poll must still record a failure. A wrapper that records
// through neither path leaves the observation empty, and the test fails without anyone
// having to list the wrapper here: its chains are in the table already.
//
// A table chain with no spec in specs/ (KOII, KOIIT) borrows the spec of another chain
// that uses the same wrapper kind (SOLANA): the wrapper is picked by chain ID, and the
// poll fails before any spec-shaped reply matters. A wrapper kind that no chain in specs/
// uses fails the test asking for a spec, rather than passing with nothing driven.
func TestEndpointMonitor_RealPoll_EveryChain_RecordsPollFailure(t *testing.T) {
	ensureRandSeeded()

	const specDir = "../../specs/"
	specs, err := specutils.GetAllSpecsFromLocalDir(specDir)
	require.NoError(t, err)

	enabled := map[string]bool{}
	for id, spec := range specs {
		if spec.Enabled {
			enabled[id] = true
		}
	}
	// Guard against the loop silently testing nothing, and make sure both wrappers are in it.
	require.True(t, enabled["ETH1"], "specs/ must carry ETH1")
	require.True(t, enabled["SOLANA"], "specs/ must carry SOLANA")

	// chainID -> the spec index to load for it. A chain in specs/ loads its own spec; a
	// wrapper-table chain without one borrows the spec of a same-kind chain that has one.
	specFor := map[string]string{}
	for id := range enabled {
		specFor[id] = id
	}
	wrapperTable := chaintracker.CustomFetcherWrapperChainIDs()
	require.NotEmpty(t, wrapperTable, "the fetcher wrapper table is empty; the SVM chains belong in it")
	for chainID, kind := range wrapperTable {
		if enabled[chainID] {
			continue
		}
		donor := ""
		for other, otherKind := range wrapperTable {
			if otherKind == kind && enabled[other] && (donor == "" || other < donor) {
				donor = other
			}
		}
		require.NotEmptyf(t, donor, "%s uses the %q fetcher wrapper but has no spec in specs/, and no other %q chain "+
			"has one either: add a spec so this canary can drive that wrapper", chainID, kind, kind)
		specFor[chainID] = donor
	}

	chainIDs := make([]string, 0, len(specFor))
	for id := range specFor {
		chainIDs = append(chainIDs, id)
	}
	sort.Strings(chainIDs)

	for _, chainID := range chainIDs {
		specIndex := specFor[chainID]
		t.Run(chainID, func(t *testing.T) {
			spec, err := specutils.GetSpecFromLocalDirs([]string{specDir}, specIndex)
			require.NoError(t, err, specIndex)
			if specIndex != chainID {
				t.Logf("%s has no spec in specs/; driving its wrapper with the %s spec", chainID, specIndex)
			}

			seen := map[string]bool{}
			apiInterfaces := make([]string, 0, len(spec.ApiCollections))
			for _, collection := range spec.ApiCollections {
				apiInterface := collection.CollectionData.ApiInterface
				if collection.Enabled && !seen[apiInterface] {
					seen[apiInterface] = true
					apiInterfaces = append(apiInterfaces, apiInterface)
				}
			}
			sort.Strings(apiInterfaces)
			require.NotEmpty(t, apiInterfaces, "%s serves no API interface", specIndex)

			for _, apiInterface := range apiInterfaces {
				t.Run(apiInterface, func(t *testing.T) {
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
					_, err = m.GetOrCreateTracker("", &routersession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
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
		})
	}
}
