package relaycore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelayErrorTally_KeepsTheTwoCountsApartPerRow(t *testing.T) {
	tally := NewRelayErrorTally()
	tally.RecordNodeError("SUI", "graphql", "primary1", "checkpoint")
	tally.RecordNodeError("SUI", "graphql", "primary1", "checkpoint")
	tally.RecordProtocolError("SUI", "graphql", "primary1", "checkpoint")
	tally.RecordProtocolError("SUI", "graphql", "primary2", "checkpoint")
	tally.RecordNodeError("ETH1", "jsonrpc", "provider1", "eth_call")
	// A GraphQL operation's root fields are named with "&"; the tally keeps the name as relayed.
	tally.RecordNodeError("SUI", "graphql", "primary1", "chainIdentifier&checkpoint")

	require.Equal(t, []RelayErrorCount{
		{ChainID: "ETH1", ApiInterface: "jsonrpc", ProviderAddress: "provider1", Method: "eth_call", NodeErrors: 1},
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary1", Method: "chainIdentifier&checkpoint", NodeErrors: 1},
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary1", Method: "checkpoint", NodeErrors: 2, ProtocolErrors: 1},
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary2", Method: "checkpoint", ProtocolErrors: 1},
	}, tally.Counts())
}

func TestRelayErrorTally_NilRecordsNothing(t *testing.T) {
	var tally *RelayErrorTally
	tally.RecordNodeError("ETH1", "jsonrpc", "p", "m")
	tally.RecordProtocolError("ETH1", "jsonrpc", "p", "m")
	require.Empty(t, tally.Counts())
	require.Empty(t, NewRelayErrorTally().Counts())
}

func TestRelayErrorTally_ConcurrentRecords(t *testing.T) {
	tally := NewRelayErrorTally()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tally.RecordNodeError("ETH1", "jsonrpc", "p", "m")
			tally.RecordProtocolError("ETH1", "jsonrpc", "p", "m")
			_ = tally.Counts()
		}()
	}
	wg.Wait()
	require.Equal(t, []RelayErrorCount{{ChainID: "ETH1", ApiInterface: "jsonrpc", ProviderAddress: "p", Method: "m", NodeErrors: 50, ProtocolErrors: 50}}, tally.Counts())
}
