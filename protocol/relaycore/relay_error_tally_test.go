package relaycore

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/metrics"
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

// rowsByMethod reads the tally as method -> row, for the one chain, interface and provider a test
// records under.
func rowsByMethod(t *testing.T, tally *RelayErrorTally) map[string]RelayErrorCount {
	t.Helper()
	out := map[string]RelayErrorCount{}
	for _, row := range tally.Counts() {
		out[row.Method] = row
	}
	return out
}

// Review 626-3: the two kinds of name that are the request's own text each get the method label's
// budget per chain and interface, and count on that kind's overflow row past it.
func TestRelayErrorTally_BoundsTheNamesARequestMintsPerChainAndInterface(t *testing.T) {
	tally := NewRelayErrorTally()
	extraDefaults, extraBatches := 8, 6
	for i := 0; i < metrics.MaxDefaultMethodsPerSpec+extraDefaults; i++ {
		tally.RecordNodeError("ETH1", "rest", "p", fmt.Sprintf("Default-/unknown/path/%d", i))
	}
	for i := 0; i < metrics.MaxBatchSignaturesPerSpec+extraBatches; i++ {
		// Short distinct names, so the budget binds rather than the size bound.
		tally.RecordProtocolError("ETH1", "rest", "p", fmt.Sprintf("eth_blockNumber&eth_call%d", i))
	}
	// A declared method is a spec name and is never folded, however full the budgets are.
	tally.RecordNodeError("ETH1", "rest", "p", "eth_call")

	rows := rowsByMethod(t, tally)
	require.Len(t, rows, metrics.MaxDefaultMethodsPerSpec+metrics.MaxBatchSignaturesPerSpec+3,
		"each budget's names, one overflow row per kind, and the declared method")
	require.Equal(t, uint64(extraDefaults), rows[metrics.DefaultMethodOther].NodeErrors)
	require.Equal(t, uint64(extraBatches), rows[metrics.BatchMethodOther].ProtocolErrors)
	require.Equal(t, uint64(1), rows["eth_call"].NodeErrors)
	require.Contains(t, rows, "Default-/unknown/path/0", "names inside the budget keep the name as relayed")
	require.NotContains(t, rows, fmt.Sprintf("Default-/unknown/path/%d", metrics.MaxDefaultMethodsPerSpec))

	// Admission is monotone: an admitted name keeps its own row once the budget is full.
	tally.RecordNodeError("ETH1", "rest", "p", "Default-/unknown/path/0")
	require.Equal(t, uint64(2), rowsByMethod(t, tally)["Default-/unknown/path/0"].NodeErrors)

	// Each chain and interface has its own budget.
	tally.RecordNodeError("ETH1", "jsonrpc", "p", "Default-/fresh")
	require.Contains(t, tally.Counts(), RelayErrorCount{ChainID: "ETH1", ApiInterface: "jsonrpc", ProviderAddress: "p", Method: "Default-/fresh", NodeErrors: 1})
}

// One oversized batch cannot pin its whole element list as a row name, even with budget left.
func TestRelayErrorTally_AnOversizedNameCountsOnTheOverflowRow(t *testing.T) {
	tally := NewRelayErrorTally()
	tally.RecordNodeError("ETH1", "jsonrpc", "p", strings.TrimPrefix(strings.Repeat("&eth_getBalance", 40), "&"))
	tally.RecordNodeError("ETH1", "jsonrpc", "p", "Default-/"+strings.Repeat("x", maxTallyMethodNameBytes))
	require.Equal(t, []RelayErrorCount{
		{ChainID: "ETH1", ApiInterface: "jsonrpc", ProviderAddress: "p", Method: metrics.DefaultMethodOther, NodeErrors: 1},
		{ChainID: "ETH1", ApiInterface: "jsonrpc", ProviderAddress: "p", Method: metrics.BatchMethodOther, NodeErrors: 1},
	}, tally.Counts())
}
