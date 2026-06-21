package endpointstate

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// These tests exercise the REAL ChainTracker poll path end to end — GetOrCreateTracker
// builds a real chaintracker.ChainTracker whose init fetch calls FetchLatestBlockNum
// synchronously, which must populate the per-endpoint observation. They are the
// regression guard the MAG-2158 bug slipped past: the direct Record* unit tests passed,
// but nothing drove a real poll, so the SVM gap (Solana polls via CustomMessage, never
// through EndpointPoller.FetchLatestBlockNum) went unnoticed.
//
// svmLatestBlockRequest must match chaintracker.latestBlockRequest byte-for-byte so the
// mock connection returns a slot response for the SVM getLatestBlockhash poll.
const svmLatestBlockRequest = `{"jsonrpc":"2.0","method":"getLatestBlockhash","params":[{"commitment":"finalized"}],"id":1}`

func ensureRandSeeded() {
	if !rand.Initialized() {
		rand.SetSpecificSeed(42)
	}
}

// newRealChainParser builds a real ChainParser from the on-disk spec. It is the
// lightweight subset of chainlib.CreateChainLibMocks (load spec, NewChainParser,
// SetSpec) WITHOUT the HTTP server/connector that helper also spins up — the connector's
// background goroutine + the helper's SetGlobalLoggingLevel write otherwise data-race
// with each other across tests under -race. We only need a parser, so we build just one.
func newRealChainParser(t *testing.T, specIndex, apiInterface string) chainlib.ChainParser {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, specIndex)
	require.NoError(t, err)
	cp, err := chainlib.NewChainParser(apiInterface)
	require.NoError(t, err)
	cp.SetSpec(spec)
	return cp
}

// TestEndpointPoller_FetchLatestBlockNum_RecordsExactlyOneObservation drives the real
// non-SVM poll path (a real ETH ChainParser) and asserts a single FetchLatestBlockNum
// records exactly one observation with the parsed block and clean poll-health.
func TestEndpointPoller_FetchLatestBlockNum_RecordsExactlyOneObservation(t *testing.T) {
	chainParser := newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)

	url := "http://eth-ep:8545"
	conn := &mockDirectRPCConnection{url: url} // default response is eth_blockNumber -> 0x100 (256)
	poller := NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		conn,
		chainParser,
		"ETH1",
		spectypes.APIInterfaceJsonRPC,
	)

	var calls int32
	var gotBlock int64
	var gotErr error
	poller.onPollObservation = func(block int64, latency time.Duration, pollErr error, at time.Time) {
		atomic.AddInt32(&calls, 1)
		atomic.StoreInt64(&gotBlock, block)
		gotErr = pollErr
	}

	block, err := poller.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(256), block, "0x100 parses to 256")

	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "exactly one observation per poll")
	require.Equal(t, int64(256), atomic.LoadInt64(&gotBlock))
	require.NoError(t, gotErr)
}

// TestEndpointMonitor_RealPoll_NonSVM_PopulatesObservation is the non-SVM end-to-end
// proof: a real ChainTracker created by GetOrCreateTracker runs its init fetch, which
// calls FetchLatestBlockNum and populates the observation record (not a direct Record*
// call). With these mocks the init's hash fetch can't parse the stub response, so init
// retries and the steady-state timer loop never runs — but the same FetchLatestBlockNum
// → observation chain under test fires on every init attempt.
func TestEndpointMonitor_RealPoll_NonSVM_PopulatesObservation(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chainParser := newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC)

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      chainParser,
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
	})
	require.NotNil(t, m)
	defer m.Stop()

	url := "http://eth-ep:8545"
	conn := &mockDirectRPCConnection{url: url}
	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		o, ok := m.GetObservation(url)
		return ok && o.LatestBlock == 256
	}, 5*time.Second, 20*time.Millisecond, "real non-SVM poll must populate the observation")

	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(256), o.LatestBlock)
	require.Equal(t, ObservationSourcePoll, o.Source)
	require.False(t, o.LastSuccessfulPoll.IsZero(), "a successful poll stamps LastSuccessfulPoll")
	require.Empty(t, o.LastPollError)
	require.Equal(t, 0, o.ConsecutivePollFailures)
}

// TestEndpointMonitor_RealPoll_SVM_PopulatesObservation is the headline regression
// guard: a real Solana ChainTracker's init fetch polls via CustomMessage, and the
// PollObserver hook must make that poll populate the observation. Before the fix this
// record stayed empty because the SVM poll never reached the poll instrumentation. The
// slot value (123456789) can only come from the SVM getLatestBlockhash response, so a
// pass proves the real SVM execution path was exercised.
func TestEndpointMonitor_RealPoll_SVM_PopulatesObservation(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const slot = int64(123456789)
	url := "https://solana-ep:443/"
	conn := &mockDirectRPCConnection{
		url: url,
		responses: map[string][]byte{
			svmLatestBlockRequest: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":123456789},"value":{"blockhash":"solhash"}}}`),
		},
	}

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "SOLANA", spectypes.APIInterfaceJsonRPC),
		ChainID:          "SOLANA",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
	})
	require.NotNil(t, m)
	defer m.Stop()

	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		o, ok := m.GetObservation(url)
		return ok && o.LatestBlock == slot
	}, 5*time.Second, 20*time.Millisecond, "real SVM poll must populate the observation via the PollObserver hook")

	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, slot, o.LatestBlock, "SVM observation records the polled slot")
	require.Equal(t, ObservationSourcePoll, o.Source)
	require.False(t, o.LastSuccessfulPoll.IsZero(), "a successful SVM poll stamps LastSuccessfulPoll")
	require.Empty(t, o.LastPollError)
}

// TestEndpointMonitor_RealPoll_SVM_FailureRecordsFailure mirrors the SVM success e2e for
// the failure path, asserting the MONITOR FIELDS (not just the callback args) so the
// "Solana failure updates the correct fields" contract is discharged through the real
// SVM execution path. The mock has no mapping for the getLatestBlockhash body, so it
// returns the generic eth_blockNumber stub; that cannot unmarshal into the SVM slot
// response, so the real SVM poll fails to parse a block.
func TestEndpointMonitor_RealPoll_SVM_FailureRecordsFailure(t *testing.T) {
	ensureRandSeeded()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	url := "https://solana-bad:443/"
	conn := &mockDirectRPCConnection{url: url} // no getLatestBlockhash mapping → unparseable as SVM

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainParser:      newRealChainParser(t, "SOLANA", spectypes.APIInterfaceJsonRPC),
		ChainID:          "SOLANA",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
	})
	require.NotNil(t, m)
	defer m.Stop()

	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, conn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		o, ok := m.GetObservation(url)
		return ok && o.ConsecutivePollFailures > 0
	}, 5*time.Second, 20*time.Millisecond, "a failing real SVM poll must record a poll failure")

	o, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(0), o.LatestBlock, "a failed SVM poll observes no block")
	require.Equal(t, ObservationSourceUnknown, o.Source, "no block observed → Source stays Unknown")
	require.True(t, o.LastSuccessfulPoll.IsZero(), "a failed SVM poll does not stamp LastSuccessfulPoll")
	require.NotEmpty(t, o.LastPollError, "the SVM poll error is recorded")
}
