package chaintracker_test

// Head-only on the SOLANA/SVM path.
//
// The MAG-2218 head-only tests only ever exercised the generic (EVM) fetcher — they set no
// chain id, so newCustomChainTracker gave them DefaultChainTrackerFetcher. Making fork
// detection operator-switchable puts Solana into head-only on day one, which means the
// SVMChainTracker wrapper is now the FIRST thing that runs in this mode. These tests cover
// that combination.
//
// What must hold: the tracker publishes and advances the head, records a poll observation
// for every poll (the signal that feeds the endpointtip store / ChainState / consistency
// pre-validation), and never fetches a block hash — so it never reaches waitForSlotVisible,
// which in head-only has no cache to wait on.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	chaintracker "github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// svmHeadOnlyFetcher models a Solana node. getLatestBlockhash (CustomMessage) returns an
// advancing slot plus its blockhash; getBlock (FetchBlockHashByNum) counts calls and must
// never be reached in head-only. It also implements chaintracker.PollObserver, which is the
// ONLY way a Solana endpoint records a poll observation.
type svmHeadOnlyFetcher struct {
	slot        atomic.Int64
	customCalls atomic.Int64
	hashCalls   atomic.Int64

	observedCalls  atomic.Int64
	observedBlock  atomic.Int64
	observedErrors atomic.Int64
}

func (f *svmHeadOnlyFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{ChainID: "SOLANA", ApiInterface: "jsonrpc"}
}

func (f *svmHeadOnlyFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType, apiName string) ([]byte, error) {
	f.customCalls.Add(1)
	slot := f.slot.Load()
	return []byte(fmt.Sprintf(
		`{"jsonrpc":"2.0","result":{"context":{"slot":%d},"value":{"blockhash":"hash-%d"}},"id":1}`,
		slot, slot)), nil
}

func (f *svmHeadOnlyFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	f.hashCalls.Add(1)
	return "", fmt.Errorf("head-only must never fetch a block hash (slot %d)", blockNum)
}

// Unused on the SVM path (the wrapper polls via CustomMessage) but required by ChainFetcher.
func (f *svmHeadOnlyFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	return f.slot.Load(), nil
}

func (f *svmHeadOnlyFetcher) FetchChainID(ctx context.Context) (string, string, error) {
	return "", "", utils.FormatError("FetchChainID not supported", nil)
}

// ObserveLatestBlockPoll makes this fetcher a chaintracker.PollObserver.
func (f *svmHeadOnlyFetcher) ObserveLatestBlockPoll(block int64, transportLatency time.Duration, err error) {
	f.observedCalls.Add(1)
	f.observedBlock.Store(block)
	if err != nil {
		f.observedErrors.Add(1)
	}
}

func svmHeadOnlyConfig(headOnly bool) chaintracker.ChainTrackerConfig {
	return chaintracker.ChainTrackerConfig{
		ChainId:               "SOLANA", // selects SVMChainTracker
		BlocksToSave:          1,        // what the monitor forces for the Solana family
		AverageBlockTime:      TimeForPollingMock,
		ServerBlockMemory:     100,
		ParseDirectiveEnabled: true,
		HeadOnlyTracking:      headOnly,
		FlatPollInterval:      TimeForPollingMock,
	}
}

// TestHeadOnlySVM_NeverFetchesBlockHashes is the core guarantee: with fork detection off, a
// Solana tracker makes exactly one upstream call per poll — getLatestBlockhash — and never
// getBlock.
func TestHeadOnlySVM_NeverFetchesBlockHashes(t *testing.T) {
	fetcher := &svmHeadOnlyFetcher{}
	fetcher.slot.Store(300_000_000)

	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx), "head-only must start without any hash fetch")

	for i := 0; i < 4; i++ {
		fetcher.slot.Add(1)
		time.Sleep(TimeForPollingMock * 2)
	}
	cancel()

	require.Zero(t, fetcher.hashCalls.Load(), "head-only must never call getBlock on the SVM path")
	require.Greater(t, fetcher.customCalls.Load(), int64(1), "the latest-slot poll must keep running")
}

// TestHeadOnlySVM_HeadAdvancesAndIsPublished proves the endpoint latest-block mechanism still
// works: the tracker's published head follows the node, which is what consistency
// pre-validation falls back to and what advanceHeadOnly writes.
func TestHeadOnlySVM_HeadAdvancesAndIsPublished(t *testing.T) {
	fetcher := &svmHeadOnlyFetcher{}
	fetcher.slot.Store(300_000_000)

	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	require.Equal(t, int64(300_000_000), tracker.GetAtomicLatestBlockNum(), "init must publish the head")

	fetcher.slot.Store(300_000_007)
	require.Eventually(t, func() bool {
		return tracker.GetAtomicLatestBlockNum() == 300_000_007
	}, time.Second*3, TimeForPollingMock, "the published head must follow the node")

	block, changeTime := tracker.GetLatestBlockNum()
	require.Equal(t, int64(300_000_007), block)
	require.False(t, changeTime.IsZero(), "a head advance must stamp the change time")
	require.Zero(t, fetcher.hashCalls.Load())
}

// TestHeadOnlySVM_RecordsPollObservations is the one that guards the tip pipeline. On Solana
// the PollObserver hook is the ONLY place a poll observation is recorded, and that observation
// is what feeds the endpointtip store, ChainState consensus and consistency pre-validation.
// If head-only ever short-circuited before it, the tip would silently die.
func TestHeadOnlySVM_RecordsPollObservations(t *testing.T) {
	fetcher := &svmHeadOnlyFetcher{}
	fetcher.slot.Store(300_000_000)

	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	before := fetcher.observedCalls.Load()
	require.Positive(t, before, "init's poll must already have been observed")

	fetcher.slot.Add(3)
	require.Eventually(t, func() bool {
		return fetcher.observedCalls.Load() > before
	}, time.Second*3, TimeForPollingMock, "every poll must record an observation in head-only")

	require.Equal(t, fetcher.slot.Load(), fetcher.observedBlock.Load(), "the observed block must be the polled slot")
	require.Zero(t, fetcher.observedErrors.Load(), "a healthy head-only poll must not be recorded as failed")
}

// TestHeadOnlySVM_LatestBlockDataHasNoHashes mirrors the generic head-only contract on the SVM
// path: answer with the head and an empty hash slice rather than erroring.
func TestHeadOnlySVM_LatestBlockDataHasNoHashes(t *testing.T) {
	fetcher := &svmHeadOnlyFetcher{}
	fetcher.slot.Store(300_000_000)

	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	latest, hashes, _, err := tracker.GetLatestBlockData(spectypes.NOT_APPLICABLE, spectypes.NOT_APPLICABLE, spectypes.NOT_APPLICABLE)
	require.NoError(t, err, "head-only must not treat an empty queue as a fault")
	require.Equal(t, int64(300_000_000), latest)
	require.Empty(t, hashes)
}

// TestHeadOnlySVM_MalformedReplyIsAFailedPoll guards the parse itself: head-only returns before
// the hash work, but it must still read context.slot out of the getLatestBlockhash reply. A
// malformed reply must be a failed poll, not a silent zero head.
func TestHeadOnlySVM_MalformedReplyIsAFailedPoll(t *testing.T) {
	fetcher := &svmBadReplyFetcher{body: []byte("<html>502 bad gateway</html>")}

	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)
	// Init fetches the head; a reply that cannot be parsed must fail the tracker's start
	// rather than publish a zero head.
	err = tracker.StartAndServe(context.Background())
	require.Error(t, err, "an unparseable latest-slot reply must fail start, not publish head 0")
	require.Zero(t, fetcher.hashCalls.Load(), "still no hash fetch on the failure path")
}

// TestHeadOnlySVM_ParseableReplyWithNoSlotIsAFailedPoll is the case the malformed-reply test
// above does NOT cover. These bodies are valid JSON and unmarshal without error — they simply
// carry no usable context.slot, so the poll yields slot 0 with a nil error. Head-only returns
// straight after publishing the head, so nothing downstream would reject it: without the init
// guard the tracker would publish head 0 and report itself started.
func TestHeadOnlySVM_ParseableReplyWithNoSlotIsAFailedPoll(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "result object with no context", body: `{"jsonrpc":"2.0","result":{},"id":1}`},
		{name: "context with no slot", body: `{"jsonrpc":"2.0","result":{"context":{}},"id":1}`},
		{name: "slot explicitly zero", body: `{"jsonrpc":"2.0","result":{"context":{"slot":0}},"id":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &svmBadReplyFetcher{body: []byte(tc.body)}

			tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
			require.NoError(t, err)

			err = tracker.StartAndServe(context.Background())
			require.Error(t, err, "a reply carrying no usable slot must fail start, not publish head 0")
			require.Zero(t, tracker.GetAtomicLatestBlockNum(), "no head may be published from a slotless reply")
			require.Zero(t, fetcher.hashCalls.Load(), "still no hash fetch on the failure path")
		})
	}
}

// svmBadReplyFetcher returns a fixed body for every latest-slot poll, so one type covers both
// the unparseable and the parses-but-slotless cases.
type svmBadReplyFetcher struct {
	body      []byte
	hashCalls atomic.Int64
}

func (f *svmBadReplyFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{ChainID: "SOLANA", ApiInterface: "jsonrpc"}
}

func (f *svmBadReplyFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType, apiName string) ([]byte, error) {
	return f.body, nil
}

func (f *svmBadReplyFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	f.hashCalls.Add(1)
	return "", fmt.Errorf("must not be called")
}

func (f *svmBadReplyFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) { return 0, nil }

func (f *svmBadReplyFetcher) FetchChainID(ctx context.Context) (string, string, error) {
	return "", "", utils.FormatError("FetchChainID not supported", nil)
}

// compile-time proof the fetcher really is the observer hook the SVM path looks for.
var _ chaintracker.PollObserver = (*svmHeadOnlyFetcher)(nil)
