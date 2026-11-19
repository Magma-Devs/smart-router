package chaintracker

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
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// newSolanaFetcher models a Solana node. getLatestBlockhash (CustomMessage) answers the head as
// the slot, with that slot's blockhash; getBlock (FetchBlockHashByNum) fails, and must never be
// reached in head-only. It is a PollObserver, which is the ONLY way a Solana endpoint records a
// poll observation.
func newSolanaFetcher(slot int64) *observingFetcher {
	f := newFakeFetcher(slot)
	f.endpoint = routersession.RPCProviderEndpoint{ChainID: "SOLANA", ApiInterface: "jsonrpc"}
	f.custom = func(context.Context) ([]byte, error) {
		slot := f.headBlock()
		return []byte(fmt.Sprintf(
			`{"jsonrpc":"2.0","result":{"context":{"slot":%d},"value":{"blockhash":"hash-%d"}},"id":1}`,
			slot, slot)), nil
	}
	f.hash = func(_ context.Context, block int64) (string, error) {
		return "", fmt.Errorf("head-only must never fetch a block hash (slot %d)", block)
	}
	return &observingFetcher{fakeFetcher: f}
}

func svmHeadOnlyConfig(headOnly bool) ChainTrackerConfig {
	return ChainTrackerConfig{
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
	fetcher := newSolanaFetcher(300_000_000)

	tracker, err := NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx), "head-only must start without any hash fetch")

	for i := 0; i < 4; i++ {
		fetcher.advance()
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
	fetcher := newSolanaFetcher(300_000_000)

	tracker, err := NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	require.Equal(t, int64(300_000_000), tracker.GetAtomicLatestBlockNum(), "init must publish the head")

	fetcher.setHead(300_000_007)
	require.Eventually(t, func() bool {
		return tracker.GetAtomicLatestBlockNum() == 300_000_007
	}, time.Second*3, TimeForPollingMock, "the published head must follow the node")

	require.Zero(t, fetcher.hashCalls.Load())
}

// TestHeadOnlySVM_RecordsPollObservations is the one that guards the tip pipeline. On Solana
// the PollObserver hook is the ONLY place a poll observation is recorded, and that observation
// is what feeds the endpointtip store, ChainState consensus and consistency pre-validation.
// If head-only ever short-circuited before it, the tip would silently die.
func TestHeadOnlySVM_RecordsPollObservations(t *testing.T) {
	fetcher := newSolanaFetcher(300_000_000)

	tracker, err := NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	before := len(fetcher.observed())
	require.Positive(t, before, "init's poll must already have been observed")

	fetcher.setHead(fetcher.headBlock() + 3)
	require.Eventually(t, func() bool {
		return len(fetcher.observed()) > before
	}, time.Second*3, TimeForPollingMock, "every poll must record an observation in head-only")

	observed := fetcher.observed()
	require.Equal(t, fetcher.headBlock(), observed[len(observed)-1].block, "the observed block must be the polled slot")
	for _, observation := range observed {
		require.NoError(t, observation.err, "a healthy head-only poll must not be recorded as failed")
	}
}

// TestHeadOnlySVM_MalformedReplyIsAFailedPoll guards the parse itself: head-only returns before
// the hash work, but it must still read context.slot out of the getLatestBlockhash reply. A
// malformed reply must be a failed poll, not a silent zero head.
func TestHeadOnlySVM_MalformedReplyIsAFailedPoll(t *testing.T) {
	fetcher := newBadReplySolanaFetcher("<html>502 bad gateway</html>")

	tracker, err := NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
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
			fetcher := newBadReplySolanaFetcher(tc.body)

			tracker, err := NewChainTracker(context.Background(), fetcher, svmHeadOnlyConfig(true))
			require.NoError(t, err)

			err = tracker.StartAndServe(context.Background())
			require.Error(t, err, "a reply carrying no usable slot must fail start, not publish head 0")
			require.Zero(t, tracker.GetAtomicLatestBlockNum(), "no head may be published from a slotless reply")
			require.Zero(t, fetcher.hashCalls.Load(), "still no hash fetch on the failure path")
		})
	}
}

// newBadReplySolanaFetcher answers every latest-slot poll with body, so one fetcher covers both
// the unparseable and the parses-but-slotless cases. It is not a PollObserver.
func newBadReplySolanaFetcher(body string) *fakeFetcher {
	f := newFakeFetcher(0).answerCustom(body, nil)
	f.endpoint = routersession.RPCProviderEndpoint{ChainID: "SOLANA", ApiInterface: "jsonrpc"}
	f.hash = func(context.Context, int64) (string, error) { return "", fmt.Errorf("must not be called") }
	return f
}
