package chaintracker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// errInjectedFetchFailure is what every head and hash fetch answers while a fakeFetcher fails.
var errInjectedFetchFailure = fmt.Errorf("injected poll failure")

// fakeFetcher is the ChainFetcher the tracker tests drive. Its chain is a head that a test moves,
// and a hash for every block: "hash-<block>", plus the fork's name once the chain forks. Every
// method counts its calls. On top of the chain:
//   - fail makes every head and hash fetch fail, and delay makes each take that long first;
//   - latest, hash and custom, when set, answer in place of the chain;
//   - CustomMessage, the SVM latest-slot poll, answers custom, or else the reply answerCustom set,
//     and fails while neither is set;
//   - a windowed fetcher answers the hash only of the blocks it still keeps, the head and the
//     ones below it down to the oldest, as a node that prunes does;
//   - onLatest runs, under the fetcher's lock, on every head fetch.
type fakeFetcher struct {
	endpoint routersession.RPCProviderEndpoint

	latest func(ctx context.Context, call int) (int64, error) // call counts from 1
	hash   func(ctx context.Context, block int64) (string, error)
	custom func(ctx context.Context) ([]byte, error)

	fail  atomic.Bool
	delay time.Duration

	mu        sync.Mutex
	head      int64
	windowed  bool
	oldest    int64
	fork      string
	onLatest  func()
	headCalls []time.Time

	customSet   bool
	customReply []byte
	customErr   error

	hashCalls   atomic.Int64
	customCalls atomic.Int64
}

// newFakeFetcher is a fakeFetcher whose head is head.
func newFakeFetcher(head int64) *fakeFetcher {
	return &fakeFetcher{head: head}
}

// newWindowedFetcher is a fakeFetcher that keeps the hashes of blocksToSave blocks, from start
// up to its head, start+blocksToSave-1. Each advance moves the window with the head.
func newWindowedFetcher(start, blocksToSave int64, onLatest func()) *fakeFetcher {
	return &fakeFetcher{head: start + blocksToSave - 1, windowed: true, oldest: start, onLatest: onLatest}
}

func (f *fakeFetcher) FetchEndpoint() routersession.RPCProviderEndpoint {
	return f.endpoint
}

func (f *fakeFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	f.mu.Lock()
	f.headCalls = append(f.headCalls, time.Now())
	call := len(f.headCalls)
	if f.onLatest != nil {
		f.onLatest()
	}
	f.mu.Unlock()
	if err := f.wait(ctx); err != nil {
		return 0, err
	}
	if f.fail.Load() {
		return 0, errInjectedFetchFailure
	}
	if f.latest != nil {
		return f.latest(ctx, call)
	}
	return f.headBlock(), nil
}

func (f *fakeFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	f.hashCalls.Add(1)
	if err := f.wait(ctx); err != nil {
		return "", err
	}
	if f.fail.Load() {
		return "", errInjectedFetchFailure
	}
	if f.hash != nil {
		return f.hash(ctx, blockNum)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.windowed && (blockNum < f.oldest || blockNum > f.head) {
		return "", fmt.Errorf("invalid block num requested %d, the fetcher keeps %d..%d", blockNum, f.oldest, f.head)
	}
	return f.hashOf(blockNum), nil
}

func (f *fakeFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	f.customCalls.Add(1)
	if f.custom != nil {
		return f.custom(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.customSet {
		return nil, fmt.Errorf("fakeFetcher: no custom message")
	}
	return f.customReply, f.customErr
}

// answerCustom sets what every CustomMessage answers from now on.
func (f *fakeFetcher) answerCustom(reply string, err error) *fakeFetcher {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.customSet, f.customReply, f.customErr = true, []byte(reply), err
	return f
}

// wait takes the fetcher's delay, or until ctx ends.
func (f *fakeFetcher) wait(ctx context.Context) error {
	if f.delay <= 0 {
		return nil
	}
	return sleepCtx(ctx, f.delay)
}

// hashOf is the chain's hash of block; the caller holds f.mu.
func (f *fakeFetcher) hashOf(block int64) string {
	return "hash-" + strconv.FormatInt(block, 10) + f.fork
}

// headBlock is the head the chain answers with.
func (f *fakeFetcher) headBlock() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.head
}

// setHead moves the head to block.
func (f *fakeFetcher) setHead(block int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.head = block
}

// advance moves the head up one block, and a windowed fetcher's window with it, and returns the
// new head.
func (f *fakeFetcher) advance() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.head++
	if f.windowed {
		f.oldest++
	}
	return f.head
}

// forkTo gives every block a hash of fork's chain.
func (f *fakeFetcher) forkTo(fork string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fork = fork
}

// shrink drops the oldest kept blocks, down to size.
func (f *fakeFetcher) shrink(size int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.head-f.oldest+1 > int64(size) {
		f.oldest = f.head - int64(size) + 1
	}
}

// isCorrectHash reports whether hash is the chain's current hash of block.
func (f *fakeFetcher) isCorrectHash(hash string, block int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return hash == f.hashOf(block)
}

// latestCalls counts the head fetches so far.
func (f *fakeFetcher) latestCalls() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.headCalls))
}

// latestCallAt is when the i-th head fetch, counting from 0, was made.
func (f *fakeFetcher) latestCallAt(i int) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headCalls[i]
}

// upstreamCalls counts every call that reached the node: head, hash and custom.
func (f *fakeFetcher) upstreamCalls() int64 {
	return f.latestCalls() + f.hashCalls.Load() + f.customCalls.Load()
}

// pollObservation is one ObserveLatestBlockPoll callback.
type pollObservation struct {
	block            int64
	transportLatency time.Duration
	err              error
}

// observingFetcher is a fakeFetcher that is also a PollObserver, the hook the SVM tracker
// reports every latest-slot poll to. A plain fakeFetcher is not one, as a fetcher without the
// hook is not.
type observingFetcher struct {
	*fakeFetcher

	observedMu   sync.Mutex
	observations []pollObservation
}

var _ PollObserver = (*observingFetcher)(nil)

func (f *observingFetcher) ObserveLatestBlockPoll(block int64, transportLatency time.Duration, err error) {
	f.observedMu.Lock()
	defer f.observedMu.Unlock()
	f.observations = append(f.observations, pollObservation{block: block, transportLatency: transportLatency, err: err})
}

// observed is every observation so far.
func (f *observingFetcher) observed() []pollObservation {
	f.observedMu.Lock()
	defer f.observedMu.Unlock()
	return append([]pollObservation(nil), f.observations...)
}

// fakeDataFetcher is the IChainTrackerDataFetcher the SVM tracker tests hand it.
type fakeDataFetcher struct {
	latestBlock  int64
	serverMemory uint64
}

func (f fakeDataFetcher) GetAtomicLatestBlockNum() int64 { return f.latestBlock }
func (f fakeDataFetcher) GetServerBlockMemory() uint64   { return f.serverMemory }
