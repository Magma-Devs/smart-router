package endpointstate

// The tracker request counter (rpc_endpoint_tracker_requests_total).
//
// It exists because rpc_endpoint_fetch_latest_{success,fails} count EVENTS — a new block was
// detected, a latest-block fetch failed — not REQUESTS. Neither moves when the tracker's
// request volume changes, which is why its block-hash traffic was invisible until it was
// measured by hand. These tests pin the property that makes the new counter useful: it is
// incremented once per upstream request actually sent, at the transport chokepoint.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// recordingPoller captures what the transport hook reports.
type requestRecorder struct {
	mu    sync.Mutex
	kinds []string
	urls  []string
}

func (r *requestRecorder) record(url, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.urls = append(r.urls, url)
	r.kinds = append(r.kinds, kind)
}

func (r *requestRecorder) count(kind string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, k := range r.kinds {
		if k == kind {
			n++
		}
	}
	return n
}

// TestSendRawRequest_CountsRequestsThatReachTheTransport pins what the counter means: one
// increment per request actually sent, tagged with its kind.
func TestSendRawRequest_CountsRequestsThatReachTheTransport(t *testing.T) {
	rec := &requestRecorder{}
	conn := &recordingConnection{url: "http://node.example", respBody: []byte(`{"ok":true}`)}
	poller := NewEndpointPoller(
		&routersession.Endpoint{NetworkAddress: "http://node.example", Enabled: true},
		conn, nil, "ETH1", spectypes.APIInterfaceJsonRPC,
	)
	poller.onTrackerRequest = func(kind string) { rec.record("http://node.example", kind) }

	for i := 0; i < 3; i++ {
		_, err := poller.sendRawRequest(context.Background(), []byte("{}"), "POST", "eth_blockNumber", metrics.TrackerRequestKindLatestBlock)
		require.NoError(t, err)
	}
	_, err := poller.sendRawRequest(context.Background(), []byte("{}"), "POST", "eth_getBlockByNumber", metrics.TrackerRequestKindBlockHash)
	require.NoError(t, err)

	require.Equal(t, 3, rec.count(metrics.TrackerRequestKindLatestBlock))
	require.Equal(t, 1, rec.count(metrics.TrackerRequestKindBlockHash),
		"block-hash requests must be counted separately — this is the series that goes to zero when fork detection is off")
}

// TestSendRawRequest_NotCountedWhenNothingIsSent: with no connection the request never
// leaves, so counting it would over-report load the customer's node never saw.
func TestSendRawRequest_NotCountedWhenNothingIsSent(t *testing.T) {
	rec := &requestRecorder{}
	poller := &EndpointPoller{endpointURL: "http://node.example"}
	poller.onTrackerRequest = func(kind string) { rec.record("http://node.example", kind) }

	_, err := poller.sendRawRequest(context.Background(), []byte("{}"), "POST", "eth_blockNumber", metrics.TrackerRequestKindLatestBlock)
	require.Error(t, err, "no direct connection is wired")
	require.Zero(t, rec.count(metrics.TrackerRequestKindLatestBlock),
		"a request that never left must not be counted as upstream load")
}

// TestSendRawRequest_NoHookIsSafe: the hook is only set when a consumer is wired.
func TestSendRawRequest_NoHookIsSafe(t *testing.T) {
	poller := &EndpointPoller{endpointURL: "http://node.example"}
	require.NotPanics(t, func() {
		_, _ = poller.sendRawRequest(context.Background(), []byte("{}"), "POST", "eth_blockNumber", metrics.TrackerRequestKindLatestBlock)
	})
}

// TestTrackerRequestKindsAreClosedSet guards the Prometheus label. A raw method name here
// would blow up cardinality on a chain with many parse directives.
func TestTrackerRequestKindsAreClosedSet(t *testing.T) {
	require.Equal(t, "latest_block", metrics.TrackerRequestKindLatestBlock)
	require.Equal(t, "block_hash", metrics.TrackerRequestKindBlockHash)
}

// TestRecordTrackerRequest_NilManagerIsSafe mirrors every other metrics helper: the manager is
// nil whenever metrics are disabled, and the tracker must not care.
func TestRecordTrackerRequest_NilManagerIsSafe(t *testing.T) {
	var m *metrics.SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordTrackerRequest("ETH1", "jsonrpc", "http://node.example", metrics.TrackerRequestKindLatestBlock)
	})
}

// TestTrackerRequestCounter_ByKind_EndToEnd drives a REAL EndpointMonitor + ChainTracker over a
// mock connection and reports what the counter would show, in both modes. It is the end-to-end
// proof of the counter's purpose: block_hash is the series that collapses when fork detection is
// off, and latest_block keeps running.
//
// Read the logged numbers as SHAPE, not as rates. The mock does not serve real block hashes, so
// the fork-detection-ON run dies during init retries and stops polling — which is itself a fair
// illustration of the row-3 effect measured in the research doc (a failing hash fetch aborts the
// whole cycle), but it means the two totals are not a like-for-like rate comparison. The
// assertions below deliberately test presence and absence, not magnitude.
func TestTrackerRequestCounter_ByKind_EndToEnd(t *testing.T) {
	ensureRandSeeded()

	run := func(t *testing.T, enableForkDetection bool) map[string]int {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		counts := map[string]int{}

		m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
			ChainParser:         newRealChainParser(t, "ETH1", spectypes.APIInterfaceJsonRPC),
			ChainID:             "ETH1",
			ApiInterface:        spectypes.APIInterfaceJsonRPC,
			AverageBlockTime:    100 * time.Millisecond,
			BlocksToSave:        1,
			EnableForkDetection: enableForkDetection,
			OnTrackerRequest: func(endpointURL, kind string) {
				mu.Lock()
				defer mu.Unlock()
				counts[kind]++
			},
		})
		require.NotNil(t, m)
		defer m.Stop()

		url := "http://eth-ep:8545"
		_, err := m.GetOrCreateTracker(&routersession.Endpoint{NetworkAddress: url, Enabled: true}, &mockDirectRPCConnection{url: url})
		require.NoError(t, err)

		// Let the poll loop run a few cycles at the 50ms flat cadence (avgBlockTime/2).
		time.Sleep(600 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		out := map[string]int{}
		for k, v := range counts {
			out[k] = v
		}
		return out
	}

	on := run(t, true)
	off := run(t, false)

	t.Logf("fork detection ON  -> latest_block=%d  block_hash=%d  (total %d)",
		on[metrics.TrackerRequestKindLatestBlock], on[metrics.TrackerRequestKindBlockHash],
		on[metrics.TrackerRequestKindLatestBlock]+on[metrics.TrackerRequestKindBlockHash])
	t.Logf("fork detection OFF -> latest_block=%d  block_hash=%d  (total %d)",
		off[metrics.TrackerRequestKindLatestBlock], off[metrics.TrackerRequestKindBlockHash],
		off[metrics.TrackerRequestKindLatestBlock]+off[metrics.TrackerRequestKindBlockHash])

	require.Positive(t, on[metrics.TrackerRequestKindBlockHash],
		"with fork detection ON the tracker must send block-hash requests")
	require.Zero(t, off[metrics.TrackerRequestKindBlockHash],
		"with fork detection OFF the block_hash series must be exactly zero — this is the whole point")
	require.Positive(t, off[metrics.TrackerRequestKindLatestBlock],
		"the latest-block poll must keep running either way")
}
