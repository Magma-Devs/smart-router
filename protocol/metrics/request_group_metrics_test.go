package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// requestGroupRunner is a thin adapter that lets the shared test case table
// drive SmartRouterMetricsManager.
type requestGroupRunner struct {
	// invoke dispatches a single relay through the manager under test.
	// err==nil means success; err!=nil means failure.
	invoke  func(relay RelayMetrics, err error)
	labels  func(method string) []string
	total   *prometheus.CounterVec
	success *prometheus.CounterVec
	failed  *prometheus.CounterVec
	read    *prometheus.CounterVec
	write   *prometheus.CounterVec
	archive *prometheus.CounterVec
	debug   *prometheus.CounterVec
	batch   *prometheus.CounterVec
}

func newSmartRouterRequestGroupRunner(t *testing.T) *requestGroupRunner {
	m := newTestMetricsManager(t)
	return &requestGroupRunner{
		invoke: func(r RelayMetrics, e error) {
			outcome := RelayOutcomeSuccess
			if e != nil {
				outcome = RelayOutcomeError
			}
			m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", r.ApiMethod, 10, outcome, &r)
		},
		labels:  func(method string) []string { return []string{"ETH1", "jsonrpc", "ep1", method} },
		total:   m.routerRequestsTotal,
		success: m.routerRequestsSuccess,
		failed:  m.routerRequestsFailed,
		read:    m.routerRequestsRead,
		write:   m.routerRequestsWrite,
		archive: m.routerRequestsArchive,
		debug:   m.routerRequestsDebugTrace,
		batch:   m.routerRequestsBatch,
	}
}

func (r *requestGroupRunner) assertCounters(t *testing.T, method string,
	wantTotal, wantSuccess, wantFailed,
	wantRead, wantWrite, wantArchive, wantDebug, wantBatch float64,
) {
	t.Helper()
	l := r.labels(method)
	require.Equal(t, wantTotal, testutil.ToFloat64(r.total.WithLabelValues(l...)), "total")
	require.Equal(t, wantSuccess, testutil.ToFloat64(r.success.WithLabelValues(l...)), "success")
	require.Equal(t, wantFailed, testutil.ToFloat64(r.failed.WithLabelValues(l...)), "failed")
	require.Equal(t, wantRead, testutil.ToFloat64(r.read.WithLabelValues(l...)), "read")
	require.Equal(t, wantWrite, testutil.ToFloat64(r.write.WithLabelValues(l...)), "write")
	require.Equal(t, wantArchive, testutil.ToFloat64(r.archive.WithLabelValues(l...)), "archive")
	require.Equal(t, wantDebug, testutil.ToFloat64(r.debug.WithLabelValues(l...)), "debug")
	require.Equal(t, wantBatch, testutil.ToFloat64(r.batch.WithLabelValues(l...)), "batch")
}

// TestSetRelayMetrics covers the request-group counter logic for
// SmartRouterMetricsManager (via RecordDirectRelayEnd) using a table of cases.
func TestSetRelayMetrics(t *testing.T) {
	cases := []struct {
		name        string
		relay       RelayMetrics
		err         error // nil = success
		wantTotal   float64
		wantSuccess float64
		wantFailed  float64
		wantRead    float64
		wantWrite   float64
		wantArchive float64
		wantDebug   float64
		wantBatch   float64
	}{
		{
			name:      "read/success",
			relay:     RelayMetrics{ApiMethod: "eth_blockNumber"},
			wantTotal: 1, wantSuccess: 1,
			wantRead: 1,
		},
		{
			name:      "read/failure",
			relay:     RelayMetrics{ApiMethod: "eth_blockNumber"},
			err:       errors.New("relay failed"),
			wantTotal: 1, wantFailed: 1,
			wantRead: 1,
		},
		{
			name:      "write",
			relay:     RelayMetrics{ApiMethod: "eth_sendRawTransaction", IsWrite: true},
			wantTotal: 1, wantSuccess: 1,
			wantWrite: 1,
		},
		{
			name:      "archive",
			relay:     RelayMetrics{ApiMethod: "eth_getBlockByNumber", IsArchive: true},
			wantTotal: 1, wantSuccess: 1,
			wantRead: 1, wantArchive: 1,
		},
		{
			name:      "debug_trace",
			relay:     RelayMetrics{ApiMethod: "debug_traceTransaction", IsDebugTrace: true},
			wantTotal: 1, wantSuccess: 1,
			wantRead: 1, wantDebug: 1,
		},
		{
			// IsBatch is mutually exclusive: read/write/archive/debug must all stay zero.
			name:      "batch/mutually_exclusive",
			relay:     RelayMetrics{ApiMethod: "batch", IsBatch: true, IsWrite: true, IsArchive: true, IsDebugTrace: true},
			wantTotal: 1, wantSuccess: 1,
			wantBatch: 1,
		},
	}

	runners := []struct {
		name string
		new  func(*testing.T) *requestGroupRunner
	}{
		{"smart_router", newSmartRouterRequestGroupRunner},
	}

	for _, r := range runners {
		t.Run(r.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					runner := r.new(t)
					runner.invoke(tc.relay, tc.err)
					runner.assertCounters(t, tc.relay.ApiMethod,
						tc.wantTotal, tc.wantSuccess, tc.wantFailed,
						tc.wantRead, tc.wantWrite, tc.wantArchive, tc.wantDebug, tc.wantBatch,
					)
				})
			}
		})
	}
}

func TestSmartRouterRecordDirectRelayEnd_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "method", 10, RelayOutcomeSuccess, &RelayMetrics{})
	})
}
