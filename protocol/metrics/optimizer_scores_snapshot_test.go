package metrics

// Tests for the on-demand score snapshot behind GET /debug/provider-scores (MAG-2707).
//
// The load-bearing one is TestSnapshotReports_DoesNotEmitToUsageSink: the snapshot reuses the
// periodic sampler's report assembly, and the whole point of splitting buildOptimizerQoSReport out
// of appendOptimizerQoSReport is that a READ must not publish usage events. A regression there is
// invisible in the response body and only shows up as double-counted dashboards.

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// countingSink records how many optimizer-QoS reports were published, so a test can assert that a
// read published none.
type countingSink struct {
	NoopUsageSink
	optimizerQoSEmits int
}

func (s *countingSink) EmitOptimizerQoS(report OptimizerQoSReportToSend) {
	s.optimizerQoSEmits++
}

// staticOptimizer is an OptimizerInf returning fixed scores for the addresses it is asked about, so
// the snapshot's enumeration and assembly can be tested without a live optimizer.
type staticOptimizer struct {
	calls   int
	scores  map[string]float64
	raw     map[string]float64
	unknown bool // when true, return no reports at all regardless of the addresses asked for
}

func (o *staticOptimizer) CalculateQoSScoresForMetrics(allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) []*OptimizerQoSReport {
	o.calls++
	if o.unknown {
		return nil
	}
	reports := make([]*OptimizerQoSReport, 0, len(allAddresses))
	for i, addr := range allAddresses {
		reports = append(reports, &OptimizerQoSReport{
			ProviderAddress:    addr,
			AvailabilityScore:  o.raw[addr],
			SelectionComposite: o.scores[addr],
			EntryIndex:         i,
		})
	}
	return reports
}

func newSnapshotFixture(t *testing.T, sink UsageEventSink) (*ConsumerOptimizerQoSClient, *staticOptimizer) {
	t.Helper()
	coqc := NewConsumerOptimizerQoSClient("consumer", sink)
	opt := &staticOptimizer{
		scores: map[string]float64{"lava@provider1": 0.85},
		raw:    map[string]float64{"lava@provider1": 0.99},
	}
	coqc.RegisterOptimizer(opt, "ETH1")
	// A provider is only scored once it is known through the stake map — the same gate the periodic
	// sampler applies.
	coqc.UpdatePairingListStake(map[string]int64{"lava@provider1": 1000}, "ETH1", 7)
	return coqc, opt
}

// TestSnapshotReports_ReturnsScoresForKnownProviders is the basic contract: a registered optimizer
// with a known provider yields one fully-assembled row, carrying both the raw EWMA value and the
// normalised composite, plus the enrichment (stake, chain, epoch) the sampler adds.
func TestSnapshotReports_ReturnsScoresForKnownProviders(t *testing.T) {
	coqc, opt := newSnapshotFixture(t, NoopUsageSink{})

	snapshot := coqc.SnapshotReports("")
	require.Equal(t, []string{"ETH1"}, snapshot.ChainsRegistered)
	require.Empty(t, snapshot.ChainsUnavailable)
	require.Len(t, snapshot.Reports, 1)

	report := snapshot.Reports[0]
	require.Equal(t, "ETH1", report.ChainId)
	require.Equal(t, "lava@provider1", report.ProviderAddress)
	require.Equal(t, 0.85, report.SelectionComposite)
	require.Equal(t, 0.99, report.AvailabilityScore, "the raw EWMA value is reported alongside the normalised one")
	require.Equal(t, int64(1000), report.ProviderStake)
	require.Equal(t, uint64(7), report.Epoch)
	require.False(t, report.Timestamp.IsZero())
	require.Equal(t, 1, opt.calls, "the snapshot computes on demand, exactly once")
}

// TestSnapshotReports_DoesNotEmitToUsageSink is the MAG-2707 guard on "reading changes nothing":
// the periodic sampler publishes every report it builds, and the snapshot must reuse that assembly
// WITHOUT publishing — otherwise a debug read double-counts every dashboard fed by the sink.
func TestSnapshotReports_DoesNotEmitToUsageSink(t *testing.T) {
	sink := &countingSink{}
	coqc, _ := newSnapshotFixture(t, sink)

	snapshot := coqc.SnapshotReports("")
	require.Len(t, snapshot.Reports, 1)
	require.Zero(t, sink.optimizerQoSEmits, "a read must publish nothing")

	// Positive control: the periodic path DOES emit, so the assertion above is a real difference
	// between the two paths and not a sink that never fires.
	reports := coqc.getReportsFromOptimizers()
	require.Len(t, reports, 1)
	require.Equal(t, 1, sink.optimizerQoSEmits, "the periodic sampler still publishes")
}

// TestSnapshotReports_SanitizesNonFiniteScores: scores can legitimately be NaN/Inf, and
// encoding/json ERRORS on NaN — which, in a debug handler that has already written its status line,
// would surface as a truncated body under a 200. Going through the shared builder neutralises them.
func TestSnapshotReports_SanitizesNonFiniteScores(t *testing.T) {
	coqc := NewConsumerOptimizerQoSClient("consumer", NoopUsageSink{})
	coqc.RegisterOptimizer(&staticOptimizer{
		scores: map[string]float64{"lava@provider1": math.NaN()},
		raw:    map[string]float64{"lava@provider1": math.Inf(1)},
	}, "ETH1")
	coqc.UpdatePairingListStake(map[string]int64{"lava@provider1": 1}, "ETH1", 1)

	snapshot := coqc.SnapshotReports("")
	require.Len(t, snapshot.Reports, 1)
	require.Equal(t, float64(0), snapshot.Reports[0].SelectionComposite, "NaN is neutralised")
	require.Equal(t, float64(0), snapshot.Reports[0].AvailabilityScore, "Inf is neutralised")
}

// TestSnapshotReports_ReportsChainsWithNoProviders covers the case the periodic sampler drops
// silently: a registered chain whose provider set is still empty. The snapshot must NAME it, so the
// caller can answer "unavailable" rather than returning an empty list that reads like a clean run.
func TestSnapshotReports_ReportsChainsWithNoProviders(t *testing.T) {
	coqc := NewConsumerOptimizerQoSClient("consumer", NoopUsageSink{})
	coqc.RegisterOptimizer(&staticOptimizer{}, "ETH1") // registered, but no stake/provider data yet

	snapshot := coqc.SnapshotReports("")
	require.Equal(t, []string{"ETH1"}, snapshot.ChainsRegistered)
	require.Equal(t, []string{"ETH1"}, snapshot.ChainsUnavailable)
	require.Empty(t, snapshot.Reports)

	// Same when the optimizer knows the provider but produces no scores for it.
	coqc2 := NewConsumerOptimizerQoSClient("consumer", NoopUsageSink{})
	coqc2.RegisterOptimizer(&staticOptimizer{unknown: true}, "ETH1")
	coqc2.UpdatePairingListStake(map[string]int64{"lava@provider1": 1}, "ETH1", 1)
	snapshot = coqc2.SnapshotReports("")
	require.Equal(t, []string{"ETH1"}, snapshot.ChainsUnavailable)
	require.Empty(t, snapshot.Reports)
}

// TestSnapshotReports_ChainFilter: the filter narrows to one chain, case-insensitively, and a
// filter matching no registered chain yields no registered chains — which the handler turns into a
// 404 rather than an empty success.
func TestSnapshotReports_ChainFilter(t *testing.T) {
	coqc, _ := newSnapshotFixture(t, NoopUsageSink{})
	coqc.RegisterOptimizer(&staticOptimizer{scores: map[string]float64{"lava@provider2": 0.5}}, "SOLANA")
	coqc.UpdatePairingListStake(map[string]int64{"lava@provider2": 5}, "SOLANA", 7)

	require.Len(t, coqc.SnapshotReports("").Reports, 2, "no filter returns every chain")

	filtered := coqc.SnapshotReports("eth1")
	require.Equal(t, []string{"ETH1"}, filtered.ChainsRegistered, "the filter matches case-insensitively")
	require.Len(t, filtered.Reports, 1)
	require.Equal(t, "lava@provider1", filtered.Reports[0].ProviderAddress)

	missing := coqc.SnapshotReports("NOSUCHCHAIN")
	require.Empty(t, missing.ChainsRegistered)
	require.Empty(t, missing.Reports)
}

// TestSnapshotReports_NilClientIsSafe: a fixture with no QoS sampler wired must not panic; it
// reports nothing registered, which the handler turns into "unavailable".
func TestSnapshotReports_NilClientIsSafe(t *testing.T) {
	var coqc *ConsumerOptimizerQoSClient
	snapshot := coqc.SnapshotReports("")
	require.Empty(t, snapshot.ChainsRegistered)
	require.Empty(t, snapshot.Reports)
	require.NotNil(t, snapshot.Reports, "empty, not nil, so callers can range without a guard")
}
