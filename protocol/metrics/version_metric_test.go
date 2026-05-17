package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestSetVersionInner pins the version-string normalizer's behavior. The
// release pipeline injects strings shaped like `git describe` output
// (v6.2.2, v6.2.2-3-gabc1234, v6.2.2-3-gabc1234-dirty), and dev builds
// carry the default "dev". The metric must parse all of these without
// silently zeroing the gauge for normal release builds.
func TestSetVersionInner(t *testing.T) {
	cases := []struct {
		name    string
		version string
		want    float64
	}{
		{"plain semver", "6.2.2", 6002002},
		{"v-prefixed semver", "v6.2.2", 6002002},
		{"git describe past tag", "v6.2.2-3-gabc1234", 6002002},
		{"git describe dirty", "v6.2.2-3-gabc1234-dirty", 6002002},
		{"prerelease suffix", "v6.2.2-rc1", 6002002},
		{"build metadata", "v6.2.2+build.123", 6002002},
		{"large numbers", "v123.456.789", 123_456_789},
		{"zero version", "v0.0.0", 0},

		// Inputs that should fail to parse — gauge set to 0, error logged.
		{"default dev string", "dev", 0},
		{"empty string", "", 0},
		{"only major.minor", "v1.2", 0},
		{"only major", "v1", 0},
		{"non-numeric segments", "vfoo.bar.baz", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metric := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{Name: "test_protocol_version_" + sanitizeMetricName(tc.name)},
				[]string{"version"},
			)
			SetVersionInner(metric, tc.version)
			got := testutil.ToFloat64(metric.WithLabelValues("version"))
			if got != tc.want {
				t.Errorf("SetVersionInner(%q) = %v, want %v", tc.version, got, tc.want)
			}
		})
	}
}

// sanitizeMetricName converts a test case name into a valid Prometheus metric
// suffix so each subtest registers a distinct gauge.
func sanitizeMetricName(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
