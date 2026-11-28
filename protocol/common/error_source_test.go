package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorSourceOf(t *testing.T) {
	require.Equal(t, ErrorSourceClient, ErrorSourceOf(context.Background()), "an untagged ctx is a client's")
	require.Equal(t, ErrorSourceClient, ErrorSourceOf(nil)) //nolint:staticcheck // a nil ctx must not panic
	boot := WithErrorSource(context.Background(), ErrorSourceBoot)
	require.Equal(t, ErrorSourceBoot, ErrorSourceOf(boot))
	require.Equal(t, ErrorSourceProbe, ErrorSourceOf(WithErrorSource(boot, ErrorSourceProbe)), "the innermost tag wins")
	require.Equal(t, ErrorSourceBoot, ErrorSourceOf(WithDefaultErrorSource(boot, ErrorSourcePoll)),
		"a default never replaces a caller's tag: a poll the boot checks ask for is boot's")
	require.Equal(t, ErrorSourcePoll, ErrorSourceOf(WithDefaultErrorSource(context.Background(), ErrorSourcePoll)))
	require.Equal(t, ErrorOrigin{APIInterface: "rest", Source: ErrorSourceBoot}, ErrorOriginOf(boot, "rest"))
}

// An origin without a source is counted as a client's, never with an empty source, which the SLA's
// source=~"client|" would read as a router from before the label.
func TestEmitErrorMetricNeverWritesAnEmptySource(t *testing.T) {
	var sources []ErrorSource
	SetErrorMetricsCallback(func(_ uint32, _, _ string, _ bool, _, _ string, source ErrorSource) {
		sources = append(sources, source)
	})
	t.Cleanup(func() { SetErrorMetricsCallback(nil) })
	EmitErrorMetric(RouterErrorUnknown, "ETH1", ErrorOrigin{APIInterface: "jsonrpc"})
	EmitErrorMetric(RouterErrorUnknown, "ETH1", ErrorOrigin{APIInterface: "jsonrpc", Source: ErrorSourceReverify})
	require.Equal(t, []ErrorSource{ErrorSourceClient, ErrorSourceReverify}, sources)
}
