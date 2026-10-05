package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLogCodedNodeAnswerStillCountsTheError pins the one invariant the level change must not break:
// dropping a not-at-fault node error from ERROR to DEBUG changes how loud it is, not whether it is
// counted. smartrouter_errors_total is what operators alert on, and a REST not-found must keep
// showing up there.
func TestLogCodedNodeAnswerStillCountsTheError(t *testing.T) {
	type emitted struct {
		code      uint32
		name      string
		retryable bool
		chainID   string
	}
	var got []emitted
	SetErrorMetricsCallback(func(code uint32, name, category string, retryable bool, chainID string) {
		got = append(got, emitted{code, name, retryable, chainID})
	})
	t.Cleanup(func() { SetErrorMetricsCallback(nil) })

	LogCodedNodeAnswer("received node error reply from provider", errors.New("tx not found"),
		LavaErrorNodeDataNotHeld, "COSMOSHUB", 404, "tx not found: 0000")

	require.Len(t, got, 1, "the debug path must still fire the metric exactly once")
	require.Equal(t, LavaErrorNodeDataNotHeld.Code, got[0].code)
	require.Equal(t, "NODE_DATA_NOT_HELD", got[0].name)
	require.True(t, got[0].retryable)
	require.Equal(t, "COSMOSHUB", got[0].chainID)
}

// TestLogCodedNodeAnswerTreatsNilAsUnknown mirrors LogCodedError: an unclassified answer is still
// counted, under UNKNOWN_ERROR, rather than dropping the metric on a nil.
func TestLogCodedNodeAnswerTreatsNilAsUnknown(t *testing.T) {
	var names []string
	SetErrorMetricsCallback(func(code uint32, name, category string, retryable bool, chainID string) {
		names = append(names, name)
	})
	t.Cleanup(func() { SetErrorMetricsCallback(nil) })

	LogCodedNodeAnswer("node answer", nil, nil, "ETH1", 0, "")
	require.Equal(t, []string{"UNKNOWN_ERROR"}, names)
}
