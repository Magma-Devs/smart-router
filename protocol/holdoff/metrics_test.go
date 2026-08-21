package holdoff

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func eventCount(t *testing.T, provider, event string) float64 {
	t.Helper()
	return testutil.ToFloat64(holdoffEvents.WithLabelValues(provider, event))
}

func TestMetrics_RecordEscalateClear(t *testing.T) {
	r, _ := newTestRegistry(t0)
	base := map[string]float64{
		eventRecorded:  eventCount(t, "vendorM", eventRecorded),
		eventEscalated: eventCount(t, "vendorM", eventEscalated),
		eventCleared:   eventCount(t, "vendorM", eventCleared),
	}

	r.RecordRateLimit("vendorM", "a", 5*time.Minute)
	require.Equal(t, base[eventRecorded]+1, eventCount(t, "vendorM", eventRecorded))
	require.Equal(t, base[eventEscalated], eventCount(t, "vendorM", eventEscalated), "one URL does not escalate")

	r.RecordRateLimit("vendorM", "b", 5*time.Minute)
	require.Equal(t, base[eventEscalated]+1, eventCount(t, "vendorM", eventEscalated))

	// A third strike while already escalated refreshes but does not re-count.
	r.RecordRateLimit("vendorM", "c", 5*time.Minute)
	require.Equal(t, base[eventEscalated]+1, eventCount(t, "vendorM", eventEscalated))

	r.RecordAnswer("vendorM", "a")
	require.Equal(t, base[eventCleared]+1, eventCount(t, "vendorM", eventCleared))

	// An answer for an unknown URL of an unknown provider clears nothing.
	r.RecordAnswer("vendorX", "never-seen")
	require.Equal(t, float64(0), eventCount(t, "vendorX", eventCleared))
}

// URL-shaped keys must never reach a label verbatim — ws node URLs can embed API keys.
func TestMetrics_ProviderLabelSanitizesURLs(t *testing.T) {
	require.Equal(t, "wss://node.example.com", providerLabel("wss://node.example.com/ws/SECRET-API-KEY"))
	require.Equal(t, "https://rpc.example.com:8545", providerLabel("https://rpc.example.com:8545/v1/KEY?token=x"))
	require.Equal(t, "tatum", providerLabel("tatum"), "plain provider names pass through")
	require.Equal(t, "invalid-url", providerLabel("://not-a-url"))
}
