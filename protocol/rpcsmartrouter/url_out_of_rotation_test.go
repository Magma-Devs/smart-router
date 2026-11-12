package rpcsmartrouter

import (
	"sort"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// MAG-5244 — collectURLsOutOfRotation, the read-only step that turns the endpoints' own records into
// what the per-url out-of-rotation metrics publish.

// outFor finds the record of one url and sorts its reasons, which come from a map.
func outFor(t *testing.T, recs []metrics.URLOutOfRotation, provider, url string) metrics.URLOutOfRotation {
	t.Helper()
	for _, r := range recs {
		if r.Provider == provider && r.URL == url {
			sort.Strings(r.Reasons)
			return r
		}
	}
	t.Fatalf("no record for %s %s", provider, url)
	return metrics.URLOutOfRotation{}
}

func disableForTest(t *testing.T, e *routersession.Endpoint, reason routersession.EndpointDisableReason) {
	t.Helper()
	for i := uint64(0); i < routersession.MaxConsecutiveConnectionAttempts && e.IsEnabled(); i++ {
		e.MarkUnhealthy(reason)
	}
	require.False(t, e.IsEnabled())
}

func TestURLOutOfRotation_ReportsEachReasonThatHolds(t *testing.T) {
	healthy := ep("https://a.example", "provA", true)
	stuck := ep("https://b.example", "provB", true)
	require.True(t, stuck.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	both := ep("https://c.example", "provC", true)
	disableForTest(t, both.Endpoint, routersession.EndpointDisableNodeError)
	require.True(t, both.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))

	recs, _ := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{healthy, stuck, both}, nil)

	require.Len(t, recs, 3, "every url is listed, in or out, so the publisher can clear the ones that came back")
	require.Empty(t, outFor(t, recs, "provA", "https://a.example").Reasons)
	require.Equal(t, []string{"head-stalled"}, outFor(t, recs, "provB", "https://b.example").Reasons)
	require.Equal(t, []string{"head-stalled", "node-error"}, outFor(t, recs, "provC", "https://c.example").Reasons)
}

func TestURLOutOfRotation_CountsAnExitOnceAcrossCycles(t *testing.T) {
	e := ep("https://a.example", "provA", true)
	eps := []*routersession.EndpointWithDirectConnection{e}
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))

	recs, seen := collectURLsOutOfRotation(eps, nil)
	require.Equal(t, map[string]uint64{"head-stalled": 1}, outFor(t, recs, "provA", "https://a.example").WentOut)

	recs, seen = collectURLsOutOfRotation(eps, seen)
	require.Empty(t, outFor(t, recs, "provA", "https://a.example").WentOut, "still out, but it did not go out again")

	// Out, back and out again between two cycles: the gauge would show one hold, the count shows two.
	require.True(t, e.Endpoint.RemoveStateReason(routersession.EndpointDisableHeadStalled))
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 101))
	require.True(t, e.Endpoint.RemoveStateReason(routersession.EndpointDisableHeadStalled))
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 102))
	recs, _ = collectURLsOutOfRotation(eps, seen)
	require.Equal(t, map[string]uint64{"head-stalled": 2}, outFor(t, recs, "provA", "https://a.example").WentOut)
}

// A hold that started and ended between two cycles: nothing is out when the step looks, and it is
// still counted.
func TestURLOutOfRotation_CountsAHoldThatEndedBeforeTheCycle(t *testing.T) {
	e := ep("https://a.example", "provA", true)
	eps := []*routersession.EndpointWithDirectConnection{e}
	_, seen := collectURLsOutOfRotation(eps, nil)

	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	require.True(t, e.Endpoint.RemoveStateReason(routersession.EndpointDisableHeadStalled))

	recs, _ := collectURLsOutOfRotation(eps, seen)
	rec := outFor(t, recs, "provA", "https://a.example")
	require.Empty(t, rec.Reasons)
	require.Equal(t, map[string]uint64{"head-stalled": 1}, rec.WentOut)
}

// The chart writes an archive url twice: two endpoints, one node, one series. Head-stall takes both
// out together, and that is one exit, not two.
func TestURLOutOfRotation_URLListedTwiceIsOneRecordAndOneExit(t *testing.T) {
	plain := ep("https://a.example", "provA", true)
	archive := ep("https://a.example", "provA", true)
	require.True(t, plain.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	require.True(t, archive.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))

	recs, _ := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{plain, archive}, nil)

	require.Len(t, recs, 1)
	require.Equal(t, []string{"head-stalled"}, recs[0].Reasons)
	require.Equal(t, map[string]uint64{"head-stalled": 1}, recs[0].WentOut)
}

// One of the two entries of a url out is enough to show the url as out for that reason.
func TestURLOutOfRotation_URLListedTwiceIsOutWhenEitherEntryIs(t *testing.T) {
	plain := ep("https://a.example", "provA", true)
	archive := ep("https://a.example", "provA", true)
	disableForTest(t, archive.Endpoint, routersession.EndpointDisableUnreachable)

	recs, _ := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{plain, archive}, nil)

	require.Len(t, recs, 1)
	require.Equal(t, []string{"unreachable"}, recs[0].Reasons)
}

// A provider in both pools lists the same Endpoint twice. It is one endpoint.
func TestURLOutOfRotation_SameEndpointListedTwiceCountsOnce(t *testing.T) {
	e := ep("https://a.example", "provA", true)
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	dup := &routersession.EndpointWithDirectConnection{Endpoint: e.Endpoint, ProviderAddress: "provA", Backup: true}

	recs, seen := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{e, dup}, nil)

	require.Len(t, recs, 1)
	require.Equal(t, map[string]uint64{"head-stalled": 1}, recs[0].WentOut)
	require.Len(t, seen, 1)
}

// A pairing rebuild replaces the Endpoint objects. The new one starts counting at zero, and that
// must not hide its first exit behind the old object's count.
func TestURLOutOfRotation_RebuiltEndpointCountsFromZero(t *testing.T) {
	old := ep("https://a.example", "provA", true)
	require.True(t, old.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	_, seen := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{old}, nil)

	rebuilt := ep("https://a.example", "provA", true)
	require.True(t, rebuilt.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	recs, seen := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{rebuilt}, seen)

	require.Equal(t, map[string]uint64{"head-stalled": 1}, recs[0].WentOut)
	require.Len(t, seen, 1, "the replaced endpoint is forgotten")
}
