package lavasession

import (
	"testing"
	"time"
)

// The pool-empty reason exists to separate causes that are investigated in different places. A test
// per branch, because collapsing any two of them back together is exactly the regression that makes
// the line worthless again.
func TestPoolEmptyReason_NamesTheCauseNotTheSymptom(t *testing.T) {
	for _, tt := range []struct {
		name        string
		pairingSize int
		blocked     int
		addon       string
		extensions  []string
		want        string
	}{
		{
			// The zxb5l case: nothing was ever registered. Not a routing problem, and the block
			// reasons have nothing to say about it — there is no provider to have blocked.
			name: "nothing registered", pairingSize: 0, blocked: 0, want: "pairing-empty",
		},
		{
			// pairing-empty wins even when a stale blocked count is present: with no pairing there is
			// nothing for the reset to restore, which is the actionable fact.
			name:        "nothing registered outranks a stale blocked count",
			pairingSize: 0, blocked: 3, want: "pairing-empty",
		},
		{
			name:        "pairing present and every member blocked",
			pairingSize: 2, blocked: 2, want: "all-blocked",
		},
		{
			name:        "addon filtered out the whole pairing",
			pairingSize: 3, blocked: 0, addon: "debug", want: "addon-filtered",
		},
		{
			name:        "extension filtered out the whole pairing",
			pairingSize: 3, blocked: 0, extensions: []string{"archive"}, want: "addon-filtered",
		},
		{
			// Unblocked providers serving the default collection, yet nothing selectable. Not a shape
			// we model — it must not borrow one of the names above.
			name:        "unmodelled shape is named, not mislabelled",
			pairingSize: 3, blocked: 0, want: "unspecified",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := poolEmptyReason(tt.pairingSize, tt.blocked, tt.addon, tt.extensions); got != tt.want {
				t.Fatalf("poolEmptyReason(%d, %d, %q, %v) = %q, want %q",
					tt.pairingSize, tt.blocked, tt.addon, tt.extensions, got, tt.want)
			}
		})
	}
}

// The whole value of the pairing line is `removed` — a provider silently dropping out between
// epochs is the failure it exists to catch.
func TestDiffAddressSets_ReportsWhatChanged(t *testing.T) {
	added, removed, carried := diffAddressSets(
		[]string{"tatum", "lava", "blockdaemon"},
		[]string{"lava", "blockdaemon", "chainstack"},
	)
	assertAddresses(t, "added", added, []string{"chainstack"})
	assertAddresses(t, "removed", removed, []string{"tatum"})
	assertAddresses(t, "carried_over", carried, []string{"blockdaemon", "lava"})
}

// Inputs come from Go map iteration, which is deliberately randomised. Unsorted output would read as
// churn on every epoch even when membership never moved.
func TestDiffAddressSets_OutputIsSortedRegardlessOfInputOrder(t *testing.T) {
	first, _, firstCarried := diffAddressSets([]string{"b", "a"}, []string{"a", "b", "z", "c"})
	second, _, secondCarried := diffAddressSets([]string{"a", "b"}, []string{"c", "z", "b", "a"})
	assertAddresses(t, "added (first order)", first, []string{"c", "z"})
	assertAddresses(t, "added (second order)", second, []string{"c", "z"})
	assertAddresses(t, "carried (first order)", firstCarried, []string{"a", "b"})
	assertAddresses(t, "carried (second order)", secondCarried, []string{"a", "b"})
}

// A nil slice renders as "" in the log line, which reads as a missing field rather than as
// "nothing changed" — the two must not look alike on the one line that reports pool churn.
func TestDiffAddressSets_UnchangedMembershipRendersEmptyNotNil(t *testing.T) {
	added, removed, carried := diffAddressSets([]string{"lava"}, []string{"lava"})
	if added == nil || removed == nil {
		t.Fatalf("added/removed must be non-nil when nothing changed, got added=%#v removed=%#v", added, removed)
	}
	assertAddresses(t, "added", added, []string{})
	assertAddresses(t, "removed", removed, []string{})
	assertAddresses(t, "carried_over", carried, []string{"lava"})
}

// "Never updated" and "updated at the zero time" are different states; only one of them is real.
func TestFormatPairingUpdateTime_ZeroMeansNever(t *testing.T) {
	if got := formatPairingUpdateTime(time.Time{}); got != "never" {
		t.Fatalf("zero time rendered %q, want \"never\"", got)
	}
	at := time.Date(2026, 8, 27, 19, 58, 2, 0, time.UTC)
	if got := formatPairingUpdateTime(at); got != "2026-08-27T19:58:02Z" {
		t.Fatalf("rendered %q, want RFC3339 in UTC", got)
	}
}

func assertAddresses(t *testing.T, field string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", field, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", field, got, want)
		}
	}
}
